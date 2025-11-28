from fastapi import FastAPI, HTTPException, Body  # FastAPI 核心对象与异常处理、请求体验证
from fastapi.responses import HTMLResponse  # 用于返回 HTML 内容
from sse_starlette.sse import EventSourceResponse  # SSE 流式响应
from typing import Dict, Any, Optional  # 类型标注
import json  # JSON 序列化
from pydantic import BaseModel  # 请求 / 响应数据模型基础类
from config.loader import load_config  # 配置加载工具（支持 YAML/JSON）
from engine.workflow import WorkflowEngine  # 工作流引擎
from utils.diagram import (  # 流程图相关工具
    generate_mermaid_diagram,
    generate_diagram_from_file,
    generate_html_viewer,
)

# 创建 FastAPI 应用实例
app = FastAPI()

# 内存中维护的已加载工作流集合，key 为 workflow_id
workflows: Dict[str, WorkflowEngine] = {}


class LoadWorkflowRequest(BaseModel):
    """加载工作流配置的请求体"""

    config_path: str  # 配置文件路径（相对/绝对）


class ExecuteWorkflowRequest(BaseModel):
    """执行工作流的请求体"""

    inputs: Optional[Dict[str, Any]] = {}  # 传入工作流的初始输入


@app.post("/workflow/load")
async def load_workflow(request: LoadWorkflowRequest):
    """根据配置文件路径加载工作流到内存"""
    try:
        # 读取并解析配置文件
        config = load_config(request.config_path)
        # 没有显式 id 时默认使用 'default'
        workflow_id = config.get("id", "default")
        # 创建工作流引擎实例并缓存
        workflows[workflow_id] = WorkflowEngine(config)
        return {"status": "success", "workflow_id": workflow_id}
    except Exception as e:
        # 出错时统一返回 500
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflow/{workflow_id}/execute")
async def execute_workflow(workflow_id: str, request: ExecuteWorkflowRequest = Body(...)):
    """以 SSE 形式执行指定工作流，向前端流式推送节点输出"""
    if workflow_id not in workflows:
        # 指定工作流尚未加载
        raise HTTPException(status_code=404, detail="Workflow not found")

    async def generate():
        # 遍历工作流执行过程中的每一步输出
        async for result in workflows[workflow_id].execute(request.inputs or {}):
            # 按 SSE 协议格式推送到前端（data: ...\\n\\n）
            yield f"data: {json.dumps(result, ensure_ascii=False)}\n\n"

    # 返回 SSE 响应，浏览器可通过 EventSource 订阅
    return EventSourceResponse(generate())


@app.get("/workflows")
async def list_workflows():
    """列出当前已加载的所有工作流 ID"""
    return {"workflows": list(workflows.keys())}


@app.post("/workflow/diagram/mermaid")
async def get_diagram_mermaid(request: LoadWorkflowRequest):
    """根据配置文件生成 Mermaid 文本（未加载到内存亦可使用）"""
    try:
        mermaid_code = generate_diagram_from_file(request.config_path)
        return {"mermaid": mermaid_code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/{workflow_id}/diagram/mermaid")
async def get_workflow_diagram_mermaid(workflow_id: str):
    """基于内存中已加载的工作流配置生成 Mermaid 文本"""
    if workflow_id not in workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")

    try:
        mermaid_code = generate_mermaid_diagram(workflows[workflow_id].config)
        return {"mermaid": mermaid_code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflow/diagram/html")
async def get_diagram_html(request: LoadWorkflowRequest):
    """根据配置文件生成可直接在浏览器预览的流程图 HTML"""
    try:
        # 从配置文件生成 Mermaid 定义
        mermaid_code = generate_diagram_from_file(request.config_path)
        # 包装成完整 HTML 页面
        html = generate_html_viewer(mermaid_code)
        return HTMLResponse(content=html)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/{workflow_id}/diagram/html")
async def get_workflow_diagram_html(workflow_id: str):
    """基于内存中的工作流生成流程图 HTML"""
    if workflow_id not in workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")

    try:
        mermaid_code = generate_mermaid_diagram(workflows[workflow_id].config)
        html = generate_html_viewer(mermaid_code)
        return HTMLResponse(content=html)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


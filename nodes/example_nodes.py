from typing import AsyncIterator, Dict, Any  # 异步迭代器与通用类型
from nodes.base import BaseNode, NodeInput, NodeOutput  # 节点基类与输入 / 输出基类
from pydantic import BaseModel  # 额外的 Pydantic 能力预留
from tools import TOOLS_REGISTRY


class TextInput(NodeInput):
    """简单文本输入模型"""

    text: str


class TextOutput(NodeOutput):
    """简单文本输出模型"""

    result: str


class EchoNode(BaseNode):
    """回声节点：原样返回输入文本"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        text = inputs.get("text", "")
        # 单次输出一个结果
        yield {"result": f"Echo: {text}", "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return TextInput

    def get_output_schema(self) -> type[NodeOutput]:
        return TextOutput


class TransformNode(BaseNode):
    """前缀拼接节点：给文本增加前缀"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        text = inputs.get("text", "")
        prefix = self.config.get("prefix", "")
        yield {"result": f"{prefix}{text}", "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return TextInput

    def get_output_schema(self) -> type[NodeOutput]:
        return TextOutput


class StreamNode(BaseNode):
    """流式节点：按字符切分文本并逐字符输出"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        text = inputs.get("text", "")
        # 每个字符作为一个 chunk 推送
        for i, char in enumerate(text):
            yield {"result": char, "node_id": self.node_id, "chunk_index": i}

    def get_input_schema(self) -> type[NodeInput]:
        return TextInput

    def get_output_schema(self) -> type[NodeOutput]:
        return TextOutput


class QuestionInput(NodeInput):
    """问题输入模型"""

    question: str


class ThinkOutput(NodeOutput):
    """思考节点输出模型"""

    thought: str


class ThinkNode(BaseNode):
    """思考节点：对原始问题做一次抽象分析描述"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        # 兼容直接传字符串或 dict 的情况
        question = inputs.get("question", "")
        if isinstance(question, dict):
            question = question.get("result", question.get("text", str(question)))
        thought = f"思考问题: {question}，分析问题的关键点和需要的信息。"
        yield {"result": thought, "thought": thought, "question": question, "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return QuestionInput

    def get_output_schema(self) -> type[NodeOutput]:
        return ThinkOutput


class TaskPlannerOutput(NodeOutput):
    """任务规划节点输出模型"""

    tasks: list


class TaskPlannerNode(BaseNode):
    """任务规划节点：把问题拆分为若干检索任务"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        # 从上游节点中解析出原始问题文本
        question = inputs.get("question", "")
        if isinstance(question, dict):
            question = question.get("question", question.get("result", question.get("text", str(question))))
        elif not isinstance(question, str):
            question = str(question)

        max_tasks = self.config.get("max_tasks", 3)

        # 根据问题内容简单地生成若干“子任务”描述
        tasks = []
        if "Python" in question or "python" in question:
            tasks = ["查找Python基础语法", "查找Python最佳实践", "查找Python常见问题"]
        elif "API" in question or "api" in question:
            tasks = ["查找API设计规范", "查找API调用示例", "查找API测试方法"]
        else:
            tasks = [
                f"任务1: 查找{question}相关信息",
                f"任务2: 分析{question}的解决方案",
                f"任务3: 总结{question}的最佳实践",
            ]

        # 根据配置裁剪任务数量
        tasks = tasks[:max_tasks]
        yield {"result": tasks, "tasks": tasks, "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return QuestionInput

    def get_output_schema(self) -> type[NodeOutput]:
        return TaskPlannerOutput


class TaskInput(NodeInput):
    """RAG 节点的单个任务输入"""

    task: str


class RAGOutput(NodeOutput):
    """RAG 节点输出模型"""

    rag_result: str


class RAGNode(BaseNode):
    """RAG 节点：模拟根据任务从知识库中检索信息"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        task = inputs.get("task", "")

        # 简单内置“知识库”示例，真实场景应替换为向量检索等
        knowledge_base = {
            "Python基础语法": "Python是一种解释型、面向对象的高级编程语言。支持多种编程范式。",
            "Python最佳实践": "遵循PEP 8编码规范，使用类型提示，编写单元测试，保持代码简洁。",
            "Python常见问题": "常见问题包括内存管理、GIL限制、性能优化等。",
            "API设计规范": "RESTful API应使用标准HTTP方法，返回JSON格式，提供清晰的错误处理。",
            "API调用示例": "使用requests库: response = requests.get(url, params=params)",
        }

        rag_result = knowledge_base.get(
            task,
            f"关于'{task}'的相关信息：这是模拟的RAG检索结果，实际应用中会从向量数据库检索相关内容。",
        )

        yield {"result": rag_result, "rag_result": rag_result, "task": task, "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return TaskInput

    def get_output_schema(self) -> type[NodeOutput]:
        return RAGOutput


class SummarizeInput(NodeInput):
    """总结节点输入模型"""

    question: str
    rag_results: list


class SummarizeOutput(NodeOutput):
    """总结节点输出模型"""

    final_answer: str


class SummarizeNode(BaseNode):
    """总结节点：汇总所有 RAG 结果并组织成最终答案"""

    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        # 解析原始问题
        question = inputs.get("question", "")
        if isinstance(question, dict):
            question = question.get("question", question.get("result", question.get("text", str(question))))
        elif not isinstance(question, str):
            question = str(question)

        # 解析 RAG 结果列表
        rag_results = inputs.get("rag_results", [])

        if isinstance(rag_results, str):
            rag_results = [rag_results]
        elif not isinstance(rag_results, list):
            rag_results = []

        # 尝试从“思考问题: xxx”中提取原始问题
        original_question = question
        if "思考问题:" in question:
            parts = question.split("思考问题:")
            if len(parts) > 1:
                original_question = parts[1].split("，")[0].strip()

        summary = f"基于以下信息回答'{original_question}':\n\n"
        valid_results = []
        for i, result in enumerate(rag_results, 1):
            if result is None:
                continue
            if isinstance(result, dict):
                result_text = result.get("rag_result", result.get("result", str(result)))
            else:
                result_text = str(result)
            if result_text:
                valid_results.append(result_text)
                summary += f"{i}. {result_text}\n"

        if not valid_results:
            summary = f"未能获取到相关信息来回答'{original_question}'"
        else:
            summary += (
                f"\n总结：综合以上{len(valid_results)}条信息，"
                f"{original_question}的答案是：需要结合具体场景选择合适的Python API开发框架和工具。"
            )

        final_answer = summary

        yield {"result": final_answer, "final_answer": final_answer, "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return SummarizeInput

    def get_output_schema(self) -> type[NodeOutput]:
        return SummarizeOutput


# 工具节点：根据工具名调用已有的工具函数
class ToolInput(NodeInput):
    tool_name: str
    params: Dict[str, Any]


class ToolOutput(NodeOutput):
    result: Any


class ToolNode(BaseNode):
    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        tool_name = inputs.get("tool_name") or self.config.get("tool_name")
        params = inputs.get("params", {})
        fn = TOOLS_REGISTRY.get(tool_name)
        if not fn:
            yield {"result": None, "error": f"tool '{tool_name}' not found", "node_id": self.node_id}
            return
        try:
            result = fn(**params) if isinstance(params, dict) else fn(params)
        except TypeError:
            result = fn(params)
        yield {"result": result, "tool_name": tool_name, "node_id": self.node_id}

    def get_input_schema(self) -> type[NodeInput]:
        return ToolInput

    def get_output_schema(self) -> type[NodeOutput]:
        return ToolOutput


# 节点注册表：工作流引擎根据 type 字段查找并实例化对应节点
NODE_REGISTRY = {
    "echo": EchoNode,
    "transform": TransformNode,
    "stream": StreamNode,
    "think": ThinkNode,
    "task_planner": TaskPlannerNode,
    "rag": RAGNode,
    "summarize": SummarizeNode,
    "tool": ToolNode,
}


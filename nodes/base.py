from abc import ABC, abstractmethod  # 抽象基类支持
from typing import Any, Dict, AsyncIterator, Optional  # 类型标注
from pydantic import BaseModel  # 节点入参 / 出参模式


class NodeInput(BaseModel):
    """所有节点输入模型的基类"""

    pass


class NodeOutput(BaseModel):
    """所有节点输出模型的基类"""

    pass


class BaseNode(ABC):
    """所有业务节点的抽象基类"""

    def __init__(self, node_id: str, config: Dict[str, Any] = None):
        # 节点在工作流中的唯一标识
        self.node_id = node_id
        # 节点级别配置（来自工作流配置文件）
        self.config = config or {}

    @abstractmethod
    async def execute(self, inputs: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """节点执行入口，支持异步与多次 yield（流式输出）"""
        raise NotImplementedError

    @abstractmethod
    def get_input_schema(self) -> type[NodeInput]:
        """返回节点期望的输入模型类型（用于校验 / 文档）"""
        raise NotImplementedError

    @abstractmethod
    def get_output_schema(self) -> type[NodeOutput]:
        """返回节点输出模型类型（用于校验 / 文档）"""
        raise NotImplementedError


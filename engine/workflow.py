import asyncio  # 并发执行支持
from typing import Dict, Any, AsyncIterator, List, Optional  # 类型工具
from nodes.base import BaseNode  # 节点基类
from nodes.example_nodes import NODE_REGISTRY  # 已注册的节点类型


class WorkflowEngine:
    """基于配置的通用工作流引擎，负责节点实例化和执行编排"""

    def __init__(self, config: Dict[str, Any]):
        # 原始配置（通常来自 YAML / JSON）
        self.config = config
        # 已实例化的节点对象；key 为节点 id
        self.nodes: Dict[str, BaseNode] = {}
        # 连接关系（用来推导执行顺序）
        self.connections: List[Dict[str, Any]] = config.get("connections", [])
        # 缓存每个节点的原始配置
        self.node_configs: Dict[str, Dict[str, Any]] = {}
        # 根据配置创建节点实例
        self._build_nodes()

    def _build_nodes(self):
        """根据配置中的 nodes 列表构建节点实例"""
        for node_config in self.config.get("nodes", []):
            node_id = node_config["id"]
            node_type = node_config["type"]
            # 保存原始配置
            self.node_configs[node_id] = node_config

            # loop / switch 为控制流节点，不对应具体 BaseNode 实例
            if node_type in ["loop", "switch"]:
                continue

            # 根据类型从注册表中找到节点类并实例化
            node_cls = NODE_REGISTRY.get(node_type)
            if not node_cls:
                raise ValueError(f"Unknown node type: {node_type}")
            self.nodes[node_id] = node_cls(node_id, node_config.get("config", {}))
    
    def _resolve_inputs(self, node_id: str, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """根据工作流状态解析某个节点的 inputs 配置"""
        # 找到当前节点的原始配置
        node_config = next(n for n in self.config["nodes"] if n["id"] == node_id)
        inputs: Dict[str, Any] = {}

        # 配置中未声明 inputs 时直接返回空字典
        if "inputs" in node_config:
            for input_key, input_source in node_config["inputs"].items():
                # 字符串以 $ 开头，表示从前置节点引用数据
                if isinstance(input_source, str) and input_source.startswith("$"):
                    path_parts = input_source[1:].split(".")
                    source_node = path_parts[0]

                    if source_node in workflow_state:
                        # 从 workflow_state 取对应节点的输出
                        value: Any = workflow_state[source_node]
                        if isinstance(value, dict):
                            # 处理层级访问和数组下标（例如 $node.result[0]）
                            if len(path_parts) > 1:
                                for part in path_parts[1:]:
                                    # 支持 key[index] 形式
                                    if "[" in part and "]" in part:
                                        key_part = part[: part.index("[")]
                                        index_part = part[part.index("[") + 1 : part.index("]")]
                                        if key_part:
                                            value = value.get(key_part) if isinstance(value, dict) else None
                                        if isinstance(value, (list, tuple)) and index_part.isdigit():
                                            idx = int(index_part)
                                            if 0 <= idx < len(value):
                                                value = value[idx]
                                            else:
                                                value = None
                                                break
                                    else:
                                        if isinstance(value, dict):
                                            value = value.get(part)
                                        else:
                                            value = None
                                            break
                            else:
                                # 默认优先取 result，其次 output
                                if "result" in value:
                                    value = value["result"]
                                elif "output" in value:
                                    value = value["output"]
                        inputs[input_key] = value
                    else:
                        # 上游节点尚未写入状态
                        inputs[input_key] = None
                # 输入为 dict 时，递归解析内部的 $ 引用
                elif isinstance(input_source, dict):
                    resolved_dict: Dict[str, Any] = {}
                    for k, v in input_source.items():
                        # 字符串引用（同上）
                        if isinstance(v, str) and v.startswith("$"):
                            path_parts = v[1:].split(".")
                            source_node = path_parts[0]
                            if source_node in workflow_state:
                                val: Any = workflow_state[source_node]
                                if isinstance(val, dict) and len(path_parts) > 1:
                                    for part in path_parts[1:]:
                                        if "[" in part and "]" in part:
                                            key_part = part[: part.index("[")]
                                            index_part = part[part.index("[") + 1 : part.index("]")]
                                            if key_part:
                                                val = val.get(key_part) if isinstance(val, dict) else None
                                            if isinstance(val, (list, tuple)) and index_part.isdigit():
                                                idx = int(index_part)
                                                if 0 <= idx < len(val):
                                                    val = val[idx]
                                                else:
                                                    val = None
                                                    break
                                        else:
                                            val = val.get(part) if isinstance(val, dict) else None
                                elif isinstance(val, dict):
                                    val = val.get("result", val.get("output", val))
                                resolved_dict[k] = val
                            else:
                                resolved_dict[k] = None
                        # 列表场景：支持 list 中混合常量与 $ 引用
                        elif isinstance(v, list):
                            resolved_list: List[Any] = []
                            for item in v:
                                if isinstance(item, str) and item.startswith("$"):
                                    path_parts = item[1:].split(".")
                                    source_node = path_parts[0]
                                    if source_node in workflow_state:
                                        val: Any = workflow_state[source_node]
                                        if isinstance(val, dict) and len(path_parts) > 1:
                                            for part in path_parts[1:]:
                                                if "[" in part and "]" in part:
                                                    key_part = part[: part.index("[")]
                                                    index_part = part[part.index("[") + 1 : part.index("]")]
                                                    if key_part:
                                                        val = val.get(key_part) if isinstance(val, dict) else None
                                                    if isinstance(val, (list, tuple)) and index_part.isdigit():
                                                        idx = int(index_part)
                                                        if 0 <= idx < len(val):
                                                            val = val[idx]
                                                        else:
                                                            val = None
                                                            break
                                                else:
                                                    val = val.get(part) if isinstance(val, dict) else None
                                        elif isinstance(val, dict):
                                            val = val.get("result", val.get("output", val))
                                        resolved_list.append(val)
                                    else:
                                        resolved_list.append(None)
                                else:
                                    # 普通值直接透传
                                    resolved_list.append(item)
                            resolved_dict[k] = resolved_list
                        else:
                            resolved_dict[k] = v
                    inputs[input_key] = resolved_dict
                else:
                    # 普通字面量直接作为输入
                    inputs[input_key] = input_source

        return inputs
    
    async def execute(self, initial_inputs: Dict[str, Any] = None) -> AsyncIterator[Dict[str, Any]]:
        """工作流执行入口，负责初始化状态并串联执行所有节点"""
        workflow_state: Dict[str, Any] = {}
        if initial_inputs:
            # 记录初始输入，供后续节点通过 $ _initial 引用
            workflow_state["_initial"] = initial_inputs

        # 按拓扑排序后的顺序依次执行节点，逐步 yield 每个节点的输出
        async for result in self._execute_nodes(self._get_execution_order(), workflow_state, initial_inputs):
            yield result
    
    async def _execute_nodes(
        self,
        node_order: List[str],
        workflow_state: Dict[str, Any],
        initial_inputs: Dict[str, Any] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """按照拓扑排序后的节点顺序依次执行，负责调度 loop / switch / 普通节点"""
        executed_nodes = set()

        for node_id in node_order:
            if node_id in executed_nodes:
                # 避免重复执行同一个节点
                continue

            node_config = self.node_configs[node_id]
            node_type = node_config["type"]

            # loop 控制流节点
            if node_type == "loop":
                async for result in self._execute_loop(node_id, node_config, workflow_state, initial_inputs):
                    yield result
            # switch 条件分支节点
            elif node_type == "switch":
                selected_nodes = await self._execute_switch(node_id, node_config, workflow_state)
                if selected_nodes:
                    # 对选中的子节点逐个执行
                    for sub_node_config in selected_nodes:
                        sub_node_id = f"{node_id}.{sub_node_config['id']}"
                        sub_node_type = sub_node_config.get("type")

                        # 嵌套控制流暂不支持
                        if sub_node_type in ["loop", "switch"]:
                            continue

                        # 使用子节点专用的输入解析逻辑
                        inputs = self._resolve_inputs_for_subnode(sub_node_config, workflow_state, node_id)
                        node = self.nodes.get(sub_node_id)

                        # 如未实例化则临时创建子节点实例
                        if not node:
                            node_cls = NODE_REGISTRY.get(sub_node_type)
                            if node_cls:
                                node = node_cls(sub_node_id, sub_node_config.get("config", {}))
                                self.nodes[sub_node_id] = node

                        # 执行子节点，并将每次 yield 的输出都向外转发
                        if node:
                            last_output: Any = None
                            async for output in node.execute(inputs):
                                last_output = output
                                workflow_state[sub_node_id] = output
                                yield {
                                    "node_id": sub_node_id,
                                    "output": output,
                                    "is_final": False,
                                }

                            # 将子节点的最后一次输出写回状态
                            if last_output:
                                workflow_state[sub_node_id] = last_output
            # 普通节点（真正执行业务逻辑）
            else:
                # 把上游节点的结果映射到当前节点输入
                inputs = self._resolve_inputs(node_id, workflow_state)
                # 第一个节点可额外混入 initial_inputs
                if node_id == node_order[0] and initial_inputs:
                    inputs.update(initial_inputs)

                node = self.nodes[node_id]
                last_output: Any = None
                # 节点内部可能多次 yield（流式输出）
                async for output in node.execute(inputs):
                    last_output = output
                    workflow_state[node_id] = output
                    # 将每次输出透传给外层（FastAPI SSE）
                    yield {
                        "node_id": node_id,
                        "output": output,
                        "is_final": False,
                    }

                # 记录节点最终输出，便于后续节点引用
                if last_output:
                    workflow_state[node_id] = last_output

            executed_nodes.add(node_id)
    
    async def _execute_loop(
        self,
        loop_id: str,
        loop_config: Dict[str, Any],
        workflow_state: Dict[str, Any],
        initial_inputs: Dict[str, Any],
    ) -> AsyncIterator[Dict[str, Any]]:
        """执行 loop 节点，将一组子节点按条件重复执行。
        支持两种模式：
        - 默认线性：config.concurrent 未开启
        - 并发：config.concurrent = true 时，同一轮内子节点并发执行
        """
        loop_nodes = loop_config.get("nodes", [])
        condition = loop_config.get("condition")
        max_iterations = loop_config.get("max_iterations", 1000)
        concurrent = bool(loop_config.get("concurrent", False))

        iteration = 0
        workflow_state[f"{loop_id}.iteration"] = {"result": 0}

        while iteration < max_iterations:
            iteration += 1
            workflow_state[f"{loop_id}.iteration"] = {"result": iteration}

            if not concurrent:
                # 线性模式：按顺序依次执行子节点
                for sub_node_config in loop_nodes:
                    sub_node_id = f"{loop_id}.{sub_node_config['id']}"
                    sub_node_type = sub_node_config["type"]
                    if sub_node_type in ["loop", "switch"]:
                        continue

                    inputs = self._resolve_inputs_for_subnode(sub_node_config, workflow_state, loop_id)
                    node = self.nodes.get(sub_node_id)
                    if not node:
                        node_cls = NODE_REGISTRY.get(sub_node_type)
                        if node_cls:
                            node = node_cls(sub_node_id, sub_node_config.get("config", {}))
                            self.nodes[sub_node_id] = node

                    if node:
                        last_output: Any = None
                        async for output in node.execute(inputs):
                            last_output = output
                            workflow_state[sub_node_id] = output
                            yield {
                                "node_id": sub_node_id,
                                "output": output,
                                "loop_iteration": iteration,
                                "is_final": False,
                            }

                        if last_output:
                            workflow_state[sub_node_id] = last_output
            else:
                # 并发模式：同一轮内所有子节点并发执行
                queue: asyncio.Queue = asyncio.Queue()
                tasks: List[asyncio.Task] = []

                async def run_sub_node(sub_cfg: Dict[str, Any]) -> tuple[str, Any]:
                    sub_id = f"{loop_id}.{sub_cfg['id']}"
                    sub_type = sub_cfg["type"]
                    if sub_type in ["loop", "switch"]:
                        return sub_id, None

                    inputs_local = self._resolve_inputs_for_subnode(sub_cfg, workflow_state, loop_id)
                    node_local = self.nodes.get(sub_id)
                    if not node_local:
                        node_cls_local = NODE_REGISTRY.get(sub_type)
                        if node_cls_local:
                            node_local = node_cls_local(sub_id, sub_cfg.get("config", {}))
                            self.nodes[sub_id] = node_local

                    last_out: Any = None
                    if node_local:
                        async for out in node_local.execute(inputs_local):
                            last_out = out
                            await queue.put((sub_id, out))
                    return sub_id, last_out

                for sub_node_config in loop_nodes:
                    tasks.append(asyncio.create_task(run_sub_node(sub_node_config)))

                pending = set(tasks)
                while pending or not queue.empty():
                    if pending:
                        done, pending = await asyncio.wait(
                            pending, timeout=0.01, return_when=asyncio.FIRST_COMPLETED
                        )
                    while not queue.empty():
                        sub_id, output = await queue.get()
                        workflow_state[sub_id] = output
                        yield {
                            "node_id": sub_id,
                            "output": output,
                            "loop_iteration": iteration,
                            "is_final": False,
                        }

                results = await asyncio.gather(*tasks, return_exceptions=False)
                for sub_id, last_out in results:
                    if last_out is not None:
                        workflow_state[sub_id] = last_out

            if condition:
                should_continue = self._evaluate_condition(condition, workflow_state, loop_id)
                if not should_continue:
                    break

        workflow_state[loop_id] = {"result": {"iteration": iteration, "completed": True}}
        yield {
            "node_id": loop_id,
            "output": {"iteration": iteration, "completed": True},
            "is_final": False,
        }
    
    async def _execute_switch(
        self,
        switch_id: str,
        switch_config: Dict[str, Any],
        workflow_state: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """执行 switch 条件分支节点，返回需执行的子节点配置列表"""
        condition = switch_config.get("condition")
        cases = switch_config.get("cases", [])
        default = switch_config.get("default")

        # 解析 condition 对应的真实值，可以是 $ 引用或表达式
        if isinstance(condition, str):
            condition_value = self._get_value(condition, workflow_state)
        elif isinstance(condition, dict):
            condition_value = self._evaluate_condition_value(condition, workflow_state)
        else:
            condition_value = condition

        # 在 cases 中查找匹配的分支
        for case in cases:
            case_value = case.get("value")
            if condition_value == case_value:
                target_nodes = case.get("nodes", [])
                return target_nodes

        # 未命中任何分支则走 default（如果存在）
        if default:
            default_nodes = default.get("nodes", [])
            return default_nodes

        # 没有匹配结果时返回空列表
        return []
    
    def _resolve_inputs_for_subnode(
        self,
        node_config: Dict[str, Any],
        workflow_state: Dict[str, Any],
        parent_id: str,
    ) -> Dict[str, Any]:
        """解析 loop / switch 中子节点的 inputs，支持父作用域前缀"""
        inputs: Dict[str, Any] = {}

        if "inputs" in node_config:
            for input_key, input_source in node_config["inputs"].items():
                # 子节点同样支持 $ 引用
                if isinstance(input_source, str) and input_source.startswith("$"):
                    path_parts = input_source[1:].split(".")
                    source_node = path_parts[0]

                    if source_node in workflow_state:
                        value: Any = workflow_state[source_node]
                        if isinstance(value, dict):
                            if len(path_parts) > 1:
                                for part in path_parts[1:]:
                                    if isinstance(value, dict):
                                        value = value.get(part)
                                    else:
                                        value = None
                                        break
                            else:
                                if "result" in value:
                                    value = value["result"]
                                elif "output" in value:
                                    value = value["output"]
                        inputs[input_key] = value
                    else:
                        # 不在全局作用域时，尝试从父作用域查找（parent_id.child）
                        full_source = f"{parent_id}.{source_node}"
                        if full_source in workflow_state:
                            value = workflow_state[full_source]
                            if isinstance(value, dict):
                                if len(path_parts) > 1:
                                    for part in path_parts[1:]:
                                        value = value.get(part) if isinstance(value, dict) else None
                                else:
                                    value = value.get("result", value.get("output", value))
                            inputs[input_key] = value
                        else:
                            inputs[input_key] = None
                else:
                    # 普通常量输入
                    inputs[input_key] = input_source

        return inputs
    
    def _evaluate_condition(
        self,
        condition: Dict[str, Any],
        workflow_state: Dict[str, Any],
        context_id: str = None,
    ) -> bool:
        """根据 condition 配置和当前 workflow_state 计算布尔结果"""
        condition_type = condition.get("type")

        # 表达式类型：expression
        if condition_type == "expression":
            expr = condition.get("expression")
            value = self._evaluate_expression(expr, workflow_state, context_id)
            return bool(value)
        # 比较类型：left / operator / right
        elif condition_type == "compare":
            left = self._get_value(condition.get("left"), workflow_state, context_id)
            operator = condition.get("operator", "==")
            right = self._get_value(condition.get("right"), workflow_state, context_id)

            if operator == "==":
                return left == right
            elif operator == "!=":
                return left != right
            elif operator == ">":
                return left > right
            elif operator == "<":
                return left < right
            elif operator == ">=":
                return left >= right
            elif operator == "<=":
                return left <= right
            elif operator == "in":
                return left in right if isinstance(right, (list, str)) else False
            elif operator == "not_in":
                return left not in right if isinstance(right, (list, str)) else True

        return False

    def _evaluate_condition_value(self, condition: Dict[str, Any], workflow_state: Dict[str, Any]) -> Any:
        """用于 switch，返回条件表达式的原始值"""
        if isinstance(condition, str) and condition.startswith("$"):
            return self._get_value(condition, workflow_state)
        elif isinstance(condition, dict):
            expr = condition.get("expression")
            if expr:
                return self._evaluate_expression(expr, workflow_state)
        return condition

    def _evaluate_expression(
        self,
        expr: str,
        workflow_state: Dict[str, Any],
        context_id: str = None,
    ) -> Any:
        """安全地计算一个表达式，禁用内置函数，仅依赖 workflow_state"""
        if expr.startswith("$"):
            return self._get_value(expr, workflow_state, context_id)
        try:
            # 禁用 __builtins__，仅允许访问 workflow_state 中的数据
            return eval(expr, {"__builtins__": {}}, workflow_state)
        except Exception:
            return None

    def _get_value(
        self,
        path: str,
        workflow_state: Dict[str, Any],
        context_id: str = None,
    ) -> Any:
        """根据 $node.field 形式从 workflow_state 取值"""
        if not isinstance(path, str) or not path.startswith("$"):
            return path

        path_parts = path[1:].split(".")
        source_node = path_parts[0]

        # 先在全局作用域查找
        if source_node in workflow_state:
            value: Any = workflow_state[source_node]
        # 再在上下文（如 loop_id.node）下查找
        elif context_id and f"{context_id}.{source_node}" in workflow_state:
            value = workflow_state[f"{context_id}.{source_node}"]
        else:
            return None

        if isinstance(value, dict):
            if len(path_parts) > 1:
                for part in path_parts[1:]:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        return None
            else:
                value = value.get("result", value.get("output", value))

        return value

    def _get_execution_order(self) -> List[str]:
        """根据 connections 做一次简单的拓扑排序，推导节点执行顺序"""
        node_ids = [n["id"] for n in self.config["nodes"]]
        # 若未显式声明 connections，则按声明顺序执行
        if not self.connections:
            return node_ids

        # 统计入度并构建邻接表
        in_degree: Dict[str, int] = {nid: 0 for nid in node_ids}
        graph: Dict[str, List[str]] = {nid: [] for nid in node_ids}

        for conn in self.connections:
            source = conn["from"]
            target = conn["to"]
            if source in graph:
                graph[source].append(target)
                in_degree[target] = in_degree.get(target, 0) + 1

        # Kahn 算法求拓扑序
        queue: List[str] = [nid for nid in node_ids if in_degree[nid] == 0]
        order: List[str] = []

        while queue:
            node_id = queue.pop(0)
            order.append(node_id)
            for neighbor in graph[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # 如果图中存在环路，退回到原始声明顺序
        return order if len(order) == len(node_ids) else node_ids


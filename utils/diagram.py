from typing import Dict, Any  # 类型标注
from config.loader import load_config  # 复用通用配置加载器


def generate_mermaid_diagram(config: Dict[str, Any]) -> str:
    """根据工作流配置生成 Mermaid 流程图定义"""
    nodes = config.get("nodes", [])
    connections = config.get("connections", [])

    # 使用自上而下的有向图
    lines = ["graph TD"]

    for node in nodes:
        node_id = node["id"]
        node_type = node.get("type", "unknown")

        # loop 节点渲染为方形+标记
        if node_type == "loop":
            node_label = f"{node_id}\\n[LOOP]"
            lines.append(f'    {node_id}["{node_label}"]')
            loop_nodes = node.get("nodes", [])
            # 将 loop 内部子节点展开为独立节点
            for sub_node in loop_nodes:
                sub_node_id = f"{node_id}_{sub_node['id']}"
                sub_node_type = sub_node.get("type", "unknown")
                sub_label = f"{sub_node['id']}\\n({sub_node_type})"
                lines.append(f'    {sub_node_id}["{sub_label}"]')
                lines.append(f"    {node_id} --> {sub_node_id}")
        # switch 节点渲染为菱形，并带上分支 label
        elif node_type == "switch":
            node_label = f"{node_id}\\n[SWITCH]"
            lines.append(f'    {node_id}{{"{node_label}"}}')
            cases = node.get("cases", [])
            for i, case in enumerate(cases):
                case_value = case.get("value", f"case{i}")
                case_nodes = case.get("nodes", [])
                for sub_node in case_nodes:
                    sub_node_id = f"{node_id}_{case_value}_{sub_node['id']}"
                    sub_node_type = sub_node.get("type", "unknown")
                    sub_label = f"{sub_node['id']}\\n({sub_node_type})"
                    lines.append(f'    {sub_node_id}["{sub_label}"]')
                    lines.append(f'    {node_id} -->|"{case_value}"| {sub_node_id}')
            # 默认分支
            default = node.get("default")
            if default:
                default_nodes = default.get("nodes", [])
                for sub_node in default_nodes:
                    sub_node_id = f"{node_id}_default_{sub_node['id']}"
                    sub_node_type = sub_node.get("type", "unknown")
                    sub_label = f"{sub_node['id']}\\n({sub_node_type})"
                    lines.append(f'    {sub_node_id}["{sub_label}"]')
                    lines.append(f'    {node_id} -->|"default"| {sub_node_id}')
        # 普通节点：矩形 + type 标记
        else:
            node_label = f"{node_id}\\n({node_type})"
            lines.append(f'    {node_id}["{node_label}"]')

    # 连接关系
    for conn in connections:
        source = conn.get("from")
        target = conn.get("to")
        if source and target:
            lines.append(f"    {source} --> {target}")

    return "\n".join(lines)


def generate_diagram_from_file(config_path: str) -> str:
    """从 YAML/JSON 配置文件直接生成 Mermaid 文本"""
    config = load_config(config_path)
    return generate_mermaid_diagram(config)


def generate_html_viewer(mermaid_code: str) -> str:
    """包装 Mermaid 文本为可直接预览的 HTML 页面"""
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Workflow Diagram</title>
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{ startOnLoad: true, theme: 'default' }});
    </script>
</head>
<body>
    <div class="mermaid">
{mermaid_code}
    </div>
</body>
</html>"""


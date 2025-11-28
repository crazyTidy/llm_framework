import json  # 处理 JSON 配置
import yaml  # 处理 YAML 配置
from pathlib import Path  # 文件路径工具
from typing import Dict, Any  # 类型标注


def load_config(config_path: str) -> Dict[str, Any]:
    """根据文件后缀自动加载 YAML / JSON 配置为 Python 字典"""
    path = Path(config_path)
    if not path.exists():
        # 配置文件不存在时抛出明确异常
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # 统一以 UTF-8 打开文件
    with open(path, "r", encoding="utf-8") as f:
        # 支持 .yaml / .yml 后缀
        if path.suffix in [".yaml", ".yml"]:
            return yaml.safe_load(f)
        # 支持 .json 后缀
        elif path.suffix == ".json":
            return json.load(f)
        # 其他格式暂不支持
        else:
            raise ValueError(f"Unsupported config format: {path.suffix}")


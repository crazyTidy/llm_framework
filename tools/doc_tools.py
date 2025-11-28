import json
from pathlib import Path
from typing import Any, Dict, List

import yaml


def doc_parse(url: str) -> List[str]:
    if url.startswith("file://"):
        path = Path(url[len("file://") :])
        text = path.read_text(encoding="utf-8")
        return [text]
    if url.startswith("http://") or url.startswith("https://"):
        return [f"remote_doc:{url}"]
    path = Path(url)
    if path.exists():
        text = path.read_text(encoding="utf-8")
        return [text]
    return [str(url)]


def load_prompt(prompt_path: str) -> str:
    path = Path(prompt_path)
    return path.read_text(encoding="utf-8")


def rag_search(question: str, doc_chunks: Any, prompt: str, retriever_config: Dict[str, Any] | None = None) -> str:
    if isinstance(doc_chunks, str):
        chunks = [doc_chunks]
    else:
        chunks = list(doc_chunks or [])
    merged = "\n\n".join(str(c) for c in chunks)
    return f"{prompt}\n\n问题: {question}\n\n上下文:\n{merged}"


def dify_config_parse(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(config_path)
    text = path.read_text(encoding="utf-8")
    if path.suffix in [".yaml", ".yml"]:
        cfg = yaml.safe_load(text)
    elif path.suffix == ".json":
        cfg = json.loads(text)
    else:
        cfg = {}
    prompt_path = None
    retriever = {}
    if isinstance(cfg, dict):
        prompt_path = cfg.get("prompt_path") or cfg.get("promptFile") or cfg.get("prompt_file")
        retriever = cfg.get("retriever") or cfg.get("rag") or {}
    return {
        "raw": cfg,
        "prompt_path": prompt_path,
        "retriever": retriever,
    }



from typing import Callable, Any, Dict

from .json_tools import json_to_md_table, json_pretty
from .doc_tools import doc_parse, load_prompt, rag_search, dify_config_parse


TOOLS_REGISTRY: Dict[str, Callable[..., Any]] = {
    "json_to_md_table": json_to_md_table,
    "json_pretty": json_pretty,
    "doc_parse": doc_parse,
    "load_prompt": load_prompt,
    "rag_search": rag_search,
    "dify_config_parse": dify_config_parse,
}


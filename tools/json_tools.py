import json
from typing import Any, Dict


def json_to_md_table(data: Any) -> str:
    if isinstance(data, str):
        data = json.loads(data)

    if isinstance(data, dict):
        headers = ["key", "value"]
        rows = [[str(k), str(v)] for k, v in data.items()]
    elif isinstance(data, list) and data and isinstance(data[0], dict):
        headers = list(data[0].keys())
        rows = [[str(item.get(h, "")) for h in headers] for item in data]
    else:
        headers = ["value"]
        rows = [[str(x)] for x in (data if isinstance(data, list) else [data])]

    header_line = "| " + " | ".join(headers) + " |"
    sep_line = "| " + " | ".join("---" for _ in headers) + " |"
    row_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join([header_line, sep_line, *row_lines])


def json_pretty(data: Any) -> str:
    if isinstance(data, str):
        try:
            parsed = json.loads(data)
        except Exception:
            return data
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    return json.dumps(data, ensure_ascii=False, indent=2)



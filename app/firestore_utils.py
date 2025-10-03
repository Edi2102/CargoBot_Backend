import re


def doc_id_from_username(user_name: str) -> str:

    user_name = (user_name or "").strip()
    safe = re.sub(r"[/\r\n\t]", "_", user_name)
    safe = re.sub(r"\s+", " ", safe)
    return safe or "unknown"

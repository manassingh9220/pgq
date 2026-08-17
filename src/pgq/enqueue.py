import json
from .queries import INSERT

def enqueue(conn, task: str, args: dict | None = None, *, run_at=None,
            priority: int = 0, max_attempts: int = 5,
            dedupe_key: str | None = None) -> int | None:
    row = conn.execute(INSERT, {
        "task": task,
        "args": json.dumps(args or {}),
        "run_at": run_at,
        "priority": priority,
        "max_attempts": max_attempts,
        "dedupe_key": dedupe_key,
    }).fetchone()

    if row is None:
        return None                    # dedupe_key already existed
    return row["id"] if isinstance(row, dict) else row[0]
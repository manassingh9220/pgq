import json
from .queries import INSERT

def enqueue(conn, task:str, args:dict | None=None, *, run_at = None, priority:int = 0, max_attempts = 5) -> int:
    row = conn.execute(INSERT,{
        "task":task,
        "args":json.dumps(args or {}),
        "run_at":run_at,
        "priority":priority,
        "max_attempts":max_attempts,
    }).fetchone()
    return row["id"] if isinstance(row,dict) else row[0]
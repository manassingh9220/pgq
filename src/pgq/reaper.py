import time

import psycopg
from psycopg.rows import dict_row

from .queries import REAP


def reap(conn) -> int:
    """Reclaim jobs whose worker vanished. Returns how many."""
    rows = conn.execute(REAP).fetchall()
    return len(rows)


def run_reaper(dsn: str, interval: float = 30.0):
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        print(f"reaper started (every {interval}s)")
        while True:
            n = reap(conn)
            if n:
                print(f"reclaimed {n} job(s)")
            time.sleep(interval)
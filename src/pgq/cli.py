import os
import sys
import argparse
import json

import psycopg
from psycopg.rows import dict_row

from .worker import Worker
from .enqueue import enqueue
from .reaper import run_reaper, reap


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")
    return dsn


def main():
    p = argparse.ArgumentParser(prog="pgq")
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("worker")
    w.add_argument("--poll-interval", type=float, default=0.5)

    r = sub.add_parser("reaper")
    r.add_argument("--interval", type=float, default=30.0)
    r.add_argument("--once", action="store_true")

    e = sub.add_parser("enqueue")
    e.add_argument("task")
    e.add_argument("args", nargs="?", default="{}")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--max-attempts", type=int, default=5)
    e.add_argument("--dedupe-key")
    e.add_argument("--run-at", help="interval from now, e.g. '30 seconds', '1 hour'")

    sub.add_parser("stats")

    args = p.parse_args()

    if args.cmd == "worker":
        sys.path.insert(0, os.getcwd())
        import tasks  # noqa: F401 — registers @task functions
        Worker(_dsn(), poll_interval=args.poll_interval).run()

    elif args.cmd == "enqueue":
        with psycopg.connect(_dsn(), autocommit=True, row_factory=dict_row) as conn:
            run_at = None
            if args.run_at:
                run_at = conn.execute(
                    "SELECT now() + %s::interval AS t", (args.run_at,)
                ).fetchone()["t"]

            job_id = enqueue(
                conn, args.task, json.loads(args.args),
                priority=args.priority,
                max_attempts=args.max_attempts,
                dedupe_key=args.dedupe_key,
                run_at=run_at,
            )
            if job_id is None:
                print(f"skipped — dedupe_key {args.dedupe_key!r} already queued")
            else:
                print(f"enqueued {job_id}")

    elif args.cmd == "reaper":
        if args.once:
            with psycopg.connect(_dsn(), autocommit=True, row_factory=dict_row) as conn:
                print(f"reclaimed {reap(conn)}")
        else:
            run_reaper(_dsn(), interval=args.interval)

    elif args.cmd == "stats":
        with psycopg.connect(_dsn(), autocommit=True, row_factory=dict_row) as conn:
            rows = conn.execute("""
                SELECT status, count(*) AS n FROM jobs GROUP BY status ORDER BY status
            """).fetchall()
            if not rows:
                print("queue is empty")
                return
            for row in rows:
                print(f"{row['status']:>10}  {row['n']}")

            oldest = conn.execute("""
                SELECT extract(epoch from now() - min(run_at)) AS s
                FROM jobs WHERE status = 'pending' AND run_at <= now()
            """).fetchone()["s"]
            if oldest is not None:
                print(f"\noldest ready job: {oldest:.1f}s old")
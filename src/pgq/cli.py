
import os
import sys
import argparse
from .worker import Worker
import json
from .enqueue import enqueue
from .reaper import run_reaper, reap
import psycopg
from psycopg.rows import dict_row

def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("Database URL is not set")
    return dsn

def main():
    p = argparse.ArgumentParser(prog='pgq')
    sub = p.add_subparsers(dest = "cmd", required = True)

    w = sub.add_parser('worker')
    w.add_argument("--poll-interval", type=float, default = 0.5)

    r = sub.add_parser("reaper")
    r.add_argument("--interval", type=float, default=30.0)
    r.add_argument("--once", action="store_true")

    e = sub.add_parser("enqueue")
    e.add_argument('task')
    e.add_argument('args',nargs ="?", default ="{}")
    e.add_argument("--priority", type = int, default = 0)

    args = p.parse_args()

    if args.cmd == 'worker':
        sys.path.insert(0, os.getcwd())
        import tasks
        Worker(_dsn(), poll_interval=args.poll_interval).run()
    elif args.cmd == 'enqueue':
        with psycopg.connect(_dsn(), autocommit = True, row_factory = dict_row) as conn:
            job_id = enqueue(conn, args.task, json.loads(args.args), priority=args.priority)
            print(f"Enqueued {job_id}")
    elif args.cmd == "reaper":
        if args.once:
            with psycopg.connect(_dsn(), autocommit=True, row_factory=dict_row) as conn:
                print(f"reclaimed {reap(conn)}")
        else:
            run_reaper(_dsn(), interval=args.interval)
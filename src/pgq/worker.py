import os
import signal
import socket
import time
import traceback

import psycopg
from psycopg.rows import dict_row

from . import registry
from .queries import CLAIM, SUCCEED, KILL, RETRY
from .backoff import backoff_seconds

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"


class Worker:
    def __init__(self, dsn: str, poll_interval: float = 0.5, max_idle: float = 5.0):
        self.dsn = dsn
        self.poll_interval = poll_interval
        self.max_idle = max_idle
        self.running = True

    def _stop(self, *_):
        print("\nshutting down after current job...")
        self.running = False

    def run(self):
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)

        idle = self.poll_interval
        with psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row) as conn:
            print(f"worker {WORKER_ID} started")
            while self.running:
                job = conn.execute(CLAIM, {"worker": WORKER_ID}).fetchone()
                if job is None:
                    time.sleep(idle)
                    idle = min(idle * 1.5, self.max_idle)
                    continue
                idle = self.poll_interval
                self._execute(conn, job)
        print("stopped")

    def _execute(self, conn, job):
        fn = registry.get(job["task"])
        if fn is None:
            conn.execute(KILL, {"id": job["id"],
                                "error": f"unknown task {job['task']!r}"})
            print(f"[{job['id']}] unknown task {job['task']!r} - dead")
            return

        print(f"[{job['id']}] {job['task']} starting (attempt {job['attempts']})")
        try:
            fn(**job["args"])
        except Exception:
            err = traceback.format_exc()
            self._fail(conn, job, err)
        else:
            conn.execute(SUCCEED, {"id": job["id"]})
            print(f"[{job['id']}] done")

    def run_until_idle(self, max_seconds: float = 60.0) -> int:
        """Process jobs until none remain. Returns how many ran."""
        deadline = time.monotonic() + max_seconds
        processed = 0
        with psycopg.connect(self.dsn, autocommit=True, row_factory=dict_row) as conn:
            while time.monotonic() < deadline:
                job = conn.execute(CLAIM, {"worker": WORKER_ID}).fetchone()
                if job is None:
                    break
                self._execute(conn, job)
                processed += 1
        return processed

    def _fail(self, conn, job, err: str):
        if job["attempts"] >= job["max_attempts"]:
            conn.execute(KILL, {"id": job["id"], "error": err[:8000]})
            print(f"[{job['id']}] failed permanently after {job['attempts']} attempts")
        else:
            delay = backoff_seconds(job["attempts"])
            conn.execute(RETRY, {"id": job["id"], "error": err[:8000], "delay": delay})
            print(f"[{job['id']}] failed, retrying in {delay:.1f}s")
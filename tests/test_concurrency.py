import multiprocessing as mp
import pytest

from pgq import enqueue, Worker
from tests.tasks import DSN

JOBS = 1000
WORKERS = 10


def _run_worker():
    import tests.tasks  # noqa: F401 — registers @task in this new process
    Worker(DSN, poll_interval=0.01).run_until_idle(max_seconds=60)


@pytest.mark.slow
def test_no_double_execution(conn):
    for i in range(JOBS):
        enqueue(conn, "record", {"job_id": i})

    procs = [mp.Process(target=_run_worker) for _ in range(WORKERS)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    total = conn.execute("SELECT count(*) AS n FROM results").fetchone()["n"]
    distinct = conn.execute("SELECT count(DISTINCT job_id) AS n FROM results").fetchone()["n"]
    succeeded = conn.execute(
        "SELECT count(*) AS n FROM jobs WHERE status='succeeded'").fetchone()["n"]
    workers = conn.execute("SELECT count(DISTINCT worker) AS n FROM results").fetchone()["n"]

    assert total == JOBS, f"{total} executions for {JOBS} jobs — duplicates"
    assert distinct == JOBS, f"only {distinct} distinct jobs ran — some were skipped"
    assert succeeded == JOBS, f"{succeeded} marked succeeded"
    assert workers > 1, f"only {workers} worker did any work — no real concurrency"
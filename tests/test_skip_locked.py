import multiprocessing as mp
import psycopg
import pytest

from pgq import enqueue, Worker
from tests.tasks import DSN


def _worker_run(max_seconds):
    import tests.tasks  # noqa: F401 — spawn needs the registry
    Worker(DSN, poll_interval=0.01).run_until_idle(max_seconds=max_seconds)


def test_workers_make_progress_past_a_locked_row(conn):
    """One row held by an outside transaction must not block the others."""
    for i in range(10):
        enqueue(conn, "record", {"job_id": i})

    # a separate transaction grabs the head row and holds it
    holder = psycopg.connect(DSN)          # NOT autocommit — lock persists
    with holder.cursor() as cur:
        cur.execute("""
            SELECT id FROM jobs
            WHERE status = 'pending'
            ORDER BY priority DESC, run_at
            FOR UPDATE LIMIT 1
        """)
        locked_id = cur.fetchone()[0]

        p = mp.Process(target=_worker_run, args=(5,))
        p.start()
        p.join(timeout=15)

        hung = p.is_alive()
        if hung:
            p.terminate()
            p.join()

        done = conn.execute("SELECT count(*) AS n FROM results").fetchone()["n"]

    holder.rollback()
    holder.close()

    assert not hung, "worker blocked on the held row instead of skipping it"
    assert done == 9, f"expected 9 of 10 jobs processed, got {done}"

    row = conn.execute("SELECT status FROM jobs WHERE id=%s", (locked_id,)).fetchone()
    assert row["status"] == "pending", "the locked row should be untouched"
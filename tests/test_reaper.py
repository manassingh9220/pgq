from pgq import enqueue, reap
from pgq.queries import CLAIM


def test_reap_returns_orphan_to_pending(conn):
    enqueue(conn, "record", {"job_id": 1}, max_attempts=5)
    job = conn.execute(CLAIM, {"worker": "ghost"}).fetchone()
    assert job is not None

    # simulate the lease expiring
    conn.execute("UPDATE jobs SET locked_at = now() - interval '10 minutes'")

    assert reap(conn) == 1

    row = conn.execute("SELECT * FROM jobs").fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1          # THE assertion — count preserved
    assert row["locked_by"] is None
    assert row["locked_at"] is None
    assert "vanished" in row["last_error"]


def test_reap_kills_when_attempts_exhausted(conn):
    enqueue(conn, "record", {"job_id": 1}, max_attempts=1)
    conn.execute(CLAIM, {"worker": "ghost"})
    conn.execute("UPDATE jobs SET locked_at = now() - interval '10 minutes'")

    reap(conn)
    assert conn.execute("SELECT status FROM jobs").fetchone()["status"] == "dead"


def test_reap_ignores_live_workers(conn):
    enqueue(conn, "record", {"job_id": 1})
    conn.execute(CLAIM, {"worker": "alive"})     # locked_at = now()
    assert reap(conn) == 0                        # well within the timeout
    assert conn.execute("SELECT status FROM jobs").fetchone()["status"] == "running"
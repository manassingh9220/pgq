import pytest
from pgq import enqueue, task
from pgq.queries import CLAIM

# side-effect probe: proves the function actually ran
ran = []

@task("test_ok")
def _ok(x):
    ran.append(x)

@task("test_boom")
def _boom():
    raise RuntimeError("boom")


def claim(conn, worker="w1"):
    return conn.execute(CLAIM, {"worker": worker}).fetchone()


def test_enqueue_creates_pending_row(conn):
    job_id = enqueue(conn, "test_ok", {"x": 1})
    row = conn.execute("SELECT * FROM jobs WHERE id=%s", (job_id,)).fetchone()
    assert row["status"] == "pending"
    assert row["task"] == "test_ok"
    assert row["args"] == {"x": 1}
    assert row["attempts"] == 0


def test_claim_marks_running_and_increments(conn):
    enqueue(conn, "test_ok", {"x": 1})
    job = claim(conn)
    assert job is not None
    assert job["attempts"] == 1

    row = conn.execute("SELECT status, locked_by FROM jobs").fetchone()
    assert row["status"] == "running"
    assert row["locked_by"] == "w1"


def test_claim_returns_none_when_empty(conn):
    assert claim(conn) is None


def test_priority_wins(conn):
    enqueue(conn, "test_ok", {"x": "low"}, priority=0)
    enqueue(conn, "test_ok", {"x": "high"}, priority=10)
    assert claim(conn)["args"]["x"] == "high"


def test_fifo_within_same_priority(conn):
    enqueue(conn, "test_ok", {"x": "first"})
    enqueue(conn, "test_ok", {"x": "second"})
    assert claim(conn)["args"]["x"] == "first"


def test_future_job_not_claimed(conn):
    conn.execute("""INSERT INTO jobs (task, args, run_at)
                    VALUES ('test_ok', '{"x":1}', now() + interval '1 hour')""")
    assert claim(conn) is None


def test_claimed_job_not_claimed_twice(conn):
    enqueue(conn, "test_ok", {"x": 1})
    assert claim(conn, "w1") is not None
    assert claim(conn, "w2") is None      # already running
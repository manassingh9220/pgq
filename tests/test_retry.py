import time
import psycopg
import pytest

from pgq import enqueue, Worker, reap, task
from pgq.queries import CLAIM
from pgq.backoff import backoff_seconds
from tests.tasks import DSN
import tests.tasks

failures = {}

@task("flaky")
def flaky(key: str, fail_times: int):
    failures[key] = failures.get(key, 0) + 1
    if failures[key] <= fail_times:
        raise RuntimeError(f"attempt {failures[key]} fails by design")


def run_one(conn, worker="w1"):
    """Claim and execute exactly one job. Returns the job or None."""
    w = Worker(DSN)
    job = conn.execute(CLAIM, {"worker": worker}).fetchone()
    if job is None:
        return None
    w._execute(conn, job)
    return job


def test_failure_retries_not_kills(conn):
    enqueue(conn, "always_fails", {}, max_attempts=5)
    run_one(conn)
    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1


def test_retry_delay_is_in_the_future(conn):
    enqueue(conn, "always_fails", {}, max_attempts=5)
    run_one(conn)
    row = conn.execute("SELECT run_at > now() AS future FROM jobs").fetchone()
    assert row["future"] is True


def test_backoff_grows(conn):
    delays = []
    for attempt in range(1, 6):
        samples = [backoff_seconds(attempt) for _ in range(50)]
        delays.append(sum(samples) / len(samples))
    assert delays == sorted(delays), f"backoff not monotonic: {delays}"
    assert delays[-1] > delays[0] * 4


def test_jitter_varies(conn):
    samples = {backoff_seconds(3) for _ in range(20)}
    assert len(samples) > 15, "delays are identical — jitter missing"


def test_poison_job_dies_after_max_attempts(conn):
    enqueue(conn, "always_fails", {}, max_attempts=3)
    for _ in range(3):
        conn.execute("UPDATE jobs SET run_at = now()")   # skip the wait
        assert run_one(conn) is not None

    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 3

    conn.execute("UPDATE jobs SET run_at = now()")
    assert run_one(conn) is None, "dead job was claimed again"


def test_transient_failure_eventually_succeeds(conn):
    key = "t1"
    failures.pop(key, None)
    enqueue(conn, "flaky", {"key": key, "fail_times": 2}, max_attempts=5)

    for _ in range(3):
        conn.execute("UPDATE jobs SET run_at = now()")
        run_one(conn)

    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "succeeded"
    assert row["attempts"] == 3


def test_unknown_task_dies_without_retry(conn):
    enqueue(conn, "no_such_task", {}, max_attempts=5)
    run_one(conn)
    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 1     # no retries — it will never resolve
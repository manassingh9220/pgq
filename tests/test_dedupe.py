from pgq import enqueue
import tests.tasks  # noqa: F401 — registers @task functions


def test_dedupe_key_prevents_duplicates(conn):
    a = enqueue(conn, "record", {"job_id": 1}, dedupe_key="order-42")
    b = enqueue(conn, "record", {"job_id": 1}, dedupe_key="order-42")
    assert a is not None
    assert b is None
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1


def test_null_dedupe_keys_dont_collide(conn):
    assert enqueue(conn, "record", {"job_id": 1}) is not None
    assert enqueue(conn, "record", {"job_id": 2}) is not None
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 2
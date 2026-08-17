import psycopg
import pytest
from pgq import enqueue
from tests.tasks import DSN


def test_rollback_leaves_no_job(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY)")
    conn.execute("TRUNCATE orders")

    with pytest.raises(RuntimeError):
        with psycopg.connect(DSN) as tx:          # NOT autocommit
            tx.execute("INSERT INTO orders DEFAULT VALUES")
            enqueue(tx, "record", {"job_id": 1})
            raise RuntimeError("failed after enqueue")

    assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 0
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0


def test_commit_persists_both(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY)")
    conn.execute("TRUNCATE orders")

    with psycopg.connect(DSN) as tx:
        tx.execute("INSERT INTO orders DEFAULT VALUES")
        enqueue(tx, "record", {"job_id": 1})

    assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 1
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1
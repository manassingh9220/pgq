import os
import psycopg
from pgq import task

DSN = os.environ["DATABASE_URL"]

@task("always_fails")
def always_fails():
    raise RuntimeError("boom")

@task("record")
def record(job_id: int):
    """Writes exactly one row per execution. The correctness probe."""
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("INSERT INTO results (job_id, worker) VALUES (%s, %s)",
                  (job_id, str(os.getpid())))
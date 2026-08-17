# pgq — Build & Verify Guide

What you're building, what "done" means for each piece, and exactly how to prove
each piece works.

This is the working document. `README.md` is the polished project-facing doc you
write at the end.

---

## Part 1 — What this project does

A job queue that stores jobs in a PostgreSQL table instead of a message broker.

Your application calls `enqueue(conn, "send_email", {...})`, which inserts a row
and returns immediately. Separate worker processes claim rows, run the matching
Python function, and mark the outcome. Failures retry with exponential backoff.
Workers that die have their jobs reclaimed and re-run.

### The one thing that makes it interesting

Because the queue lives in the same database as your application data, enqueueing
a job and writing the data that job operates on happen in **one transaction**:

```python
with conn.transaction():
    order = create_order(conn, payload)
    enqueue(conn, "send_confirmation", {"order_id": str(order.id)})
```

Roll back → no order, no job. Commit → both, guaranteed. Celery on Redis cannot
do this, because the broker is a separate system with no shared transaction. The
usual workaround is the transactional outbox pattern; this design removes the need
for it by making the outbox *be* the queue.

That sentence is the pitch. Everything else in the project exists to make it
production-shaped.

### What it is not

Not an event log. One job, one consumer, run once. No fan-out to multiple
subscribers, no replay of processed history. If you need those, you need Kafka.

---

## Part 2 — Definition of done

Four milestones. Each is independently demoable. Do not start the next until the
current one's tests pass.

### Milestone 1 — It runs jobs

- [ ] `schema.sql` creates the `jobs` table, the `job_status` enum, and both partial indexes
- [ ] `@task("name")` decorator registers a function
- [ ] `enqueue(conn, name, args)` inserts a pending row
- [ ] A single worker claims a job, runs it, marks it `succeeded`
- [ ] Unknown task name → job goes straight to `dead`, worker keeps running

**Demo:** enqueue a task that writes to a file. Start a worker. File appears.

### Milestone 2 — It runs jobs concurrently and correctly

- [ ] Claim query uses `FOR UPDATE SKIP LOCKED` in a subquery
- [ ] `attempts` increments at claim time, not on failure
- [ ] Ten workers against a thousand jobs: each job runs exactly once
- [ ] Idle workers back off instead of hammering the database

**Demo:** the concurrency test below, run with and without `SKIP LOCKED`.

### Milestone 3 — It survives failure

- [ ] A raising task is caught, logged to `last_error`, and set back to `pending`
- [ ] Retry delay is exponential with jitter
- [ ] After `max_attempts`, the job becomes `dead` and is not retried
- [ ] The reaper reclaims jobs whose `locked_at` exceeds `visibility_timeout`
- [ ] `SIGTERM` finishes the current job before exiting

**Demo:** `kill -9` a worker mid-job; the job completes on another worker after
the timeout.

### Milestone 4 — It's usable

- [ ] `run_at` supports scheduling into the future
- [ ] `priority` orders the claim
- [ ] `dedupe_key` prevents duplicate enqueue
- [ ] `pgq worker` and `pgq reaper` CLI commands
- [ ] Transactional enqueue verified by test

**Stop here.** No dashboard, no cron, no task chains.

---

## Part 3 — Setup

```bash
# Postgres for tests — throwaway container
docker run -d --name pgq-test \
  -e POSTGRES_PASSWORD=pgq -e POSTGRES_DB=pgq_test \
  -p 5433:5432 postgres:16

export PGQ_TEST_DSN="postgresql://postgres:pgq@localhost:5433/pgq_test"

pip install -e ".[dev]"          # psycopg[binary], pytest, pytest-timeout
```

Port 5433 deliberately — keeps the test database off your dev Postgres on 5432 so
a `TRUNCATE` in a test can never touch real data.

---

## Part 4 — Test harness

### `tests/conftest.py`

```python
import os, pathlib, pytest, psycopg
from psycopg.rows import dict_row

DSN = os.environ["PGQ_TEST_DSN"]

@pytest.fixture(scope="session")
def schema():
    sql = pathlib.Path("schema.sql").read_text()
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS jobs, results CASCADE")
        c.execute("DROP TYPE IF EXISTS job_status CASCADE")
        c.execute(sql)
        # side-effect table the test tasks write to
        c.execute("""
            CREATE TABLE results (
                id BIGSERIAL PRIMARY KEY,
                job_id BIGINT NOT NULL,
                worker TEXT,
                at TIMESTAMPTZ NOT NULL DEFAULT now()
            )""")

@pytest.fixture
def conn(schema):
    with psycopg.connect(DSN, autocommit=True, row_factory=dict_row) as c:
        c.execute("TRUNCATE jobs, results RESTART IDENTITY")
        yield c
```

`results` is how you prove a job ran. Never assert on `status` alone — a bug that
marks jobs succeeded without executing them would pass that check.

### `tests/tasks.py`

```python
import os, time, psycopg
from pgq import task

DSN = os.environ["PGQ_TEST_DSN"]

@task("record")
def record(job_id: int):
    """Writes exactly one row. The core correctness probe."""
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("INSERT INTO results (job_id, worker) VALUES (%s, %s)",
                  (job_id, os.getpid()))

@task("always_fails")
def always_fails():
    raise RuntimeError("boom")

@task("fails_n_times")
def fails_n_times(n: int, marker: str):
    """Fails the first n attempts, then succeeds. Uses results as the counter."""
    with psycopg.connect(DSN, autocommit=True) as c:
        row = c.execute("SELECT count(*) FROM results WHERE worker=%s",
                        (marker,)).fetchone()
        c.execute("INSERT INTO results (job_id, worker) VALUES (0, %s)", (marker,))
        if row[0] < n:
            raise RuntimeError(f"attempt {row[0] + 1} fails by design")

@task("sleeps")
def sleeps(seconds: float):
    time.sleep(seconds)
```

---

## Part 5 — The tests that matter

### 5.1 No double execution — the important one

```python
# tests/test_concurrency.py
import multiprocessing as mp, psycopg, pytest
from pgq import enqueue, Worker
from tests.tasks import DSN

JOBS, WORKERS = 1000, 10

def _run_worker(deadline):
    w = Worker(DSN, poll_interval=0.01)
    w.run_until_idle(max_seconds=deadline)

@pytest.mark.timeout(120)
def test_no_double_execution(conn):
    for i in range(JOBS):
        enqueue(conn, "record", {"job_id": i})

    procs = [mp.Process(target=_run_worker, args=(60,)) for _ in range(WORKERS)]
    for p in procs: p.start()
    for p in procs: p.join()

    total    = conn.execute("SELECT count(*) AS n FROM results").fetchone()["n"]
    distinct = conn.execute("SELECT count(DISTINCT job_id) AS n FROM results").fetchone()["n"]
    done     = conn.execute("SELECT count(*) AS n FROM jobs WHERE status='succeeded'").fetchone()["n"]
    workers  = conn.execute("SELECT count(DISTINCT worker) AS n FROM results").fetchone()["n"]

    assert total == JOBS, f"{total} executions for {JOBS} jobs — duplicates"
    assert distinct == JOBS, "some jobs never ran"
    assert done == JOBS
    assert workers > 1, "only one worker participated — no real concurrency tested"
```

That last assertion is easy to omit and worth keeping. A bug that serializes all
work into one worker would otherwise pass every other check.

**Prove `SKIP LOCKED` is load-bearing.** Comment it out of the claim query and
re-run:

```
FAILED test_no_double_execution - 1000 executions for 1000 jobs
                                  (but took 47s instead of 3s)
```

Correctness usually survives — the lock still serializes access. What collapses is
throughput. Time both runs and put the numbers in your README; "10 workers went
from 47s to 3s" is a more convincing artifact than a paragraph of explanation.

### 5.2 Crash recovery

Deterministic version — no real killing, so it's fast and stable in CI:

```python
# tests/test_reclaim.py
from pgq import enqueue, claim, reap

def test_reclaim_after_worker_vanishes(conn):
    enqueue(conn, "record", {"job_id": 1})

    job = claim(conn, worker_id="ghost")          # claim, then never report
    assert job is not None
    assert job["attempts"] == 1

    # simulate the lease expiring
    conn.execute("UPDATE jobs SET locked_at = now() - interval '10 minutes'")

    reclaimed = reap(conn)
    assert reclaimed == 1

    row = conn.execute("SELECT * FROM jobs").fetchone()
    assert row["status"] == "pending"
    assert row["attempts"] == 1                   # preserved — this is the point
    assert row["locked_by"] is None
    assert "vanished" in row["last_error"]

def test_reclaim_respects_max_attempts(conn):
    enqueue(conn, "record", {"job_id": 1}, max_attempts=1)
    claim(conn, worker_id="ghost")
    conn.execute("UPDATE jobs SET locked_at = now() - interval '10 minutes'")
    reap(conn)
    assert conn.execute("SELECT status FROM jobs").fetchone()["status"] == "dead"
```

`attempts == 1` after reclaim is the assertion that catches the increment-on-
failure bug. If you increment on failure instead of at claim, this reads 0 and the
job can retry forever.

Also do it for real, once, by hand:

```bash
pgq enqueue sleeps '{"seconds": 300}'
pgq worker &
sleep 2
kill -9 %1
psql "$PGQ_TEST_DSN" -c "select id, status, locked_by, attempts from jobs"
# → running, <dead pid>, 1
sleep 300   # or shorten visibility_timeout for the demo
pgq reaper --once
psql "$PGQ_TEST_DSN" -c "select id, status, attempts from jobs"
# → pending, 1
```

### 5.3 Retry and backoff

```python
# tests/test_retry.py
from pgq import enqueue, run_one

def test_backoff_grows_and_is_jittered(conn):
    enqueue(conn, "always_fails", {}, max_attempts=5)

    delays = []
    for _ in range(4):
        before = conn.execute("SELECT now() AS t").fetchone()["t"]
        run_one(conn, worker_id="w1")
        row = conn.execute("SELECT run_at, attempts, status FROM jobs").fetchone()
        delays.append((row["run_at"] - before).total_seconds())
        conn.execute("UPDATE jobs SET run_at = now()")   # skip the wait

    # 2^1..2^4 with 50–100% jitter
    for i, d in enumerate(delays, start=1):
        lo, hi = (2 ** i) * 0.5, (2 ** i) * 1.0
        assert lo <= d <= hi + 1, f"attempt {i}: delay {d}s outside [{lo}, {hi}]"

    assert delays == sorted(delays), "backoff is not monotonic"

def test_jitter_actually_varies(conn):
    """Ten identical failures should not produce ten identical delays."""
    seen = set()
    for i in range(10):
        conn.execute("TRUNCATE jobs RESTART IDENTITY")
        enqueue(conn, "always_fails", {})
        run_one(conn, worker_id="w1")
        seen.add(conn.execute("SELECT run_at FROM jobs").fetchone()["run_at"])
    assert len(seen) > 8, "delays are identical — jitter is missing"

def test_poison_job_dies_exactly_once(conn):
    enqueue(conn, "always_fails", {}, max_attempts=3)
    for _ in range(3):
        conn.execute("UPDATE jobs SET run_at = now()")
        run_one(conn, worker_id="w1")

    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 3
    assert "boom" in conn.execute("SELECT last_error FROM jobs").fetchone()["last_error"]

    conn.execute("UPDATE jobs SET run_at = now()")
    assert run_one(conn, worker_id="w1") is None, "dead job was claimed again"

def test_recovers_after_transient_failure(conn):
    enqueue(conn, "fails_n_times", {"n": 2, "marker": "t1"}, max_attempts=5)
    for _ in range(3):
        conn.execute("UPDATE jobs SET run_at = now()")
        run_one(conn, worker_id="w1")
    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "succeeded"
    assert row["attempts"] == 3
```

`test_jitter_actually_varies` catches a real and common bug: writing the backoff
formula but forgetting the random term. Every other retry test passes without
jitter.

### 5.4 Transactional enqueue — the headline feature

```python
# tests/test_transactional.py
import psycopg, pytest
from pgq import enqueue
from tests.tasks import DSN

def test_rollback_leaves_no_job(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY)")
    conn.execute("TRUNCATE orders")

    with pytest.raises(RuntimeError):
        with psycopg.connect(DSN) as tx:          # not autocommit
            tx.execute("INSERT INTO orders DEFAULT VALUES")
            enqueue(tx, "record", {"job_id": 1})
            raise RuntimeError("something failed after enqueue")

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
```

Two tests, and together they're the entire justification for the project.

### 5.5 Scheduling, priority, dedupe

```python
# tests/test_semantics.py
from datetime import timedelta
from pgq import enqueue, claim

def test_future_job_not_claimed(conn):
    enqueue(conn, "record", {"job_id": 1}, run_at="now() + interval '1 hour'")
    assert claim(conn, worker_id="w1") is None

def test_priority_ordering(conn):
    enqueue(conn, "record", {"job_id": 1}, priority=0)
    enqueue(conn, "record", {"job_id": 2}, priority=10)
    assert claim(conn, worker_id="w1")["args"]["job_id"] == 2

def test_fifo_within_priority(conn):
    enqueue(conn, "record", {"job_id": 1})
    enqueue(conn, "record", {"job_id": 2})
    assert claim(conn, worker_id="w1")["args"]["job_id"] == 1

def test_dedupe_key(conn):
    a = enqueue(conn, "record", {"job_id": 1}, dedupe_key="order-42")
    b = enqueue(conn, "record", {"job_id": 1}, dedupe_key="order-42")
    assert a is not None and b is None
    assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1

def test_unknown_task_dies_immediately(conn):
    enqueue(conn, "no_such_task", {})
    run_one(conn, worker_id="w1")
    row = conn.execute("SELECT status, attempts FROM jobs").fetchone()
    assert row["status"] == "dead"
    assert row["attempts"] == 1        # no retries — it will never exist
```

### 5.6 Graceful shutdown

```python
# tests/test_shutdown.py
import multiprocessing as mp, os, signal, time
from pgq import enqueue, Worker
from tests.tasks import DSN

def _worker():
    Worker(DSN, poll_interval=0.01).run()

def test_sigterm_finishes_current_job(conn):
    enqueue(conn, "sleeps", {"seconds": 3})
    p = mp.Process(target=_worker); p.start()
    time.sleep(1)                                  # let it claim and start

    os.kill(p.pid, signal.SIGTERM)
    p.join(timeout=10)

    assert p.exitcode == 0
    assert conn.execute("SELECT status FROM jobs").fetchone()["status"] == "succeeded"
```

A worker that exits immediately on SIGTERM leaves the job `running`, and this
fails. Every rolling deploy would strand work for one full visibility timeout.

---

## Part 6 — Running everything

```bash
pytest tests/ -v                          # all
pytest tests/ -v -m "not slow"            # skip the 1000-job concurrency run
pytest tests/test_concurrency.py -v -s    # the one that matters
```

`pyproject.toml`:

```toml
[tool.pytest.ini_options]
timeout = 180
markers = ["slow: takes more than 10 seconds"]
```

The timeout is important. Queue bugs manifest as hangs — a worker waiting on a
lock forever, a retry loop that never terminates. Without a timeout your test
suite stops instead of failing.

---

## Part 7 — Manual smoke test

Do this once end to end before calling it finished.

```bash
psql "$DATABASE_URL" -f schema.sql

# terminal 1
pgq worker --concurrency 2

# terminal 2
pgq enqueue record '{"job_id": 1}'
pgq enqueue always_fails '{}' --max-attempts 3
pgq enqueue record '{"job_id": 2}' --run-at '+30 seconds'
pgq enqueue record '{"job_id": 3}' --priority 10

watch -n1 'psql "$DATABASE_URL" -c "
  SELECT id, task, status, attempts, run_at - now() AS eta FROM jobs ORDER BY id"'
```

What you should see: job 3 runs first despite being enqueued last. Job 1 runs
next. Job 2 sits pending with a shrinking ETA, then runs. `always_fails` retries
three times with widening gaps, then goes `dead`.

Then kill the worker with `kill -9` while a long job is running and confirm the
reaper picks it up.

---

## Part 8 — Benchmark

Take the numbers; they belong in the README.

```python
# bench.py — enqueue 10k no-op jobs, N workers, measure wall time
```

Record throughput at 1, 2, 4, 8, 16 workers, with and without `SKIP LOCKED`. Two
numbers to publish:

- **Jobs/sec at your best worker count** — the honest ceiling of the design
- **The `SKIP LOCKED` speedup** — the concrete evidence behind the design decision

A README that says "3,200 jobs/sec across 8 workers; removing `SKIP LOCKED` drops
it to 210" is worth more than any amount of prose about lock contention.

---

## Part 9 — Talking about it

Three things to have ready:

**The one-liner.** "A Postgres-backed job queue where enqueueing a job and writing
the data it depends on happen in the same transaction — so you get the outbox
pattern's guarantee without the outbox."

**The design decision.** `attempts` increments at claim rather than on failure,
because a worker killed mid-job never records its attempt, and a poison job would
then retry forever. Concrete, non-obvious, clearly learned by doing.

**The measurement.** The `SKIP LOCKED` benchmark. It shows you validated a design
choice rather than copying it.

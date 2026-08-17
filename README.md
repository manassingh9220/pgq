# pgq

A job queue built on PostgreSQL. No broker, no Redis, no extra service to operate.

Jobs live in a table in the same database as your application data, which means
**enqueueing a job and writing the data that job depends on happen in one
transaction.**

```python
with conn.transaction():
    order = create_order(conn, payload)
    enqueue(conn, "send_confirmation", {"order_id": str(order.id)})
```

Roll back and the job never existed. Commit and it is guaranteed to run. There is
no window where the order exists but the confirmation was never queued.

That property is the reason this exists, and it is verified by
`tests/test_transactional.py`.

---

## Why not Celery

Celery is the right tool for most people. This is worth building when one of the
following applies.

### You need transactional enqueue

With Celery on Redis, the database write and the broker publish are two separate
systems with no shared transaction. Whatever order you put them in, there is a
failure window:

```python
order = create_order(data)          # commits
send_email.delay(order.id)          # process dies here → email never sends
```

The standard fix is the transactional outbox pattern: write the event to a table
in the same transaction, then relay it to the broker. But if you are already
writing jobs to a Postgres table, the relay step is the only part you do not
need. `pgq` is what the outbox pattern collapses into when you drop the broker.

### You already run Postgres

A broker is another service to deploy, monitor, secure, back up, and get paged
about. Below a few thousand jobs per second, Postgres handles the load.

### You want to inspect the queue with SQL

"Why is this job stuck." "What failed in the last hour." "Requeue everything that
died during the outage." One-line queries here; tooling problems elsewhere.

### When to use something else

| Situation | Use instead |
|---|---|
| Sustained throughput above a few thousand jobs/sec | Kafka, SQS, RabbitMQ |
| Multiple services consuming the same events | Kafka |
| Events must be replayable after processing | Kafka |
| Your team already runs and knows Celery | Celery |

`pgq` is a task queue — one job, one worker, run once, retry on failure. It is not
an event log.

---

## Quick start

```bash
createdb pgq
psql pgq -f schema.sql

pip install -e .
export DATABASE_URL="postgresql://localhost:5432/pgq"
```

Define tasks in `tasks.py` at the project root:

```python
from pgq import task

@task("hello")
def hello(name):
    print(f"hello {name}")
```

Run a worker and enqueue something:

```bash
pgq worker                                       # terminal 1
pgq enqueue hello '{"name": "alice"}'            # terminal 2
```

Other commands:

```bash
pgq enqueue slow '{}' --priority 10 --max-attempts 3
pgq enqueue report '{}' --run-at '1 hour'
pgq enqueue email '{}' --dedupe-key order-42     # idempotent
pgq reaper                                       # reclaim orphaned jobs
pgq reaper --once
pgq stats
```

---

## How it works

A job moves through four states:

```
                  reclaimed after visibility timeout
        +------------------------------------------+
        |                                          |
        v                                          |
   +---------+   claim    +---------+   ok    +-----------+
   | pending | ---------> | running | ------> | succeeded |
   +---------+            +---------+         +-----------+
        ^                      |
        |                      | attempts exhausted
        |  retry w/ backoff    v
        +---------------   +--------+
                           |  dead  |
                           +--------+
```

Two paths return a job to `pending`: a failure with attempts remaining, and
reclamation of a job whose worker disappeared without reporting anything.

### The claim query

Everything hinges on one statement:

```sql
UPDATE jobs
SET status = 'running',
    locked_by = %(worker)s,
    locked_at = now(),
    attempts = attempts + 1
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'pending' AND run_at <= now()
    ORDER BY priority DESC, run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, task, args, attempts, max_attempts;
```

The subquery selects one row under a lock; the outer `UPDATE` claims it by primary
key. `RETURNING` hands the row back, so a claim is one round trip rather than a
select followed by an update.

`UPDATE` does not accept `ORDER BY ... LIMIT`, which is why the subquery exists.

---

## Design decisions

The parts that look wrong until you know what they prevent.

### `attempts` increments at claim time, not on failure

If you increment when a job fails, a worker killed with `SIGKILL` never records
its attempt. The reaper returns the job to `pending` with an unchanged count and it
retries forever — a job that crashes workers becomes an infinite loop that takes
down the fleet.

Incrementing at claim means every attempt is paid for whether or not the worker
survives to report an outcome. A job with `max_attempts = 5` runs at most five
times however it dies.

Asserted by `test_reap_returns_orphan_to_pending`, which checks `attempts == 1`
after reclamation. That assertion is the regression guard for this decision.

### Partial indexes on status

```sql
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at)
    WHERE status = 'pending';
CREATE INDEX jobs_reclaim_idx ON jobs (locked_at)
    WHERE status = 'running';
```

The claim index contains only claimable rows. Ten million completed jobs have no
effect on claim performance because they are not in the index. A full index on
`(priority, run_at)` would grow without bound and slow every claim as history
accumulates.

Column order and direction mirror the query's `ORDER BY priority DESC, run_at`
exactly, so Postgres walks to the first entry and stops — no sort step. The `DESC`
is load-bearing: a backwards index scan would flip both columns, and the query
needs mixed directions.

### `run_at` handles delays and retries

There is no scheduler table and no retry table. A delayed job has a future
`run_at`. A retry is a job set back to `pending` with `run_at` pushed forward by
the backoff interval. The claim query's `run_at <= now()` filter covers both.

### Backoff is jittered

```python
delay = min(2 ** attempts, 3600) * (0.5 + random.random() * 0.5)
```

Without jitter, a downstream outage that fails 1,000 jobs at the same instant
produces 1,000 retries at the same instant, which fail together and retry together
— permanently synchronised, hitting the recovering service in waves.

`test_jitter_varies` asserts twenty calls produce more than fifteen distinct
values. Every other retry test passes without jitter, which is why this one
exists.

### Visibility timeout is per-job

A thumbnail resize should be reclaimed after 30 seconds. A nightly report should
not be reclaimed after 30 minutes. A single global timeout forces the worst case,
which means genuinely dead jobs sit stuck for as long as the slowest legitimate
task might take.

### `SIGTERM` sets a flag rather than raising

The current job finishes and is marked, then the loop exits. A worker that dies
mid-job on every deploy strands work for one full visibility timeout — with
rolling deploys several times a day, that is a lot of mysteriously delayed jobs.

### Unknown tasks die immediately

An unregistered task name will never become registered. Retrying it five times
with exponential backoff accomplishes nothing, so it goes straight to `dead`.
Asserted by `test_unknown_task_dies_without_retry`.

---

## On `SKIP LOCKED`

`SKIP LOCKED` is usually presented as a throughput optimisation. Measured on this
implementation, that turned out not to be the interesting part.

### The benchmark showed nothing

1,000 jobs across 10 worker processes:

| Claim mode | Wall time |
|---|---|
| `FOR UPDATE SKIP LOCKED` | 1.05s |
| `FOR UPDATE` | 1.11s |

No meaningful difference. The reason: each job is a single `INSERT` taking a few
hundred microseconds, and the claim runs in autocommit, so the row lock is held
for a sub-millisecond window. Ten workers rarely collide at all. `SKIP LOCKED`
only pays off in throughput when the lock is held long enough for others to queue
behind it.

### The property that does matter is liveness

`tests/test_skip_locked.py` holds a lock on the head row from a separate
transaction and asserts a worker still processes the remaining 9 of 10 jobs.

- With `SKIP LOCKED`: 9 jobs complete, the locked row stays `pending`.
- With plain `FOR UPDATE`: the worker blocks on its first claim and processes
  zero. It never reaches any other job.

That is what production failure looks like — one hung transaction, one stalled
connection, one long-running query — and it is the case worth protecting against.
`SKIP LOCKED` is a liveness guarantee here, not a speed one.

---

## Delivery guarantees

**At-least-once.** A job can run more than once. If a worker completes a job and
dies before the `UPDATE` marking it succeeded, nobody can know the work happened,
and the reaper will run it again.

This is not fixable. Exactly-once delivery across two systems is not achievable.
Exactly-once *effects* are, and they are the task's responsibility:

```python
@task("charge_card")
def charge_card(order_id: str, idempotency_key: str):
    stripe.PaymentIntent.create(..., idempotency_key=idempotency_key)
```

Write tasks assuming they will run twice, because eventually one will. Use
upserts, natural keys, or the downstream API's idempotency support.

`dedupe_key` covers the producer side — the same key twice yields one job, via a
unique constraint and `ON CONFLICT DO NOTHING`. Note that SQL treats `NULL` as
distinct from `NULL`, so jobs without a key never collide
(`test_null_dedupe_keys_dont_collide`).

---

## Operations

### The reaper

```sql
UPDATE jobs
SET status = CASE WHEN attempts >= max_attempts THEN 'dead'::job_status
                  ELSE 'pending'::job_status END,
    locked_by = NULL, locked_at = NULL,
    last_error = 'reclaimed: worker vanished'
WHERE status = 'running'
  AND locked_at < now() - visibility_timeout;
```

Run every 30 seconds. Idempotent and cheap thanks to `jobs_reclaim_idx`.

`test_reap_ignores_live_workers` guards the other direction — a reaper that is too
aggressive steals jobs from workers still running them, which is worse than having
no reaper at all.

### Useful queries

```sql
-- backlog by task
SELECT task, count(*) FROM jobs WHERE status='pending' GROUP BY task;

-- the metric that actually matters: how far behind are the workers
SELECT now() - min(run_at) FROM jobs WHERE status='pending' AND run_at <= now();

-- what is failing
SELECT task, count(*), max(last_error) FROM jobs
WHERE status='dead' AND created_at > now() - interval '1 hour'
GROUP BY task;

-- requeue everything that died during an outage
UPDATE jobs SET status='pending', attempts=0, run_at=now()
WHERE status='dead' AND task='send_email'
  AND created_at BETWEEN '2026-08-10 09:00' AND '2026-08-10 11:00';
```

That last one is the argument for a SQL-native queue in one statement.

### Housekeeping

```sql
DELETE FROM jobs
WHERE status = 'succeeded' AND created_at < now() - interval '7 days';
```

Keep `dead` rows longer — they are the debugging record.

---

## Tests

```bash
createdb pgq_test
psql pgq_test -f schema.sql
psql pgq_test -f tests/fixtures.sql

DATABASE_URL="postgresql://localhost:5432/pgq_test" pytest tests/ -v
```

23 tests. The ones that carry weight:

| Test | What it proves |
|---|---|
| `test_no_double_execution` | 1,000 jobs, 10 processes, each runs exactly once |
| `test_workers_make_progress_past_a_locked_row` | `SKIP LOCKED` liveness — 9 of 10 jobs complete past a held lock |
| `test_rollback_leaves_no_job` | Rolled-back transaction leaves no job — the headline feature |
| `test_commit_persists_both` | Committed transaction persists data and job together |
| `test_reap_returns_orphan_to_pending` | Reclamation preserves `attempts` |
| `test_reap_ignores_live_workers` | The reaper does not steal live jobs |
| `test_poison_job_dies_after_max_attempts` | Exactly `max_attempts` runs, then `dead`, never claimed again |
| `test_jitter_varies` | Backoff includes a random term |

Correctness is asserted against a side-effect table (`results`), not against
`status = 'succeeded'`. A bug that marked jobs done without executing them would
pass a status check.

`test_no_double_execution` uses `multiprocessing`, which on macOS spawns rather
than forks — each child re-imports modules from scratch, so worker processes must
import the task module themselves to populate the registry.

---

## Limitations

- **Not an event log.** One job, one consumer. No fan-out, no replay.
- **Throughput ceiling** in the low thousands of jobs/sec on a single primary.
  Beyond that, use a real broker.
- **Polling latency.** Workers poll with backoff; `LISTEN`/`NOTIFY` would cut it
  but is not implemented. Notifications fire on commit and are lost if nobody is
  listening, so polling would still be needed as a fallback.
- **No heartbeat.** A task running longer than its `visibility_timeout` will be
  reclaimed and run concurrently with itself. Set the timeout generously for slow
  tasks.
- **No cron.** Schedule with `run_at` or drive it externally.
- **No workflows.** No chains or groups. Enqueue the next job from inside the
  previous one.

---

## Schema

| Column | Type | Purpose |
|---|---|---|
| `id` | `BIGSERIAL` | Primary key |
| `task` | `TEXT` | Registered task name |
| `args` | `JSONB` | Keyword arguments |
| `status` | `job_status` | `pending` / `running` / `succeeded` / `dead` |
| `priority` | `SMALLINT` | Higher claims first |
| `run_at` | `TIMESTAMPTZ` | Earliest execution time; also the retry mechanism |
| `attempts` | `INT` | Incremented at claim |
| `max_attempts` | `INT` | Attempts before `dead` |
| `locked_by` | `TEXT` | `host:pid` of the claiming worker |
| `locked_at` | `TIMESTAMPTZ` | Lease start; drives reclamation |
| `visibility_timeout` | `INTERVAL` | Per-job lease duration |
| `last_error` | `TEXT` | Most recent traceback |
| `dedupe_key` | `TEXT UNIQUE` | Optional idempotent-enqueue key |
| `created_at` | `TIMESTAMPTZ` | Insert time |

---

## License

MIT

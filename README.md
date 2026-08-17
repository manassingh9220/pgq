# pgq

A job queue built on PostgreSQL. No Redis, no RabbitMQ, no broker to operate.

Jobs live in a table in the same database as your application data, which means
**enqueueing a job and writing the data that job depends on happen in one
transaction.** That single property is the reason this exists.

```python
with conn.transaction():
    order = create_order(conn, payload)
    enqueue(conn, "send_confirmation", {"order_id": str(order.id)})
```

If the transaction rolls back, the job was never enqueued. If it commits, the job
is guaranteed to run. There is no window where the order exists but the email
never sends.

---

## Why not Celery

Celery is a better tool for most people. Use it unless one of these applies.

**You already run Postgres and don't want another moving part.** A broker is one
more service to deploy, monitor, secure, back up, and page someone about at 3am.
Below a few thousand jobs per second, Postgres handles the load fine.

**You need transactional enqueue.** This is the real argument. With Celery on
Redis, `create_order()` and `send_email.delay()` touch two different systems with
no shared transaction. Whatever order you put them in, there is a failure window:

```python
order = create_order(data)          # commits
send_email.delay(order.id)          # process dies here → email never sends
```

The standard fix is the transactional outbox pattern — write the event to a table
in the same transaction, then relay it to the broker. But if you're already
writing jobs to a Postgres table, the relay is the only part you don't need.
`pgq` is what the outbox pattern collapses into when you stop pretending the
broker is necessary.

**You want to inspect and manipulate the queue with SQL.** Why is this job stuck.
What failed in the last hour. Requeue everything that hit the Mailgun outage.
These are one-line queries here and awkward tooling problems elsewhere.

### When to use something else

| Situation                                    | Use instead          |
| -------------------------------------------- | -------------------- |
| Sustained throughput above ~5k jobs/sec      | Kafka, SQS, RabbitMQ |
| Multiple services consuming the same events  | Kafka                |
| Events must be replayable after processing   | Kafka                |
| You need fan-out to unknown future consumers | Kafka                |
| Your team already runs and knows Celery      | Celery               |

`pgq` is a task queue: one job, one worker, run it once, retry on failure. It is
not an event log.

---

## Quick start

```bash
psql "$DATABASE_URL" -f schema.sql
pip install -e .
```

Define tasks:

```python
# tasks.py
from pgq import task

@task("send_confirmation")
def send_confirmation(order_id: str):
    order = Order.objects.get(id=order_id)
    mailer.send(order.email, "Your order", render(order))
```

Enqueue:

```python
from pgq import enqueue

enqueue(conn, "send_confirmation", {"order_id": "abc-123"})
enqueue(conn, "nightly_report", {}, run_at=tomorrow_at_2am)
enqueue(conn, "resize_image", {"key": "..."}, priority=10, max_attempts=3)
```

Run workers:

```bash
pgq worker --concurrency 4
pgq reaper                    # or let a worker run it on a timer
```

---

## How it works

A job moves through four states:

```
                  reclaimed after visibility timeout
        ┌──────────────────────────────────────────┐
        │                                          │
        v                                          │
   ┌─────────┐   claim    ┌─────────┐   ok    ┌───────────┐
   │ pending │ ─────────> │ running │ ──────> │ succeeded │
   └─────────┘            └─────────┘         └───────────┘
        ^                      │
        │                      │ attempts exhausted
        │  retry w/ backoff    v
        └──────────────   ┌────────┐
                          │  dead  │
                          └────────┘
```

Two paths lead back to `pending`: a normal failure that still has attempts left,
and reclamation of a job whose worker disappeared without reporting anything.

### Claiming

The whole design hinges on one query:

```sql
UPDATE jobs
SET status     = 'running',
    locked_at  = now(),
    locked_by  = %(worker)s,
    attempts   = attempts + 1,
    updated_at = now()
WHERE id = (
    SELECT id FROM jobs
    WHERE status = 'pending'
      AND run_at <= now()
    ORDER BY priority DESC, run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, task, args, attempts, max_attempts;
```

`SKIP LOCKED` is what makes Postgres usable as a queue. Without it, a second
worker running the same query blocks on the first worker's row lock, waits for it
to commit, re-evaluates, discovers the row is no longer `pending`, and returns
nothing — having spent the entire wait to accomplish nothing. Ten workers
serialize into approximately one. `SKIP LOCKED` tells Postgres to pass over
locked rows and take the next available one, so N workers claim N distinct jobs
without contending.

The subquery exists because `UPDATE` doesn't accept `ORDER BY ... LIMIT`. Select
the id under a lock, then update by primary key.

---

## Design decisions

The parts that are non-obvious, and why they are the way they are.

### `attempts` increments at claim time, not on failure

This looks like a bug and isn't. If you increment when a job fails, a worker that
gets `SIGKILL`ed never records its attempt. The reaper returns the job to
`pending` with an unchanged count, and it retries forever — a poison job that
takes down workers becomes an infinite loop.

Incrementing at claim means every attempt is paid for whether or not the worker
survives long enough to report an outcome. A job with `max_attempts = 5` runs at
most five times no matter how it dies.

### Partial indexes on status

```sql
CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at)
    WHERE status = 'pending';
```

The index contains only claimable rows. Ten million completed jobs in the table
have zero effect on claim performance, because they aren't in the index. A full
index on `(priority, run_at)` would grow without bound and slow every claim as
history accumulates.

### `run_at` handles both delays and retries

There is no separate scheduler and no separate retry table. A delayed job is one
with a future `run_at`. A retry is a job set back to `pending` with `run_at` moved
forward by the backoff interval. The claim query's `run_at <= now()` filter
handles both cases identically.

### Visibility timeout is per-job

A thumbnail resize should be reclaimed after 30 seconds. A nightly report should
not be reclaimed after 30 minutes. A single global timeout forces you to pick the
worst case, which means genuinely dead jobs sit stuck for as long as your slowest
task might legitimately take.

### Retry backoff is jittered

```python
delay = min(2 ** attempts, 3600) * (0.5 + random.random() * 0.5)
```

Without jitter, a downstream outage that fails 1,000 jobs at the same moment
produces 1,000 retries at the same moment, which fail together and retry together
— permanently synchronized, hammering the recovering service in waves. Spreading
retries across a window is what breaks the herd.

### Graceful shutdown flips a flag

`SIGTERM` sets `running = False`; the current job finishes and is marked, then the
loop exits. It does not raise or exit immediately.

If a worker dies mid-job on every deploy, every deploy strands work for one full
visibility timeout. With rolling deploys several times a day, that's a lot of
mysteriously delayed jobs.

### `dedupe_key` is a unique constraint

```sql
INSERT INTO jobs (...) VALUES (...) ON CONFLICT (dedupe_key) DO NOTHING
```

Idempotent enqueue with no application-level checking and no race between "does
it exist" and "insert it". The database enforces it.

---

## Delivery guarantees

**At-least-once.** A job can run more than once. This is not a bug to be fixed;
it's inherent. If a worker completes a job and then dies before the `UPDATE`
marking it succeeded, there is no way for anyone else to know the work happened.
The reaper will run it again.

Exactly-once delivery is not achievable across two systems. Exactly-once
_effects_ are, and they're your responsibility:

```python
@task("charge_card")
def charge_card(order_id: str, idempotency_key: str):
    stripe.PaymentIntent.create(..., idempotency_key=idempotency_key)
```

Make tasks idempotent. Use natural keys, upserts, or the downstream API's
idempotency support. Assume every task will run twice, because eventually one
will.

---

## Operations

### The reaper

```sql
UPDATE jobs
SET status = CASE WHEN attempts >= max_attempts THEN 'dead'::job_status
                  ELSE 'pending'::job_status END,
    locked_at = NULL, locked_by = NULL,
    last_error = 'reclaimed: worker vanished',
    updated_at = now()
WHERE status = 'running'
  AND locked_at < now() - visibility_timeout;
```

Run every 30 seconds. Idempotent and cheap — `jobs_reclaim_idx` keeps it fast.

### Heartbeats for long jobs

A task that legitimately runs longer than its visibility timeout will be reclaimed
and run again concurrently with itself. Extend the lease from a background thread:

```python
UPDATE jobs SET locked_at = now() WHERE id = %s AND locked_by = %s
```

The `locked_by` guard stops a zombie worker from re-extending a lease that has
already been reclaimed and handed to someone else.

### Housekeeping

```sql
DELETE FROM jobs
WHERE status = 'succeeded' AND completed_at < now() - interval '7 days';
```

Keep `dead` rows longer — they're the debugging record. At high volume, partition
by `created_at` and drop partitions instead of deleting rows.

### Useful queries

```sql
-- current backlog by task
SELECT task, count(*) FROM jobs WHERE status='pending' GROUP BY task;

-- oldest unclaimed job (your real latency metric)
SELECT now() - min(run_at) FROM jobs WHERE status='pending' AND run_at <= now();

-- what's failing
SELECT task, count(*), max(last_error) FROM jobs
WHERE status='dead' AND completed_at > now() - interval '1 hour'
GROUP BY task;

-- requeue everything that died during an outage
UPDATE jobs SET status='pending', attempts=0, run_at=now()
WHERE status='dead' AND task='send_email'
  AND completed_at BETWEEN '2026-08-10 09:00' AND '2026-08-10 11:00';
```

That last one is the argument for SQL-native queues in one line.

---

## Reducing latency

Polling adds up to `poll_interval` of latency. `LISTEN`/`NOTIFY` removes it:

```sql
CREATE FUNCTION notify_new_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('jobs', '');
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_notify AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION notify_new_job();
```

**Keep polling as a fallback.** Notifications fire on commit and are lost if
nobody is listening at that instant — a worker reconnecting after a network blip
misses them silently. Poll every 5 seconds as a safety net, with `NOTIFY` as the
fast path.

---

## Testing

The tests that matter are the concurrency and crash-recovery ones.

```bash
pytest tests/ -v
```

| Test                         | What it proves                                                         |
| ---------------------------- | ---------------------------------------------------------------------- |
| `test_no_double_execution`   | 1,000 jobs, 10 workers, each runs exactly once                         |
| `test_reclaim_after_crash`   | `SIGKILL` a worker; job returns to `pending`, attempts preserved       |
| `test_backoff_schedule`      | Retry delays grow exponentially and stay within the jitter band        |
| `test_poison_job_dies`       | A task that always fails reaches `dead` in exactly `max_attempts` runs |
| `test_transactional_enqueue` | Rolled-back transaction leaves no job row                              |
| `test_dedupe`                | Same `dedupe_key` twice yields one job                                 |

`test_no_double_execution` is the important one. Remove `SKIP LOCKED` from the
claim query and watch it fail — that's the demonstration that the design decision
was necessary rather than decorative.

---

## Limitations

- **Not an event log.** One job, one consumer. No fan-out, no replay.
- **Throughput ceiling** around a few thousand jobs/sec on a single Postgres
  primary, depending on hardware and job size. Beyond that, use a real broker.
- **Long transactions hurt.** A slow claim holds a row lock; an open transaction
  elsewhere blocks vacuum. Keep transactions short.
- **No cron.** Schedule with `run_at`, or drive it from an external scheduler.
- **No workflows.** No chains, groups, or chords. Enqueue the next job from
  inside the previous one if you need sequencing.

---

## Schema reference

| Column               | Type          | Purpose                                           |
| -------------------- | ------------- | ------------------------------------------------- |
| `id`                 | `BIGSERIAL`   | Primary key                                       |
| `task`               | `TEXT`        | Registered task name                              |
| `args`               | `JSONB`       | Keyword arguments                                 |
| `status`             | `job_status`  | `pending` / `running` / `succeeded` / `dead`      |
| `priority`           | `SMALLINT`    | Higher runs first                                 |
| `run_at`             | `TIMESTAMPTZ` | Earliest execution time; also the retry mechanism |
| `attempts`           | `INT`         | Incremented at claim                              |
| `max_attempts`       | `INT`         | Attempts before `dead`                            |
| `locked_at`          | `TIMESTAMPTZ` | Lease start; drives reclamation                   |
| `locked_by`          | `TEXT`        | `host:pid` of the claiming worker                 |
| `visibility_timeout` | `INTERVAL`    | Per-job lease duration                            |
| `last_error`         | `TEXT`        | Most recent traceback                             |
| `dedupe_key`         | `TEXT UNIQUE` | Optional idempotent-enqueue key                   |

---

## License

MIT


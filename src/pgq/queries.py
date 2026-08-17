CLAIM = """
UPDATE jobs SET status = 'running', attempts = attempts + 1, locked_at = now(), locked_by = %(worker)s
WHERE id = (
    SELECT id FROM jobs WHERE status = 'pending' AND run_at <= now()
    ORDER BY priority DESC, run_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, task, args, attempts, max_attempts
"""

SUCCEED = """
UPDATE jobs SET status = 'succeeded', locked_by = NULL WHERE id = %(id)s
"""

RETRY = """
UPDATE jobs
SET status = 'pending',
    last_error = %(error)s,
    run_at = now() + (%(delay)s * interval '1 second'),
    locked_by = NULL,
    locked_at = NULL
WHERE id = %(id)s
"""

KILL = """
UPDATE jobs SET status = 'dead', locked_by = NULL, last_error = %(error)s WHERE id = %(id)s
"""

INSERT = """
INSERT INTO jobs (task, args, run_at, priority, max_attempts) VALUES (
%(task)s, %(args)s, COALESCE(%(run_at)s, now()),%(priority)s, %(max_attempts)s
) RETURNING id
"""

REAP = """
UPDATE jobs
SET status = CASE WHEN attempts >= max_attempts THEN 'dead'::job_status
                  ELSE 'pending'::job_status END,
    locked_by = NULL,
    locked_at = NULL,
    last_error = 'reclaimed: worker ' || COALESCE(locked_by, '?') || ' vanished'
WHERE status = 'running'
  AND locked_at < now() - visibility_timeout
RETURNING id
"""
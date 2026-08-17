CREATE TYPE job_status AS ENUM ('pending','running','succeeded','dead');

CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    task TEXT NOT NULL,
    args JSONB NOT NULL DEFAULT '{}',
    status job_status NOT NULL DEFAULT 'pending',
    locked_by TEXT,
    attempts INT NOT NULL DEFAULT 0,
    priority SMALLINT NOT NULL DEFAULT 0,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_claim_idx ON jobs (priority DESC, run_at)
    WHERE status = 'pending';
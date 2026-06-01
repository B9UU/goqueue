CREATE TYPE job_status AS ENUM ('pending', 'running', 'succeeded', 'failed');

CREATE TABLE jobs (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue         TEXT NOT NULL DEFAULT 'default',
    kind          TEXT NOT NULL,
    payload       JSONB NOT NULL DEFAULT '{}',
    status        job_status NOT NULL DEFAULT 'pending',
    priority      INT NOT NULL DEFAULT 0,
    attempts      INT NOT NULL DEFAULT 0,
    max_attempts  INT NOT NULL DEFAULT 3,
    run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_jobs_queue_status_run_at
    ON jobs (queue, status, priority DESC, run_at ASC)
    WHERE status = 'pending';

CREATE TABLE dead_letter_jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID NOT NULL,
    queue       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    payload     JSONB NOT NULL,
    attempts    INT NOT NULL,
    last_error  TEXT,
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

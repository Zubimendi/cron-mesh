-- CronMesh schema. Idempotent so `make migrate` is safe to re-run.

CREATE TABLE IF NOT EXISTS jobs (
    id                          UUID PRIMARY KEY,
    name                        TEXT NOT NULL UNIQUE,
    cron_expr                   TEXT NOT NULL,
    queue_name                  TEXT NOT NULL,
    payload                     JSONB NOT NULL DEFAULT '{}'::jsonb,
    misfire_grace_period_seconds INTEGER NOT NULL DEFAULT 60,
    last_run_at                 TIMESTAMPTZ,
    is_active                   BOOLEAN NOT NULL DEFAULT true,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_runs (
    id                UUID PRIMARY KEY,
    job_id            UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    scheduled_time    TIMESTAMPTZ NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('triggered', 'missed')),
    queueline_job_id  TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, scheduled_time)
);

CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs (is_active) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs (job_id);

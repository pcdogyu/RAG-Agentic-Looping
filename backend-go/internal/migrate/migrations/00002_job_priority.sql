DROP INDEX IF EXISTS ix_go_jobs_claim;
CREATE INDEX ix_go_jobs_claim
    ON go_jobs(queue, status, priority ASC, available_at, created_at);

DROP INDEX IF EXISTS idx_jobs_queue_status_run_at;
DROP TABLE IF EXISTS jobs;
DROP TYPE IF EXISTS job_status;

DROP TABLE IF EXISTS dead_letter_jobs;

-- Removing an enum value requires recreating the type.
-- Ensure no rows have status='cancelled' before running this.
UPDATE jobs SET status = 'failed' WHERE status = 'cancelled';

ALTER TYPE job_status RENAME TO job_status_old;
CREATE TYPE job_status AS ENUM ('pending', 'running', 'succeeded', 'failed');
ALTER TABLE jobs ALTER COLUMN status TYPE job_status USING status::text::job_status;
DROP TYPE job_status_old;

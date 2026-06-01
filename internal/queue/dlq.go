package queue

import (
	"context"
	"database/sql"
	"errors"
)

func (s *PostgresStore) MoveToDLQ(ctx context.Context, job *Job) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
        INSERT INTO dead_letter_jobs (job_id, queue, kind, payload, attempts, last_error)
        VALUES ($1, $2, $3, $4, $5, $6)`,
		job.ID, job.Queue, job.Kind, job.Payload, job.Attempts, job.LastError,
	)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
        UPDATE jobs SET status = 'failed', finished_at = NOW() WHERE id = $1`, job.ID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *PostgresStore) ListDLQ(ctx context.Context, queue string, limit, offset int) ([]*DeadLetterJob, error) {
	if limit == 0 {
		limit = 50
	}
	var jobs []*DeadLetterJob
	err := s.db.SelectContext(ctx, &jobs, `
        SELECT * FROM dead_letter_jobs
        WHERE ($1 = '' OR queue = $1)
        ORDER BY failed_at DESC
        LIMIT $2 OFFSET $3`,
		queue, limit, offset,
	)
	return jobs, err
}

func (s *PostgresStore) RequeueDLQ(ctx context.Context, id string) error {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var dlj DeadLetterJob
	err = tx.QueryRowxContext(ctx, `
        DELETE FROM dead_letter_jobs WHERE id = $1 RETURNING *`, id).StructScan(&dlj)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
        UPDATE jobs SET status = 'pending', attempts = 0, run_at = NOW(), last_error = NULL
        WHERE id = $1`, dlj.JobID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

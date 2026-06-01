package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type Store interface {
	Ping(ctx context.Context) error
	Enqueue(ctx context.Context, p EnqueueParams) (*Job, error)
	ClaimJobs(ctx context.Context, queue string, limit int) ([]*Job, error)
	MarkSucceeded(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, jobErr error, nextRunAt *time.Time) error
	MoveToDLQ(ctx context.Context, job *Job) error
	GetJob(ctx context.Context, id string) (*Job, error)
	ListJobs(ctx context.Context, queue string, status Status, limit, offset int) ([]*Job, error)
	CancelJob(ctx context.Context, id string) error
	ListDLQ(ctx context.Context, queue string, limit, offset int) ([]*DeadLetterJob, error)
	RequeueDLQ(ctx context.Context, id string) error
	GetStats(ctx context.Context) (*Stats, error)
}

type QueueStats struct {
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
}

type Stats struct {
	Queues map[string]QueueStats `json:"queues"`
	DLQ    map[string]int        `json:"dlq"`
}
type DeadLetterJob struct {
	ID        uuid.UUID `db:"id"         json:"id"`
	JobID     uuid.UUID `db:"job_id"     json:"job_id"`
	Queue     string    `db:"queue"      json:"queue"`
	Kind      string    `db:"kind"       json:"kind"`
	Payload   []byte    `db:"payload"    json:"payload"`
	Attempts  int       `db:"attempts"   json:"attempts"`
	LastError *string   `db:"last_error" json:"last_error,omitempty"`
	FailedAt  time.Time `db:"failed_at"  json:"failed_at"`
}

type PostgresStore struct {
	db *sqlx.DB
}

func NewPostgresStore(db *sqlx.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *PostgresStore) Enqueue(ctx context.Context, p EnqueueParams) (*Job, error) {
	payload, err := json.Marshal(p.Payload)
	if err != nil {
		return nil, err
	}
	queue := p.Queue
	if queue == "" {
		queue = "default"
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	runAt := time.Now()
	if p.RunAt != nil {
		runAt = *p.RunAt
	}
	var job Job
	q := `INSERT INTO jobs (queue, kind, payload, priority, max_attempts, run_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING *`
	err = s.db.QueryRowxContext(
		ctx, q, queue,
		p.Kind, payload, p.Priority,
		maxAttempts, runAt).StructScan(&job)
	return &job, err
}
func (s *PostgresStore) ClaimJobs(ctx context.Context, queue string, limit int) ([]*Job, error) {
	var jobs []*Job
	err := s.db.SelectContext(ctx, &jobs, `
		UPDATE jobs
		SET status = 'running', started_at = NOW(), attempts = attempts + 1
		WHERE id IN (
			SELECT id FROM jobs
			WHERE queue = $1
			  AND status = 'pending'
			  AND run_at <= NOW()
			ORDER BY priority DESC, run_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING *`,
		queue, limit,
	)
	return jobs, err
}

func (s *PostgresStore) MarkSucceeded(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = 'succeeded', finished_at = NOW() WHERE id = $1`, id)
	return err
}

func (s *PostgresStore) MarkFailed(ctx context.Context, id string, jobErr error, nextRunAt *time.Time) error {
	errMsg := jobErr.Error()
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'pending', last_error = $2, run_at = $3, finished_at = NULL, started_at = NULL
		WHERE id = $1`,
		id, errMsg, nextRunAt,
	)
	return err
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*Job, error) {
	var job Job
	err := s.db.GetContext(ctx, &job, `SELECT * FROM jobs WHERE id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &job, err
}

func (s *PostgresStore) ListJobs(ctx context.Context, queue string, status Status, limit, offset int) ([]*Job, error) {
	if limit == 0 {
		limit = 50
	}
	var jobs []*Job
	err := s.db.SelectContext(ctx, &jobs, `
		SELECT * FROM jobs
		WHERE ($1 = '' OR queue = $1)
		  AND ($2 = '' OR status = $2::job_status)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`,
		queue, string(status), limit, offset,
	)
	return jobs, err
}

func (s *PostgresStore) CancelJob(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = 'cancelled', finished_at = NOW()
		WHERE id = $1 AND status = 'pending'`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

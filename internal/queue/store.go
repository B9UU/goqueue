package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
)

type Store interface {
	Enqueue(ctx context.Context, p EnqueueParams) (*Job, error)
	ClaimJobs(ctx context.Context, queue string, limit int) ([]*Job, error)
	MarkSucceeded(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, err error) error
	MoveToDLQ(ctx context.Context, job *Job) error
	GetJob(ctx context.Context, id string) (*Job, error)
	ListJobs(ctx context.Context, queue string, status Status, limit, offset int) ([]*Job, error)
	CancelJob(ctx context.Context, id string) error
}

type PostgresStore struct {
	db *sqlx.DB
}

func NewPostgresStore(db *sqlx.DB) *PostgresStore {
	return &PostgresStore{db: db}
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

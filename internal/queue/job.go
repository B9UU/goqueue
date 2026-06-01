package queue

import (
	"github.com/google/uuid"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID          uuid.UUID  `db:"id" json:"id"`
	Queue       string     `db:"queue" json:"queue"`
	Kind        string     `db:"kind" json:"kind"`
	Payload     []byte     `db:"payload" json:"payload"`
	Status      Status     `db:"status" json:"status"`
	Priority    int        `db:"priority" json:"priority"`
	Attempts    int        `db:"attempts" json:"attempts"`
	MaxAttempts int        `db:"max_attempts" json:"max_attempts"`
	RunAt       time.Time  `db:"run_at" json:"run_at"`
	StartedAt   *time.Time `db:"started_at" json:"started_at,omitempty"`
	FinishedAt  *time.Time `db:"finished_at" json:"finished_at,omitempty"`
	LastError   *string    `db:"last_error" json:"last_error,omitempty"`
	CreatedAt   time.Time  `db:"created_at" json:"created_at"`
}

type EnqueueParams struct {
	Queue       string     `json:"queue"`
	Kind        string     `json:"kind"`
	Payload     any        `json:"payload"`
	Priority    int        `json:"priority"`
	MaxAttempts int        `json:"max_attempts"`
	RunAt       *time.Time `json:"run_at"` // nil = now
}

package queue

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// testDB returns a real DB connection, skipping the test if DATABASE_URL is not set.
// It registers cleanup to wipe test data and close the connection after each test.
func testDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set — skipping integration test")
	}
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		db.MustExec(`DELETE FROM dead_letter_jobs`)
		db.MustExec(`DELETE FROM jobs`)
		db.Close()
	})
	return db
}

func enqueueJob(t *testing.T, store *PostgresStore, q, kind string) *Job {
	t.Helper()
	job, err := store.Enqueue(context.Background(), EnqueueParams{Queue: q, Kind: kind, Payload: map[string]string{}})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return job
}

func claimOne(t *testing.T, store *PostgresStore, q string) *Job {
	t.Helper()
	jobs, err := store.ClaimJobs(context.Background(), q, 1)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatal("expected at least one job to claim")
	}
	return jobs[0]
}

func TestPostgresStore_Enqueue(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job, err := store.Enqueue(context.Background(), EnqueueParams{
		Queue:       "test-enqueue",
		Kind:        "email",
		Payload:     map[string]string{"to": "user@example.com"},
		Priority:    5,
		MaxAttempts: 4,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if job.Kind != "email" || job.Queue != "test-enqueue" || job.Priority != 5 || job.MaxAttempts != 4 {
		t.Errorf("unexpected job fields: %+v", job)
	}
	if job.Status != StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}

func TestPostgresStore_Enqueue_DefaultsQueue(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job, err := store.Enqueue(context.Background(), EnqueueParams{Kind: "task"})
	if err != nil {
		t.Fatal(err)
	}
	if job.Queue != "default" {
		t.Errorf("expected queue 'default', got %q", job.Queue)
	}
	if job.MaxAttempts != 3 {
		t.Errorf("expected max_attempts 3, got %d", job.MaxAttempts)
	}
}

func TestPostgresStore_ClaimJobs(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	enqueueJob(t, store, "test-claim", "work")

	jobs, err := store.ClaimJobs(context.Background(), "test-claim", 10)
	if err != nil {
		t.Fatalf("ClaimJobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Status != StatusRunning {
		t.Errorf("expected status running, got %s", jobs[0].Status)
	}
	if jobs[0].Attempts != 1 {
		t.Errorf("expected attempts=1, got %d", jobs[0].Attempts)
	}
}

func TestPostgresStore_ClaimJobs_SkipsLockedRows(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	enqueueJob(t, store, "test-skip-locked", "task")
	claimOne(t, store, "test-skip-locked") // claim it (status = running)

	// Second claim should return nothing (job is already running).
	jobs, err := store.ClaimJobs(context.Background(), "test-skip-locked", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected 0 claimable jobs, got %d", len(jobs))
	}
}

func TestPostgresStore_MarkSucceeded(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job := enqueueJob(t, store, "test-success", "task")
	claimed := claimOne(t, store, "test-success")

	if err := store.MarkSucceeded(context.Background(), claimed.ID.String()); err != nil {
		t.Fatalf("MarkSucceeded: %v", err)
	}
	got, err := store.GetJob(context.Background(), job.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusSucceeded {
		t.Errorf("expected succeeded, got %s", got.Status)
	}
	if got.FinishedAt == nil {
		t.Error("expected finished_at to be set")
	}
}

func TestPostgresStore_MarkFailed_SetsRetry(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job := enqueueJob(t, store, "test-retry", "task")
	claimOne(t, store, "test-retry")

	nextRunAt := time.Now().Add(60 * time.Second)
	if err := store.MarkFailed(context.Background(), job.ID.String(), errors.New("boom"), &nextRunAt); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	got, _ := store.GetJob(context.Background(), job.ID.String())
	if got.Status != StatusPending {
		t.Errorf("expected pending for retry, got %s", got.Status)
	}
	if got.LastError == nil || *got.LastError != "boom" {
		t.Errorf("expected last_error='boom', got %v", got.LastError)
	}
	if got.StartedAt != nil || got.FinishedAt != nil {
		t.Error("expected started_at and finished_at to be cleared on retry")
	}
}

func TestPostgresStore_GetJob_NotFound(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	_, err := store.GetJob(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err == nil {
		t.Error("expected error for unknown job id")
	}
}

func TestPostgresStore_ListJobs(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	for i := 0; i < 3; i++ {
		enqueueJob(t, store, "test-list", "task")
	}

	jobs, err := store.ListJobs(context.Background(), "test-list", StatusPending, 10, 0)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(jobs) < 3 {
		t.Errorf("expected at least 3 jobs, got %d", len(jobs))
	}
}

func TestPostgresStore_ListJobs_FilterByStatus(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	enqueueJob(t, store, "test-list-status", "task")
	enqueueJob(t, store, "test-list-status", "task")
	claimOne(t, store, "test-list-status") // one moves to running

	pending, _ := store.ListJobs(context.Background(), "test-list-status", StatusPending, 10, 0)
	running, _ := store.ListJobs(context.Background(), "test-list-status", StatusRunning, 10, 0)

	if len(pending) != 1 {
		t.Errorf("expected 1 pending, got %d", len(pending))
	}
	if len(running) != 1 {
		t.Errorf("expected 1 running, got %d", len(running))
	}
}

func TestPostgresStore_CancelJob(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job := enqueueJob(t, store, "test-cancel", "task")

	if err := store.CancelJob(context.Background(), job.ID.String()); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	got, _ := store.GetJob(context.Background(), job.ID.String())
	if got.Status != StatusCancelled {
		t.Errorf("expected cancelled after cancel, got %s", got.Status)
	}
}

func TestPostgresStore_MoveToDLQ(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job := enqueueJob(t, store, "test-dlq", "task")
	claimed := claimOne(t, store, "test-dlq")

	if err := store.MoveToDLQ(context.Background(), claimed); err != nil {
		t.Fatalf("MoveToDLQ: %v", err)
	}

	got, _ := store.GetJob(context.Background(), job.ID.String())
	if got.Status != StatusFailed {
		t.Errorf("expected failed status, got %s", got.Status)
	}

	dlq, err := store.ListDLQ(context.Background(), "test-dlq", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 || dlq[0].JobID != job.ID {
		t.Errorf("unexpected DLQ entries: %+v", dlq)
	}
}

func TestPostgresStore_ListDLQ_FiltersByQueue(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	ctx := context.Background()

	enqueueJob(t, store, "test-dlq-filter-a", "task")
	j1 := claimOne(t, store, "test-dlq-filter-a")
	_ = store.MoveToDLQ(ctx, j1)

	enqueueJob(t, store, "test-dlq-filter-b", "task")
	j2 := claimOne(t, store, "test-dlq-filter-b")
	_ = store.MoveToDLQ(ctx, j2)

	dlq, err := store.ListDLQ(ctx, "test-dlq-filter-a", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ entry for queue a, got %d", len(dlq))
	}
	if dlq[0].Queue != "test-dlq-filter-a" {
		t.Errorf("expected queue test-dlq-filter-a, got %s", dlq[0].Queue)
	}
}

func TestPostgresStore_RequeueDLQ(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	job := enqueueJob(t, store, "test-requeue", "task")
	claimed := claimOne(t, store, "test-requeue")
	_ = store.MoveToDLQ(context.Background(), claimed)

	dlq, _ := store.ListDLQ(context.Background(), "test-requeue", 10, 0)
	if len(dlq) == 0 {
		t.Fatal("no DLQ entries found")
	}

	if err := store.RequeueDLQ(context.Background(), dlq[0].ID.String()); err != nil {
		t.Fatalf("RequeueDLQ: %v", err)
	}

	// DLQ entry should be gone.
	dlqAfter, _ := store.ListDLQ(context.Background(), "test-requeue", 10, 0)
	if len(dlqAfter) != 0 {
		t.Errorf("expected DLQ to be empty after requeue, got %d entries", len(dlqAfter))
	}

	// Job should be pending again with attempts reset.
	got, _ := store.GetJob(context.Background(), job.ID.String())
	if got.Status != StatusPending {
		t.Errorf("expected pending after requeue, got %s", got.Status)
	}
	if got.Attempts != 0 {
		t.Errorf("expected attempts reset to 0, got %d", got.Attempts)
	}
}

// TestPostgresStore_Enqueue_FutureRunAt verifies that a job scheduled in the
// future is not picked up until its run_at time arrives.
func TestPostgresStore_Enqueue_FutureRunAt(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	future := time.Now().Add(1 * time.Hour)
	_, err := store.Enqueue(context.Background(), EnqueueParams{
		Queue: "test-future",
		Kind:  "task",
		RunAt: &future,
	})
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := store.ClaimJobs(context.Background(), "test-future", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected no jobs claimable before run_at, got %d", len(jobs))
	}
}

// TestPostgresStore_ClaimJobs_RespectsPriority verifies that when multiple jobs
// are pending, the one with the highest priority is claimed first.
func TestPostgresStore_ClaimJobs_RespectsPriority(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	ctx := context.Background()

	_, err := store.Enqueue(ctx, EnqueueParams{Queue: "test-priority", Kind: "low-pri", Priority: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Enqueue(ctx, EnqueueParams{Queue: "test-priority", Kind: "high-pri", Priority: 10})
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := store.ClaimJobs(ctx, "test-priority", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].Kind != "high-pri" {
		t.Errorf("expected high-priority job to be claimed first, got kind=%q", jobs[0].Kind)
	}
}

// TestPostgresStore_ListJobs_Pagination verifies that limit and offset correctly
// page through results with no duplicates across pages.
func TestPostgresStore_ListJobs_Pagination(t *testing.T) {
	store := NewPostgresStore(testDB(t))
	for i := 0; i < 5; i++ {
		enqueueJob(t, store, "test-pagination", "task")
	}

	page1, err := store.ListJobs(context.Background(), "test-pagination", StatusPending, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	page2, err := store.ListJobs(context.Background(), "test-pagination", StatusPending, 3, 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(page1) != 3 {
		t.Errorf("expected 3 on page 1, got %d", len(page1))
	}
	if len(page2) != 2 {
		t.Errorf("expected 2 on page 2, got %d", len(page2))
	}

	seen := make(map[string]bool)
	for _, j := range append(page1, page2...) {
		if seen[j.ID.String()] {
			t.Errorf("duplicate job %s across pages", j.ID)
		}
		seen[j.ID.String()] = true
	}
}

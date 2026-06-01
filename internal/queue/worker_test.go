package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// mockStore is a test double for Store with optional callbacks for synchronization.
type mockStore struct {
	mu sync.Mutex

	succeededCount int
	failedCount    int
	dlqCount       int
	claimedCount   int

	lastNextRunAt   *time.Time
	claimJobsResult []*Job

	onSucceeded func()
	onFailed    func()
	onDLQ       func()
}

func (m *mockStore) Enqueue(_ context.Context, _ EnqueueParams) (*Job, error) { return nil, nil }
func (m *mockStore) ClaimJobs(_ context.Context, _ string, _ int) ([]*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimedCount++
	return m.claimJobsResult, nil
}
func (m *mockStore) MarkSucceeded(_ context.Context, _ string) error {
	m.mu.Lock()
	m.succeededCount++
	fn := m.onSucceeded
	m.mu.Unlock()
	if fn != nil {
		fn()
	}
	return nil
}
func (m *mockStore) MarkFailed(_ context.Context, _ string, _ error, nextRunAt *time.Time) error {
	m.mu.Lock()
	m.failedCount++
	m.lastNextRunAt = nextRunAt
	fn := m.onFailed
	m.mu.Unlock()
	if fn != nil {
		fn()
	}
	return nil
}
func (m *mockStore) MoveToDLQ(_ context.Context, _ *Job) error {
	m.mu.Lock()
	m.dlqCount++
	fn := m.onDLQ
	m.mu.Unlock()
	if fn != nil {
		fn()
	}
	return nil
}
func (m *mockStore) GetJob(_ context.Context, _ string) (*Job, error)     { return nil, nil }
func (m *mockStore) CancelJob(_ context.Context, _ string) error          { return nil }
func (m *mockStore) RequeueDLQ(_ context.Context, _ string) error         { return nil }
func (m *mockStore) ListJobs(_ context.Context, _ string, _ Status, _, _ int) ([]*Job, error) {
	return nil, nil
}
func (m *mockStore) ListDLQ(_ context.Context, _ string, _, _ int) ([]*DeadLetterJob, error) {
	return nil, nil
}

func (m *mockStore) snapshot() (succeeded, failed, dlq int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.succeededCount, m.failedCount, m.dlqCount
}

func makeJob(kind string, attempts, maxAttempts int) *Job {
	return &Job{ID: uuid.New(), Kind: kind, Attempts: attempts, MaxAttempts: maxAttempts}
}

// --- WorkerPool tests ---

func TestWorkerPool_SuccessfulJob(t *testing.T) {
	t.Parallel()
	store := &mockStore{}
	pool := NewWorkerPool(2, store)
	pool.Register("task", func(_ context.Context, _ *Job) error { return nil })

	pool.execute(context.Background(), makeJob("task", 1, 3))

	succeeded, failed, dlq := store.snapshot()
	if succeeded != 1 || failed != 0 || dlq != 0 {
		t.Errorf("got succeeded=%d failed=%d dlq=%d, want 1 0 0", succeeded, failed, dlq)
	}
}

func TestWorkerPool_NoHandler_MovesToDLQ(t *testing.T) {
	t.Parallel()
	store := &mockStore{}
	pool := NewWorkerPool(2, store)

	pool.execute(context.Background(), makeJob("unknown", 1, 3))

	succeeded, failed, dlq := store.snapshot()
	if succeeded != 0 || failed != 0 || dlq != 1 {
		t.Errorf("got succeeded=%d failed=%d dlq=%d, want 0 0 1", succeeded, failed, dlq)
	}
}

func TestWorkerPool_FailedJob_Retries(t *testing.T) {
	t.Parallel()
	store := &mockStore{}
	pool := NewWorkerPool(2, store)
	pool.Register("flaky", func(_ context.Context, _ *Job) error {
		return errors.New("transient error")
	})

	before := time.Now()
	pool.execute(context.Background(), makeJob("flaky", 1, 3)) // attempts=1 < max=3 → retry

	succeeded, failed, dlq := store.snapshot()
	if succeeded != 0 || failed != 1 || dlq != 0 {
		t.Errorf("got succeeded=%d failed=%d dlq=%d, want 0 1 0", succeeded, failed, dlq)
	}

	if store.lastNextRunAt == nil || !store.lastNextRunAt.After(before) {
		t.Fatal("expected nextRunAt to be set in the future")
	}
	// attempts=1 → backoff = 2^1 * 30s = 60s
	got := store.lastNextRunAt.Sub(before)
	if got < 59*time.Second || got > 61*time.Second {
		t.Errorf("expected ~60s backoff, got %v", got)
	}
}

func TestWorkerPool_FailedJob_ExhaustsRetries_MovesToDLQ(t *testing.T) {
	t.Parallel()
	store := &mockStore{}
	pool := NewWorkerPool(2, store)
	pool.Register("broken", func(_ context.Context, _ *Job) error {
		return errors.New("permanent error")
	})

	pool.execute(context.Background(), makeJob("broken", 3, 3)) // attempts == max → DLQ

	succeeded, failed, dlq := store.snapshot()
	if succeeded != 0 || failed != 0 || dlq != 1 {
		t.Errorf("got succeeded=%d failed=%d dlq=%d, want 0 0 1", succeeded, failed, dlq)
	}
}

func TestWorkerPool_Available(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(5, &mockStore{})

	if got := pool.Available(); got != 5 {
		t.Errorf("expected 5 available, got %d", got)
	}
	pool.sem <- struct{}{} // simulate one running worker
	if got := pool.Available(); got != 4 {
		t.Errorf("expected 4 available, got %d", got)
	}
	<-pool.sem
}

func TestWorkerPool_Submit_RunsAsync(t *testing.T) {
	t.Parallel()
	done := make(chan struct{})
	store := &mockStore{onSucceeded: func() { close(done) }}
	pool := NewWorkerPool(2, store)
	pool.Register("async", func(_ context.Context, _ *Job) error { return nil })

	pool.Submit(makeJob("async", 1, 3))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for job to complete")
	}
}

// TestWorkerPool_ConcurrentSubmit verifies the pool handles many goroutines
// submitting jobs simultaneously without data races or dropped jobs.
func TestWorkerPool_ConcurrentSubmit(t *testing.T) {
	t.Parallel()
	const numJobs = 50
	var successCount atomic.Int64
	store := &mockStore{onSucceeded: func() { successCount.Add(1) }}
	pool := NewWorkerPool(10, store)
	pool.Register("concurrent", func(_ context.Context, _ *Job) error { return nil })

	var wg sync.WaitGroup
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Submit(makeJob("concurrent", 1, 3))
		}()
	}
	wg.Wait()
	pool.Wait()

	if got := int(successCount.Load()); got != numJobs {
		t.Errorf("expected %d successful jobs, got %d", numJobs, got)
	}
}

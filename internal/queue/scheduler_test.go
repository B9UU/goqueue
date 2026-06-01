package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestScheduler_Poll_ClaimsAndSubmitsJobs(t *testing.T) {
	job := &Job{ID: uuid.New(), Kind: "work", MaxAttempts: 3, Attempts: 1}
	store := &mockStore{claimJobsResult: []*Job{job}}
	pool := NewWorkerPool(4, store, nil)
	pool.Register("work", func(_ context.Context, _ *Job) error { return nil })

	sched := NewScheduler(store, pool, "default", time.Second)
	sched.poll(context.Background())
	pool.Wait()

	store.mu.Lock()
	claimed := store.claimedCount
	store.mu.Unlock()

	if claimed != 1 {
		t.Errorf("expected ClaimJobs called once, got %d", claimed)
	}
	succeeded, _, _ := store.snapshot()
	if succeeded != 1 {
		t.Errorf("expected job to be marked succeeded, got %d", succeeded)
	}
}

func TestScheduler_Poll_SkipsWhenNoWorkersAvailable(t *testing.T) {
	store := &mockStore{}
	pool := NewWorkerPool(2, store, nil)
	// Fill all slots to simulate a full pool.
	pool.sem <- struct{}{}
	pool.sem <- struct{}{}

	sched := NewScheduler(store, pool, "default", time.Second)
	sched.poll(context.Background())

	store.mu.Lock()
	claimed := store.claimedCount
	store.mu.Unlock()

	if claimed != 0 {
		t.Errorf("expected ClaimJobs not called, got %d", claimed)
	}
	// Release the artificial slots.
	<-pool.sem
	<-pool.sem
}

func TestScheduler_Run_StopsOnContextCancel(t *testing.T) {
	store := &mockStore{}
	pool := NewWorkerPool(2, store, nil)
	sched := NewScheduler(store, pool, "default", 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		sched.Run(ctx)
		close(stopped)
	}()

	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not stop after context cancellation")
	}
}

func TestScheduler_Run_PollsRepeatedly(t *testing.T) {
	store := &mockStore{}
	pool := NewWorkerPool(4, store, nil)
	sched := NewScheduler(store, pool, "default", 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	sched.Run(ctx)

	store.mu.Lock()
	count := store.claimedCount
	store.mu.Unlock()

	if count < 3 {
		t.Errorf("expected at least 3 polls in 100ms, got %d", count)
	}
}

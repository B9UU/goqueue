package queue

import (
	"context"
	"log/slog"
	"time"
)

type Scheduler struct {
	store    Store
	workers  *WorkerPool
	queue    string
	interval time.Duration
}

func NewScheduler(store Store, workers *WorkerPool, queue string, interval time.Duration) *Scheduler {
	return &Scheduler{store: store, workers: workers, queue: queue, interval: interval}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

func (s *Scheduler) poll(ctx context.Context) {
	batchSize := s.workers.Available()
	if batchSize == 0 {
		return
	}
	jobs, err := s.store.ClaimJobs(ctx, s.queue, batchSize)
	if err != nil {
		slog.Error("scheduler: claim failed", "err", err)
		return
	}
	for _, job := range jobs {
		s.workers.Submit(job)
	}
}

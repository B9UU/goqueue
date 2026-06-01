package queue

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type HandlerFunc func(ctx context.Context, job *Job) error

type WorkerPool struct {
	concurrency int
	sem         chan struct{}
	handlers    map[string]HandlerFunc
	store       Store
	mu          sync.RWMutex
	wg          sync.WaitGroup
}

func NewWorkerPool(concurrency int, store Store) *WorkerPool {
	return &WorkerPool{
		concurrency: concurrency,
		sem:         make(chan struct{}, concurrency),
		handlers:    make(map[string]HandlerFunc),
		store:       store,
	}
}

func (p *WorkerPool) Register(kind string, fn HandlerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[kind] = fn
}
func (p *WorkerPool) Available() int {
	return p.concurrency - len(p.sem)
}

func (p *WorkerPool) Submit(job *Job) {
	p.sem <- struct{}{}
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			p.wg.Done()
		}()
		p.execute(context.Background(), job)
	}()
}

func (p *WorkerPool) Wait() { p.wg.Wait() }

func (p *WorkerPool) execute(ctx context.Context, job *Job) {
	p.mu.RLock()
	handler, ok := p.handlers[job.Kind]
	p.mu.RUnlock()
	if !ok {
		slog.Warn("no handler for job, moving to DLQ", "kind", job.Kind, "id", job.ID)
		if err := p.store.MoveToDLQ(ctx, job); err != nil {
			slog.Error("failed to move job to DLQ", "id", job.ID, "err", err)
		}
		return
	}
	err := handler(ctx, job)
	if err == nil {
		if err := p.store.MarkSucceeded(ctx, job.ID.String()); err != nil {
			slog.Error("failed to mark job succeeded", "id", job.ID, "err", err)
		}
		return
	}
	slog.Warn("job failed", "id", job.ID, "attempt", job.Attempts, "max", job.MaxAttempts, "err", err)
	if job.Attempts >= job.MaxAttempts {
		slog.Warn("job exhausted retries, moving to DLQ", "id", job.ID)
		if err := p.store.MoveToDLQ(ctx, job); err != nil {
			slog.Error("failed to move job to DLQ", "id", job.ID, "err", err)
		}
		return
	}
	backoff := time.Duration(1<<uint(job.Attempts)) * 30 * time.Second
	nextRunAt := time.Now().Add(backoff)
	if err := p.store.MarkFailed(ctx, job.ID.String(), err, &nextRunAt); err != nil {
		slog.Error("failed to mark job failed", "id", job.ID, "err", err)
	}
}

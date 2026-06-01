package queue

import (
	"context"
	"log/slog"
	"sync"
)

type HandlerFunc func(ctx context.Context, job *Job) error

type WorkerPool struct {
	concurrency int
	sem         chan struct{}
	handlers    map[string]HandlerFunc
	store       Store
	mu          sync.RWMutex
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
	go func() {
		defer func() { <-p.sem }()
		p.execute(context.Background(), job)
	}()
}

func (p *WorkerPool) execute(ctx context.Context, job *Job) {
	p.mu.RLock()
	handler, ok := p.handlers[job.Kind]
	p.mu.RUnlock()
	if !ok {
		slog.Warn("No handler for job moving to DLQ", "kind", job.Kind, "id", job.ID)
		_ = p.store.MoveToDLQ(ctx, job)
		return
	}
	err := handler(ctx, job)
	if err == nil {
		_ = p.store.MarkSucceeded(ctx, job.ID.String())
		return
	}
	slog.Warn("Job failed", "id", job.ID, "attempt", job.Attempts, "err", err)
	// TODO: retry logic
	_ = p.store.MarkFailed(ctx, job.ID.String(), err)

}

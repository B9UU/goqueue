package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/b9uu/goqueue/internal/api"
	"github.com/b9uu/goqueue/internal/config"
	"github.com/b9uu/goqueue/internal/queue"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func main() {
	cfg := config.Load()
	slog.Info("connecting to db", "dsn", cfg.DatabseURL)
	db, err := sqlx.Connect("postgres", cfg.DatabseURL)
	if err != nil {
		slog.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	store := queue.NewPostgresStore(db)
	workers := queue.NewWorkerPool(cfg.WorkerConcurrency, store)

	workers.Register("email", func(ctx context.Context, job *queue.Job) error {
		slog.Info("sending email", "payload", string(job.Payload))
		return nil
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	for _, q := range []string{"default"} {
		sched := queue.NewScheduler(store, workers, q, cfg.PollInterval)
		go sched.Run(ctx)
	}

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      api.NewRouter(store),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	}()

	slog.Info("server listening", "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/b9uu/goqueue/internal/api"
	"github.com/b9uu/goqueue/internal/config"
	"github.com/b9uu/goqueue/internal/metrics"
	"github.com/b9uu/goqueue/internal/queue"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	cfg := config.Load()
	if u, err := url.Parse(cfg.DatabaseURL); err == nil {
		slog.Info("connecting to db", "host", u.Host, "dbname", strings.TrimPrefix(u.Path, "/"))
	} else {
		slog.Info("connecting to db")
	}
	db, err := sqlx.Connect("postgres", cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	m := metrics.New(prometheus.DefaultRegisterer)

	store := queue.NewPostgresStore(db)
	workers := queue.NewWorkerPool(cfg.WorkerConcurrency, store, m)

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
		Handler:      api.NewRouter(store, m),
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
	workers.Wait()
	slog.Info("shutdown complete")
}

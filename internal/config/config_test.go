package config

import (
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("PORT", "")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("POLL_INTERVAL", "")

	cfg := Load()

	if cfg.Port != "8080" {
		t.Errorf("expected port 8080, got %q", cfg.Port)
	}
	if cfg.WorkerConcurrency != 10 {
		t.Errorf("expected concurrency 10, got %d", cfg.WorkerConcurrency)
	}
	if cfg.PollInterval != 2*time.Second {
		t.Errorf("expected 2s poll interval, got %v", cfg.PollInterval)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://test:secret@localhost/testdb")
	t.Setenv("PORT", "9090")
	t.Setenv("WORKER_CONCURRENCY", "20")
	t.Setenv("POLL_INTERVAL", "5s")

	cfg := Load()

	if cfg.DatabseURL != "postgres://test:secret@localhost/testdb" {
		t.Errorf("unexpected DATABASE_URL: %q", cfg.DatabseURL)
	}
	if cfg.Port != "9090" {
		t.Errorf("expected port 9090, got %q", cfg.Port)
	}
	if cfg.WorkerConcurrency != 20 {
		t.Errorf("expected concurrency 20, got %d", cfg.WorkerConcurrency)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("expected 5s poll interval, got %v", cfg.PollInterval)
	}
}

func TestLoad_InvalidEnvFallsBackToDefault(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "not-a-number")
	t.Setenv("POLL_INTERVAL", "not-a-duration")

	cfg := Load()

	if cfg.WorkerConcurrency != 10 {
		t.Errorf("expected default concurrency 10, got %d", cfg.WorkerConcurrency)
	}
	if cfg.PollInterval != 2*time.Second {
		t.Errorf("expected default 2s poll interval, got %v", cfg.PollInterval)
	}
}

# goqueue

A persistent job queue backed by PostgreSQL, written in Go. Supports priority scheduling, exponential backoff retries, a dead-letter queue (DLQ), Prometheus metrics, and a REST API for managing jobs.

## Features

- **Priority queues** — jobs are claimed in priority-descending, `run_at`-ascending order
- **Delayed jobs** — schedule a job to run at a future time via `run_at`
- **Automatic retries** — failed jobs are rescheduled with exponential backoff (`2^attempt × 30s`, capped at 24h)
- **Dead-letter queue** — jobs that exhaust all retries are moved to the DLQ and can be requeued via the API
- **Concurrency control** — semaphore-based worker pool with configurable parallelism
- **Graceful shutdown** — stops polling on SIGTERM/SIGINT and drains all in-flight jobs before exiting
- **Crash-safe polling** — uses `SELECT ... FOR UPDATE SKIP LOCKED` so multiple instances never double-claim a job
- **Observability** — Prometheus metrics (`/metrics`) and a human-readable stats endpoint (`/stats`)

## Stack

- **Language:** Go 1.22+ (stdlib `net/http`, `log/slog`)
- **Database:** PostgreSQL 16
- **Libraries:** `sqlx`, `lib/pq`, `google/uuid`, `golang-migrate`, `prometheus/client_golang`

## Getting started

**Prerequisites:** Docker, Go 1.22+, [`migrate` CLI](https://github.com/golang-migrate/migrate)

```bash
# 1. Start Postgres
docker compose up -d

# 2. Run migrations
export DATABASE_URL="postgres://goqueue:secret@localhost:5432/goqueue?sslmode=disable"
make migrate-up

# 3. Run the server
go run ./cmd/server
```

The server listens on `:8080` by default.

## Configuration

| Env var              | Required | Default | Description                          |
|----------------------|----------|---------|--------------------------------------|
| `DATABASE_URL`       | Yes      | —       | PostgreSQL connection string         |
| `PORT`               | No       | `8080`  | HTTP listen port                     |
| `WORKER_CONCURRENCY` | No       | `10`    | Max parallel jobs                    |
| `POLL_INTERVAL`      | No       | `2s`    | How often the scheduler polls the DB |

## API

### Enqueue a job

```
POST /jobs
```

```json
{
  "kind": "email",
  "queue": "default",
  "payload": { "to": "user@example.com" },
  "priority": 5,
  "max_attempts": 3,
  "run_at": "2025-01-01T12:00:00Z"
}
```

`queue` defaults to `"default"`, `max_attempts` defaults to `3`, `run_at` defaults to now. Request body is limited to 1 MB.

### Get a job

```
GET /jobs/{id}
```

### List jobs

```
GET /jobs?queue=default&status=pending&limit=50&offset=0
```

Status values: `pending`, `running`, `succeeded`, `failed`, `cancelled`

### Cancel a job

Only `pending` jobs can be cancelled.

```
DELETE /jobs/{id}
```

Returns `404` if the job doesn't exist or is not in a cancellable state.

### Dead-letter queue

```
GET  /dlq?queue=default&limit=50&offset=0   # list DLQ entries
POST /dlq/{id}/retry                         # requeue a DLQ entry
```

Returns `404` if the DLQ entry doesn't exist.

### Health check

Pings the database. Returns `503` if the database is unreachable.

```
GET /healthz
```

### Stats

Returns job counts by queue and status, and DLQ size per queue. Counts are read inside a single transaction for consistency.

```
GET /stats
```

```json
{
  "queues": {
    "default": {
      "pending": 12,
      "running": 3,
      "succeeded": 840,
      "failed": 2,
      "cancelled": 1
    }
  },
  "dlq": {
    "default": 5
  }
}
```

### Prometheus metrics

```
GET /metrics
```

Exposes the following custom metrics alongside standard Go runtime and process metrics:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `goqueue_jobs_enqueued_total` | Counter | `queue`, `kind` | Total jobs enqueued |
| `goqueue_jobs_succeeded_total` | Counter | `queue`, `kind` | Total jobs completed successfully |
| `goqueue_jobs_failed_total` | Counter | `queue`, `kind` | Total jobs failed and rescheduled |
| `goqueue_jobs_dlq_total` | Counter | `queue`, `kind` | Total jobs moved to the DLQ |
| `goqueue_job_duration_seconds` | Histogram | `queue`, `kind` | Job handler execution time |
| `goqueue_workers_active` | Gauge | — | Workers currently executing a job |

## Running tests

Unit tests (no DB required):

```bash
go test ./internal/queue ./internal/api
```

Integration tests (requires `DATABASE_URL` to be set):

```bash
DATABASE_URL="postgres://goqueue:secret@localhost:5432/goqueue?sslmode=disable" go test ./internal/queue/...
```

## Architecture

```
cmd/server/          # main — wires up config, DB, worker pool, scheduler, HTTP server
internal/
  config/            # env-based config
  metrics/           # Prometheus metric definitions
  queue/
    job.go           # Job type, status enum, EnqueueParams
    store.go         # Store interface + PostgresStore implementation
    dlq.go           # MoveToDLQ, ListDLQ, RequeueDLQ
    stats.go         # GetStats — consistent snapshot of job counts
    worker.go        # WorkerPool — semaphore concurrency, handler dispatch, retry logic
    scheduler.go     # polls DB on a ticker, submits jobs to the worker pool
  api/
    router.go        # registers routes
    handler.go       # HTTP handlers
    middleware.go    # request logging, body size limit, panic recovery
migrations/          # SQL migrations (golang-migrate)
```

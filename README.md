# goqueue

A persistent job queue backed by PostgreSQL, written in Go. Supports priority scheduling, exponential backoff retries, a dead-letter queue (DLQ), and a REST API for managing jobs.

## Features

- **Priority queues** — jobs are claimed in priority-descending, `run_at`-ascending order
- **Delayed jobs** — schedule a job to run at a future time via `run_at`
- **Automatic retries** — failed jobs are rescheduled with exponential backoff (`2^attempt × 30s`)
- **Dead-letter queue** — jobs that exhaust all retries are moved to the DLQ and can be requeued via the API
- **Concurrency control** — semaphore-based worker pool with configurable parallelism
- **Graceful shutdown** — drains in-flight jobs on SIGTERM/SIGINT
- **Crash-safe polling** — uses `SELECT ... FOR UPDATE SKIP LOCKED` so multiple instances never double-claim a job

## Stack

- **Language:** Go 1.22+ (stdlib `net/http`, `log/slog`)
- **Database:** PostgreSQL 16
- **Libraries:** `sqlx`, `lib/pq`, `google/uuid`, `golang-migrate`

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

| Env var              | Default    | Description                        |
|----------------------|------------|------------------------------------|
| `DATABASE_URL`       | (localhost) | PostgreSQL connection string       |
| `PORT`               | `8080`     | HTTP listen port                   |
| `WORKER_CONCURRENCY` | `10`       | Max parallel jobs                  |
| `POLL_INTERVAL`      | `2s`       | How often the scheduler polls the DB |

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

`kind`, `queue`, `priority`, `max_attempts`, and `run_at` are all optional and have sensible defaults.

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

Only pending jobs can be cancelled.

```
DELETE /jobs/{id}
```

### Dead-letter queue

```
GET  /dlq?queue=default&limit=50&offset=0   # list DLQ entries
POST /dlq/{id}/retry                         # requeue a DLQ entry
```

### Health check

```
GET /healthz
```

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
  queue/
    job.go           # Job type, status enum, EnqueueParams
    store.go         # Store interface + PostgresStore implementation
    dlq.go           # MoveToDLQ, ListDLQ, RequeueDLQ
    worker.go        # WorkerPool — semaphore concurrency, handler dispatch, retry logic
    scheduler.go     # polls DB on a ticker, submits jobs to the worker pool
  api/
    router.go        # registers routes
    handler.go       # HTTP handlers
    middleware.go    # request logging (with status code), panic recovery
migrations/          # SQL migrations (golang-migrate)
```

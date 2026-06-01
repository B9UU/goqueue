package api

import (
	"net/http"

	"github.com/b9uu/goqueue/internal/metrics"
	"github.com/b9uu/goqueue/internal/queue"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(store queue.Store, m *metrics.Metrics) http.Handler {
	r := &Router{store: store, metrics: m}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", r.healthz)
	mux.HandleFunc("POST /jobs", r.enqueue)
	mux.HandleFunc("GET /jobs/{id}", r.getJob)
	mux.HandleFunc("GET /jobs", r.listJobs)
	mux.HandleFunc("DELETE /jobs/{id}", r.cancelJob)
	mux.HandleFunc("GET /dlq", r.listDLQ)
	mux.HandleFunc("POST /dlq/{id}/retry", r.retryDLQ)
	mux.HandleFunc("GET /stats", r.stats)
	mux.Handle("GET /metrics", promhttp.Handler())

	return middleware(mux)
}

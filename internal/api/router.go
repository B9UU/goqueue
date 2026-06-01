package api

import (
	"net/http"

	"github.com/b9uu/goqueue/internal/queue"
)

func NewRouter(store queue.Store) http.Handler {
	r := &Router{store: store}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", r.healthz)
	mux.HandleFunc("POST /jobs", r.enqueue)
	mux.HandleFunc("GET /jobs/{id}", r.getJob)
	mux.HandleFunc("GET /jobs", r.listJobs)
	mux.HandleFunc("DELETE /jobs/{id}", r.cancelJob)
	mux.HandleFunc("GET /dlq", r.listDLQ)
	mux.HandleFunc("POST /dlq/{id}/retry", r.retryDLQ)

	return middleware(mux)
}

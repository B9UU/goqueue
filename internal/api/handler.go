package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/b9uu/goqueue/internal/queue"
)

type Router struct {
	store queue.Store
}

func (r *Router) healthz(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) enqueue(w http.ResponseWriter, req *http.Request) {
	var p queue.EnqueueParams
	if err := json.NewDecoder(req.Body).Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body")
		return
	}
	if p.Kind == "" {
		writeError(w, http.StatusBadRequest, "kind is required")
		return
	}
	job, err := r.store.Enqueue(req.Context(), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (r *Router) getJob(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	job, err := r.store.GetJob(req.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (r *Router) listJobs(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	jobs, err := r.store.ListJobs(req.Context(), q.Get("queue"), queue.Status(q.Get("status")), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (r *Router) cancelJob(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	if err := r.store.CancelJob(req.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (r *Router) listDLQ(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	jobs, err := r.store.ListDLQ(req.Context(), q.Get("queue"), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (r *Router) retryDLQ(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	if err := r.store.RequeueDLQ(req.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

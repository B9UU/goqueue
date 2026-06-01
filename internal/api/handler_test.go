package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/b9uu/goqueue/internal/queue"
	"github.com/google/uuid"
)

// apiMockStore implements queue.Store for handler tests.
type apiMockStore struct {
	enqueueResult  *queue.Job
	enqueueErr     error
	getJobResult   *queue.Job
	getJobErr      error
	listJobsResult []*queue.Job
	listJobsErr    error
	cancelJobErr   error
	listDLQResult  []*queue.DeadLetterJob
	listDLQErr     error
	requeueDLQErr  error
}

func (m *apiMockStore) Enqueue(_ context.Context, _ queue.EnqueueParams) (*queue.Job, error) {
	return m.enqueueResult, m.enqueueErr
}
func (m *apiMockStore) ClaimJobs(_ context.Context, _ string, _ int) ([]*queue.Job, error) {
	return nil, nil
}
func (m *apiMockStore) MarkSucceeded(_ context.Context, _ string) error { return nil }
func (m *apiMockStore) MarkFailed(_ context.Context, _ string, _ error, _ *time.Time) error {
	return nil
}
func (m *apiMockStore) MoveToDLQ(_ context.Context, _ *queue.Job) error { return nil }
func (m *apiMockStore) GetJob(_ context.Context, _ string) (*queue.Job, error) {
	return m.getJobResult, m.getJobErr
}
func (m *apiMockStore) ListJobs(_ context.Context, _ string, _ queue.Status, _, _ int) ([]*queue.Job, error) {
	return m.listJobsResult, m.listJobsErr
}
func (m *apiMockStore) CancelJob(_ context.Context, _ string) error { return m.cancelJobErr }
func (m *apiMockStore) ListDLQ(_ context.Context, _ string, _, _ int) ([]*queue.DeadLetterJob, error) {
	return m.listDLQResult, m.listDLQErr
}
func (m *apiMockStore) RequeueDLQ(_ context.Context, _ string) error { return m.requeueDLQErr }

func serve(store queue.Store, method, path string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	NewRouter(store).ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	rec := serve(&apiMockStore{}, "GET", "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestEnqueue_Valid(t *testing.T) {
	id := uuid.New()
	store := &apiMockStore{enqueueResult: &queue.Job{ID: id, Kind: "email"}}
	rec := serve(store, "POST", "/jobs", queue.EnqueueParams{Kind: "email"})

	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", rec.Code)
	}
	var got queue.Job
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.ID != id {
		t.Errorf("expected id %s, got %s", id, got.ID)
	}
}

func TestEnqueue_MissingKind(t *testing.T) {
	rec := serve(&apiMockStore{}, "POST", "/jobs", queue.EnqueueParams{})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestEnqueue_InvalidJSON(t *testing.T) {
	req := httptest.NewRequest("POST", "/jobs", bytes.NewBufferString("not-json"))
	rec := httptest.NewRecorder()
	NewRouter(&apiMockStore{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestEnqueue_StoreError(t *testing.T) {
	store := &apiMockStore{enqueueErr: errors.New("db error")}
	rec := serve(store, "POST", "/jobs", queue.EnqueueParams{Kind: "email"})
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestGetJob_Found(t *testing.T) {
	id := uuid.New()
	store := &apiMockStore{getJobResult: &queue.Job{ID: id, Kind: "email"}}
	rec := serve(store, "GET", "/jobs/"+id.String(), nil)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	store := &apiMockStore{getJobErr: queue.ErrNotFound}
	rec := serve(store, "GET", "/jobs/"+uuid.New().String(), nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestGetJob_DBError(t *testing.T) {
	store := &apiMockStore{getJobErr: errors.New("connection refused")}
	rec := serve(store, "GET", "/jobs/"+uuid.New().String(), nil)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestListJobs(t *testing.T) {
	store := &apiMockStore{
		listJobsResult: []*queue.Job{
			{ID: uuid.New(), Kind: "email", Status: queue.StatusPending},
		},
	}
	rec := serve(store, "GET", "/jobs?queue=default&status=pending", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var jobs []*queue.Job
	if err := json.NewDecoder(rec.Body).Decode(&jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Errorf("expected 1 job, got %d", len(jobs))
	}
}

func TestCancelJob(t *testing.T) {
	rec := serve(&apiMockStore{}, "DELETE", "/jobs/"+uuid.New().String(), nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
}

func TestCancelJob_NotFound(t *testing.T) {
	store := &apiMockStore{cancelJobErr: queue.ErrNotFound}
	rec := serve(store, "DELETE", "/jobs/"+uuid.New().String(), nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", rec.Code)
	}
}

func TestCancelJob_StoreError(t *testing.T) {
	store := &apiMockStore{cancelJobErr: errors.New("db error")}
	rec := serve(store, "DELETE", "/jobs/"+uuid.New().String(), nil)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestListDLQ(t *testing.T) {
	id := uuid.New()
	store := &apiMockStore{listDLQResult: []*queue.DeadLetterJob{{ID: id}}}
	rec := serve(store, "GET", "/dlq", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	var jobs []*queue.DeadLetterJob
	if err := json.NewDecoder(rec.Body).Decode(&jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != id {
		t.Errorf("unexpected DLQ response: %+v", jobs)
	}
}

func TestRetryDLQ(t *testing.T) {
	rec := serve(&apiMockStore{}, "POST", "/dlq/"+uuid.New().String()+"/retry", nil)
	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rec.Code)
	}
}

func TestRetryDLQ_StoreError(t *testing.T) {
	store := &apiMockStore{requeueDLQErr: errors.New("db error")}
	rec := serve(store, "POST", "/dlq/"+uuid.New().String()+"/retry", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

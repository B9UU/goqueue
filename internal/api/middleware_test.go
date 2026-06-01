package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecovery_CatchesPanic(t *testing.T) {
	t.Parallel()
	panicking := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went wrong")
	})
	rec := httptest.NewRecorder()
	recovery(panicking).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON error body: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected non-empty error field in body")
	}
}

func TestRecovery_PassesThroughNormal(t *testing.T) {
	t.Parallel()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	recovery(handler).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestLogging_PassesThrough(t *testing.T) {
	t.Parallel()
	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusCreated)
	})
	rec := httptest.NewRecorder()
	logging(handler).ServeHTTP(rec, httptest.NewRequest("POST", "/jobs", nil))

	if !called {
		t.Error("expected inner handler to be called")
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("expected 201 to pass through, got %d", rec.Code)
	}
}

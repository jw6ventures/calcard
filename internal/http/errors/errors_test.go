package errors

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInternalErrorWritesGenericMessage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	InternalError(rec, req, errors.New("boom"), "db failure")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if body := rec.Body.String(); body != "internal server error\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestBadRequestErrorWritesClientMessage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	BadRequestError(rec, req, errors.New("bad"), "bad input")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
	if body := rec.Body.String(); body != "bad input\n" {
		t.Fatalf("body = %q", body)
	}
}

func TestLogHelpersDoNotPanic(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	LogError(req, "message", errors.New("boom"))
	LogInfo(req, "message")
}

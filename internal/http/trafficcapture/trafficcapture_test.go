package trafficcapture

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestMiddlewareCapturesRequestAndRestoresBody(t *testing.T) {
	var output bytes.Buffer
	var handledBody string
	handler := Middleware(Options{
		Writer:       &output,
		MaxBodyBytes: 4,
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("handler body read error = %v", err)
		}
		handledBody = string(body)
		w.WriteHeader(http.StatusMultiStatus)
	}))

	req := httptest.NewRequest("REPORT", "https://calcard.example/dav/calendars/1/?token=secret&view=sync", strings.NewReader("abcdefgh"))
	req.RemoteAddr = "192.0.2.10:12345"
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Add("X-Test", "one")
	req.Header.Add("X-Test", "two")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if handledBody != "abcdefgh" {
		t.Fatalf("handler body = %q, want complete restored body", handledBody)
	}

	var captured Record
	if err := json.Unmarshal(output.Bytes(), &captured); err != nil {
		t.Fatalf("captured JSON = %q: %v", output.String(), err)
	}
	if captured.Method != "REPORT" {
		t.Fatalf("method = %q, want REPORT", captured.Method)
	}
	if captured.Path != "/dav/calendars/1/?token=%5BREDACTED%5D&view=sync" {
		t.Fatalf("path = %q", captured.Path)
	}
	if captured.Body != "abcd" || !captured.BodyTruncated {
		t.Fatalf("body = %q, truncated = %v", captured.Body, captured.BodyTruncated)
	}
	if captured.Headers["Authorization"] != "Basic ${CALCARD_DAV_BASIC_AUTH}" {
		t.Fatalf("Authorization = %q", captured.Headers["Authorization"])
	}
	if captured.Headers["Cookie"] != "[REDACTED]" {
		t.Fatalf("Cookie = %q", captured.Headers["Cookie"])
	}
	if captured.Headers["Content-Type"] != "application/xml" {
		t.Fatalf("Content-Type = %q", captured.Headers["Content-Type"])
	}
	if captured.Headers["X-Test"] != "one, two" {
		t.Fatalf("X-Test = %q", captured.Headers["X-Test"])
	}
	if captured.IP != "192.0.2.10" {
		t.Fatalf("IP = %q", captured.IP)
	}
	if captured.ExpectStatus != http.StatusMultiStatus {
		t.Fatalf("expect_status = %d", captured.ExpectStatus)
	}
	if captured.OffsetMS < 0 {
		t.Fatalf("offset_ms = %d", captured.OffsetMS)
	}
}

func TestMiddlewareSerializesConcurrentRequestsAsJSONLines(t *testing.T) {
	var output bytes.Buffer
	handler := Middleware(Options{Writer: &output})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	const requestCount = 25
	var wg sync.WaitGroup
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/dav/", nil)
			handler.ServeHTTP(httptest.NewRecorder(), req)
		}()
	}
	wg.Wait()

	scanner := bufio.NewScanner(bytes.NewReader(output.Bytes()))
	lines := 0
	for scanner.Scan() {
		lines++
		var captured Record
		if err := json.Unmarshal(scanner.Bytes(), &captured); err != nil {
			t.Fatalf("line %d is invalid JSON: %q: %v", lines, scanner.Text(), err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan capture: %v", err)
	}
	if lines != requestCount {
		t.Fatalf("captured lines = %d, want %d", lines, requestCount)
	}
}

func TestMiddlewareReportsCaptureWriteErrorsWithoutFailingRequest(t *testing.T) {
	writeErr := errors.New("disk full")
	var reported error
	handler := Middleware(Options{
		Writer: errorWriter{err: writeErr},
		OnError: func(err error) {
			reported = err
		},
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/dav/", nil))

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if !errors.Is(reported, writeErr) {
		t.Fatalf("reported error = %v, want %v", reported, writeErr)
	}
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

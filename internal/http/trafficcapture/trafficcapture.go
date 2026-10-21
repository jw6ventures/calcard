package trafficcapture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// DefaultMaxBodyBytes is the maximum request body prefix captured by default.
const DefaultMaxBodyBytes int64 = 10 * 1024 * 1024

// Options configures request traffic capture.
type Options struct {
	Writer       io.Writer
	MaxBodyBytes int64
	OnError      func(error)
}

// Record is one captured request in the replay-compatible JSONL format.
type Record struct {
	OffsetMS      int64             `json:"offset_ms"`
	Method        string            `json:"method"`
	Path          string            `json:"path"`
	Headers       map[string]string `json:"headers,omitempty"`
	Body          string            `json:"body,omitempty"`
	BodyTruncated bool              `json:"body_truncated,omitempty"`
	BodyReadError string            `json:"body_read_error,omitempty"`
	IP            string            `json:"ip,omitempty"`
	ExpectStatus  int               `json:"expect_status"`
}

type recorder struct {
	mu           sync.Mutex
	encoder      *json.Encoder
	maxBodyBytes int64
	onError      func(error)
	startedAt    time.Time
}

// Middleware captures requests when Writer is non-nil.
func Middleware(opts Options) func(http.Handler) http.Handler {
	if opts.Writer == nil {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	maxBodyBytes := opts.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = DefaultMaxBodyBytes
	}
	encoder := json.NewEncoder(opts.Writer)
	encoder.SetEscapeHTML(false)
	capture := &recorder{
		encoder:      encoder,
		maxBodyBytes: maxBodyBytes,
		onError:      opts.OnError,
		startedAt:    time.Now(),
	}

	return capture.middleware
}

func (c *recorder) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := time.Since(c.startedAt).Milliseconds()
		body, truncated, readErr := captureBody(r, c.maxBodyBytes)
		record := Record{
			OffsetMS:      offset,
			Method:        r.Method,
			Path:          sanitizedRequestPath(r.URL),
			Headers:       sanitizedHeaders(r.Header),
			Body:          body,
			BodyTruncated: truncated,
			IP:            remoteIP(r.RemoteAddr),
		}
		if readErr != nil {
			record.BodyReadError = readErr.Error()
		}

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		record.ExpectStatus = ww.Status()
		if record.ExpectStatus == 0 {
			record.ExpectStatus = http.StatusOK
		}

		c.mu.Lock()
		err := c.encoder.Encode(record)
		c.mu.Unlock()
		if err != nil && c.onError != nil {
			c.onError(fmt.Errorf("write traffic capture: %w", err))
		}
	})
}

func captureBody(r *http.Request, maxBytes int64) (string, bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return "", false, nil
	}

	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	original := r.Body
	captured, err := io.ReadAll(io.LimitReader(original, limit))
	r.Body = &replayReadCloser{
		Reader: io.MultiReader(bytes.NewReader(captured), original),
		Closer: original,
	}

	if int64(len(captured)) > maxBytes {
		return string(captured[:maxBytes]), true, err
	}
	return string(captured), false, err
}

type replayReadCloser struct {
	io.Reader
	io.Closer
}

func sanitizedHeaders(headers http.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}

	result := make(map[string]string, len(headers))
	for name, values := range headers {
		value := strings.Join(values, ", ")
		switch strings.ToLower(name) {
		case "authorization":
			value = sanitizedAuthorization(value)
		case "cookie", "proxy-authorization", "x-api-key", "x-auth-token", "x-csrf-token":
			value = "[REDACTED]"
		}
		result[name] = value
	}
	return result
}

func sanitizedAuthorization(value string) string {
	fields := strings.Fields(value)
	if len(fields) > 0 && strings.EqualFold(fields[0], "Basic") {
		return "Basic ${CALCARD_DAV_BASIC_AUTH}"
	}
	return "[REDACTED]"
}

func sanitizedRequestPath(requestURL *url.URL) string {
	if requestURL == nil {
		return ""
	}

	requestPath := requestURL.EscapedPath()
	if requestPath == "" {
		requestPath = "/"
	}
	if requestURL.RawQuery == "" {
		return requestPath
	}

	query := requestURL.Query()
	redacted := false
	for name, values := range query {
		if !sensitiveQueryParameter(name) {
			continue
		}
		for i := range values {
			values[i] = "[REDACTED]"
		}
		query[name] = values
		redacted = true
	}
	if !redacted {
		return requestPath + "?" + requestURL.RawQuery
	}
	return requestPath + "?" + query.Encode()
}

func sensitiveQueryParameter(name string) bool {
	switch strings.ToLower(name) {
	case "access_token", "api_key", "apikey", "auth", "authorization", "client_secret", "code",
		"password", "refresh_token", "secret", "session", "session_id", "token":
		return true
	default:
		return false
	}
}

func remoteIP(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		return host
	}
	return strings.Trim(remoteAddr, "[]")
}

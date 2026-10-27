package httpserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestReadinessProbeRunsOneCheckWhileTheDatabaseHangs(t *testing.T) {
	const timeout = 50 * time.Millisecond
	var calls atomic.Int32
	release := make(chan struct{})
	probe := &readinessProbe{
		timeout: timeout,
		check: func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				<-release
			}
			return nil
		},
	}

	expectUnready := func() {
		start := time.Now()
		if err := probe.ready(context.Background()); err == nil {
			t.Error("probe of a hung database reported ready")
		}
		if elapsed := time.Since(start); elapsed > timeout+250*time.Millisecond {
			t.Errorf("probe answered after %s, want within its %s budget", elapsed, timeout)
		}
	}
	for range 3 {
		expectUnready()
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(expectUnready)
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("%d checks ran while the first was hung, want 1", got)
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for {
		if err := probe.ready(context.Background()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("probe never reported ready once the database answered")
		}
	}
	if err := probe.ready(context.Background()); err != nil {
		t.Fatalf("ready() = %v after the database answered", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want a fresh check once the hung one returned", got)
	}
}

func TestReadinessProbeReportsTheCheckResult(t *testing.T) {
	refused := errors.New("refused")
	for _, want := range []error{nil, refused} {
		probe := &readinessProbe{timeout: time.Second, check: func(context.Context) error { return want }}
		if err := probe.ready(context.Background()); !errors.Is(err, want) {
			t.Fatalf("ready() = %v, want %v", err, want)
		}
	}
}

// silentListener accepts TCP connections and never writes to them, as the
// kernel of a SIGSTOPped PostgreSQL does.
func silentListener(t *testing.T) (addr string, accepted *atomic.Int32) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted = &atomic.Int32{}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go func() { _, _ = io.Copy(io.Discard, conn) }()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return ln.Addr().String(), accepted
}

func readyzRouter(t *testing.T, dsn string) http.Handler {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewRouter(&config.Config{BaseURL: "http://localhost:8080"}, store.New(db), nil)
}

func getReadyz(t *testing.T, r http.Handler) (int, time.Duration) {
	t.Helper()
	rec := httptest.NewRecorder()
	start := time.Now()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	return rec.Code, time.Since(start)
}

func TestReadyzAnswersUnreadyWithinItsBudgetWhenTheDatabaseHangs(t *testing.T) {
	addr, accepted := silentListener(t)
	r := readyzRouter(t, fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable", addr))

	for range 2 {
		code, elapsed := getReadyz(t, r)
		if code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503", code)
		}
		if elapsed > readinessTimeout+300*time.Millisecond {
			t.Fatalf("/readyz answered after %s, want within %s", elapsed, readinessTimeout)
		}
	}
	if got := accepted.Load(); got != 1 {
		t.Fatalf("hung database received %d connections, want the 1 the still-blocked check holds", got)
	}
}

func TestReadyzAnswersUnreadyPromptlyWhenTheDatabaseRefuses(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	r := readyzRouter(t, fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable", addr))

	code, elapsed := getReadyz(t, r)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", code)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("/readyz answered a refused connection after %s", elapsed)
	}
}

func TestRequestDeadlineBoundsHandlersByTheWriteTimeout(t *testing.T) {
	const writeTimeout = 15 * time.Second
	var deadline time.Time
	var ok bool
	handler := requestDeadline(writeTimeout)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadline, ok = r.Context().Deadline()
	}))
	start := time.Now()
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if !ok {
		t.Fatal("request context has no deadline")
	}
	if deadline.Before(start.Add(writeTimeout)) || deadline.After(time.Now().Add(writeTimeout)) {
		t.Fatalf("deadline is %s after the request started, want the %s write timeout", deadline.Sub(start), writeTimeout)
	}

	ok = true
	requestDeadline(0)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, ok = r.Context().Deadline()
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if ok {
		t.Fatal("a zero write timeout set a request deadline")
	}
}

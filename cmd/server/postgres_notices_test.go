package main

import (
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"

	jw6_utils "github.com/jw6ventures/jw6-go-utils"
	"github.com/lib/pq"
)

type recordedLog struct {
	level   jw6_utils.LogLevel
	message string
}

type recordingLogger struct {
	mu      sync.Mutex
	entries []recordedLog
}

func (l *recordingLogger) Log(_ string, _ string, level jw6_utils.LogLevel, message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, recordedLog{level: level, message: message})
}

// The upgrade reports what its repairs changed and left as WARNINGs, which
// PostgreSQL sends to the session that runs it. The server's connections pass
// every notice on to its own log, so an operator reads them there, beside the
// startup they belong to, rather than only in PostgreSQL's log.
func TestPostgresNoticesReachTheServerLog(t *testing.T) {
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the PostgreSQL notice test")
	}
	logger := &recordingLogger{}
	relayPostgresNotices(logger)
	t.Cleanup(func() { relayPostgresNotices(nil) })

	db, err := sql.Open(postgresDriverName, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`DO $$ BEGIN
    RAISE WARNING 'repair: rewrote % events', 3 USING DETAIL = 'events.id: 1, 2, 3', HINT = 'see the docs';
    RAISE NOTICE 'nothing to do';
END $$`); err != nil {
		t.Fatalf("raise: %v", err)
	}

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.entries) != 2 {
		t.Fatalf("logged %#v, want the warning and the notice", logger.entries)
	}
	warning, notice := logger.entries[0], logger.entries[1]
	if warning.level != jw6_utils.Warn || !strings.Contains(warning.message, "repair: rewrote 3 events") ||
		!strings.Contains(warning.message, "events.id: 1, 2, 3") || !strings.Contains(warning.message, "see the docs") {
		t.Errorf("warning logged as %#v", warning)
	}
	if notice.level != jw6_utils.Debug || !strings.Contains(notice.message, "nothing to do") {
		t.Errorf("notice logged as %#v", notice)
	}
}

// PostgreSQL words a notice's severity in the server's lc_messages, so the
// level is read from its SQLSTATE: class 01 is a warning in every language.
func TestPostgresNoticeLevelFollowsSQLState(t *testing.T) {
	logger := &recordingLogger{}
	relayPostgresNotices(logger)
	t.Cleanup(func() { relayPostgresNotices(nil) })

	logPostgresNotice(&pq.Error{Severity: "WARNUNG", Code: "01000", Message: "Reparatur"})
	logPostgresNotice(&pq.Error{Severity: "HINWEIS", Code: "00000", Message: "nichts zu tun"})

	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.entries) != 2 || logger.entries[0].level != jw6_utils.Warn || logger.entries[1].level != jw6_utils.Debug {
		t.Errorf("logged %#v, want the warning at Warn and the notice at Debug", logger.entries)
	}
}

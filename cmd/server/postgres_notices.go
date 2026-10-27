package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync/atomic"

	jw6_utils "github.com/jw6ventures/jw6-go-utils"
	"github.com/lib/pq"
)

// postgresDriverName is lib/pq with every notice the server sends passed on to
// CalCard's log, and with connections cut when the server stops answering (see
// postgresConnector). The v1.2.0 upgrade reports what its repairs changed and
// left as WARNINGs, which PostgreSQL otherwise records only in its own log.
const postgresDriverName = "calcard-postgres"

type noticeLogger interface {
	Log(class string, method string, level jw6_utils.LogLevel, message string)
}

type noticeLoggerHolder struct{ logger noticeLogger }

var noticeSink atomic.Pointer[noticeLoggerHolder]

func init() {
	sql.Register(postgresDriverName, noticeRelayDriver{})
}

// relayPostgresNotices sets the logger notices are written to; nil discards
// them.
func relayPostgresNotices(logger noticeLogger) {
	noticeSink.Store(&noticeLoggerHolder{logger: logger})
}

type noticeRelayDriver struct{}

func (d noticeRelayDriver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (noticeRelayDriver) OpenConnector(name string) (driver.Connector, error) {
	cfg, err := pq.NewConfig(name)
	if err != nil {
		return nil, err
	}
	return postgresConnector{cfg: cfg}, nil
}

// logPostgresNotice writes a warning at Warn and anything milder at Debug: a
// NOTICE is what PostgreSQL sends for every CREATE ... IF NOT EXISTS that
// finds its object, on every start. A warning is a notice of SQLSTATE class
// 01, which RAISE WARNING sends; the severity's own text follows the server's
// lc_messages.
func logPostgresNotice(notice *pq.Error) {
	holder := noticeSink.Load()
	if holder == nil || holder.logger == nil {
		return
	}
	level := jw6_utils.Debug
	if notice.Code.Class() == "01" {
		level = jw6_utils.Warn
	}
	var message strings.Builder
	fmt.Fprintf(&message, "PostgreSQL %s: %s", notice.Severity, notice.Message)
	if notice.Detail != "" {
		fmt.Fprintf(&message, "; DETAIL: %s", notice.Detail)
	}
	if notice.Hint != "" {
		fmt.Fprintf(&message, "; HINT: %s", notice.Hint)
	}
	holder.logger.Log("Database", "notice", level, message.String())
}

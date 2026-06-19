// Package logging provides a thin, leveled wrapper around the jw6-go-utils
// logger so packages can emit Trace/Debug/Info/Warn/Error logs with printf-style
// formatting while sharing a single underlying sink and log-level configuration.
package logging

import (
	"fmt"

	jw6_utils "github.com/jw6ventures/jw6-go-utils"
)

// Sink is the minimal logging surface needed by this package. It is satisfied by
// *jw6_utils.Utils, which already filters messages by the configured log level.
type Sink interface {
	Log(class string, method string, level jw6_utils.LogLevel, message string)
}

// Logger emits leveled, printf-style log lines scoped to a fixed class (the
// package or component name). A nil *Logger, or one with a nil sink, is a no-op,
// so callers never need to guard logging calls.
type Logger struct {
	sink  Sink
	class string
}

// New returns a Logger that forwards to sink, tagging every line with class.
// When sink is nil the returned Logger silently discards all output.
func New(sink Sink, class string) *Logger {
	return &Logger{sink: sink, class: class}
}

// WithClass returns a copy of the Logger scoped to a different class while
// sharing the same underlying sink.
func (l *Logger) WithClass(class string) *Logger {
	if l == nil {
		return nil
	}
	return &Logger{sink: l.sink, class: class}
}

func (l *Logger) log(method string, level jw6_utils.LogLevel, format string, args ...any) {
	if l == nil || l.sink == nil {
		return
	}
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	l.sink.Log(l.class, method, level, msg)
}

// Trace logs verbose, high-volume diagnostic detail (e.g. request entry/exit).
func (l *Logger) Trace(method, format string, args ...any) {
	l.log(method, jw6_utils.Trace, format, args...)
}

// Debug logs information useful while developing or troubleshooting.
func (l *Logger) Debug(method, format string, args ...any) {
	l.log(method, jw6_utils.Debug, format, args...)
}

// Info logs notable, expected events worth surfacing in normal operation.
func (l *Logger) Info(method, format string, args ...any) {
	l.log(method, jw6_utils.Info, format, args...)
}

// Warn logs recoverable problems or unexpected-but-handled conditions.
func (l *Logger) Warn(method, format string, args ...any) {
	l.log(method, jw6_utils.Warn, format, args...)
}

// Error logs failures that prevented an operation from completing.
func (l *Logger) Error(method, format string, args ...any) {
	l.log(method, jw6_utils.Error, format, args...)
}

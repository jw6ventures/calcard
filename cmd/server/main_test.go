package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

func TestOpenTrafficCaptureFileAppendsAndRestrictsPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic.jsonl")
	if err := os.WriteFile(path, []byte("existing\n"), 0o644); err != nil {
		t.Fatalf("seed capture file: %v", err)
	}

	file, err := openTrafficCaptureFile(path)
	if err != nil {
		t.Fatalf("openTrafficCaptureFile() error = %v", err)
	}
	if _, err := file.WriteString("new\n"); err != nil {
		t.Fatalf("write capture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close capture: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	if string(body) != "existing\nnew\n" {
		t.Fatalf("capture contents = %q", body)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat capture: %v", err)
	}
	if permissions := info.Mode().Perm(); permissions != 0o600 {
		t.Fatalf("capture permissions = %o, want 600", permissions)
	}
}

// The server embeds Go's time zone database, so every zone it resolves -- the
// backward-compatible names clients still write, such as US/Eastern, among
// them -- resolves on any host, and every reader of a calendar object reads
// such a TZID the same way, through ical.HostLocation.
func TestServerResolvesBackwardZoneNamesOnAnyHost(t *testing.T) {
	for alias, zone := range map[string]string{
		"US/Eastern": "America/New_York", "US/Central": "America/Chicago",
		"US/Mountain": "America/Denver", "US/Pacific": "America/Los_Angeles",
	} {
		aliasLoc, ok := ical.HostLocation(alias)
		if !ok {
			t.Fatalf("%s does not resolve", alias)
		}
		zoneLoc, _ := ical.HostLocation(zone)
		for _, at := range []time.Time{time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC), time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)} {
			_, aliasOffset := at.In(aliasLoc).Zone()
			_, zoneOffset := at.In(zoneLoc).Zone()
			if aliasOffset != zoneOffset {
				t.Errorf("%s keeps offset %d at %v, %s keeps %d", alias, aliasOffset, at, zone, zoneOffset)
			}
		}
	}
}

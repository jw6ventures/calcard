package main

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"io"
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
	database := embeddedZoneDatabase(t)
	for alias, zone := range backwardZoneNames {
		assertSameOffsets(t, alias, embeddedLocation(t, database, alias), zone, embeddedLocation(t, database, zone))

		aliasLoc, ok := ical.HostLocation(alias)
		if !ok {
			t.Fatalf("%s does not resolve", alias)
		}
		zoneLoc, _ := ical.HostLocation(zone)
		assertSameOffsets(t, alias, aliasLoc, zone, zoneLoc)
	}
}

var backwardZoneNames = map[string]string{
	"US/Eastern": "America/New_York", "US/Central": "America/Chicago",
	"US/Mountain": "America/Denver", "US/Pacific": "America/Los_Angeles",
}

// embeddedZoneDatabase is the time zone database time/tzdata embeds in this
// test binary, which links the server package's imports. time.LoadLocation
// reads a host's zoneinfo files before the embedded copy, so on a host that
// has them the copy never answers a lookup; the copy is instead read from the
// binary, where time/tzdata stores it as one zip archive.
func embeddedZoneDatabase(t *testing.T) *zip.Reader {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	image, err := os.ReadFile(executable)
	if err != nil {
		t.Fatalf("read test binary: %v", err)
	}
	const endRecordLength = 22
	endSignature := []byte("PK\x05\x06")
	for end := bytes.LastIndex(image, endSignature); end >= 0; end = bytes.LastIndex(image[:end], endSignature) {
		if end+endRecordLength > len(image) {
			continue
		}
		directorySize := int(binary.LittleEndian.Uint32(image[end+12:]))
		directoryOffset := int(binary.LittleEndian.Uint32(image[end+16:]))
		start := end - directorySize - directoryOffset
		if start < 0 {
			continue
		}
		archive := image[start : end+endRecordLength]
		database, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
		if err != nil {
			continue
		}
		if _, err := database.Open("America/New_York"); err == nil {
			return database
		}
	}
	t.Fatal("the binary carries no time zone database; cmd/server must import time/tzdata")
	return nil
}

func embeddedLocation(t *testing.T, database *zip.Reader, name string) *time.Location {
	t.Helper()
	file, err := database.Open(name)
	if err != nil {
		t.Fatalf("the embedded database does not hold %s: %v", name, err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("read embedded %s: %v", name, err)
	}
	loc, err := time.LoadLocationFromTZData(name, data)
	if err != nil {
		t.Fatalf("load embedded %s: %v", name, err)
	}
	return loc
}

func assertSameOffsets(t *testing.T, alias string, aliasLoc *time.Location, zone string, zoneLoc *time.Location) {
	t.Helper()
	for _, at := range []time.Time{time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC), time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)} {
		_, aliasOffset := at.In(aliasLoc).Zone()
		_, zoneOffset := at.In(zoneLoc).Zone()
		if aliasOffset != zoneOffset {
			t.Errorf("%s keeps offset %d at %v, %s keeps %d", alias, aliasOffset, at, zone, zoneOffset)
		}
	}
}

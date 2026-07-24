package main

import (
	"os"
	"path/filepath"
	"testing"
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

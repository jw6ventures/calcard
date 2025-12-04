package migrations

import "testing"

func TestMigrationsEmbedded(t *testing.T) {
	data, err := Files.ReadFile("001_init.sql")
	if err != nil {
		t.Fatalf("expected embedded migration, got error: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("embedded migration is empty")
	}
}

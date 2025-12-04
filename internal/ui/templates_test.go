package ui

import "testing"

func TestTemplatesEmbedded(t *testing.T) {
	names := []string{
		"base.html",
		"dashboard.html",
	}
	for _, name := range names {
		if _, err := templateFS.Open("templates/" + name); err != nil {
			t.Fatalf("expected embedded template %s, got error: %v", name, err)
		}
	}
}

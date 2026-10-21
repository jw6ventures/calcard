package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

// A client that prefers Digest picks it from the challenge and may not fall
// back to Basic, so a Basic only password may be unusable from it. The badge
// has to say so, and say what to do, rather than promise the password keeps
// working.
func TestAppPasswordsBasicOnlyBadgeWarnsDigestClientsCannotUseIt(t *testing.T) {
	cfg := &config.Config{BaseURL: "https://calcard.example/"}
	cfg.Session.Secret = digestReadinessSecret
	cfg.DAV.DigestEnabled = true
	handler := NewHandler(cfg, &store.Store{
		AppPasswords: &fakeAppPasswordRepo{list: []store.AppPassword{{ID: 2, Label: "legacy-token"}}},
	}, nil, dav.ValidateCalendarObject, dav.EventInstances)

	req := httptest.NewRequest(http.MethodGet, "/app-passwords", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()
	handler.AppPasswords(w, req)

	row, ok := appPasswordRowsByLabel(t, w.Body.String())["legacy-token"]
	if !ok {
		t.Fatalf("no row for legacy-token in %s", w.Body.String())
	}
	for _, want := range []string{"prefers Digest", "may not fall back to Basic", "revoke it and create a new one"} {
		if !strings.Contains(row, want) {
			t.Errorf("Basic only badge does not say %q: %s", want, row)
		}
	}
}

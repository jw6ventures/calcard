package dav

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// PostgreSQL refuses a btree entry larger than about a third of a page, so a
// UID or resource name past the limit the indexes allow is refused as the
// client's error rather than failing the write as a server one.
func TestPostgres_OversizedIdentifiersAreRefusedAsClientErrors(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "limits", "limits@example.test", "Limits", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: "Limits"})
	if err != nil {
		t.Fatalf("create address book: %v", err)
	}
	calendar, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Limits"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	h := NewDavServer(Options{Store: database})
	put := func(path, contentType, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
		req.Header.Set("Content-Type", contentType)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	long := incompressibleText(4096)
	bookPath := "/dav/addressbooks/" + itoa(book.ID) + "/"
	calendarPath := "/dav/calendars/" + itoa(calendar.ID) + "/"

	if rr := put(bookPath+"long-uid.vcf", "text/vcard", buildVCard("3.0", "UID:"+long, "FN:Long UID")); rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("contact with a 4 KB UID: status %d, want 4xx: %s", rr.Code, rr.Body.String())
	}
	if rr := put(bookPath+long+".vcf", "text/vcard", buildVCard("3.0", "UID:short-uid", "FN:Long name")); rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("contact with a 4 KB resource name: status %d, want 4xx: %s", rr.Code, rr.Body.String())
	}
	if rr := put(bookPath+"long-fn.vcf", "text/vcard", buildVCard("3.0", "UID:long-fn", "FN:"+long)); rr.Code != http.StatusCreated {
		t.Errorf("contact with a 4 KB FN: status %d, want 201: %s", rr.Code, rr.Body.String())
	}
	if rr := put(calendarPath+"long-uid.ics", "text/calendar", buildCalendarObject(buildVEvent(long, "SUMMARY:Long UID"))); rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("event with a 4 KB UID: status %d, want 4xx: %s", rr.Code, rr.Body.String())
	}
	if rr := put(calendarPath+long+".ics", "text/calendar", buildCalendarObject(buildVEvent("short-event", "SUMMARY:Long name"))); rr.Code < 400 || rr.Code >= 500 {
		t.Errorf("event with a 4 KB resource name: status %d, want 4xx: %s", rr.Code, rr.Body.String())
	}
}

func itoa(id int64) string { return strconv.FormatInt(id, 10) }

// incompressibleText is n octets PostgreSQL cannot compress below its btree
// entry limit, as a real identifier of that length would be.
func incompressibleText(n int) string {
	var b strings.Builder
	seed := sha256.Sum256([]byte("calcard"))
	for b.Len() < n {
		b.WriteString(hex.EncodeToString(seed[:]))
		seed = sha256.Sum256(seed[:])
	}
	return b.String()[:n]
}

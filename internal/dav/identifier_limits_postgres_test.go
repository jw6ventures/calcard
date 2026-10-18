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

	// A COPY or MOVE names its destination resource as a PUT does, and is
	// refused as a PUT of that name is, whether or not PostgreSQL could
	// compress the name below its index limit.
	newBook := func(name string) string {
		created, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: name})
		if err != nil {
			t.Fatalf("create address book: %v", err)
		}
		return "/dav/addressbooks/" + itoa(created.ID) + "/"
	}
	newCalendar := func(name string) string {
		created, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: name})
		if err != nil {
			t.Fatalf("create calendar: %v", err)
		}
		return "/dav/calendars/" + itoa(created.ID) + "/"
	}
	otherBookPath, thirdBookPath := newBook("Limits destination"), newBook("Limits third")
	otherCalendarPath, thirdCalendarPath := newCalendar("Limits destination"), newCalendar("Limits third")
	if rr := put(bookPath+"source.vcf", "text/vcard", buildVCard("3.0", "UID:source-contact", "FN:Source")); rr.Code != http.StatusCreated {
		t.Fatalf("PUT source contact: status %d: %s", rr.Code, rr.Body.String())
	}
	if rr := put(calendarPath+"source.ics", "text/calendar", buildCalendarObject(buildVEvent("source-event", "SUMMARY:Source"))); rr.Code != http.StatusCreated {
		t.Fatalf("PUT source event: status %d: %s", rr.Code, rr.Body.String())
	}
	transfer := func(method, source, destination string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, source, nil)
		req.Header.Set("Destination", destination)
		req = req.WithContext(auth.WithUser(req.Context(), user))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	for _, name := range []struct{ label, value string }{
		{"a 5,000-octet incompressible", incompressibleText(5000)},
		{"a 9,000-octet compressible", strings.Repeat("a", 9000)},
		{"a 1,025-octet", strings.Repeat("b", store.MaxIdentifierOctets+1)},
	} {
		for _, method := range []string{"COPY", "MOVE"} {
			t.Run(method+" to "+name.label+" name", func(t *testing.T) {
				assertErrorConditions(t, transfer(method, calendarPath+"source.ics", otherCalendarPath+name.value+".ics"), http.StatusForbidden, calQN("valid-calendar-object-resource"))
				assertErrorConditions(t, transfer(method, bookPath+"source.vcf", otherBookPath+name.value+".vcf"), http.StatusBadRequest, qn(nsCardDAV, "valid-address-data"))
				// The source is resolved first, so a missing one is 404 whatever
				// the destination's name.
				if rr := transfer(method, calendarPath+"missing.ics", otherCalendarPath+name.value+".ics"); rr.Code != http.StatusNotFound {
					t.Errorf("%s of a missing event: status %d, want 404: %s", method, rr.Code, rr.Body.String())
				}
				if rr := transfer(method, bookPath+"missing.vcf", otherBookPath+name.value+".vcf"); rr.Code != http.StatusNotFound {
					t.Errorf("%s of a missing contact: status %d, want 404: %s", method, rr.Code, rr.Body.String())
				}
			})
		}
	}
	if event, err := database.Events.GetByResourceName(ctx, calendar.ID, "source"); err != nil || event == nil {
		t.Errorf("refused MOVE removed the source event: %v, %v", event, err)
	}
	if contact, err := database.Contacts.GetByResourceName(ctx, book.ID, "source"); err != nil || contact == nil {
		t.Errorf("refused MOVE removed the source contact: %v, %v", contact, err)
	}

	// An object a 1.1.x release stored under a name past the bound, as the UI
	// named events after their UID, keeps that name wherever it is copied or
	// moved: its index entry already fits.
	legacy := incompressibleText(store.MaxIdentifierOctets + 100)
	if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: calendar.ID, UID: legacy, ResourceName: legacy, RawICAL: buildCalendarObject(buildVEvent(legacy, "SUMMARY:Legacy")), ETag: "legacy-event"}); err != nil {
		t.Fatalf("store legacy event: %v", err)
	}
	if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: legacy, ResourceName: legacy, RawVCard: buildVCard("3.0", "UID:"+legacy, "FN:Legacy"), ETag: "legacy-contact"}); err != nil {
		t.Fatalf("store legacy contact: %v", err)
	}
	for _, step := range []struct {
		method, from, to string
		want             int
	}{
		{"COPY", calendarPath, otherCalendarPath, http.StatusCreated},
		{"COPY", calendarPath, calendarPath, http.StatusNoContent},
		{"MOVE", calendarPath, calendarPath, http.StatusNoContent},
		{"MOVE", calendarPath, thirdCalendarPath, http.StatusCreated},
		{"COPY", bookPath, otherBookPath, http.StatusCreated},
		{"COPY", bookPath, bookPath, http.StatusNoContent},
		{"MOVE", bookPath, bookPath, http.StatusNoContent},
		{"MOVE", bookPath, thirdBookPath, http.StatusCreated},
	} {
		extension := ".ics"
		if strings.HasPrefix(step.from, "/dav/addressbooks/") {
			extension = ".vcf"
		}
		if rr := transfer(step.method, step.from+legacy+extension, step.to+legacy+extension); rr.Code != step.want {
			t.Errorf("%s of a legacy object from %s to %s keeping its %d-octet name: status %d, want %d: %s", step.method, step.from, step.to, len(legacy), rr.Code, step.want, rr.Body.String())
		}
	}

	// A name at the bound is still a name the store keeps.
	atBound := strings.Repeat("c", store.MaxIdentifierOctets)
	for _, step := range []struct{ method, calendarPath, bookPath string }{
		{"COPY", otherCalendarPath, otherBookPath},
		{"MOVE", thirdCalendarPath, thirdBookPath},
	} {
		method := step.method
		if rr := transfer(method, calendarPath+"source.ics", step.calendarPath+atBound+".ics"); rr.Code != http.StatusCreated {
			t.Errorf("%s of an event to a 1,024-octet resource name: status %d, want 201: %s", method, rr.Code, rr.Body.String())
		}
		if rr := transfer(method, bookPath+"source.vcf", step.bookPath+atBound+".vcf"); rr.Code != http.StatusCreated {
			t.Errorf("%s of a contact to a 1,024-octet resource name: status %d, want 201: %s", method, rr.Code, rr.Body.String())
		}
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

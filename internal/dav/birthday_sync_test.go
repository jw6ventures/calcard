package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// birthdaySyncServer holds one address book whose state versions the generated
// birthday collection, and one contact in it carrying a birthday.
func birthdaySyncServer(book store.AddressBook, contacts map[string]*store.Contact) *DavServer {
	return &DavServer{store: &store.Store{
		Calendars:        &fakeCalendarRepo{},
		Events:           &fakeEventRepo{},
		DeletedResources: &fakeDeletedResourceRepo{},
		AddressBooks:     &fakeAddressBookRepo{books: map[int64]*store.AddressBook{book.ID: &book}},
		Contacts:         &fakeContactRepo{contacts: contacts},
	}}
}

func birthdayCollectionProps(t *testing.T, h *DavServer, user *store.User) prop {
	t.Helper()
	res, err := h.calendarResponses(context.Background(), birthdayCalendarHref(), "0", user)
	if err != nil {
		t.Fatalf("calendarResponses on the birthday collection returned error: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("birthday collection PROPFIND returned %d responses, want 1", len(res))
	}
	if len(res[0].Propstat) == 0 {
		t.Fatalf("birthday collection response carried no propstat")
	}
	return res[0].Propstat[0].Prop
}

func birthdayContact(uid string, birthday time.Time, lastModified time.Time) *store.Contact {
	name := uid
	return &store.Contact{
		AddressBookID: 5,
		UID:           uid,
		ResourceName:  uid,
		DisplayName:   &name,
		Birthday:      &birthday,
		LastModified:  lastModified,
	}
}

// A client re-reads a collection only when its version moves, so the generated
// birthday collection has to version itself from the contacts it is built from.
func TestBirthdayCalendarVersionTracksContactState(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
	})

	before := birthdayCollectionProps(t, h, user)
	if before.CTag == "" {
		t.Fatal("birthday collection advertised no getctag")
	}
	if before.SyncToken == "" {
		t.Fatal("birthday collection advertised no sync-token")
	}

	// Adding a contact bumps the owning book's ctag and updated_at, which is
	// the only record the generated collection has that its content moved.
	books := h.store.AddressBooks.(*fakeAddressBookRepo)
	books.books[5].CTag = 5
	books.books[5].UpdatedAt = created.Add(time.Hour)
	contacts := h.store.Contacts.(*fakeContactRepo)
	contacts.contacts["5:bob"] = birthdayContact("bob", birthday, created.Add(time.Hour))

	after := birthdayCollectionProps(t, h, user)
	if after.CTag == before.CTag {
		t.Errorf("getctag stayed %q after a birthday contact was added; a client never re-reads the collection", after.CTag)
	}
	if after.SyncToken == before.SyncToken {
		t.Errorf("sync-token stayed %q after a birthday contact was added; a client never re-reads the collection", after.SyncToken)
	}
}

// An address book removed whole takes its contacts with it without touching any
// surviving book, so the count of books is part of the collection's version.
func TestBirthdayCalendarVersionTracksRemovedAddressBook(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
	})
	books := h.store.AddressBooks.(*fakeAddressBookRepo)
	books.books[6] = &store.AddressBook{ID: 6, UserID: 1, Name: "Work", CTag: 1, UpdatedAt: created}

	before := birthdayCollectionProps(t, h, user)

	delete(books.books, 6)

	after := birthdayCollectionProps(t, h, user)
	if after.CTag == before.CTag {
		t.Errorf("getctag stayed %q after an address book was removed", after.CTag)
	}
	if after.SyncToken == before.SyncToken {
		t.Errorf("sync-token stayed %q after an address book was removed", after.SyncToken)
	}
}

func birthdaySyncReport(t *testing.T, h *DavServer, user *store.User, token string) ([]response, string, error) {
	t.Helper()
	report := reportRequest{
		XMLName:   xml.Name{Local: "sync-collection"},
		SyncToken: token,
		Prop:      &reportProp{},
	}
	return h.birthdayCalendarReportResponses(context.Background(), user, "/dav/principals/1/", birthdayCalendarHref(), "", report, nil)
}

// RFC 6578 §3.2: a collection that keeps no change history cannot answer a
// token naming an older state, so it refuses the token and the client
// resynchronizes against the collection whole. Without the refusal a contact
// removed since the token was issued would never be reported gone.
func TestBirthdayCalendarSyncCollectionRefusesStaleToken(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
		"5:bob":   birthdayContact("bob", birthday, created),
	})

	_, token, err := birthdaySyncReport(t, h, user, "")
	if err != nil {
		t.Fatalf("initial sync-collection returned error: %v", err)
	}
	if token == "" {
		t.Fatal("initial sync-collection returned no sync-token")
	}

	books := h.store.AddressBooks.(*fakeAddressBookRepo)
	books.books[5].CTag = 5
	books.books[5].UpdatedAt = created.Add(time.Hour)
	contacts := h.store.Contacts.(*fakeContactRepo)
	delete(contacts.contacts, "5:bob")

	if _, _, err := birthdaySyncReport(t, h, user, token); !errors.Is(err, errInvalidSyncToken) {
		t.Fatalf("sync-collection with a stale token = %v, want errInvalidSyncToken", err)
	}
}

// A token naming the current state has nothing to report: the collection is
// answered alone, without re-listing every generated resource.
func TestBirthdayCalendarSyncCollectionCurrentTokenReportsNoChanges(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", birthday, created),
	})

	first, token, err := birthdaySyncReport(t, h, user, "")
	if err != nil {
		t.Fatalf("initial sync-collection returned error: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("initial sync-collection returned %d responses, want the collection plus one birthday", len(first))
	}

	second, again, err := birthdaySyncReport(t, h, user, token)
	if err != nil {
		t.Fatalf("sync-collection with the current token returned error: %v", err)
	}
	if again != token {
		t.Errorf("sync-token changed to %q with no change to the collection, want %q", again, token)
	}
	if len(second) != 1 {
		t.Fatalf("sync-collection with the current token returned %d responses, want the collection alone", len(second))
	}
	if !strings.HasSuffix(second[0].Href, "/") {
		t.Errorf("collection response href = %q, want a collection href", second[0].Href)
	}
}

// A token for another collection, or one the server never spelled, is refused
// rather than answered against the birthday collection.
func TestBirthdayCalendarSyncCollectionRefusesForeignToken(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, nil)

	for name, token := range map[string]string{
		"other calendar": buildSyncToken("cal", 2, created),
		"address book":   buildSyncToken("card", birthdayCalendarID, created),
		"malformed":      "not-a-sync-token",
	} {
		if _, _, err := birthdaySyncReport(t, h, user, token); !errors.Is(err, errInvalidSyncToken) {
			t.Errorf("sync-collection with a %s token = %v, want errInvalidSyncToken", name, err)
		}
	}
}

// Two contacts are two birthday resources. The resource's href is built from the
// generated event UID, so contacts whose stored UIDs differ only in an octet a
// content line cannot carry have to be spelled apart: sharing a UID makes one
// contact's birthday shadow the other's, and the collection reports one resource
// where it owes two -- a client synchronizing by href is handed a set that is
// silently short one birthday.
func TestBirthdayEventsSpellDistinctContactsDistinctly(t *testing.T) {
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	writable := birthdayContact("alice", birthday, created)
	writable.ID = 1
	withControlOctet := birthdayContact("al\vice", birthday, created)
	withControlOctet.ID = 2

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:" + writable.UID:         writable,
		"5:" + withControlOctet.UID: withControlOctet,
	})

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("generated %d events, want one per contact", len(events))
	}
	if events[0].UID == events[1].UID {
		t.Fatalf("contacts %q and %q both generated the resource %q; one birthday shadows the other", writable.UID, withControlOctet.UID, events[0].UID)
	}
	// A UID a content line carries verbatim is spelled as it is stored, so the
	// href a client has already synchronized for that contact does not move.
	if want := birthdayEventUID(5, "alice"); events[0].UID != want {
		t.Errorf("contact %q generated resource %q, want %q", writable.UID, events[0].UID, want)
	}

	// The href and ETag a client synchronized are only worth keeping if an
	// unchanged contact generates the same resource on the next request.
	again, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents: %v", err)
	}
	for i := range events {
		if again[i].UID != events[i].UID || again[i].ETag != events[i].ETag {
			t.Errorf("unchanged contact regenerated as %q/%q, was %q/%q", again[i].UID, again[i].ETag, events[i].UID, events[i].ETag)
		}
	}
}

// The escape introducer is escaped along with the octets it escapes, which is
// what leaves exactly one stored UID behind every spelling: a UID carrying the
// literal text %0B and one carrying that control octet would otherwise name the
// same birthday resource.
func TestICalSafeUIDSpellsEachStoredUIDOnce(t *testing.T) {
	spellings := map[string]string{}
	for _, uid := range []string{
		"alice", "al\vice", "alice\v", "\valice", "a%0Bb", "a\vb", "a%25b", "a%b",
		"urn:uuid:8f0c1d2e", "contact\x7f1", "contact\x1b1",
		"a,b", "a%2Cb", "a;b", "a%3Bb", `a\b`, "a%5Cb",
	} {
		safe := icalSafeUID(uid)
		for i := 0; i < len(safe); i++ {
			if isICalControlOctet(safe[i]) {
				t.Errorf("UID %q is spelled %q, which a content line cannot carry", uid, safe)
				break
			}
		}
		if other, taken := spellings[safe]; taken {
			t.Errorf("stored UIDs %q and %q are both spelled %q", other, uid, safe)
		}
		spellings[safe] = uid
	}
	for _, uid := range []string{"alice", "urn:uuid:8f0c1d2e", "a@b.example", "a\tb", "Ada Lovelace", "a/b"} {
		if got := icalSafeUID(uid); got != uid {
			t.Errorf("icalSafeUID(%q) = %q, want the stored UID unchanged", uid, got)
		}
	}
}

// The generated VEVENT is assembled from a contact, and a contact carries
// whatever the CardDAV client that wrote it chose to put in its display name
// and UID. Both reach a content line through escaping and folding.
func TestBirthdayEventsCannotBeMadeToWriteAnExtraContentLine(t *testing.T) {
	written := map[string]struct{}{
		"BEGIN": {}, "VERSION": {}, "PRODID": {}, "UID": {}, "DTSTAMP": {}, "DTSTART": {},
		"SUMMARY": {}, "RRULE": {}, "TRANSP": {}, "CLASS": {}, "X-CALCARD-TYPE": {},
		"X-CALCARD-CONTACT-UID": {}, "END": {},
	}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		uid         string
		displayName string
	}{
		{name: "display name opens a component", uid: "alice", displayName: "Alice\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:planted"},
		{name: "display name carries a bare CR", uid: "alice", displayName: "Alice\rDESCRIPTION:planted"},
		{name: "display name carries other control characters", uid: "alice", displayName: "A\x00B\x1bC\x7fD"},
		{name: "uid carries control characters", uid: "a\x00b\vc", displayName: "Alice"},
		{name: "uid is longer than a content line", uid: strings.Repeat("a", 200), displayName: strings.Repeat("Ada ", 40)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contact := birthdayContact(tt.uid, birthday, created)
			contact.DisplayName = &tt.displayName
			book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
			h := birthdaySyncServer(book, map[string]*store.Contact{"5:" + tt.uid: contact})

			events, err := h.generateBirthdayEvents(context.Background(), 1)
			if err != nil {
				t.Fatalf("generateBirthdayEvents: %v", err)
			}
			if len(events) != 1 {
				t.Fatalf("generated %d events, want 1", len(events))
			}
			raw := events[0].RawICAL

			var begins, ends int
			var writtenUID string
			for _, line := range strings.Split(strings.TrimSuffix(raw, "\r\n"), "\r\n") {
				if len(line) > 75 {
					t.Errorf("content line is %d octets, want at most 75: %q", len(line), line)
				}
			}
			for _, line := range ical.UnfoldLines(raw) {
				if strings.TrimSpace(line) == "" {
					continue
				}
				name, value, ok := strings.Cut(line, ":")
				if !ok {
					t.Errorf("content line has no value delimiter: %q", line)
					continue
				}
				name, _, _ = strings.Cut(name, ";")
				switch name {
				case "BEGIN":
					begins++
				case "END":
					ends++
				case "UID":
					writtenUID = value
				}
				if _, known := written[name]; !known {
					t.Errorf("event holds property %q, which the builder never writes:\n%s", name, raw)
				}
			}
			if begins != 2 || ends != 2 {
				t.Errorf("event holds %d BEGIN and %d END content lines, want 2 of each:\n%s", begins, ends, raw)
			}
			for _, control := range []string{"\x00", "\x01", "\v", "\f", "\x1b", "\x7f"} {
				if strings.Contains(raw, control) {
					t.Errorf("event holds control character %q:\n%q", control, raw)
				}
			}
			// The href a client is handed for this resource is built from the
			// event UID, so a UID the builder rewrote has to be the one the
			// event carries or the resource cannot be fetched by name.
			if writtenUID != events[0].UID {
				t.Errorf("event UID is %q but the card carries %q; the resource cannot be fetched by its href", events[0].UID, writtenUID)
			}
		})
	}
}

// UID is a TEXT value, so its delimiters are percent-encoded along with the
// octets a content line cannot carry; the event UID then reads back unescaped.
func TestICalSafeUIDEncodesTextDelimiters(t *testing.T) {
	for uid, want := range map[string]string{"a,b": "a%2Cb", "a;b": "a%3Bb", `a\b`: "a%5Cb", "a%b": "a%25b"} {
		if got := icalSafeUID(uid); got != want {
			t.Errorf("icalSafeUID(%q) = %q, want %q", uid, got, want)
		}
	}
}

func generatedBirthday(t *testing.T, contact *store.Contact) store.Event {
	t.Helper()
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	book := store.AddressBook{ID: contact.AddressBookID, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{"x": contact})
	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("generated %d events, want 1", len(events))
	}
	return events[0]
}

func birthdayOccurrences(t *testing.T, ev store.Event, from, to time.Time) []time.Time {
	t.Helper()
	instances, err := ical.RecurrenceInstances(ev.RawICAL, "VEVENT", *ev.DTStart, 24*time.Hour, from, to, 1000, nil)
	if err != nil {
		t.Fatalf("RecurrenceInstances: %v\n%s", err, ev.RawICAL)
	}
	var starts []time.Time
	for _, instance := range instances {
		if !instance.Start.Before(from) && instance.Start.Before(to) {
			starts = append(starts, instance.Start)
		}
	}
	return starts
}

func icalHasLine(raw, want string) bool {
	for _, line := range ical.UnfoldLines(raw) {
		if line == want {
			return true
		}
	}
	return false
}

// A February 29 birthday falls on February 28 in a common year. Rolling the
// date through time.Date instead lands on March 1, and a plain yearly rule
// started there repeats on March 1 in leap years too.
func TestBirthdayOnFebruary29RecursOnTheLastDayOfFebruary(t *testing.T) {
	for name, birthday := range map[string]time.Time{
		"with year":    time.Date(1992, 2, 29, 0, 0, 0, 0, time.UTC),
		"without year": time.Date(store.NoYearBirthdayYear, 2, 29, 0, 0, 0, 0, time.UTC),
	} {
		t.Run(name, func(t *testing.T) {
			ev := generatedBirthday(t, birthdayContact("leap", birthday, time.Time{}))
			if !icalHasLine(ev.RawICAL, "RRULE:FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=-1") {
				t.Fatalf("February 29 birthday lacks the last-day-of-February rule:\n%s", ev.RawICAL)
			}
			for year, want := range map[int]int{2027: 28, 2028: 29} {
				from := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
				starts := birthdayOccurrences(t, ev, from, from.AddDate(1, 0, 0))
				if len(starts) != 1 || starts[0].Month() != time.February || starts[0].Day() != want {
					t.Errorf("%d occurrences = %v, want February %d", year, starts, want)
				}
			}
		})
	}
}

// The generated body depends only on the contact: DTSTART is the birth date
// (or a fixed leap year when the card omits the year), so the ETag does not move
// after each birthday while the sync token, derived from the address books,
// stays put.
func TestBirthdayEventStartsOnAFixedDate(t *testing.T) {
	withYear := generatedBirthday(t, birthdayContact("y", time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC), time.Time{}))
	if !icalHasLine(withYear.RawICAL, "DTSTART;VALUE=DATE:19900515") {
		t.Errorf("birthday with a year does not start on the birth date:\n%s", withYear.RawICAL)
	}
	if !icalHasLine(withYear.RawICAL, "RRULE:FREQ=YEARLY") {
		t.Errorf("birthday lacks its yearly rule:\n%s", withYear.RawICAL)
	}
	noYear := generatedBirthday(t, birthdayContact("n", time.Date(store.NoYearBirthdayYear, 5, 15, 0, 0, 0, 0, time.UTC), time.Time{}))
	want := fmt.Sprintf("DTSTART;VALUE=DATE:%04d0515", birthdayNoYearStartYear)
	if !icalHasLine(noYear.RawICAL, want) {
		t.Errorf("year-less birthday lacks %q:\n%s", want, noYear.RawICAL)
	}
	legacy := generatedBirthday(t, birthdayContact("n", time.Date(1, 5, 15, 0, 0, 0, 0, time.UTC), time.Time{}))
	if legacy.RawICAL != noYear.RawICAL {
		t.Errorf("a year-1 placeholder generated a different body than the current placeholder:\n%s\n---\n%s", legacy.RawICAL, noYear.RawICAL)
	}
	// Past occurrences stay in the set.
	if starts := birthdayOccurrences(t, withYear, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)); len(starts) != 1 {
		t.Errorf("2000 occurrences = %v, want the birthday that year", starts)
	}
}

// The same contact UID in two address books is two contacts, so it is two
// birthday resources with two hrefs.
func TestBirthdayEventsAreUniquePerAddressBook(t *testing.T) {
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
	home := birthdayContact("alice", birthday, created)
	home.ID = 1
	work := birthdayContact("alice", birthday, created)
	work.ID, work.AddressBookID = 2, 6

	book := store.AddressBook{ID: 5, UserID: 1, Name: "Home", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{"5:alice": home, "6:alice": work})
	h.store.AddressBooks.(*fakeAddressBookRepo).books[6] = &store.AddressBook{ID: 6, UserID: 1, Name: "Work", CTag: 1, UpdatedAt: created}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents: %v", err)
	}
	if len(events) != 2 || events[0].UID == events[1].UID {
		t.Fatalf("generated %d events with UIDs %v, want two distinct", len(events), eventUIDs(events))
	}

	collection := birthdayCalendarHref()
	hrefs := []string{calendarObjectHref(collection, events[0].UID), calendarObjectHref(collection, events[1].UID)}
	request := httptest.NewRequest("REPORT", collection, nil)
	responses, err := h.birthdayCalendarMultiGet(context.Background(), &store.User{ID: 1}, events, hrefs, collection, "", propertySelector{Prop: &reportProp{}}, calendarDataProjection{}, request)
	if err != nil {
		t.Fatalf("birthdayCalendarMultiGet: %v", err)
	}
	for _, res := range responses {
		if res.Status != "" {
			t.Errorf("multiget answered %s with %s, want the resource", res.Href, res.Status)
		}
	}
	if len(responses) != 2 || responses[0].Href == responses[1].Href {
		t.Fatalf("multiget returned %d responses, want one per href", len(responses))
	}
}

func eventUIDs(events []store.Event) []string {
	uids := make([]string, len(events))
	for i := range events {
		uids[i] = events[i].UID
	}
	return uids
}

// A token issued before the generator changed names bodies and hrefs that no
// longer exist, so it is refused and the client resynchronizes once.
func TestBirthdayCalendarTokenCarriesTheGeneratorVersion(t *testing.T) {
	user := &store.User{ID: 1}
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{
		"5:alice": birthdayContact("alice", time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC), created),
	})
	previousGenerator := buildSyncTokenWithState("cal", birthdayCalendarID, created, fmt.Sprintf("%d-%d-%d", 1, 4, syncTokenNanos(created)))
	if _, _, err := birthdaySyncReport(t, h, user, previousGenerator); !errors.Is(err, errInvalidSyncToken) {
		t.Fatalf("sync-collection with a previous generator's token = %v, want errInvalidSyncToken", err)
	}
	props := birthdayCollectionProps(t, h, user)
	if props.CTag == fmt.Sprintf("%d-%d-%d", 1, 4, syncTokenNanos(created)) {
		t.Fatalf("getctag %q does not carry the generator version", props.CTag)
	}
}

// Each generated resource is fetched by the href the collection lists for it.
func TestBirthdayResourceIsFetchedByItsListedHref(t *testing.T) {
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	contact := birthdayContact("alice", time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC), created)
	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{"5:alice": contact})

	href := calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(5, "alice"))
	req := httptest.NewRequest("GET", href, nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d: %s", href, rr.Code, rr.Body.String())
	}
	if !icalHasLine(rr.Body.String(), "UID:birthday-5-alice@calcard") {
		t.Fatalf("GET %s returned another resource:\n%s", href, rr.Body.String())
	}
}

// Clients that cannot write a year-less date use an early stand-in year (Apple
// writes 1604), and some cards carry years such as 0002. Such a year is not a
// birth year, so the birthday starts at the fixed placeholder and still falls
// in every year's time ranges.
func TestBirthdayWithAStandInYearAppearsInATimeRange(t *testing.T) {
	created := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	apple := birthdayContact("apple", time.Date(1604, 9, 15, 0, 0, 0, 0, time.UTC), created)
	apple.ID = 1
	early := birthdayContact("early", time.Date(2, 9, 20, 0, 0, 0, 0, time.UTC), created)
	early.ID = 2
	book := store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", CTag: 4, UpdatedAt: created}
	h := birthdaySyncServer(book, map[string]*store.Contact{"5:apple": apple, "5:early": early})

	body := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop>` +
		`<C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT">` +
		`<C:time-range start="20260901T000000Z" end="20261001T000000Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`
	req := httptest.NewRequest("REPORT", birthdayCalendarHref(), strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("REPORT status = %d: %s", rr.Code, rr.Body.String())
	}
	decodeMultistatus(t, rr).assertHrefs(t,
		calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(5, "apple")),
		calendarObjectHref(birthdayCalendarHref(), birthdayEventUID(5, "early")),
	)
	for _, c := range []*store.Contact{apple, early} {
		ev := generatedBirthday(t, c)
		if want := fmt.Sprintf("DTSTART;VALUE=DATE:%04d%02d%02d", birthdayNoYearStartYear, c.Birthday.Month(), c.Birthday.Day()); !icalHasLine(ev.RawICAL, want) {
			t.Errorf("%s lacks %q:\n%s", c.UID, want, ev.RawICAL)
		}
	}
}

package dav

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/logging"
	"github.com/jw6ventures/calcard/internal/store"
	jw6_utils "github.com/jw6ventures/jw6-go-utils"
	"github.com/lib/pq"
)

// storeFailures are the ways a repository read fails when the fault is the
// server's: the database refusing the connection, refusing a new client, or
// failing the statement itself.
func storeFailures() []struct {
	name       string
	err        error
	wantStatus int
} {
	return []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "connection refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, wantStatus: http.StatusServiceUnavailable},
		{name: "too many clients", err: &pq.Error{Severity: "FATAL", Code: "53300", Message: "sorry, too many clients already"}, wantStatus: http.StatusServiceUnavailable},
		{name: "statement failure", err: errors.New(`pq: relation "calendars" does not exist`), wantStatus: http.StatusInternalServerError},
	}
}

func assertStoreFailureAnswer(t *testing.T, rr *httptest.ResponseRecorder, recorder *levelRecorder, cause error, wantStatus int) {
	t.Helper()
	if rr.Code != wantStatus {
		t.Fatalf("status = %d, want %d: %s", rr.Code, wantStatus, rr.Body.String())
	}
	if got := rr.Header().Get("Retry-After") != ""; got != (wantStatus == http.StatusServiceUnavailable) {
		t.Fatalf("Retry-After present = %v for status %d", got, rr.Code)
	}
	if got := strings.TrimSpace(rr.Body.String()); got != http.StatusText(wantStatus) {
		t.Fatalf("body = %q, want the status text alone", got)
	}
	if !recorder.logged(jw6_utils.Error, cause.Error()) {
		t.Fatalf("logged %q, want the store's cause at the error level", recorder.messages)
	}
}

func TestPropfindAnswersStoreFailureAsTheServers(t *testing.T) {
	const body = `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:propfind>`
	for _, failure := range storeFailures() {
		for _, target := range []struct {
			name string
			path string
			h    func(error) *DavServer
		}{
			{name: "calendar home", path: "/dav/calendars/", h: func(err error) *DavServer {
				return &DavServer{store: &store.Store{Calendars: &fakeCalendarRepo{accessErr: err}}}
			}},
			{name: "address book home", path: "/dav/addressbooks/", h: func(err error) *DavServer {
				return &DavServer{store: &store.Store{AddressBooks: &fakeAddressBookRepo{readErr: err}}}
			}},
		} {
			t.Run(target.name+" "+failure.name, func(t *testing.T) {
				h := target.h(failure.err)
				recorder := &levelRecorder{}
				h.log = logging.New(recorder, logClass)
				req := httptest.NewRequest("PROPFIND", target.path, strings.NewReader(body))
				req.Header.Set("Depth", "1")
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
				rr := httptest.NewRecorder()

				h.Propfind(rr, req)

				assertStoreFailureAnswer(t, rr, recorder, failure.err, failure.wantStatus)
			})
		}
	}
}

func TestProppatchAnswersStoreFailureWithoutItsDetail(t *testing.T) {
	const body = `<?xml version="1.0" encoding="utf-8"?><D:propertyupdate xmlns:D="DAV:"><D:set><D:prop><D:displayname>Renamed</D:displayname></D:prop></D:set></D:propertyupdate>`
	for _, failure := range storeFailures() {
		for _, target := range []struct {
			name string
			path string
			h    func(error) *DavServer
		}{
			{name: "calendar", path: "/dav/calendars/2", h: func(err error) *DavServer {
				return &DavServer{store: &store.Store{Calendars: &fakeCalendarRepo{accessErr: err}}}
			}},
			{name: "address book", path: "/dav/addressbooks/5", h: func(err error) *DavServer {
				return &DavServer{store: &store.Store{AddressBooks: &fakeAddressBookRepo{readErr: err}}}
			}},
		} {
			t.Run(target.name+" "+failure.name, func(t *testing.T) {
				h := target.h(failure.err)
				recorder := &levelRecorder{}
				h.log = logging.New(recorder, logClass)
				req := httptest.NewRequest("PROPPATCH", target.path, strings.NewReader(body))
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
				rr := httptest.NewRecorder()

				h.Proppatch(rr, req)

				assertStoreFailureAnswer(t, rr, recorder, failure.err, failure.wantStatus)
			})
		}
	}
}

// unreachableDatabase is a connection pool whose every attempt to connect
// fails with err, as lib/pq's does while PostgreSQL is stopped (a refused
// dial) or out of connection slots (SQLSTATE 53300). The repositories built
// on it are the production ones, so each request meets the failure where it
// first reads the database.
type unreachableDatabase struct{ err error }

func (c unreachableDatabase) Connect(context.Context) (driver.Conn, error) { return nil, c.err }
func (c unreachableDatabase) Driver() driver.Driver                        { return c }
func (c unreachableDatabase) Open(string) (driver.Conn, error)             { return nil, c.err }

func unreachableStore(t *testing.T, err error) *store.Store {
	t.Helper()
	pool := sql.OpenDB(unreachableDatabase{err: err})
	t.Cleanup(func() { _ = pool.Close() })
	return store.New(pool)
}

// assertServerFailureAnswer checks a request the store failed was answered as
// the server's failure, with no driver detail in the body; while the store was
// unavailable, with 503, Retry-After and nothing but the status text, so no
// misleading "not found" reaches the client either.
func assertServerFailureAnswer(t *testing.T, rr *httptest.ResponseRecorder, cause error, wantStatus int) {
	t.Helper()
	if rr.Code != wantStatus {
		t.Fatalf("status = %d, want %d: %q", rr.Code, wantStatus, rr.Body.String())
	}
	body := rr.Body.String()
	if strings.Contains(body, cause.Error()) || strings.Contains(body, "pq:") || strings.Contains(body, "connect:") {
		t.Fatalf("body %q exposes the store error", body)
	}
	if wantStatus != http.StatusServiceUnavailable {
		if got := rr.Header().Get("Retry-After"); got != "" {
			t.Fatalf("Retry-After = %q on a %d", got, rr.Code)
		}
		return
	}
	if got := rr.Header().Get("Retry-After"); got != davRetryAfterSeconds {
		t.Fatalf("Retry-After = %q, want %q", got, davRetryAfterSeconds)
	}
	if got := strings.TrimSpace(body); got != http.StatusText(http.StatusServiceUnavailable) {
		t.Fatalf("body = %q, want the status text alone", got)
	}
}

// Every DAV method answers a database it cannot reach with 503, whatever it
// was doing when it found out: an outage is no verdict on the resource, so it
// must not read as one missing, forbidden, locked or in conflict. A statement
// the database failed is the server's own failure, a 500.
func TestEveryDAVMethodAnswersAStoreFailureAsTheServers(t *testing.T) {
	const (
		event = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//test//EN\r\nBEGIN:VEVENT\r\nUID:event\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260102T100000Z\r\nDTEND:20260102T110000Z\r\nSUMMARY:Event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		card  = "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:card\r\nFN:Card\r\nN:Card;;;;\r\nEND:VCARD\r\n"
		lock  = `<?xml version="1.0" encoding="utf-8"?><D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockinfo>`
		acl   = `<?xml version="1.0" encoding="utf-8"?><D:acl xmlns:D="DAV:"><D:ace><D:principal><D:href>/dav/principals/2/</D:href></D:principal><D:grant><D:privilege><D:read/></D:privilege></D:grant></D:ace></D:acl>`
	)
	requests := []struct {
		name    string
		method  string
		path    string
		body    string
		headers map[string]string
	}{
		{name: "GET event", method: http.MethodGet, path: "/dav/calendars/2/event.ics"},
		{name: "HEAD event", method: http.MethodHead, path: "/dav/calendars/2/event.ics"},
		{name: "GET contact", method: http.MethodGet, path: "/dav/addressbooks/5/card.vcf"},
		{name: "PUT event", method: http.MethodPut, path: "/dav/calendars/2/event.ics", body: event, headers: map[string]string{"Content-Type": "text/calendar"}},
		{name: "PUT contact", method: http.MethodPut, path: "/dav/addressbooks/5/card.vcf", body: card, headers: map[string]string{"Content-Type": "text/vcard"}},
		{name: "DELETE event", method: http.MethodDelete, path: "/dav/calendars/2/event.ics"},
		{name: "DELETE contact", method: http.MethodDelete, path: "/dav/addressbooks/5/card.vcf"},
		{name: "DELETE calendar", method: http.MethodDelete, path: "/dav/calendars/2/"},
		{name: "MKCOL", method: "MKCOL", path: "/dav/addressbooks/fresh/"},
		{name: "MKCALENDAR", method: "MKCALENDAR", path: "/dav/calendars/fresh/"},
		{name: "COPY event", method: "COPY", path: "/dav/calendars/2/event.ics", headers: map[string]string{"Destination": "/dav/calendars/3/event.ics"}},
		{name: "MOVE event", method: "MOVE", path: "/dav/calendars/2/event.ics", headers: map[string]string{"Destination": "/dav/calendars/3/event.ics"}},
		{name: "MOVE contact", method: "MOVE", path: "/dav/addressbooks/5/card.vcf", headers: map[string]string{"Destination": "/dav/addressbooks/6/card.vcf"}},
		{name: "COPY calendar", method: "COPY", path: "/dav/calendars/2/", headers: map[string]string{"Destination": "/dav/calendars/copied/"}},
		{name: "MOVE calendar", method: "MOVE", path: "/dav/calendars/2/", headers: map[string]string{"Destination": "/dav/calendars/moved/"}},
		{name: "LOCK event", method: "LOCK", path: "/dav/calendars/2/event.ics", body: lock},
		{name: "LOCK refresh", method: "LOCK", path: "/dav/calendars/2/event.ics", headers: map[string]string{"If": "(<opaquelocktoken:held>)"}},
		{name: "UNLOCK event", method: "UNLOCK", path: "/dav/calendars/2/event.ics", headers: map[string]string{"Lock-Token": "<opaquelocktoken:held>"}},
		{name: "ACL calendar", method: "ACL", path: "/dav/calendars/2/", body: acl},
		{name: "PROPFIND calendar", method: "PROPFIND", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`, headers: map[string]string{"Depth": "1"}},
		{name: "PROPFIND event", method: "PROPFIND", path: "/dav/calendars/2/event.ics", body: `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`, headers: map[string]string{"Depth": "0"}},
		{name: "PROPPATCH calendar", method: "PROPPATCH", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><D:propertyupdate xmlns:D="DAV:"><D:set><D:prop><D:displayname>Renamed</D:displayname></D:prop></D:set></D:propertyupdate>`},
		{name: "REPORT sync-collection", method: "REPORT", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><D:sync-collection xmlns:D="DAV:"><D:sync-token/><D:sync-level>1</D:sync-level><D:prop><D:getetag/></D:prop></D:sync-collection>`},
		{name: "REPORT calendar-query", method: "REPORT", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`, headers: map[string]string{"Depth": "1"}},
		{name: "REPORT calendar-multiget", method: "REPORT", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><D:href>/dav/calendars/2/event.ics</D:href></C:calendar-multiget>`, headers: map[string]string{"Depth": "1"}},
		{name: "REPORT free-busy-query", method: "REPORT", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="20260101T000000Z" end="20260201T000000Z"/></C:free-busy-query>`, headers: map[string]string{"Depth": "1"}},
		{name: "REPORT addressbook-query", method: "REPORT", path: "/dav/addressbooks/5/", body: `<?xml version="1.0" encoding="utf-8"?><A:addressbook-query xmlns:D="DAV:" xmlns:A="urn:ietf:params:xml:ns:carddav"><D:prop><D:getetag/></D:prop><A:filter/></A:addressbook-query>`, headers: map[string]string{"Depth": "1"}},
		{name: "REPORT addressbook sync-collection", method: "REPORT", path: "/dav/addressbooks/5/", body: `<?xml version="1.0" encoding="utf-8"?><D:sync-collection xmlns:D="DAV:"><D:sync-token/><D:sync-level>1</D:sync-level><D:prop><D:getetag/></D:prop></D:sync-collection>`},
		{name: "REPORT acl-principal-prop-set", method: "REPORT", path: "/dav/calendars/2/", body: `<?xml version="1.0" encoding="utf-8"?><D:acl-principal-prop-set xmlns:D="DAV:"><D:prop><D:displayname/></D:prop></D:acl-principal-prop-set>`, headers: map[string]string{"Depth": "0"}},
		{name: "REPORT principal-property-search", method: "REPORT", path: "/dav/principals", body: `<?xml version="1.0" encoding="utf-8"?><D:principal-property-search xmlns:D="DAV:"><D:property-search><D:prop><D:displayname/></D:prop><D:match>a</D:match></D:property-search></D:principal-property-search>`, headers: map[string]string{"Depth": "0"}},
	}
	// The collections are either unreadable too, so every request fails at its
	// first read, or readable, so it fails reading members, locks or ACLs.
	stores := []struct {
		name  string
		build func(*testing.T, error) *store.Store
	}{
		{name: "database down", build: unreachableStore},
		{name: "collections readable", build: func(t *testing.T, err error) *store.Store {
			st := unreachableStore(t, err)
			calendars := map[int64]*store.Calendar{
				2: {ID: 2, UserID: 1, Name: "Work", CTag: 1},
				3: {ID: 3, UserID: 1, Name: "Home", CTag: 1},
			}
			st.Calendars = &fakeCalendarRepo{calendars: calendars, accessible: []store.CalendarAccess{
				{Calendar: *calendars[2], Editor: true}, {Calendar: *calendars[3], Editor: true},
			}}
			st.AddressBooks = &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
				5: {ID: 5, UserID: 1, Name: "Contacts", CTag: 1},
				6: {ID: 6, UserID: 1, Name: "Other", CTag: 1},
			}}
			return st
		}},
	}
	for _, failure := range storeFailures() {
		for _, variant := range stores {
			for _, request := range requests {
				t.Run(variant.name+" "+failure.name+" "+request.name, func(t *testing.T) {
					h := NewDavServer(Options{Store: variant.build(t, failure.err)})
					req := httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
					for name, value := range request.headers {
						req.Header.Set(name, value)
					}
					req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1, PrimaryEmail: "user@example.com"}))
					rr := httptest.NewRecorder()

					h.ServeHTTP(rr, req)

					assertServerFailureAnswer(t, rr, failure.err, failure.wantStatus)
				})
			}
		}
	}
}

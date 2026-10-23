package dav

import (
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

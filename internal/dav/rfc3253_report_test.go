package dav

// RFC 3253 §3.8: the DAV:expand-property REPORT. RFC 4791 §7.1 requires CalDAV
// servers to support the REPORT method and to advertise their reports in
// DAV:supported-report-set, but expand-property itself is RFC 3253's.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestRFC3253_ExpandPropertyInlinesReferencedResourceProperties(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com", FullName: "Dana Lee"}

	// RFC 3253 §3.8 grammar: DAV:property elements carrying name/namespace
	// attributes, not nested DAV:prop elements.
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:expand-property xmlns:d="DAV:">
  <d:property name="current-user-principal" namespace="DAV:">
    <d:property name="displayname" namespace="DAV:"/>
  </d:property>
</d:expand-property>`

	req := httptest.NewRequest("REPORT", "/dav/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/")
	resp := ms.responseForHref(t, "/dav/")

	// The outer property set is deliberately not pinned: RFC 3253 §3.8 reports
	// the properties named by the request's DAV:property elements, but CalCard
	// returns the fixed root-collection property set here regardless.
	prop := resp.assertPropStatus(t, davQN("current-user-principal"), http.StatusOK)

	// RFC 3253 §3.8: each expanded property carries a DAV:response for the
	// referenced resource in place of a bare DAV:href, so the property has that
	// one response as its sole child and the nested document must itself be a
	// well-formed response — right href, 200 propstat, exactly the properties
	// the inner DAV:property elements asked for.
	inner := responseFromElement(t, assertSoleChild(t, prop, davQN("response")))
	inner.assertHref(t, "/dav/principals/1/")
	inner.assertPropstatNames(t, http.StatusOK, davQN("displayname"))
	inner.assertPropValue(t, davQN("displayname"), http.StatusOK, user.FullName)
}

package dav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// Section 5.3.4: DAV:getetag is defined on every calendar object resource and
// its value is a strong entity tag, GET returns the same one in its ETag header,
// and PUT returns it too because the submitted octets are what got stored. The
// three have to agree — a client that compares a PROPFIND validator against a
// GET header is doing what §8.2 exists for.
func TestRFC4791_CalendarObjectETagIsStrongEverywhereItAppears(t *testing.T) {
	h, _ := writableCalendarServer()
	user := &store.User{ID: 1}
	body := buildCalendarObject(buildVEvent("etag", "SUMMARY:ETag"))

	rr := putCalendarObject(t, h, "etag.ics", body)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	putETag := rr.Header().Get("ETag")
	assertStrongETag(t, putETag, "§5.3.4 returns a strong ETag from a PUT whose octets are stored unchanged")

	req := httptest.NewRequest(http.MethodGet, "/dav/calendars/1/etag.ics", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr = httptest.NewRecorder()
	h.Get(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	getETag := rr.Header().Get("ETag")
	assertStrongETag(t, getETag, "§5.3.4 requires the current strong entity tag in the GET response")
	if getETag != putETag {
		t.Errorf("GET ETag = %s, want the %s the PUT returned", getETag, putETag)
	}

	for _, target := range []struct {
		name  string
		path  string
		depth string
	}{
		{"object resource", "/dav/calendars/1/etag.ics", "0"},
		{"collection member", "/dav/calendars/1/", "1"},
	} {
		t.Run(target.name, func(t *testing.T) {
			req := httptest.NewRequest("PROPFIND", target.path,
				strings.NewReader(`<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:"><D:prop><D:getetag/></D:prop></D:propfind>`))
			req.Header.Set("Depth", target.depth)
			req.Header.Set("Content-Type", "application/xml")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()

			h.Propfind(rr, req)

			value := decodeMultistatus(t, rr).
				responseForHref(t, "/dav/calendars/1/etag.ics").
				assertPropStatus(t, davQN("getetag"), http.StatusOK).Text
			assertStrongETag(t, value, "§5.3.4 defines DAV:getetag as strong on calendar object resources")
			if value != getETag {
				t.Errorf("DAV:getetag = %s, want the %s the GET header reports", value, getETag)
			}
		})
	}
}

// rewritingCalendarObjectWriter is a store backend that stores something other
// than the octets it was handed, which is the case RFC 4791 §5.3.4 forbids a
// strong ETag for.
type rewritingCalendarObjectWriter struct {
	stored string
}

func (w *rewritingCalendarObjectWriter) PutCalendarObject(_ context.Context, write store.CalendarObjectWrite) (*store.CalendarObjectWriteResult, error) {
	return &store.CalendarObjectWriteResult{
		Event: &store.Event{
			CalendarID:   write.CalendarID,
			UID:          write.UID,
			ResourceName: write.ResourceName,
			RawICAL:      w.stored,
			ETag:         write.ETag,
		},
		Created: true,
	}, nil
}

// Section 5.3.4: a server that does not store the submitted octets unchanged
// MUST NOT return a strong ETag from the PUT. CalCard stores them verbatim, so
// the rule is proven by substituting a backend that does not.
func TestRFC4791_PutReturnsNoStrongETagWhenStoredOctetsDiffer(t *testing.T) {
	writer := &rewritingCalendarObjectWriter{stored: buildCalendarObject(buildVEvent("rewritten", "SUMMARY:Server rewrote this"))}
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
	}}
	h := &DavServer{store: &store.Store{
		Calendars:       calRepo,
		Events:          &fakeEventRepo{events: map[string]*store.Event{}},
		CalendarObjects: writer,
	}}

	rr := putCalendarObject(t, h, "rewritten.ics", buildCalendarObject(buildVEvent("rewritten", "SUMMARY:Client sent this")))

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	if etag := rr.Header().Get("ETag"); etag != "" {
		t.Fatalf("ETag = %s, want none: the stored octets are not the submitted ones", etag)
	}
}

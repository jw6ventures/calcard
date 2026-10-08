package dav

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// A collection COPY or MOVE hands the store each member's octets as they are
// stored, a leading byte order mark included, which it guards on and stores,
// with the metadata the validator reads. A COPY gives each copy a fresh ETag;
// a MOVE keeps every member's.
func TestCalendarCollectionTransferStoresValidatedMemberBodies(t *testing.T) {
	bomBody := buildCalendarObject(buildVEvent("bom"))
	bomRaw := ical.ByteOrderMark + bomBody
	plainRaw := buildCalendarObject(buildVEvent("plain"))

	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			sourceSlug := "source"
			source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug, CTag: 7}
			calendars := &fakeCalendarRepo{
				accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
				calendars:  map[int64]*store.Calendar{1: &source},
			}
			events := &fakeEventRepo{events: map[string]*store.Event{
				"1:bom":   {ID: 11, CalendarID: 1, UID: "bom", ResourceName: "bom", RawICAL: bomRaw, ETag: "bom-etag"},
				"1:plain": {ID: 12, CalendarID: 1, UID: "plain", ResourceName: "plain", RawICAL: plainRaw, ETag: "plain-etag"},
			}}
			backend := &recordingCalendarCollectionBackend{result: &store.CalendarCollectionTransferResult{
				Calendar: &store.Calendar{ID: 9, UserID: 1, Name: "Source"}, Created: true,
			}}
			h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})
			req := httptest.NewRequest(method, "/dav/calendars/source/", nil)
			req.Header.Set("Destination", "/dav/calendars/transferred")
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusCreated {
				t.Fatalf("%s status = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			if len(backend.transfers) != 1 {
				t.Fatalf("backend calls = %d, want 1", len(backend.transfers))
			}
			members := make(map[string]store.CalendarCollectionMember)
			for _, member := range backend.transfers[0].Members {
				members[member.UID] = member
			}

			bom, ok := members["bom"]
			if !ok {
				t.Fatal("BOM member missing from the transfer")
			}
			if bom.RawICAL != bomRaw {
				t.Errorf("BOM member body = %q, want it as stored %q", bom.RawICAL, bomRaw)
			}
			if bom.ETag != "bom-etag" {
				t.Errorf("BOM member guard ETag = %q, want the member's", bom.ETag)
			}
			if copied := method == "COPY"; (bom.NewETag != "") != copied || bom.NewETag == "bom-etag" {
				t.Errorf("%s BOM member's copy ETag = %q, want a fresh one only for a COPY", method, bom.NewETag)
			}
			if bom.Metadata == nil {
				t.Error("BOM member carries no validated metadata")
			}

			plain, ok := members["plain"]
			if !ok {
				t.Fatal("plain member missing from the transfer")
			}
			if plain.RawICAL != plainRaw {
				t.Errorf("plain member body = %q, want %q", plain.RawICAL, plainRaw)
			}
			if copied := method == "COPY"; (plain.NewETag != "") != copied || plain.NewETag == "plain-etag" {
				t.Errorf("%s plain member's copy ETag = %q, want a fresh one only for a COPY", method, plain.NewETag)
			}
		})
	}
}

// A collection DELETE stores nothing, but its guard must still name each
// member as it was read.
func TestCalendarCollectionDeleteGuardsMembersAsRead(t *testing.T) {
	sourceSlug := "source"
	source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug, CTag: 7}
	raw := ical.ByteOrderMark + buildCalendarObject(buildVEvent("first"))
	calendars := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
		calendars:  map[int64]*store.Calendar{1: &source},
	}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:first": {ID: 11, CalendarID: 1, UID: "first", ResourceName: "first", RawICAL: raw, ETag: "etag-first"},
	}}
	backend := &recordingCalendarCollectionBackend{result: &store.CalendarCollectionTransferResult{}}
	h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})
	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/source/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 1 || len(backend.transfers[0].Members) != 1 {
		t.Fatalf("delete transfers = %#v, want one transfer with one member", backend.transfers)
	}
	if member := backend.transfers[0].Members[0]; member.RawICAL != raw || member.ETag != "etag-first" {
		t.Fatalf("delete member guard = %q/%q, want the member as read", member.RawICAL, member.ETag)
	}
}

package dav

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// COPY and MOVE of calendar objects and calendar collections, which RFC 4791
// §5.3.2.1 holds to every precondition PUT applies, and the collection DELETE
// that shares their atomic transfer path.

type recordingCalendarCollectionBackend struct {
	transfers []store.CalendarCollectionTransfer
	result    *store.CalendarCollectionTransferResult
	err       error
}

type stateChangingCalendarObjectBackend struct {
	events *fakeEventRepo
	calls  int
}

func (b *stateChangingCalendarObjectBackend) TransferCalendarObject(ctx context.Context, transfer store.CalendarObjectTransfer) (*store.CalendarObjectTransferResult, error) {
	b.calls++
	if b.calls == 1 {
		source := b.events.events[b.events.key(transfer.SourceCalendarID, transfer.SourceUID)]
		source.RawICAL = buildCalendarObject(buildVEvent(transfer.SourceUID, "SUMMARY:Changed during transfer"))
		source.ETag = "changed-etag"
		return nil, store.ErrResourceStateChanged
	}
	return b.events.TransferCalendarObject(ctx, transfer)
}

func (f *recordingCalendarCollectionBackend) TransferCalendarCollection(_ context.Context, transfer store.CalendarCollectionTransfer) (*store.CalendarCollectionTransferResult, error) {
	f.transfers = append(f.transfers, transfer)
	return f.result, f.err
}

func TestCalendarObjectCopyMoveApplyPutCalendarPreconditions(t *testing.T) {
	tests := []struct {
		name           string
		raw            string
		destComponents []string
		wantCondition  string
	}{
		{
			name:          "invalid calendar data",
			raw:           "not an iCalendar object",
			wantCondition: "valid-calendar-data",
		},
		{
			name:           "unsupported destination component",
			raw:            buildCalendarObject(buildVTodo("source")),
			destComponents: []string{"VEVENT"},
			wantCondition:  "supported-calendar-component",
		},
		{
			name: "date before minimum",
			raw: buildCalendarObject(buildVEvent("source",
				"DTSTART:18991231T235959Z",
			)),
			wantCondition: "min-date-time",
		},
	}

	for _, method := range []string{"COPY", "MOVE"} {
		for _, tt := range tests {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
					{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination", SupportedComponents: tt.destComponents}, Editor: true},
				}}
				events := &fakeEventRepo{events: map[string]*store.Event{
					"1:source": {
						CalendarID:   1,
						UID:          "source",
						ResourceName: "source",
						RawICAL:      tt.raw,
						ETag:         "source-etag",
					},
				}}
				h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events}})
				req := httptest.NewRequest(method, "/dav/calendars/1/source.ics", nil)
				req.Header.Set("Destination", "/dav/calendars/2/destination.ics")
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
				rr := httptest.NewRecorder()

				h.ServeHTTP(rr, req)

				assertErrorConditions(t, rr, http.StatusForbidden, calQN(tt.wantCondition))
				if got := events.events[events.key(2, "source")]; got != nil {
					t.Fatalf("%s stored invalid destination object: %#v", method, got)
				}
				if method == "MOVE" && events.events[events.key(1, "source")] == nil {
					t.Fatal("MOVE removed its source after destination validation failed")
				}
			})
		}
	}
}

func TestCalendarObjectCopyMoveRetryOneConcurrentStateChangeFromFreshSnapshot(t *testing.T) {
	for _, method := range []string{"COPY", "MOVE"} {
		t.Run(method, func(t *testing.T) {
			calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
				{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination"}, Editor: true},
			}}
			events := &fakeEventRepo{events: map[string]*store.Event{
				"1:source": {ID: 10, CalendarID: 1, UID: "source", ResourceName: "source", RawICAL: buildCalendarObject(buildVEvent("source")), ETag: "initial-etag"},
			}}
			backend := &stateChangingCalendarObjectBackend{events: events}
			h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarTransfers: backend}})
			req := httptest.NewRequest(method, "/dav/calendars/1/source.ics", nil)
			req.Header.Set("Destination", "/dav/calendars/2/destination.ics")
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusCreated {
				t.Fatalf("%s status = %d, want 201: %s", method, rr.Code, rr.Body.String())
			}
			if backend.calls != 2 {
				t.Fatalf("backend calls = %d, want one retry", backend.calls)
			}
			destination := events.events[events.key(2, "source")]
			if destination == nil || !strings.Contains(destination.RawICAL, "Changed during transfer") {
				t.Fatalf("destination was not written from the refreshed source snapshot: %#v", destination)
			}
		})
	}
}

func TestCalendarCollectionCopyMoveAdvertisedAndValidateHeaders(t *testing.T) {
	calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
	}}
	h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: &fakeEventRepo{events: map[string]*store.Event{}}}})

	t.Run("OPTIONS advertises collection COPY and MOVE", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/dav/calendars/1/", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		allow := rr.Header().Get("Allow")
		for _, method := range []string{"COPY", "MOVE"} {
			if !strings.Contains(allow, method) {
				t.Errorf("Allow missing %s: %s", method, allow)
			}
		}
	})

	tests := []struct {
		name        string
		method      string
		depth       string
		overwrite   string
		destination string
		wantStatus  int
	}{
		{name: "COPY rejects Depth 1", method: "COPY", depth: "1", destination: "/dav/calendars/copied", wantStatus: http.StatusBadRequest},
		{name: "MOVE rejects Depth 0", method: "MOVE", depth: "0", destination: "/dav/calendars/moved", wantStatus: http.StatusBadRequest},
		{name: "invalid Overwrite", method: "COPY", overwrite: "yes", destination: "/dav/calendars/copied", wantStatus: http.StatusBadRequest},
		{name: "cross authority", method: "COPY", destination: "https://other.example/dav/calendars/copied", wantStatus: http.StatusBadGateway},
		// A protocol-relative Destination names another server just as an
		// absolute URL does, and RFC 4918 §9.8.6 makes that a 502 rather than a
		// local path with the authority ignored.
		{name: "protocol-relative cross authority", method: "COPY", destination: "//other.example/dav/calendars/copied", wantStatus: http.StatusBadGateway},
		// The same-authority form is local, so it gets past the authority check
		// and is answered exactly as the equivalent relative path would be --
		// here a 404, because this fixture's store holds no source calendar.
		{name: "protocol-relative same authority", method: "MOVE", destination: "//calcard.example/dav/calendars/moved", wantStatus: http.StatusNotFound},
		{name: "relative destination for comparison", method: "MOVE", destination: "/dav/calendars/moved", wantStatus: http.StatusNotFound},
		{name: "non-http scheme", method: "COPY", destination: "ftp://calcard.example/dav/calendars/copied", wantStatus: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "https://calcard.example/dav/calendars/1/", nil)
			req.Header.Set("Destination", tt.destination)
			if tt.depth != "" {
				req.Header.Set("Depth", tt.depth)
			}
			if tt.overwrite != "" {
				req.Header.Set("Overwrite", tt.overwrite)
			}
			req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rr.Code, tt.wantStatus, rr.Body.String())
			}
		})
	}
}

func TestCalendarCollectionCopyPreflightsAndTransfersWholeCollection(t *testing.T) {
	sourceSlug := "source"
	source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug, CTag: 7}
	calendars := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
		calendars:  map[int64]*store.Calendar{1: &source},
	}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:first":  {ID: 11, CalendarID: 1, UID: "first", ResourceName: "first", RawICAL: buildCalendarObject(buildVEvent("first")), ETag: "etag-first"},
		"1:second": {ID: 12, CalendarID: 1, UID: "second", ResourceName: "custom-name", RawICAL: buildCalendarObject(buildVTodo("second")), ETag: "etag-second"},
	}}
	backend := &recordingCalendarCollectionBackend{result: &store.CalendarCollectionTransferResult{
		Calendar: &store.Calendar{ID: 9, UserID: 1, Name: "Source"}, Created: true,
	}}
	h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})

	req := httptest.NewRequest("COPY", "https://calcard.example/dav/calendars/source/", nil)
	req.Header.Set("Destination", "/dav/calendars/copied")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("COPY status = %d, want 201: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Location"); got != "/dav/calendars/9/" {
		t.Fatalf("Location = %q, want /dav/calendars/9/", got)
	}
	if len(backend.transfers) != 1 {
		t.Fatalf("backend calls = %d, want 1", len(backend.transfers))
	}
	transfer := backend.transfers[0]
	if transfer.Operation != store.CalendarCollectionCopy || transfer.Depth != "infinity" || transfer.DestinationSlug != "copied" {
		t.Fatalf("unexpected transfer: %#v", transfer)
	}
	if len(transfer.Members) != 2 {
		t.Fatalf("member snapshots = %d, want 2", len(transfer.Members))
	}
	for _, member := range transfer.Members {
		if member.NewETag == "" || member.NewETag == member.ETag {
			t.Errorf("member %q did not receive a fresh ETag: %#v", member.UID, member)
		}
	}
}

func TestCalendarCollectionCopyRequiresReadOnEverySourceMember(t *testing.T) {
	owner := store.User{ID: 1}
	delegate := store.User{ID: 2}
	source := store.Calendar{ID: 1, UserID: owner.ID, Name: "Shared", CTag: 7}
	calendars := &fakeCalendarRepo{
		calendars: map[int64]*store.Calendar{source.ID: &source},
	}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:visible": {ID: 11, CalendarID: 1, UID: "visible", ResourceName: "visible", RawICAL: buildCalendarObject(buildVEvent("visible")), ETag: "visible-etag"},
		"1:secret":  {ID: 12, CalendarID: 1, UID: "secret", ResourceName: "secret", RawICAL: buildCalendarObject(buildVEvent("secret")), ETag: "secret-etag"},
	}}
	backend := &recordingCalendarCollectionBackend{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: calendars,
		Events:    events,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/1", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/calendars/1/secret", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
			{ResourcePath: "/dav/calendars", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "bind"},
		}},
		CalendarCollections: backend,
	}})
	req := httptest.NewRequest("COPY", "/dav/calendars/1/", nil)
	req.Header.Set("Destination", "/dav/calendars/copied")
	req = req.WithContext(auth.WithUser(req.Context(), &delegate))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("COPY status = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 0 {
		t.Fatalf("COPY reached backend despite unreadable source member: %#v", backend.transfers)
	}
	// The unreadable member is reported as a 404 against its own href, and the
	// refusal carries none of the member's calendar data.
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/secret.ics")
	if got := statusCodeFromLine(t, ms.responseForHref(t, "/dav/calendars/1/secret.ics").Status); got != http.StatusNotFound {
		t.Errorf("member status = %d, want 404", got)
	}
	if strings.Contains(rr.Body.String(), "SUMMARY") {
		t.Fatalf("COPY failure leaked source member data: %s", rr.Body.String())
	}
}

func TestCalendarCollectionMoveConcealsNonOwnedSource(t *testing.T) {
	owner := store.User{ID: 1}
	delegate := store.User{ID: 2}
	source := store.Calendar{ID: 1, UserID: owner.ID, Name: "Private"}
	backend := &recordingCalendarCollectionBackend{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:           &fakeCalendarRepo{calendars: map[int64]*store.Calendar{source.ID: &source}},
		Events:              &fakeEventRepo{events: map[string]*store.Event{}},
		CalendarCollections: backend,
	}})
	req := httptest.NewRequest("MOVE", "/dav/calendars/1/", nil)
	req.Header.Set("Destination", "/dav/calendars/moved")
	req = req.WithContext(auth.WithUser(req.Context(), &delegate))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("MOVE status = %d, want 404: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 0 {
		t.Fatalf("MOVE reached backend for non-owned source: %#v", backend.transfers)
	}
}

func TestCalendarCollectionTransferRejectsAmbiguousDestination(t *testing.T) {
	sourceSlug := "source"
	source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug, CTag: 7}
	destinationA := store.Calendar{ID: 2, UserID: 1, Name: "duplicate", CTag: 3}
	destinationB := store.Calendar{ID: 3, UserID: 1, Name: "Duplicate", CTag: 4}
	calendars := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
		calendars: map[int64]*store.Calendar{
			source.ID:       &source,
			destinationA.ID: &destinationA,
			destinationB.ID: &destinationB,
		},
	}
	backend := &recordingCalendarCollectionBackend{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: calendars, Events: &fakeEventRepo{events: map[string]*store.Event{}}, CalendarCollections: backend,
	}})
	req := httptest.NewRequest("COPY", "/dav/calendars/source/", nil)
	req.Header.Set("Destination", "/dav/calendars/duplicate")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("COPY status = %d, want 409 for ambiguous destination: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 0 {
		t.Fatalf("ambiguous destination reached backend: %#v", backend.transfers)
	}
}

func TestCalendarCollectionTransferIsAllOrNothingOnInvalidMember(t *testing.T) {
	sourceSlug := "source"
	source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug, CTag: 7}
	calendars := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
		calendars:  map[int64]*store.Calendar{1: &source},
	}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:valid":   {ID: 11, CalendarID: 1, UID: "valid", ResourceName: "valid", RawICAL: buildCalendarObject(buildVEvent("valid")), ETag: "etag-valid"},
		"1:invalid": {ID: 12, CalendarID: 1, UID: "invalid", ResourceName: "invalid", RawICAL: "not iCalendar", ETag: "etag-invalid"},
	}}
	backend := &recordingCalendarCollectionBackend{}
	h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarCollections: backend}})

	req := httptest.NewRequest("MOVE", "https://calcard.example/dav/calendars/source/", nil)
	req.Header.Set("Destination", "/dav/calendars/moved")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("MOVE status = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 0 {
		t.Fatalf("backend called despite failed preflight: %#v", backend.transfers)
	}
	// Only the offending member is reported, naming the precondition it failed.
	// The transfer is all-or-nothing, so there are no 424 dependency statuses:
	// nothing was attempted for the other members to depend on.
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/calendars/1/invalid.ics")
	failed := ms.responseForHref(t, "/dav/calendars/1/invalid.ics")
	if got := statusCodeFromLine(t, failed.Status); got != http.StatusForbidden {
		t.Errorf("member status = %d, want 403", got)
	}
	if failed.Error == nil {
		t.Fatalf("member response carries no DAV:error: %s", rr.Body.String())
	}
	var conditions []xml.Name
	for _, condition := range failed.Error.Conditions {
		conditions = append(conditions, condition.Name)
	}
	if got := qnList(conditions); got != qnList([]xml.Name{calQN("valid-calendar-data")}) {
		t.Errorf("member DAV:error conditions = %s, want %s", got, qnString(calQN("valid-calendar-data")))
	}
	if events.events[events.key(1, "valid")] == nil || events.events[events.key(1, "invalid")] == nil {
		t.Fatal("failed collection MOVE mutated source members")
	}
}

func TestDeleteCalendarCollectionUsesAtomicTreeDelete(t *testing.T) {
	sourceSlug := "source"
	source := store.Calendar{ID: 1, UserID: 1, Name: "Source", Slug: &sourceSlug, CTag: 7}
	calendars := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{{Calendar: source, Editor: true}},
		calendars:  map[int64]*store.Calendar{1: &source},
	}
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:first": {ID: 11, CalendarID: 1, UID: "first", ResourceName: "first", RawICAL: buildCalendarObject(buildVEvent("first")), ETag: "etag-first"},
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
	if len(backend.transfers) != 1 || backend.transfers[0].Operation != store.CalendarCollectionDelete {
		t.Fatalf("unexpected delete backend calls: %#v", backend.transfers)
	}
	if len(backend.transfers[0].Members) != 1 {
		t.Fatalf("delete member snapshots = %d, want 1", len(backend.transfers[0].Members))
	}
}

func TestDeleteCalendarCollectionConcealsNonOwnedSource(t *testing.T) {
	owner := store.User{ID: 1}
	attacker := store.User{ID: 2}
	source := store.Calendar{ID: 1, UserID: owner.ID, Name: "Private"}
	backend := &recordingCalendarCollectionBackend{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:           &fakeCalendarRepo{calendars: map[int64]*store.Calendar{source.ID: &source}},
		Events:              &fakeEventRepo{events: map[string]*store.Event{}},
		CalendarCollections: backend,
	}})
	req := httptest.NewRequest(http.MethodDelete, "/dav/calendars/1/", nil)
	req = req.WithContext(auth.WithUser(req.Context(), &attacker))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("DELETE status = %d, want 404: %s", rr.Code, rr.Body.String())
	}
	if len(backend.transfers) != 0 {
		t.Fatalf("DELETE reached backend for non-owned source: %#v", backend.transfers)
	}
}

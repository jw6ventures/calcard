package dav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

type recordingCalendarObjectBackend struct {
	events    *fakeEventRepo
	transfers []store.CalendarObjectTransfer
}

func (b *recordingCalendarObjectBackend) TransferCalendarObject(ctx context.Context, transfer store.CalendarObjectTransfer) (*store.CalendarObjectTransferResult, error) {
	b.transfers = append(b.transfers, transfer)
	return b.events.TransferCalendarObject(ctx, transfer)
}

// A COPY or MOVE hands the store the source's octets as they are stored, a
// leading byte order mark included, which the store's optimistic-concurrency
// guard names the source by and a COPY stores. A MOVE keeps the ETag; a COPY
// is a new resource.
func TestCalendarObjectCopyMoveStoreTheValidatedBody(t *testing.T) {
	body := buildCalendarObject(buildVEvent("source"))
	tests := []struct {
		name      string
		sourceRaw string
	}{
		{name: "source with byte order mark", sourceRaw: ical.ByteOrderMark + body},
		{name: "source without byte order mark", sourceRaw: body},
	}
	for _, method := range []string{"COPY", "MOVE"} {
		for _, tt := range tests {
			t.Run(method+"/"+tt.name, func(t *testing.T) {
				calendars := &fakeCalendarRepo{accessible: []store.CalendarAccess{
					{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Source"}, Editor: true},
					{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Destination"}, Editor: true},
				}}
				events := &fakeEventRepo{events: map[string]*store.Event{
					"1:source": {ID: 10, CalendarID: 1, UID: "source", ResourceName: "source", RawICAL: tt.sourceRaw, ETag: "source-etag"},
				}}
				backend := &recordingCalendarObjectBackend{events: events}
				h := NewDavServer(Options{Store: &store.Store{Calendars: calendars, Events: events, CalendarTransfers: backend}})
				req := httptest.NewRequest(method, "/dav/calendars/1/source.ics", nil)
				req.Header.Set("Destination", "/dav/calendars/2/destination.ics")
				req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
				rr := httptest.NewRecorder()

				h.ServeHTTP(rr, req)

				if rr.Code != http.StatusCreated {
					t.Fatalf("status = %d, want 201: %s", rr.Code, rr.Body.String())
				}
				if len(backend.transfers) != 1 {
					t.Fatalf("transfers = %d, want 1", len(backend.transfers))
				}
				transfer := backend.transfers[0]
				if transfer.RawICAL != tt.sourceRaw {
					t.Errorf("stored body = %q, want the source as stored %q", transfer.RawICAL, tt.sourceRaw)
				}
				if transfer.ExpectedSourceETag != "source-etag" {
					t.Errorf("source ETag guard = %q, want source-etag", transfer.ExpectedSourceETag)
				}
				switch method {
				case "COPY":
					if transfer.ETag == "" || transfer.ETag == "source-etag" {
						t.Errorf("copy ETag = %q, want a fresh ETag", transfer.ETag)
					}
					if got := strings.Trim(rr.Header().Get("ETag"), `"`); got != transfer.ETag {
						t.Errorf("response ETag = %q, want the stored ETag %q", got, transfer.ETag)
					}
				case "MOVE":
					if moved := events.events["2:source"]; moved == nil || moved.ETag != "source-etag" {
						t.Errorf("moved resource = %#v, want it under the source's ETag", moved)
					}
				}
			})
		}
	}
}

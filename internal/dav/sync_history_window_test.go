package dav

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

type cleanupBeforeHistoryRead struct {
	store.DeletedResourceRepository
	beforeRead func()
}

func (r cleanupBeforeHistoryRead) ListDeletedSincePageAfter(ctx context.Context, kind string, collectionID, afterID int64, since time.Time, limit int) ([]store.DeletedResource, error) {
	r.beforeRead()
	return r.DeletedResourceRepository.ListDeletedSincePageAfter(ctx, kind, collectionID, afterID, since, limit)
}

func TestPostgres_SyncRejectsHistoryPrunedDuringTheRequest(t *testing.T) {
	for _, kind := range []string{"cal", "card"} {
		t.Run(kind, func(t *testing.T) {
			database := newDAVPostgresStore(t)
			ctx := t.Context()
			user, err := database.Users.UpsertOAuthUser(ctx, "retention", "retention@example.test", "Retention", "Test")
			if err != nil {
				t.Fatal(err)
			}
			var id int64
			var tokenTime time.Time
			var resourceType, collectionPath string
			if kind == "card" {
				book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: user.ID, Name: "Retention"})
				if err != nil {
					t.Fatal(err)
				}
				id = book.ID
				if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: id, UID: "gone", ResourceName: "gone", RawVCard: buildVCard("3.0", "UID:gone", "FN:Gone"), ETag: "e"}); err != nil {
					t.Fatal(err)
				}
				book, err = database.AddressBooks.GetByID(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				tokenTime = book.UpdatedAt
				if err := database.Contacts.DeleteByUID(ctx, id, "gone"); err != nil {
					t.Fatal(err)
				}
				resourceType, collectionPath = "contact", fmt.Sprintf("/dav/addressbooks/%d/", id)
			} else {
				cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Retention"})
				if err != nil {
					t.Fatal(err)
				}
				id = cal.ID
				if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: id, UID: "gone", ResourceName: "gone", RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:gone\r\nDTSTART:20240101T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "e"}); err != nil {
					t.Fatal(err)
				}
				cal, err = database.Calendars.GetByID(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				tokenTime = cal.UpdatedAt
				if err := database.Events.DeleteByUID(ctx, id, "gone"); err != nil {
					t.Fatal(err)
				}
				resourceType, collectionPath = "event", fmt.Sprintf("/dav/calendars/%d/", id)
			}
			history, err := database.DeletedResources.ListDeletedSincePageAfter(ctx, resourceType, id, 0, tokenTime, 10)
			if err != nil || len(history) != 1 {
				t.Fatalf("history = %v, error = %v", history, err)
			}
			cfg := syncWindowConfig(time.Second)
			pruned := false
			repo := database.DeletedResources
			database.DeletedResources = cleanupBeforeHistoryRead{DeletedResourceRepository: repo, beforeRead: func() {
				time.Sleep(time.Until(history[0].DeletedAt.Add(cfg.DAV.SyncHistoryRetention + 10*time.Millisecond)))
				n, err := repo.Cleanup(ctx, cfg.DAV.SyncHistoryRetention)
				if err != nil || n != 1 {
					t.Fatalf("pruned = %d, error = %v", n, err)
				}
				pruned = true
			}}
			h := NewDavServer(Options{Store: database, Config: cfg})
			req := httptest.NewRequest("REPORT", collectionPath, strings.NewReader(syncCollectionBody(buildSyncToken(kind, id, tokenTime))))
			req.Header.Set("Depth", "0")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Report(rr, req)
			if !pruned {
				t.Fatal("sync did not reach the concurrent cleanup")
			}
			assertErrorConditions(t, rr, http.StatusForbidden, davQN("valid-sync-token"))
		})
	}
}

// syncWindowCollections is the sync-collection report over each collection kind
// that reads tombstones. The retention window is one value governing both, so
// every case below runs against both: the calendar side is RFC 4791 scope and
// the address book side RFC 6352, and neither may answer a token the other
// refuses.
var syncWindowCollections = []struct {
	name   string
	kind   string
	id     int64
	server func(t *testing.T, cfg *config.Config) *DavServer
	report func(t *testing.T, h *DavServer, body string) *httptest.ResponseRecorder
}{
	{
		name:   "calendar",
		kind:   "cal",
		id:     1,
		server: func(t *testing.T, cfg *config.Config) *DavServer { return limitsTestServer(t, cfg, 2) },
		report: limitsReportRequest,
	},
	{
		name:   "address book",
		kind:   "card",
		id:     5,
		server: func(t *testing.T, cfg *config.Config) *DavServer { return cardLimitsTestServer(t, cfg, 2) },
		report: cardLimitsReportRequest,
	},
}

// syncWindowConfig is the limits every case below shares: the response bounds at
// their defaults, so the retention window is the only thing deciding the answer.
func syncWindowConfig(retention time.Duration) *config.Config {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.SyncHistoryRetention = retention
	return cfg
}

// RFC 6578 §3.2 lets a server invalidate a sync token it can no longer answer,
// naming the case directly: a server that "might only be able to maintain up to
// 3 weeks worth of changes to a collection" refuses the token and the client
// falls back to a full synchronization. CalCard's tombstone retention is that
// window, so a token naming an instant before it is refused with
// DAV:valid-sync-token rather than answered from a pruned history.
func TestSyncCollectionOutsideTheRetentionWindowFailsValidSyncToken(t *testing.T) {
	for _, collection := range syncWindowCollections {
		t.Run(collection.name, func(t *testing.T) {
			h := collection.server(t, syncWindowConfig(24*time.Hour))

			stale := time.Now().UTC().Add(-72 * time.Hour)
			rr := collection.report(t, h, syncCollectionBody(buildSyncToken(collection.kind, collection.id, stale)))

			assertErrorConditions(t, rr, http.StatusForbidden, davQN("valid-sync-token"))
		})
	}
}

// Inside the window every tombstone the report needs is still stored, so the
// token is answered.
func TestSyncCollectionInsideTheRetentionWindowIsAnswered(t *testing.T) {
	for _, collection := range syncWindowCollections {
		t.Run(collection.name, func(t *testing.T) {
			h := collection.server(t, syncWindowConfig(72*time.Hour))

			recent := time.Now().UTC().Add(-24 * time.Hour)
			rr := collection.report(t, h, syncCollectionBody(buildSyncToken(collection.kind, collection.id, recent)))

			decodeMultistatus(t, rr)
		})
	}
}

// A token naming the collection's own last-change instant is answerable however
// old it is: nothing has changed since, so there is no deletion to report and
// none can have been pruned out from under it. Without this a collection nobody
// has touched in longer than the window would force a full resync on every sync
// forever, since the token the server hands back names that same instant.
func TestSyncCollectionAtTheCollectionWatermarkIsAnsweredHoweverOld(t *testing.T) {
	for _, collection := range syncWindowCollections {
		t.Run(collection.name, func(t *testing.T) {
			h := collection.server(t, syncWindowConfig(time.Hour))

			// limitsFixtureModified is the fixture collection's UpdatedAt and is
			// years outside the window.
			rr := collection.report(t, h, syncCollectionBody(buildSyncToken(collection.kind, collection.id, limitsFixtureModified)))

			decodeMultistatus(t, rr)
		})
	}
}

// Retention off prunes nothing, so no token becomes unanswerable through age.
func TestSyncCollectionWithRetentionOffAnswersAnyAge(t *testing.T) {
	for _, collection := range syncWindowCollections {
		t.Run(collection.name, func(t *testing.T) {
			h := collection.server(t, syncWindowConfig(0))

			ancient := time.Now().UTC().Add(-10 * 365 * 24 * time.Hour)
			rr := collection.report(t, h, syncCollectionBody(buildSyncToken(collection.kind, collection.id, ancient)))

			decodeMultistatus(t, rr)
		})
	}
}

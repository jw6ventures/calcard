package dav

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

// syncWalk is what a client holds after following a sync-collection through
// every truncated page RFC 6578 §3.6 hands back.
type syncWalk struct {
	pages int
	// state is each reported member href, present or removed, as the pages
	// leave it when applied in order.
	state map[string]bool
	// reported counts the pages that reported each member href present.
	reported map[string]int
	// token is the one the final, complete page carries.
	token string
	// sizes are the encoded size of each page.
	sizes []int
}

func (w syncWalk) present() int {
	n := 0
	for _, present := range w.state {
		if present {
			n++
		}
	}
	return n
}

// walkSync follows a sync-collection from token until a page reports no
// truncation. Every page has to name the collection exactly once: a truncated
// page with the §3.6 marker, 507 and DAV:number-of-matches-within-limits, and
// the final page with the collection's own response. A truncated page has to
// move the token, or the client could never finish.
func walkSync(t *testing.T, report func(token string) *httptest.ResponseRecorder, collectionHref, token string, maxPages int) syncWalk {
	t.Helper()
	return walkSyncChanging(t, report, collectionHref, token, maxPages, nil)
}

// walkSyncChanging is walkSync with between run after each truncated page, so
// the collection can change while the client is part way through.
func walkSyncChanging(t *testing.T, report func(token string) *httptest.ResponseRecorder, collectionHref, token string, maxPages int, between func(page int)) syncWalk {
	t.Helper()
	walk := syncWalk{state: map[string]bool{}, reported: map[string]int{}}
	for walk.pages < maxPages {
		rr := report(token)
		ms := decodeMultistatus(t, rr)
		walk.pages++
		walk.sizes = append(walk.sizes, rr.Body.Len())
		seen := map[string]bool{}
		named, truncated := false, false
		for _, resp := range ms.Responses {
			if len(resp.Hrefs) != 1 {
				t.Fatalf("page %d: a response carries hrefs %v, want one", walk.pages, resp.Hrefs)
			}
			href := resp.Hrefs[0]
			if seen[href] {
				t.Fatalf("page %d names %s twice", walk.pages, href)
			}
			seen[href] = true
			switch {
			case href == collectionHref:
				named = true
				if resp.Status != "" {
					resp.assertResponseStatus(t, http.StatusInsufficientStorage)
					if !syncErrorNames(resp.Error, "number-of-matches-within-limits") {
						t.Fatalf("page %d: the truncation marker carries no DAV:number-of-matches-within-limits", walk.pages)
					}
					truncated = true
				}
			case resp.Status != "":
				resp.assertResponseStatus(t, http.StatusNotFound)
				walk.state[href] = false
			default:
				walk.state[href] = true
				walk.reported[href]++
			}
		}
		if !named {
			t.Fatalf("page %d does not name the collection %s; hrefs %v", walk.pages, collectionHref, ms.hrefs())
		}
		if !truncated {
			walk.token = ms.SyncToken
			return walk
		}
		if ms.SyncToken == "" || ms.SyncToken == token {
			t.Fatalf("truncated page %d hands back token %q after %q", walk.pages, ms.SyncToken, token)
		}
		token = ms.SyncToken
		if between != nil {
			between(walk.pages)
		}
	}
	t.Fatalf("sync did not finish within %d pages", maxPages)
	return walk
}

func syncErrorNames(err *davError, condition string) bool {
	if err == nil {
		return false
	}
	for _, c := range err.Conditions {
		if c.Name == davQN(condition) {
			return true
		}
	}
	return false
}

// syncCollectionLimitBody is a sync-collection carrying the RFC 6578 §3.7
// DAV:limit.
func syncCollectionLimitBody(syncToken string, nresults int) string {
	return `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:"><D:sync-token>` + syncToken + `</D:sync-token><D:sync-level>1</D:sync-level>` +
		`<D:limit><D:nresults>` + strconv.Itoa(nresults) + `</D:nresults></D:limit>` +
		`<D:prop><D:getetag/></D:prop></D:sync-collection>`
}

// syncCollectionKinds runs a case over both stored collection kinds. Every
// fixture stamps all of its members and tombstones with one instant, the shape
// a migration or a v1.1.x bulk write leaves, so a page boundary always falls
// inside a run of equal stamps.
var syncCollectionKinds = []struct {
	name           string
	kind           string
	id             int64
	collectionHref string
	server         func(t *testing.T, cfg *config.Config, members, tombstones int) *DavServer
	report         func(t *testing.T, h *DavServer, body string) *httptest.ResponseRecorder
	updatedAt      func(h *DavServer) time.Time
}{
	{
		name:           "calendar",
		kind:           "cal",
		id:             1,
		collectionHref: "/dav/calendars/1/",
		server:         limitsTombstoneServer,
		report:         limitsReportRequest,
		updatedAt: func(h *DavServer) time.Time {
			return h.store.Calendars.(*fakeCalendarRepo).accessible[0].UpdatedAt
		},
	},
	{
		name:           "address book",
		kind:           "card",
		id:             5,
		collectionHref: "/dav/addressbooks/5/",
		server: func(t *testing.T, cfg *config.Config, members, tombstones int) *DavServer {
			return limitsTombstoneAddressBookServer(cfg, members, tombstones)
		},
		report: limitsAddressBookReport,
		updatedAt: func(h *DavServer) time.Time {
			return h.store.AddressBooks.(*fakeAddressBookRepo).books[5].UpdatedAt
		},
	},
}

// RFC 6578 §3.6: a server that will not return a result in full returns part
// of it, marks the Request-URI 507, and hands back a token for the partial
// state, from which the client continues. A collection past the response limit
// can then be synchronized at all, a page at a time.
func TestSyncCollectionPagesAnInitialSyncPastTheResponseLimit(t *testing.T) {
	for _, kind := range syncCollectionKinds {
		t.Run(kind.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxMultistatusResponses = 11
			h := kind.server(t, cfg, 25, 0)
			report := func(token string) *httptest.ResponseRecorder {
				return kind.report(t, h, syncCollectionBody(token))
			}

			first := decodeMultistatus(t, report(""))
			if len(first.Responses) != 11 {
				t.Fatalf("first page has %d responses, want ten changes and the marker", len(first.Responses))
			}

			walk := walkSync(t, report, kind.collectionHref, "", 10)
			if walk.pages != 3 || len(walk.state) != 25 || walk.present() != 25 {
				t.Fatalf("pages = %d, reported %d (%d present), want 3 pages reporting all 25", walk.pages, len(walk.state), walk.present())
			}
			if want := buildSyncToken(kind.kind, kind.id, kind.updatedAt(h)); walk.token != want {
				t.Fatalf("final token = %q, want the collection's own %q", walk.token, want)
			}
		})
	}
}

// The candidate-row budget bounds the rows one report reads, so it bounds a
// sync page too; past it the sync is truncated rather than refused.
func TestSyncCollectionPagesPastTheCandidateRowBudget(t *testing.T) {
	for _, kind := range syncCollectionKinds {
		for _, incremental := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/incremental=%t", kind.name, incremental), func(t *testing.T) {
				cfg := &config.Config{}
				cfg.DAV.MaxReportCandidateRows = 100
				h := kind.server(t, cfg, 400, 0)
				token := ""
				if incremental {
					token = buildSyncToken(kind.kind, kind.id, limitsFixtureModified.Add(-time.Hour))
				}

				walk := walkSync(t, func(token string) *httptest.ResponseRecorder {
					return kind.report(t, h, syncCollectionBody(token))
				}, kind.collectionHref, token, 10)

				if walk.pages != 4 || walk.present() != 400 {
					t.Fatalf("pages = %d, present = %d, want 4 pages reporting all 400", walk.pages, walk.present())
				}
			})
		}
	}
}

// Removals share the change stream with changes and are paged with them. The
// fixture's members and tombstones are all stamped at one instant, so the
// boundary between the two kinds falls inside a page as well.
func TestSyncCollectionPagesRemovalsWithChanges(t *testing.T) {
	for _, kind := range syncCollectionKinds {
		t.Run(kind.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxReportCandidateRows = 100
			h := kind.server(t, cfg, 10, 400)

			walk := walkSync(t, func(token string) *httptest.ResponseRecorder {
				return kind.report(t, h, syncCollectionBody(token))
			}, kind.collectionHref, buildSyncToken(kind.kind, kind.id, limitsFixtureModified.Add(-time.Hour)), 10)

			if walk.pages != 5 || len(walk.state) != 410 || walk.present() != 10 {
				t.Fatalf("pages = %d, reported %d (%d present), want 5 pages reporting 10 present and 400 removed", walk.pages, len(walk.state), walk.present())
			}
		})
	}
}

// A collection whose changes fit one page is answered in full with the
// collection's own token.
func TestSyncCollectionAtThePageLimitIsAnsweredInFull(t *testing.T) {
	for _, kind := range syncCollectionKinds {
		t.Run(kind.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxReportCandidateRows = 266
			h := kind.server(t, cfg, 10, 256)

			rr := kind.report(t, h, syncCollectionBody(buildSyncToken(kind.kind, kind.id, limitsFixtureModified.Add(-time.Hour))))

			ms := decodeMultistatus(t, rr)
			// The collection, its ten modified resources, and one removal per tombstone.
			if len(ms.Responses) != 267 {
				t.Fatalf("responses = %d, want 267", len(ms.Responses))
			}
			if want := buildSyncToken(kind.kind, kind.id, kind.updatedAt(h)); ms.SyncToken != want {
				t.Fatalf("token = %q, want the collection's own %q", ms.SyncToken, want)
			}
		})
	}
}

// RFC 6578 §3.7: a client may cap how many results one sync returns, and a
// truncation it causes is marked the way the server's own is. A cap the server
// cannot honor fails with DAV:number-of-matches-within-limits.
func TestSyncCollectionHonorsTheClientLimit(t *testing.T) {
	for _, kind := range syncCollectionKinds {
		t.Run(kind.name, func(t *testing.T) {
			h := kind.server(t, &config.Config{}, 25, 0)
			report := func(token string) *httptest.ResponseRecorder {
				return kind.report(t, h, syncCollectionLimitBody(token, 3))
			}

			if first := decodeMultistatus(t, report("")); len(first.Responses) != 4 {
				t.Fatalf("first page has %d responses, want three changes and the marker", len(first.Responses))
			}
			walk := walkSync(t, report, kind.collectionHref, "", 20)
			if walk.pages != 9 || walk.present() != 25 {
				t.Fatalf("pages = %d, present = %d, want 9 pages reporting all 25", walk.pages, walk.present())
			}

			for _, nresults := range []int{0, -1} {
				rr := kind.report(t, h, syncCollectionLimitBody("", nresults))
				assertErrorConditions(t, rr, http.StatusForbidden, davQN("number-of-matches-within-limits"))
			}
		})
	}
}

// The response count is bounded before a page is read, but the bytes a page
// encodes to depend on what the client asked for. A page past the byte ceiling
// is read again with fewer changes rather than refused, and only one change
// that does not fit on its own leaves nothing to answer with.
func TestSyncCollectionPagesPastTheByteLimit(t *testing.T) {
	body := func(token string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<D:sync-collection xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:sync-token>` + token +
			`</D:sync-token><D:prop><D:getetag/><C:calendar-data/></D:prop></D:sync-collection>`
	}
	whole := limitsReportRequest(t, limitsTestServer(t, &config.Config{}, 40), body(""))
	if whole.Code != http.StatusMultiStatus {
		t.Fatalf("unbounded sync = %d: %s", whole.Code, whole.Body.String())
	}

	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusBytes = whole.Body.Len() / 4
	h := limitsTestServer(t, cfg, 40)
	walk := walkSync(t, func(token string) *httptest.ResponseRecorder {
		return limitsReportRequest(t, h, body(token))
	}, "/dav/calendars/1/", "", 40)

	if walk.present() != 40 || walk.pages < 4 {
		t.Fatalf("pages = %d, present = %d, want at least 4 pages reporting all 40", walk.pages, walk.present())
	}
	for i, size := range walk.sizes {
		if size > cfg.DAV.MaxMultistatusBytes {
			t.Fatalf("page %d encodes to %d bytes, past the %d-byte ceiling", i+1, size, cfg.DAV.MaxMultistatusBytes)
		}
	}

	cfg.DAV.MaxMultistatusBytes = 600
	rr := limitsReportRequest(t, limitsTestServer(t, cfg, 3), body(""))
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("a change larger than the ceiling = %d, want 507: %s", rr.Code, rr.Body.String())
	}
}

// A listing's first pages end on the oldest stamps in the collection, so a
// continuation token measured from its cursor would be refused once the
// collection's history is older than the retention window, and the client
// would restart the listing forever. The window is measured from when the
// listing began.
func TestSyncCollectionContinuesAListingOfHistoryOlderThanTheRetentionWindow(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 11
	cfg.DAV.SyncHistoryRetention = time.Hour
	h := limitsTestServer(t, cfg, 25)
	now := time.Now().UTC()
	h.store.Calendars.(*fakeCalendarRepo).accessible[0].UpdatedAt = now
	for _, event := range h.store.Events.(*fakeEventRepo).events {
		event.LastModified = now.AddDate(-2, 0, 0)
	}

	walk := walkSync(t, func(token string) *httptest.ResponseRecorder {
		return limitsReportRequest(t, h, syncCollectionBody(token))
	}, "/dav/calendars/1/", "", 10)

	if walk.pages != 3 || walk.present() != 25 {
		t.Fatalf("pages = %d, present = %d, want 3 pages reporting all 25", walk.pages, walk.present())
	}
}

// An address book never names a contact its requester cannot read, and a
// change to one since a client's token refuses the token. A listing cannot
// refuse its own continuation that way: the client would start the listing
// over and reach the same contact again. It leaves the contact out, as its
// first page does.
func TestAddressBookListingLeavesOutAContactTheRequesterCannotRead(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tombstones := &fakeDeletedResourceRepo{}
	contacts := &fakeContactRepo{contacts: map[string]*store.Contact{}, tombstones: tombstones}
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("person-%d", i)
		contacts.contacts["3:"+name] = &store.Contact{
			ID: int64(i), AddressBookID: 3, UID: name, ResourceName: name, ETag: "e",
			RawVCard: buildVCard("3.0", "UID:"+name, "FN:"+name), LastModified: base.Add(time.Duration(i) * time.Minute),
		}
	}
	h := &DavServer{store: &store.Store{
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			3: {ID: 3, UserID: 1, Name: "Contacts", CTag: 1, UpdatedAt: base.Add(time.Hour)},
		}},
		Contacts:         contacts,
		DeletedResources: tombstones,
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/addressbooks/3", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
			{ResourcePath: "/dav/addressbooks/3/person-4", PrincipalHref: "/dav/principals/2/", IsGrant: false, Privilege: "read"},
		}},
		Locks: &fakeLockRepo{},
	}}
	report := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("REPORT", "/dav/addressbooks/3/", strings.NewReader(syncCollectionLimitBody(token, 2)))
		req.Header.Set("Depth", "1")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 2, PrimaryEmail: "delegate@example.com"}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr
	}

	walk := walkSync(t, report, "/dav/addressbooks/3/", "", 10)

	if _, named := walk.state["/dav/addressbooks/3/person-4.vcf"]; named {
		t.Fatalf("the listing named the contact the requester cannot read: %v", walk.state)
	}
	if walk.present() != 4 || len(walk.state) != 4 {
		t.Fatalf("listing reported %v, want the four readable contacts", walk.state)
	}
}

func TestStoredSyncPositionReadsThePageTokenItIssued(t *testing.T) {
	anchor := time.Date(2026, 3, 1, 9, 0, 0, 123000, time.UTC)
	for _, test := range []struct {
		name    string
		listing bool
		last    store.SyncCursor
	}{
		{"listing after a member", true, store.SyncCursor{Stamp: anchor.Add(-time.Hour), ID: 41}},
		{"changes after a tombstone", false, store.SyncCursor{Stamp: anchor.Add(time.Minute), Removal: true, ID: 7}},
	} {
		t.Run(test.name, func(t *testing.T) {
			token := buildSyncPageToken("cal", 9, syncPosition{listing: test.listing, anchor: anchor}, test.last)
			got, err := parseStoredSyncPosition(token, "cal", 9)
			if err != nil {
				t.Fatalf("parseStoredSyncPosition(%q) error = %v", token, err)
			}
			if got.fresh || got.listing != test.listing || !got.anchor.Equal(anchor) ||
				!got.after.Stamp.Equal(test.last.Stamp) || got.after.Removal != test.last.Removal || got.after.ID != test.last.ID {
				t.Fatalf("parseStoredSyncPosition(%q) = %+v, want listing=%t anchor=%v after=%+v", token, got, test.listing, anchor, test.last)
			}
			if _, err := parseStoredSyncPosition(token, "card", 9); err == nil {
				t.Fatalf("a calendar's page token was accepted for an address book")
			}
			if _, err := parseStoredSyncPosition(token, "cal", 10); err == nil {
				t.Fatalf("a page token was accepted for another calendar")
			}
		})
	}

	plain, err := parseStoredSyncPosition(buildSyncToken("card", 4, anchor), "card", 4)
	if err != nil || plain.fresh || plain.listing || plain.after != store.SyncCursorAt(anchor) {
		t.Fatalf("collection token = %+v, %v; want changes after every change at %v", plain, err, anchor)
	}
}

func TestStoredSyncPositionRefusesStatesItDidNotIssue(t *testing.T) {
	const prefix = "urn:calcard-sync:cal:9:1700000000000000000:"
	for _, state := range []string{
		"listing-x-1-5",
		"changes-m-0-5",
		"changes-m--1-5",
		"changes-m-1-0",
		"changes-m-1-x",
		"pending-m-1-5",
		"listing-m-1",
		"listing-m-1-5-6",
		// The generated birthday collection's whole-state tag.
		"g1-y2026-2-7-1700000000000000000",
	} {
		if _, err := parseStoredSyncPosition(prefix+state, "cal", 9); err == nil {
			t.Errorf("state %q was accepted", state)
		}
	}
}

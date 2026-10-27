package ui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func importUIDs(n int) []string {
	uids := make([]string, n)
	for i := range uids {
		uids[i] = fmt.Sprintf("bulk-%03d", i)
	}
	return uids
}

// importRequest is the sharee's import of file into calendarID, carrying ctx's
// values.
func (f *uiEventFixture) importRequest(ctx context.Context, calendarID int64, file string) *http.Request {
	f.t.Helper()
	req := newICSImportRequest(f.t, "/calendars/x/import", "events.ics", file)
	req = withRouteID(req.WithContext(ctx), fmt.Sprint(calendarID))
	return req.WithContext(auth.WithUser(req.Context(), f.sharee))
}

type importDecisionsKey struct{}

// importDecisions counts the name lookups of one import.
type importDecisions struct{ lookups int }

// decidedImportsBarrier holds every import whose request carries an
// importDecisions after its lookups-th name lookup, the last its decisions
// make when each event is new, until every one of them has made it: no import
// writes before all have decided, as when replicas receive one file at the same
// instant.
type decidedImportsBarrier struct {
	store.EventRepository
	lookups int
	decided sync.WaitGroup
}

func (b *decidedImportsBarrier) GetByResourceName(ctx context.Context, calendarID int64, resourceName string) (*store.Event, error) {
	event, err := b.EventRepository.GetByResourceName(ctx, calendarID, resourceName)
	if decisions, ok := ctx.Value(importDecisionsKey{}).(*importDecisions); ok {
		decisions.lookups++
		if decisions.lookups == b.lookups {
			b.decided.Done()
			b.decided.Wait()
		}
	}
	return event, err
}

// runDecidedTogether runs an import of each file into calendarID at once, every
// one deciding before any writes, and returns their flashes in file order.
func (f *uiEventFixture) runDecidedTogether(calendarID int64, events int, files ...string) []string {
	f.t.Helper()
	inner := f.database.Events
	barrier := &decidedImportsBarrier{EventRepository: inner, lookups: events}
	barrier.decided.Add(len(files))
	f.database.Events = barrier
	defer func() { f.database.Events = inner }()

	recorders := make([]*httptest.ResponseRecorder, len(files))
	requests := make([]*http.Request, len(files))
	for i, file := range files {
		ctx := context.WithValue(f.t.Context(), importDecisionsKey{}, &importDecisions{})
		requests[i] = f.importRequest(ctx, calendarID, file)
		recorders[i] = httptest.NewRecorder()
	}
	var done sync.WaitGroup
	for i := range files {
		done.Go(func() { f.handler.ImportCalendar(recorders[i], requests[i]) })
	}
	done.Wait()

	flashes := make([]string, len(files))
	for i, rr := range recorders {
		flashes[i] = importFlash(f.t, rr)
	}
	return flashes
}

func (f *uiEventFixture) storedCount(calendarID int64) int {
	f.t.Helper()
	events, err := f.database.Events.ListForCalendar(f.t.Context(), calendarID)
	if err != nil {
		f.t.Fatal(err)
	}
	return len(events)
}

// Importing a file a second time finds every event already as the file has it,
// and reports them imported.
func TestPostgres_ImportOfTheSameFileTwiceSucceedsBothTimes(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Twice")
	file := importFile(importUIDs(150)...)

	for run := 1; run <= 2; run++ {
		if flash := importFlash(t, f.runImport(f.handler, calendar.ID, file)); flash != "Imported 150 event(s)" {
			t.Errorf("run %d: flash = %q", run, flash)
		}
	}
	if got := f.storedCount(calendar.ID); got != 150 {
		t.Errorf("stored %d events, want 150", got)
	}
}

// Imports of one file that race, as replicas behind a load balancer receive one
// upload at the same instant, each find what the other wrote to be what the
// file holds, and each report the events imported.
func TestPostgres_ConcurrentImportsOfTheSameFileBothSucceed(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Racing imports")
	file := importFile(importUIDs(150)...)

	for i, flash := range f.runDecidedTogether(calendar.ID, 150, file, file) {
		if flash != "Imported 150 event(s)" {
			t.Errorf("import %d: flash = %q", i, flash)
		}
	}
	if got := f.storedCount(calendar.ID); got != 150 {
		t.Errorf("stored %d events, want 150", got)
	}
}

// importCountsFlash is a flash of an import that only wrote events or skipped
// them as changed during the import.
var importCountsFlash = regexp.MustCompile(`^(?:Imported (\d+) event\(s\)|failed to import events)(?:; skipped (\d+) changed during the import)?$`)

// Racing imports of files that differ in an event keep that event as the
// first write left it: the import that lost reports it skipped, not imported.
func TestPostgres_ConcurrentImportsOfDifferentContentSkipTheLosingEvents(t *testing.T) {
	f := newUIEventFixture(t)
	calendar := f.calendarOf(f.sharee, "Conflicting imports")
	uids := importUIDs(20)
	file := importFile(uids...)
	edited := strings.ReplaceAll(file, "SUMMARY:imported ", "SUMMARY:edited ")

	flashes := f.runDecidedTogether(calendar.ID, len(uids), file, edited)

	imported, skipped := 0, 0
	for i, flash := range flashes {
		match := importCountsFlash.FindStringSubmatch(flash)
		if match == nil {
			t.Fatalf("import %d: flash = %q", i, flash)
		}
		n, _ := strconv.Atoi(match[1])
		m, _ := strconv.Atoi(match[2])
		if n+m != len(uids) {
			t.Errorf("import %d: flash = %q accounts for %d of %d events", i, flash, n+m, len(uids))
		}
		imported += n
		skipped += m
	}
	if imported != len(uids) || skipped != len(uids) {
		t.Errorf("imported %d and skipped %d across both imports, want %d each: %q", imported, skipped, len(uids), flashes)
	}
	if got := f.storedCount(calendar.ID); got != len(uids) {
		t.Errorf("stored %d events, want %d", got, len(uids))
	}
}

// An event another writer stores between an import's decision and its write,
// holding what the import would store, counts as imported only while the
// caller may write it: one the caller is denied writing is reported as a
// change.
func TestPostgres_ImportCountsAnEventStoredAsItWouldStoreIt(t *testing.T) {
	f := newUIEventFixture(t)
	file := importFile("raced", "added")
	scratch := f.calendarOf(f.sharee, "Scratch")
	importFlash(t, f.runImport(f.handler, scratch.ID, file))
	identical := f.storedEvent(scratch.ID, "raced").RawICAL

	for _, tc := range []struct {
		name      string
		denyWrite bool
		wantFlash string
	}{
		{name: "the caller may write it", wantFlash: "Imported 2 event(s)"},
		{name: "the caller is denied writing it", denyWrite: true, wantFlash: "Imported 1 event(s); skipped 1 changed during the import"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calendarID := f.calendarWithEvent("unrelated", eventObject(singleEvent, "unrelated"), f.shareeACE(true, "read", "write"), nil)
			calls := 0
			handler := f.handlerRunningOn(func() {
				// The second event is validated after the first was decided.
				calls++
				if calls != 2 {
					return
				}
				f.putEvent(calendarID, "raced", identical)
				if tc.denyWrite {
					f.setACL(store.MemberStatePath(fmt.Sprintf("/dav/calendars/%d", calendarID), "raced"), f.shareeACE(false, "write-content"))
				}
			})

			if flash := importFlash(t, f.runImport(handler, calendarID, file)); flash != tc.wantFlash {
				t.Errorf("flash = %q, want %q", flash, tc.wantFlash)
			}
			if got := f.storedEvent(calendarID, "raced"); got == nil || got.RawICAL != identical {
				t.Errorf("the other writer's event was replaced: %+v", got)
			}
		})
	}
}

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"
)

const transferByteOrderMark = "\xef\xbb\xbf"

// storedEventRow is an events row with the columns calendarObjectColumns leaves
// out, so a test can see every column a transfer is expected to write.
type storedEventRow struct {
	ID              int64
	ResourceName    string
	RawICAL         string
	ETag            string
	Summary         sql.NullString
	RecurrenceStart sql.NullTime
	RecurrenceUntil sql.NullTime
}

func loadStoredEventRow(t *testing.T, s *Store, calendarID int64, resourceName string) *storedEventRow {
	t.Helper()
	var row storedEventRow
	err := s.Calendars.(*calendarRepo).pool.QueryRowContext(context.Background(), `
SELECT id, resource_name, raw_ical, etag, summary, recurrence_start, recurrence_until
FROM events WHERE calendar_id=$1 AND resource_name=$2`, calendarID, resourceName).Scan(
		&row.ID, &row.ResourceName, &row.RawICAL, &row.ETag, &row.Summary, &row.RecurrenceStart, &row.RecurrenceUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		t.Fatalf("load event %d/%s: %v", calendarID, resourceName, err)
	}
	return &row
}

// transferMetadata is metadata no parser would derive from the test bodies, so
// finding it stored proves the transfer wrote the caller's validated metadata.
func transferMetadata(summary string) *EventWriteMetadata {
	start := time.Date(2031, 1, 2, 3, 4, 5, 0, time.UTC)
	until := time.Date(2032, 6, 7, 8, 9, 10, 0, time.UTC)
	return &EventWriteMetadata{Summary: &summary, DTStart: &start, RecurrenceStart: &start, RecurrenceUntil: &until}
}

func assertTransferredRow(t *testing.T, row *storedEventRow, wantRaw, wantETag, wantSummary string) {
	t.Helper()
	if row == nil {
		t.Fatal("transferred event is missing")
	}
	if row.RawICAL != wantRaw {
		t.Errorf("raw_ical = %q, want %q", row.RawICAL, wantRaw)
	}
	if row.ETag != wantETag {
		t.Errorf("etag = %q, want %q", row.ETag, wantETag)
	}
	if !row.Summary.Valid || row.Summary.String != wantSummary {
		t.Errorf("summary = %#v, want %q", row.Summary, wantSummary)
	}
	want := transferMetadata(wantSummary)
	if !row.RecurrenceStart.Valid || !row.RecurrenceStart.Time.Equal(*want.RecurrenceStart) ||
		!row.RecurrenceUntil.Valid || !row.RecurrenceUntil.Time.Equal(*want.RecurrenceUntil) {
		t.Errorf("recurrence bounds = %v..%v, want %v..%v", row.RecurrenceStart, row.RecurrenceUntil, want.RecurrenceStart, want.RecurrenceUntil)
	}
}

// A source stored before PUT stripped byte order marks still carries one. A
// MOVE keeps its octets, recognizing the source by them, and its ETag, and
// sets its columns to the metadata the validator read from them.
func TestPostgres_CalendarObjectMoveStoresTransferredBody(t *testing.T) {
	for _, crossCalendar := range []bool{false, true} {
		name := "same-calendar rename"
		if crossCalendar {
			name = "cross-calendar move"
		}
		t.Run(name, func(t *testing.T) {
			s := newPostgresStore(t)
			_, sourceCalendarID := newPostgresCalendar(t, s, "move-body-source")
			destinationCalendarID := sourceCalendarID
			if crossCalendar {
				_, destinationCalendarID = newPostgresCalendar(t, s, "move-body-destination")
			}
			ctx := context.Background()
			body := postgresCalendarObject("source", "Source")
			sourceRaw := transferByteOrderMark + body
			if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
				CalendarID: sourceCalendarID, UID: "source", ResourceName: "source", RawICAL: sourceRaw, ETag: "source-etag",
			}); err != nil {
				t.Fatalf("seed source event: %v", err)
			}
			database := s.Calendars.(*calendarRepo).pool
			var destinationCTagBefore int64
			if err := database.QueryRowContext(ctx, `SELECT ctag FROM calendars WHERE id=$1`, destinationCalendarID).Scan(&destinationCTagBefore); err != nil {
				t.Fatalf("load destination ctag: %v", err)
			}

			result, err := s.TransferCalendarObject(ctx, CalendarObjectTransfer{
				Operation:               CalendarObjectMove,
				SourceCalendarID:        sourceCalendarID,
				SourceUID:               "source",
				SourceResourceName:      "source",
				ExpectedSourceETag:      "source-etag",
				DestinationCalendarID:   destinationCalendarID,
				DestinationResourceName: "moved",
				Overwrite:               true,
				RawICAL:                 sourceRaw,
				Metadata:                transferMetadata("Validated summary"),
				SourceStatePath:         fmt.Sprintf("/dav/calendars/%d/source", sourceCalendarID),
				DestinationStatePath:    fmt.Sprintf("/dav/calendars/%d/moved", destinationCalendarID),
			})
			if err != nil {
				t.Fatalf("TransferCalendarObject() error = %v", err)
			}
			if result == nil || result.Event == nil || result.Event.RawICAL != sourceRaw || result.Event.ETag != "source-etag" {
				t.Fatalf("move result = %#v, want the source's body and ETag", result)
			}
			assertTransferredRow(t, loadStoredEventRow(t, s, destinationCalendarID, "moved"), sourceRaw, "source-etag", "Validated summary")
			if source := loadStoredEventRow(t, s, sourceCalendarID, "source"); source != nil {
				t.Fatalf("source still bound after MOVE: %#v", source)
			}

			var tombstones int
			if err := database.QueryRowContext(ctx, `
SELECT COUNT(*) FROM deleted_resources WHERE resource_type='event' AND collection_id=$1 AND resource_name='source'`,
				sourceCalendarID).Scan(&tombstones); err != nil {
				t.Fatalf("count source tombstones: %v", err)
			}
			if tombstones != 1 {
				t.Fatalf("source tombstones = %d, want 1", tombstones)
			}
			var destinationTombstones int
			if err := database.QueryRowContext(ctx, `
SELECT COUNT(*) FROM deleted_resources WHERE resource_type='event' AND collection_id=$1 AND resource_name='moved'`,
				destinationCalendarID).Scan(&destinationTombstones); err != nil {
				t.Fatalf("count destination tombstones: %v", err)
			}
			if destinationTombstones != 0 {
				t.Fatalf("destination tombstones = %d, want 0", destinationTombstones)
			}
			var destinationCTagAfter int64
			if err := database.QueryRowContext(ctx, `SELECT ctag FROM calendars WHERE id=$1`, destinationCalendarID).Scan(&destinationCTagAfter); err != nil {
				t.Fatalf("reload destination ctag: %v", err)
			}
			if destinationCTagAfter <= destinationCTagBefore {
				t.Fatalf("destination ctag = %d after move, want greater than %d", destinationCTagAfter, destinationCTagBefore)
			}
		})
	}
}

// collectionTransferFixture is a source calendar holding one member stored
// with a byte order mark and one stored as validation would leave it.
type collectionTransferFixture struct {
	userID   int64
	sourceID int64
	bomBody  string
	bomRaw   string
	plainRaw string
	bomID    int64
	plainID  int64
}

func newCollectionTransferFixture(t *testing.T, s *Store, name string) collectionTransferFixture {
	t.Helper()
	ctx := context.Background()
	userID, sourceID := newPostgresCalendar(t, s, name)
	fixture := collectionTransferFixture{
		userID:   userID,
		sourceID: sourceID,
		bomBody:  postgresCalendarObject("bom", "BOM"),
		plainRaw: postgresCalendarObject("plain", "Plain"),
	}
	fixture.bomRaw = transferByteOrderMark + fixture.bomBody
	for _, seed := range []struct {
		uid string
		raw string
		id  *int64
	}{
		{uid: "bom", raw: fixture.bomRaw, id: &fixture.bomID},
		{uid: "plain", raw: fixture.plainRaw, id: &fixture.plainID},
	} {
		result, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
			CalendarID: sourceID, UID: seed.uid, ResourceName: seed.uid, RawICAL: seed.raw, ETag: seed.uid + "-etag",
		})
		if err != nil || result == nil || result.Event == nil {
			t.Fatalf("seed %s: %#v, %v", seed.uid, result, err)
		}
		*seed.id = result.Event.ID
	}
	return fixture
}

// members is what the HTTP layer's preflight hands the store: each member as
// it was read, with the columns its body reads as and, for a COPY, the ETag of
// its copy.
func (f collectionTransferFixture) members(operation CalendarCollectionTransferOperation) []CalendarCollectionMember {
	newETag := func(uid string) string {
		if operation == CalendarCollectionCopy {
			return uid + "-copy-etag"
		}
		return ""
	}
	return []CalendarCollectionMember{
		{ID: f.bomID, UID: "bom", ResourceName: "bom", RawICAL: f.bomRaw, ETag: "bom-etag",
			NewETag: newETag("bom"), Metadata: transferMetadata("Validated BOM")},
		{ID: f.plainID, UID: "plain", ResourceName: "plain", RawICAL: f.plainRaw, ETag: "plain-etag",
			NewETag: newETag("plain"), Metadata: transferMetadata("Validated plain")},
	}
}

func (f collectionTransferFixture) transfer(t *testing.T, s *Store, operation CalendarCollectionTransferOperation, members []CalendarCollectionMember) CalendarCollectionTransfer {
	t.Helper()
	source, err := s.Calendars.GetByID(context.Background(), f.sourceID)
	if err != nil || source == nil {
		t.Fatalf("load source calendar: %#v, %v", source, err)
	}
	return CalendarCollectionTransfer{
		Operation:          operation,
		Depth:              "infinity",
		Overwrite:          true,
		SourceID:           f.sourceID,
		ExpectedSourceCTag: source.CTag,
		SourceStatePath:    calendarCollectionStatePath(f.sourceID),
		DestinationOwnerID: f.userID,
		DestinationSlug:    "transferred-" + string(operation),
		Members:            members,
	}
}

func TestPostgres_CalendarCollectionTransferStoresTransferredBodies(t *testing.T) {
	for _, operation := range []CalendarCollectionTransferOperation{CalendarCollectionCopy, CalendarCollectionMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			fixture := newCollectionTransferFixture(t, s, "collection-body-"+string(operation))

			result, err := s.TransferCalendarCollection(context.Background(), fixture.transfer(t, s, operation, fixture.members(operation)))
			if err != nil {
				t.Fatalf("TransferCalendarCollection() error = %v", err)
			}
			if result == nil || result.Calendar == nil {
				t.Fatalf("transfer result = %#v, want the destination calendar", result)
			}
			destinationID := result.Calendar.ID

			// Each member is stored as it was read, BOM included, with the
			// columns its body reads as: a COPY under its copy's ETag, a
			// MOVE under its own.
			for _, member := range []struct{ uid, raw, summary string }{
				{"bom", fixture.bomRaw, "Validated BOM"}, {"plain", fixture.plainRaw, "Validated plain"},
			} {
				etag := member.uid + "-etag"
				if operation == CalendarCollectionCopy {
					etag = member.uid + "-copy-etag"
				}
				assertTransferredRow(t, loadStoredEventRow(t, s, destinationID, member.uid), member.raw, etag, member.summary)
			}
			switch operation {
			case CalendarCollectionCopy:
				if source := loadStoredEventRow(t, s, fixture.sourceID, "bom"); source == nil || source.RawICAL != fixture.bomRaw || source.ETag != "bom-etag" {
					t.Errorf("COPY changed its source member: %#v", source)
				}
			case CalendarCollectionMove:
				if plain := loadStoredEventRow(t, s, destinationID, "plain"); plain == nil || plain.ID != fixture.plainID {
					t.Errorf("moved plain member = %#v, want row %d", plain, fixture.plainID)
				}
				if bom := loadStoredEventRow(t, s, destinationID, "bom"); bom == nil || bom.ID != fixture.bomID {
					t.Errorf("moved BOM member = %#v, want row %d", bom, fixture.bomID)
				}
			}
		})
	}
}

// The guard compares the stored member with the octets it was read with, so a
// member rewritten between the preflight and the transfer stops the whole
// transfer.
func TestPostgres_CalendarCollectionTransferRejectsMemberChangedAfterPreflight(t *testing.T) {
	for _, operation := range []CalendarCollectionTransferOperation{CalendarCollectionCopy, CalendarCollectionMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			ctx := context.Background()
			fixture := newCollectionTransferFixture(t, s, "collection-guard-"+string(operation))
			transfer := fixture.transfer(t, s, operation, fixture.members(operation))

			// The ETag is left as it was so that only the body differs.
			if _, err := s.Calendars.(*calendarRepo).pool.ExecContext(ctx,
				`UPDATE events SET raw_ical=$1 WHERE id=$2`, fixture.bomBody, fixture.bomID); err != nil {
				t.Fatalf("rewrite member: %v", err)
			}
			source, err := s.Calendars.GetByID(ctx, fixture.sourceID)
			if err != nil || source == nil {
				t.Fatalf("reload source calendar: %#v, %v", source, err)
			}
			transfer.ExpectedSourceCTag = source.CTag

			if _, err := s.TransferCalendarCollection(ctx, transfer); !errors.Is(err, ErrResourceStateChanged) {
				t.Fatalf("TransferCalendarCollection() error = %v, want ErrResourceStateChanged", err)
			}
			calendars, err := s.Calendars.ListByUser(ctx, fixture.userID)
			if err != nil || len(calendars) != 1 || calendars[0].ID != fixture.sourceID {
				t.Fatalf("calendars after refused transfer = %#v, %v", calendars, err)
			}
			if member := loadStoredEventRow(t, s, fixture.sourceID, "plain"); member == nil || member.ETag != "plain-etag" {
				t.Fatalf("source member after refused transfer = %#v", member)
			}
		})
	}
}

// A collection MOVE writes each member's row once, whether its columns change
// or not: each member's last_modified is stamped against the moved collection
// and covered by its sync token, and the moved collection's ctag moves.
func TestPostgres_CalendarCollectionMoveWritesEachMemberOnce(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	pool := s.Calendars.(*calendarRepo).pool
	fixture := newCollectionTransferFixture(t, s, "collection-move-writes")
	for _, statement := range []string{
		`CREATE TABLE event_writes (event_id BIGINT, calendar_id BIGINT)`,
		`CREATE FUNCTION count_event_write() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO event_writes VALUES (NEW.id, NEW.calendar_id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`,
		`CREATE TRIGGER trg_count_event_write AFTER UPDATE ON events FOR EACH ROW EXECUTE FUNCTION count_event_write()`,
	} {
		if _, err := pool.ExecContext(ctx, statement); err != nil {
			t.Fatalf("count event writes: %v", err)
		}
	}

	// The plain member's columns already read as its body does; the BOM
	// member's change.
	members := fixture.members(CalendarCollectionMove)
	var unchanged EventWriteMetadata
	var summary, description, location sql.NullString
	var dtstart, dtend, recurrenceStart, recurrenceUntil sql.NullTime
	if err := pool.QueryRowContext(ctx, `
SELECT summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until FROM events WHERE id=$1`,
		fixture.plainID).Scan(&summary, &description, &location, &dtstart, &dtend, &unchanged.AllDay, &recurrenceStart, &recurrenceUntil); err != nil {
		t.Fatalf("load plain member's columns: %v", err)
	}
	text := func(value sql.NullString) *string {
		if !value.Valid {
			return nil
		}
		return &value.String
	}
	instant := func(value sql.NullTime) *time.Time {
		if !value.Valid {
			return nil
		}
		return &value.Time
	}
	unchanged.Summary, unchanged.Description, unchanged.Location = text(summary), text(description), text(location)
	unchanged.DTStart, unchanged.DTEnd = instant(dtstart), instant(dtend)
	unchanged.RecurrenceStart, unchanged.RecurrenceUntil = instant(recurrenceStart), instant(recurrenceUntil)
	members[1].Metadata = &unchanged

	result, err := s.TransferCalendarCollection(ctx, fixture.transfer(t, s, CalendarCollectionMove, members))
	if err != nil || result == nil || result.Calendar == nil {
		t.Fatalf("TransferCalendarCollection() = %#v, %v", result, err)
	}
	moved := result.Calendar

	writes := make(map[int64][]int64)
	rows, err := pool.QueryContext(ctx, `SELECT event_id, calendar_id FROM event_writes`)
	if err != nil {
		t.Fatalf("read event writes: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var eventID, calendarID int64
		if err := rows.Scan(&eventID, &calendarID); err != nil {
			t.Fatalf("scan event write: %v", err)
		}
		writes[eventID] = append(writes[eventID], calendarID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read event writes: %v", err)
	}
	for _, member := range []struct {
		name string
		id   int64
	}{{"BOM", fixture.bomID}, {"plain", fixture.plainID}} {
		if got := writes[member.id]; len(got) != 1 || got[0] != moved.ID {
			t.Errorf("%s member written into calendars %v, want once into %d", member.name, got, moved.ID)
		}
	}
	if row := loadStoredEventRow(t, s, moved.ID, "bom"); row == nil || row.Summary.String != "Validated BOM" {
		t.Errorf("BOM member = %#v, want its validated columns", row)
	}
	if row := loadStoredEventRow(t, s, moved.ID, "plain"); row == nil || text(row.Summary) == nil || *text(row.Summary) != *unchanged.Summary {
		t.Errorf("plain member = %#v, want its columns kept", row)
	}

	// A new calendar's ctag starts at 1.
	if moved.CTag <= 1 {
		t.Errorf("moved collection's ctag = %d, want it moved past a new calendar's 1", moved.CTag)
	}
	var stale int
	if err := pool.QueryRowContext(ctx, `
SELECT COUNT(*) FROM events e JOIN calendars c ON c.id = e.calendar_id
WHERE e.calendar_id=$1 AND e.last_modified > c.updated_at`, moved.ID).Scan(&stale); err != nil {
		t.Fatalf("compare stamps: %v", err)
	}
	if stale != 0 {
		t.Errorf("%d members stamped after the moved collection's sync token", stale)
	}
}

// A collection COPY gives each member's copy the member's dead properties,
// read from every spelling of the member's path with the member's own path
// winning, and gives no member another's.
func TestPostgres_CalendarCollectionCopyCopiesEachMembersDeadProperties(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	fixture := newCollectionTransferFixture(t, s, "collection-copy-dead")
	sourceRoot := calendarCollectionStatePath(fixture.sourceID)
	for _, seed := range []struct {
		path, local, value string
	}{
		{sourceRoot + "/bom", "shared", "bom exact"},
		{sourceRoot + "/bom.ics", "shared", "bom legacy"},
		{sourceRoot + "/bom.ics", "legacy-only", "bom legacy only"},
		{sourceRoot + "/plain.ics", "shared", "plain legacy"},
		{sourceRoot + "/unrelated", "shared", "not a member"},
	} {
		if err := s.DeadProperties.Apply(ctx, seed.path, []DeadPropertyMutation{{NamespaceURI: "urn:calcard:test", LocalName: seed.local, InnerXML: seed.value}}); err != nil {
			t.Fatalf("seed %s: %v", seed.path, err)
		}
	}

	result, err := s.TransferCalendarCollection(ctx, fixture.transfer(t, s, CalendarCollectionCopy, fixture.members(CalendarCollectionCopy)))
	if err != nil || result == nil || result.Calendar == nil {
		t.Fatalf("TransferCalendarCollection() = %#v, %v", result, err)
	}
	copyRoot := calendarCollectionStatePath(result.Calendar.ID)
	var copied []string
	rows, err := s.Calendars.(*calendarRepo).pool.QueryContext(ctx,
		`SELECT resource_path || ' ' || local_name || '=' || inner_xml FROM dav_dead_properties WHERE resource_path LIKE $1 ORDER BY 1`, copyRoot+"/%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		copied = append(copied, row)
	}
	want := []string{
		copyRoot + "/bom legacy-only=bom legacy only",
		copyRoot + "/bom shared=bom exact",
		copyRoot + "/plain shared=plain legacy",
	}
	if fmt.Sprint(copied) != fmt.Sprint(want) {
		t.Errorf("copied member dead properties = %q, want %q", copied, want)
	}
}

// A collection COPY or MOVE writes the collection it produces once however
// many members it carries, which keeps its cost linear in the member count:
// the members' triggers do not each rewrite that one calendars row. The result
// still reads as a change to every member: its ctag has moved, its sync token
// is at or past every member's stamp, and a sync from the start of its change
// stream reports every member and one from its token reports none.
func TestPostgres_CalendarCollectionTransferWritesTheResultingCollectionOnce(t *testing.T) {
	for _, operation := range []CalendarCollectionTransferOperation{CalendarCollectionCopy, CalendarCollectionMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			ctx := context.Background()
			pool := s.Calendars.(*calendarRepo).pool
			fixture := newCollectionTransferFixture(t, s, "collection-once-"+string(operation))
			for i := 0; i < 30; i++ {
				uid := fmt.Sprintf("extra-%02d", i)
				if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{
					CalendarID: fixture.sourceID, UID: uid, ResourceName: uid, RawICAL: postgresCalendarObject(uid, "Extra"), ETag: uid + "-etag",
				}); err != nil {
					t.Fatalf("seed %s: %v", uid, err)
				}
			}
			events, err := s.Events.ListForCalendar(ctx, fixture.sourceID)
			if err != nil {
				t.Fatal(err)
			}
			members := make([]CalendarCollectionMember, 0, len(events))
			for _, event := range events {
				member := CalendarCollectionMember{ID: event.ID, UID: event.UID, ResourceName: event.ResourceName, RawICAL: event.RawICAL, ETag: event.ETag,
					Metadata: transferMetadata("Transferred " + event.UID)}
				if operation == CalendarCollectionCopy {
					member.NewETag = event.ETag + "-copy"
				}
				members = append(members, member)
			}
			transfer := fixture.transfer(t, s, operation, members)
			for _, statement := range []string{
				`CREATE TABLE calendar_writes (calendar_id BIGINT)`,
				`CREATE FUNCTION count_calendar_write() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO calendar_writes VALUES (NEW.id);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql`,
				`CREATE TRIGGER trg_count_calendar_write AFTER UPDATE ON calendars FOR EACH ROW EXECUTE FUNCTION count_calendar_write()`,
			} {
				if _, err := pool.ExecContext(ctx, statement); err != nil {
					t.Fatalf("count calendar writes: %v", err)
				}
			}

			result, err := s.TransferCalendarCollection(ctx, transfer)
			if err != nil || result == nil || result.Calendar == nil {
				t.Fatalf("TransferCalendarCollection() = %#v, %v", result, err)
			}
			resultID := result.Calendar.ID
			writes := make(map[int64]int)
			rows, err := pool.QueryContext(ctx, `SELECT calendar_id FROM calendar_writes`)
			if err != nil {
				t.Fatal(err)
			}
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					t.Fatal(err)
				}
				writes[id]++
			}
			rows.Close()
			if writes[resultID] != 1 {
				t.Errorf("the resulting collection's row was written %d times for %d members, want once", writes[resultID], len(members))
			}
			if writes[fixture.sourceID] != 0 {
				t.Errorf("the source collection's row was written %d times, want none", writes[fixture.sourceID])
			}

			stored, err := s.Calendars.GetByID(ctx, resultID)
			if err != nil || stored == nil {
				t.Fatalf("load resulting calendar: %#v, %v", stored, err)
			}
			// A new calendar's ctag starts at 1, and each member written moves it.
			if want := int64(1 + len(members)); stored.CTag != want {
				t.Errorf("resulting collection's ctag = %d, want %d", stored.CTag, want)
			}
			var stale int
			if err := pool.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE calendar_id=$1 AND last_modified > $2`, resultID, stored.UpdatedAt).Scan(&stale); err != nil {
				t.Fatal(err)
			}
			if stale != 0 {
				t.Errorf("%d members stamped after the resulting collection's sync token", stale)
			}
			changes, err := s.Events.ListChangesAfter(ctx, resultID, SyncCursor{}, true, len(members)+10)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes.Members) != len(members) || changes.More {
				t.Errorf("a sync from the start reports %d members (more %v), want all %d", len(changes.Members), changes.More, len(members))
			}
			changes, err = s.Events.ListChangesAfter(ctx, resultID, SyncCursorAt(stored.UpdatedAt), true, len(members)+10)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes.Members) != 0 || len(changes.Removals) != 0 {
				t.Errorf("a sync from the resulting token reports %d members and %d removals, want none", len(changes.Members), len(changes.Removals))
			}

			if operation != CalendarCollectionCopy {
				return
			}
			// A COPY over the collection it made before leaves a ctag a client
			// polling that collection has not seen.
			again := fixture.transfer(t, s, operation, members)
			again.ExpectedDestination = CalendarCollectionState{Exists: true, ID: resultID, CTag: stored.CTag}
			again.DestinationStatePath = calendarCollectionStatePath(resultID)
			replaced, err := s.TransferCalendarCollection(ctx, again)
			if err != nil || replaced == nil || replaced.Calendar == nil {
				t.Fatalf("second TransferCalendarCollection() = %#v, %v", replaced, err)
			}
			if replaced.Calendar.CTag <= stored.CTag {
				t.Errorf("COPY over a collection at ctag %d left ctag %d", stored.CTag, replaced.Calendar.CTag)
			}
		})
	}
}

// A transaction defers the CTag moves of one calendar's member writes, named
// by its id, and no other calendar's; any other value of the setting, on
// included, defers nothing; and the setting ends with the transaction, however
// it ends.
func TestPostgres_CalendarCTagDeferralCoversOneCalendarForOneTransaction(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	pool := s.Calendars.(*calendarRepo).pool
	_, deferredID := newPostgresCalendar(t, s, "ctag-deferred")
	_, otherID := newPostgresCalendar(t, s, "ctag-other")
	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctagOf := func(q interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}, id int64) int64 {
		t.Helper()
		var ctag int64
		if err := q.QueryRowContext(ctx, `SELECT ctag FROM calendars WHERE id=$1`, id).Scan(&ctag); err != nil {
			t.Fatal(err)
		}
		return ctag
	}
	insert := func(q interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}, calendarID int64, uid string) {
		t.Helper()
		if _, err := q.ExecContext(ctx, `INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag) VALUES ($1, $2, $2, 'BEGIN:VCALENDAR', 'etag')`, calendarID, uid); err != nil {
			t.Fatal(err)
		}
	}
	setting := func() string {
		t.Helper()
		var value sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT current_setting('calcard.defer_calendar_ctag', true)`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value.String
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	deferredBefore, otherBefore := ctagOf(tx, deferredID), ctagOf(tx, otherID)
	if err := deferCalendarCTagTx(ctx, tx, deferredID, func() error {
		insert(tx, deferredID, "deferred-1")
		insert(tx, deferredID, "deferred-2")
		insert(tx, otherID, "other-1")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := ctagOf(tx, deferredID); got != deferredBefore {
		t.Errorf("deferred calendar's ctag moved from %d to %d", deferredBefore, got)
	}
	if got := ctagOf(tx, otherID); got != otherBefore+1 {
		t.Errorf("another calendar's ctag = %d, want %d", got, otherBefore+1)
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('calcard.defer_calendar_ctag', 'on', true)`); err != nil {
		t.Fatal(err)
	}
	insert(tx, deferredID, "deferred-3")
	if got := ctagOf(tx, deferredID); got != deferredBefore+1 {
		t.Errorf("with the setting on, a write moved the ctag to %d, want %d", got, deferredBefore+1)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	failure := errors.New("member write failed")
	tx, err = conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := deferCalendarCTagTx(ctx, tx, deferredID, func() error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("deferCalendarCTagTx() error = %v, want %v", err, failure)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if value := setting(); value == fmt.Sprint(deferredID) {
		t.Fatalf("the deferral outlived its transaction: setting = %q", value)
	}
	before := ctagOf(conn, deferredID)
	insert(conn, deferredID, "after")
	if got := ctagOf(conn, deferredID); got != before+1 {
		t.Errorf("a write after the transaction moved the ctag from %d to %d, want %d", before, got, before+1)
	}
}

// A COPY or MOVE that writes no member over an existing collection still
// leaves a ctag past the replaced collection's, so a client polling that
// location does not take the replacement for the collection it held.
func TestPostgres_CalendarCollectionTransferOfNoMembersMovesPastTheReplacedCTag(t *testing.T) {
	for _, operation := range []CalendarCollectionTransferOperation{CalendarCollectionCopy, CalendarCollectionMove} {
		t.Run(string(operation), func(t *testing.T) {
			s := newPostgresStore(t)
			ctx := context.Background()
			userID, sourceID := newPostgresCalendar(t, s, "empty-source-"+string(operation))
			slug := "dest"
			dest, err := s.Calendars.Create(ctx, Calendar{UserID: userID, Name: "Dest", Slug: &slug})
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 4; i++ {
				uid := fmt.Sprintf("dest-%d", i)
				if _, err := s.PutCalendarObject(ctx, CalendarObjectWrite{CalendarID: dest.ID, UID: uid, ResourceName: uid, RawICAL: postgresCalendarObject(uid, "Dest"), ETag: uid}); err != nil {
					t.Fatal(err)
				}
			}
			dest, err = s.Calendars.GetByID(ctx, dest.ID)
			if err != nil || dest == nil {
				t.Fatalf("reload destination: %#v, %v", dest, err)
			}
			source, err := s.Calendars.GetByID(ctx, sourceID)
			if err != nil || source == nil {
				t.Fatalf("load source: %#v, %v", source, err)
			}
			transfer := CalendarCollectionTransfer{
				Operation: operation, Depth: "infinity", Overwrite: true,
				SourceID: sourceID, ExpectedSourceCTag: source.CTag, SourceStatePath: calendarCollectionStatePath(sourceID),
				DestinationOwnerID: userID, DestinationSlug: slug,
				ExpectedDestination:  CalendarCollectionState{Exists: true, ID: dest.ID, CTag: dest.CTag},
				DestinationStatePath: calendarCollectionStatePath(dest.ID),
			}
			if operation == CalendarCollectionCopy {
				transfer.Depth = "0"
			}
			result, err := s.TransferCalendarCollection(ctx, transfer)
			if err != nil || result == nil || result.Calendar == nil {
				t.Fatalf("TransferCalendarCollection() = %#v, %v", result, err)
			}
			if result.Calendar.CTag <= dest.CTag {
				t.Errorf("%s of no members over a collection at ctag %d left ctag %d", operation, dest.CTag, result.Calendar.CTag)
			}
		})
	}
}

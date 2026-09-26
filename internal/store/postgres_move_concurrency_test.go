package store

import (
	"context"
	"fmt"
	"testing"
)

// oppositeMoveRounds is how many pairs of opposite-direction moves each test
// races. One pair only deadlocks when both reach their first collection row
// before either reaches its second, so a single round proves little.
const oppositeMoveRounds = 25

func calendarMove(from, to int64, uid string) CalendarObjectTransfer {
	raw := postgresCalendarObject(uid, uid)
	return CalendarObjectTransfer{
		Operation:               CalendarObjectMove,
		SourceCalendarID:        from,
		SourceUID:               uid,
		SourceResourceName:      uid,
		ExpectedSourceETag:      "etag-" + uid,
		ExpectedSourceRaw:       raw,
		DestinationCalendarID:   to,
		DestinationResourceName: uid,
		Overwrite:               true,
		RawICAL:                 raw,
		ETag:                    "etag-" + uid,
		SourceStatePath:         fmt.Sprintf("/dav/calendars/%d/%s.ics", from, uid),
		DestinationStatePath:    fmt.Sprintf("/dav/calendars/%d/%s.ics", to, uid),
		LockPreconditions: []LockPrecondition{
			{ResourcePath: fmt.Sprintf("/dav/calendars/%d/%s.ics", from, uid)},
			{ResourcePath: fmt.Sprintf("/dav/calendars/%d", from)},
			{ResourcePath: fmt.Sprintf("/dav/calendars/%d/%s.ics", to, uid)},
			{ResourcePath: fmt.Sprintf("/dav/calendars/%d", to)},
		},
	}
}

func seedCalendarObject(t *testing.T, s *Store, calendarID int64, uid string) {
	t.Helper()
	if _, err := s.PutCalendarObject(context.Background(), CalendarObjectWrite{
		CalendarID: calendarID, UID: uid, ResourceName: uid, RawICAL: postgresCalendarObject(uid, uid), ETag: "etag-" + uid,
	}); err != nil {
		t.Fatalf("seed %s: %v", uid, err)
	}
}

// Two MOVEs between the same pair of calendars in opposite directions touch
// the same two collection rows. Whatever path each takes, they must reach
// those rows in one order, or PostgreSQL aborts one of them as a deadlock.
func TestPostgres_OppositeCalendarMovesDoNotDeadlock(t *testing.T) {
	type mover func(s *Store, from, to int64, uid string) error
	davMove := func(s *Store, from, to int64, uid string) error {
		_, err := s.TransferCalendarObject(context.Background(), calendarMove(from, to, uid))
		return err
	}
	uiMove := func(s *Store, from, to int64, uid string) error {
		return s.UpdateAndMoveEventAndState(context.Background(), from, Event{
			CalendarID:   to,
			UID:          uid,
			ResourceName: uid,
			RawICAL:      postgresCalendarObject(uid, uid+" edited"),
			ETag:         "edited-" + uid,
		}, fmt.Sprintf("/dav/calendars/%d/%s", from, uid), fmt.Sprintf("/dav/calendars/%d/%s", to, uid))
	}

	for _, tt := range []struct {
		name             string
		forward, reverse mover
	}{
		{name: "DAV MOVE against DAV MOVE", forward: davMove, reverse: davMove},
		{name: "UI move against DAV MOVE", forward: uiMove, reverse: davMove},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newPostgresStore(t)
			_, first := newPostgresCalendar(t, s, "opposite-first")
			_, second := newPostgresCalendar(t, s, "opposite-second")
			for round := 0; round < oppositeMoveRounds; round++ {
				forwardUID := fmt.Sprintf("forward-%d", round)
				reverseUID := fmt.Sprintf("reverse-%d", round)
				seedCalendarObject(t, s, first, forwardUID)
				seedCalendarObject(t, s, second, reverseUID)

				errs := runConcurrently(2, func(i int) error {
					if i == 0 {
						return tt.forward(s, first, second, forwardUID)
					}
					return tt.reverse(s, second, first, reverseUID)
				})
				for i, err := range errs {
					if err != nil {
						t.Fatalf("round %d mover %d: %v", round, i, err)
					}
				}
			}
		})
	}
}

func TestPostgres_OppositeContactMovesDoNotDeadlock(t *testing.T) {
	s := newPostgresStore(t)
	ctx := context.Background()
	user, err := s.Users.UpsertOAuthUser(ctx, "opposite-contacts", "opposite-contacts@example.test", "Test", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	first, err := s.AddressBooks.Create(ctx, AddressBook{UserID: user.ID, Name: "First"})
	if err != nil {
		t.Fatalf("create first address book: %v", err)
	}
	second, err := s.AddressBooks.Create(ctx, AddressBook{UserID: user.ID, Name: "Second"})
	if err != nil {
		t.Fatalf("create second address book: %v", err)
	}

	seed := func(bookID int64, uid string) DAVResourceState {
		t.Helper()
		result, err := s.PutContactObject(ctx, ContactObjectWrite{
			AddressBookID: bookID, UID: uid, ResourceName: uid,
			RawVCard: "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + uid + "\r\nEND:VCARD\r\n",
			ETag:     "etag-" + uid,
		})
		if err != nil {
			t.Fatalf("seed %s: %v", uid, err)
		}
		return ContactDAVResourceState(result.Contact)
	}
	move := func(from, to int64, uid string, source DAVResourceState) error {
		return s.MoveContactAndState(ctx, from, to, uid, uid,
			fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", from, uid), fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", to, uid), "",
			ContactTransferExpectation{Source: source, Destination: DAVResourceState{}, Overwrite: true},
			[]LockPrecondition{
				{ResourcePath: fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", from, uid)},
				{ResourcePath: fmt.Sprintf("/dav/addressbooks/%d", from)},
				{ResourcePath: fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", to, uid)},
				{ResourcePath: fmt.Sprintf("/dav/addressbooks/%d", to)},
			})
	}

	for round := 0; round < oppositeMoveRounds; round++ {
		forwardUID := fmt.Sprintf("forward-%d", round)
		reverseUID := fmt.Sprintf("reverse-%d", round)
		forward := seed(first.ID, forwardUID)
		reverse := seed(second.ID, reverseUID)
		errs := runConcurrently(2, func(i int) error {
			if i == 0 {
				return move(first.ID, second.ID, forwardUID, forward)
			}
			return move(second.ID, first.ID, reverseUID, reverse)
		})
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d mover %d: %v", round, i, err)
			}
		}
	}
}

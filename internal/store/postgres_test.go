package store

import (
	"context"
	"maps"
	"path"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
)

func TestParseICalFields(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Test Event
DTSTART:20231225
DTEND:20231226
END:VEVENT
END:VCALENDAR`

	summary, _, _, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Test Event" {
		t.Errorf("expected summary 'Test Event', got %v", summary)
	}
	if dtstart == nil {
		t.Fatal("expected dtstart to be set")
	}
	if dtstart.Year() != 2023 || dtstart.Month() != 12 || dtstart.Day() != 25 {
		t.Errorf("unexpected dtstart: %v", dtstart)
	}
	if !allDay {
		t.Error("expected allDay to be true for YYYYMMDD format")
	}
	if dtend == nil || dtend.Day() != 26 {
		t.Errorf("unexpected dtend: %v", dtend)
	}
}

func TestParseICalFieldsWithTime(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Meeting
DTSTART:20231225T140000Z
DTEND:20231225T150000Z
END:VEVENT
END:VCALENDAR`

	summary, _, _, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Meeting" {
		t.Errorf("expected summary 'Meeting', got %v", summary)
	}
	if dtstart == nil {
		t.Fatal("expected dtstart to be set")
	}
	if dtstart.Hour() != 14 || dtstart.Minute() != 0 {
		t.Errorf("unexpected dtstart time: %v", dtstart)
	}
	if allDay {
		t.Error("expected allDay to be false for datetime format")
	}
	if dtend == nil || dtend.Hour() != 15 {
		t.Errorf("unexpected dtend: %v", dtend)
	}
}

func TestParseICalFieldsWithTZID(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Meeting East
DTSTART;TZID=America/New_York:20240201T120000
DTEND;TZID=America/New_York:20240201T130000
END:VEVENT
END:VCALENDAR`

	summary, _, _, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Meeting East" {
		t.Errorf("expected summary 'Meeting East', got %v", summary)
	}
	if dtstart == nil || dtend == nil {
		t.Fatalf("expected both dtstart and dtend to be set")
	}
	if allDay {
		t.Fatal("expected allDay to be false for TZID datetime")
	}

	if got := dtstart.In(time.UTC); got.Hour() != 17 || got.Minute() != 0 {
		t.Errorf("expected dtstart 17:00 UTC, got %v", got)
	}
	if got := dtend.In(time.UTC); got.Hour() != 18 || got.Minute() != 0 {
		t.Errorf("expected dtend 18:00 UTC, got %v", got)
	}
}

func TestParseICalFieldsWithOffset(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Offset Event
DTSTART:20240201T120000-0500
DTEND:20240201T123000-0500
END:VEVENT
END:VCALENDAR`

	summary, _, _, dtstart, dtend, allDay := parseICalFields(ical)

	if summary == nil || *summary != "Offset Event" {
		t.Errorf("expected summary 'Offset Event', got %v", summary)
	}
	if dtstart == nil || dtend == nil {
		t.Fatalf("expected both dtstart and dtend to be set")
	}
	if allDay {
		t.Fatal("expected allDay to be false for offset datetime")
	}

	if got := dtstart.UTC(); got.Hour() != 17 || got.Minute() != 0 {
		t.Errorf("expected dtstart 17:00 UTC, got %v", got)
	}
	if got := dtend.UTC(); got.Hour() != 17 || got.Minute() != 30 {
		t.Errorf("expected dtend 17:30 UTC, got %v", got)
	}
}

func TestParseICalFieldsWithFoldedLines(t *testing.T) {
	// Lines are folded by CRLF followed by space - the unfold regex handles this
	ical := "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nSUMMARY:Long summary\r\nDTSTART:20231225\r\nEND:VEVENT\r\nEND:VCALENDAR"

	summary, _, _, _, _, _ := parseICalFields(ical)

	expected := "Long summary"
	if summary == nil || *summary != expected {
		t.Errorf("expected summary %q, got %v", expected, summary)
	}
}

func TestParseICalFieldsDescriptionLocation(t *testing.T) {
	ical := `BEGIN:VCALENDAR
BEGIN:VEVENT
SUMMARY:Team sync
DESCRIPTION:Quarterly planning\, bring notes
LOCATION:Room 4B
DTSTART:20231225T140000Z
END:VEVENT
END:VCALENDAR`

	_, description, location, _, _, _ := parseICalFields(ical)

	if description == nil || *description != "Quarterly planning, bring notes" {
		t.Errorf("expected unescaped description, got %v", description)
	}
	if location == nil || *location != "Room 4B" {
		t.Errorf("expected location 'Room 4B', got %v", location)
	}
}

func TestParseICalFieldsPreservesWhitespaceAtFoldBoundary(t *testing.T) {
	ical := "BEGIN:VCALENDAR\r\n" +
		"BEGIN:VEVENT\r\n" +
		"DESCRIPTION:Room A,\r\n" +
		"  second floor \r\n" +
		"END:VEVENT\r\n" +
		"END:VCALENDAR\r\n"

	_, description, _, _, _, _ := parseICalFields(ical)
	if description == nil || *description != "Room A, second floor " {
		t.Fatalf("description = %v, want %q", description, "Room A, second floor ")
	}
}

func TestParseVCardFields(t *testing.T) {
	vcard := `BEGIN:VCARD
VERSION:3.0
FN:John Doe
EMAIL:john@example.com
END:VCARD`

	displayName, primaryEmail, birthday := parseVCardFields(vcard)

	if displayName == nil || *displayName != "John Doe" {
		t.Errorf("expected displayName 'John Doe', got %v", displayName)
	}
	if primaryEmail == nil || *primaryEmail != "john@example.com" {
		t.Errorf("expected primaryEmail 'john@example.com', got %v", primaryEmail)
	}
	if birthday != nil {
		t.Errorf("expected birthday to be nil, got %v", birthday)
	}
}

func TestParseVCardFieldsMultipleEmails(t *testing.T) {
	vcard := `BEGIN:VCARD
VERSION:3.0
FN:Jane Smith
EMAIL;TYPE=work:jane.work@example.com
EMAIL;TYPE=home:jane.home@example.com
END:VCARD`

	displayName, primaryEmail, _ := parseVCardFields(vcard)

	if displayName == nil || *displayName != "Jane Smith" {
		t.Errorf("expected displayName 'Jane Smith', got %v", displayName)
	}
	// Should return the first email
	if primaryEmail == nil || *primaryEmail != "jane.work@example.com" {
		t.Errorf("expected primaryEmail 'jane.work@example.com', got %v", primaryEmail)
	}
}

func TestParseVCardFieldsEscapedCharacters(t *testing.T) {
	vcard := `BEGIN:VCARD
VERSION:3.0
FN:John\, Jr. Doe
END:VCARD`

	displayName, _, _ := parseVCardFields(vcard)

	if displayName == nil || *displayName != "John, Jr. Doe" {
		t.Errorf("expected displayName 'John, Jr. Doe', got %v", displayName)
	}
}

func TestParseVCardFieldsWithBirthday(t *testing.T) {
	testCases := []struct {
		name      string
		vcard     string
		wantYear  int
		wantMonth int
		wantDay   int
	}{
		{
			name: "YYYY-MM-DD format",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:John Doe
BDAY:1990-05-15
END:VCARD`,
			wantYear: 1990, wantMonth: 5, wantDay: 15,
		},
		{
			name: "YYYYMMDD format",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:Jane Doe
BDAY:19850720
END:VCARD`,
			wantYear: 1985, wantMonth: 7, wantDay: 20,
		},
		{
			name: "--MM-DD format (no year)",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:Bob Smith
BDAY:--03-10
END:VCARD`,
			wantYear: NoYearBirthdayYear, wantMonth: 3, wantDay: 10,
		},
		{
			name: "--MMDD format (no year)",
			vcard: `BEGIN:VCARD
VERSION:4.0
FN:Bob Smith
BDAY:--1231
END:VCARD`,
			wantYear: NoYearBirthdayYear, wantMonth: 12, wantDay: 31,
		},
		{
			name: "February 29 without a year survives",
			vcard: `BEGIN:VCARD
VERSION:3.0
FN:Leap Day
BDAY:--02-29
END:VCARD`,
			wantYear: NoYearBirthdayYear, wantMonth: 2, wantDay: 29,
		},
		{
			name:     "Apple's omitted year",
			vcard:    "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Apple\r\nBDAY;X-APPLE-OMIT-YEAR=1604;VALUE=date:1604-02-29\r\nEND:VCARD\r\n",
			wantYear: NoYearBirthdayYear, wantMonth: 2, wantDay: 29,
		},
		{
			name:     "Apple's omit-year parameter naming a different year keeps the date",
			vcard:    "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Apple\r\nBDAY;X-APPLE-OMIT-YEAR=1604:1990-02-01\r\nEND:VCARD\r\n",
			wantYear: 1990, wantMonth: 2, wantDay: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, birthday := parseVCardFields(tc.vcard)

			if birthday == nil {
				t.Fatal("expected birthday to be set")
			}
			if birthday.Year() != tc.wantYear {
				t.Errorf("expected year %d, got %d", tc.wantYear, birthday.Year())
			}
			if int(birthday.Month()) != tc.wantMonth {
				t.Errorf("expected month %d, got %d", tc.wantMonth, birthday.Month())
			}
			if birthday.Day() != tc.wantDay {
				t.Errorf("expected day %d, got %d", tc.wantDay, birthday.Day())
			}
		})
	}
}

func TestDAVPathLocksHoldTheParentCollectionExclusively(t *testing.T) {
	tests := []struct {
		name          string
		resourcePaths []string
		want          map[string]bool
	}{
		{
			name:          "member write holds its collection and shares the roots",
			resourcePaths: []string{"/dav/calendars/5/x.ics"},
			want: map[string]bool{
				"/dav":                   false,
				"/dav/calendars":         false,
				"/dav/calendars/5":       true,
				"/dav/calendars/5/x.ics": true,
			},
		},
		{
			name:          "collection target shares the roots",
			resourcePaths: []string{"/dav/calendars/5"},
			want: map[string]bool{
				"/dav":             false,
				"/dav/calendars":   false,
				"/dav/calendars/5": true,
			},
		},
		{
			name:          "a per-kind root named as a target is exclusive",
			resourcePaths: []string{"/dav/calendars"},
			want: map[string]bool{
				"/dav":           false,
				"/dav/calendars": true,
			},
		},
		{
			name:          "a target that is also another target's ancestor is taken once",
			resourcePaths: []string{"/dav/calendars/5", "/dav/calendars/5/x.ics"},
			want: map[string]bool{
				"/dav":                   false,
				"/dav/calendars":         false,
				"/dav/calendars/5":       true,
				"/dav/calendars/5/x.ics": true,
			},
		},
		{
			name:          "duplicate targets are acquired once",
			resourcePaths: []string{"/dav/calendars/5/x.ics", "/dav/calendars/5/x.ics/"},
			want: map[string]bool{
				"/dav":                   false,
				"/dav/calendars":         false,
				"/dav/calendars/5":       true,
				"/dav/calendars/5/x.ics": true,
			},
		},
		{
			name:          "a pending collection holds its owner's pending scope only",
			resourcePaths: []string{"/dav/calendars/.pending/4/work"},
			want: map[string]bool{
				"/dav":                           false,
				"/dav/calendars":                 false,
				"/dav/calendars/.pending":        false,
				"/dav/calendars/.pending/4":      true,
				"/dav/calendars/.pending/4/work": true,
			},
		},
		{
			name:          "a cross-collection move holds both collections",
			resourcePaths: []string{"/dav/addressbooks/5/a.vcf", "/dav/addressbooks/9/a.vcf"},
			want: map[string]bool{
				"/dav":                      false,
				"/dav/addressbooks":         false,
				"/dav/addressbooks/5":       true,
				"/dav/addressbooks/5/a.vcf": true,
				"/dav/addressbooks/9":       true,
				"/dav/addressbooks/9/a.vcf": true,
			},
		},
		{
			name:          "no paths means no locks",
			resourcePaths: nil,
			want:          map[string]bool{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := davPathLocks(tt.resourcePaths...)
			gotModes := make(map[string]bool, len(got))
			for i, lock := range got {
				if lock.key != davPathLockKey(lock.path) {
					t.Fatalf("davPathLocks(%q)[%d] key %d does not match its path %q", tt.resourcePaths, i, lock.key, lock.path)
				}
				if i > 0 && got[i-1].key >= lock.key {
					t.Fatalf("davPathLocks(%q) is not in ascending key order: %+v", tt.resourcePaths, got)
				}
				gotModes[lock.path] = lock.exclusive
			}
			if !maps.Equal(gotModes, tt.want) {
				t.Fatalf("davPathLocks(%q) = %v, want %v", tt.resourcePaths, gotModes, tt.want)
			}
		})
	}
}

// An ACL write serializes on the resource's state path, and the transaction
// that wraps it can name the enclosing collection as well. Taking such a path
// shared as an ancestor and later exclusively as a target would let two
// transactions holding it shared deadlock on the upgrade, so every target has
// to come out of one pass already marked exclusive.
func TestDAVPathLocksAssignOneModePerNestedStatePath(t *testing.T) {
	for _, resourcePath := range []string{"/dav/addressbooks/5/alice.vcf", "/dav/calendars/5/x.ics"} {
		targets := []string{resourcePath, path.Dir(resourcePath)}
		locks := davPathLocks(targets...)
		modes := make(map[string]bool, len(locks))
		for _, lock := range locks {
			if _, duplicate := modes[lock.path]; duplicate {
				t.Fatalf("davPathLocks(%q) acquires %q twice: %+v", targets, lock.path, locks)
			}
			modes[lock.path] = lock.exclusive
		}
		for _, target := range targets {
			if !modes[path.Clean(target)] {
				t.Fatalf("davPathLocks(%q) takes target %q shared; every target must be exclusive: %+v", targets, target, locks)
			}
		}
	}
}

// SetACLAndState writes the ACL at the resource's state path, so that path is
// part of its exclusive set and has to be held before it reads the
// state its preconditions are checked against. One acquisition covers the whole
// transaction whether or not the request carried lock preconditions: a second
// one would decide the exclusive set again, and reads taken between the two
// would not be serialized by either. The collection is only share-locked
// against deletion, never compared by CTag, so a sibling's write cannot fail it.
func TestSetACLAndStateTakesOneOrderedLockSet(t *testing.T) {
	const resourcePath = "/dav/addressbooks/7/alice.vcf"
	const addressBookID = int64(7)
	lockSet := map[string]bool{
		"/dav":                          false,
		"/dav/addressbooks":             false,
		"/dav/addressbooks/7":           true,
		"/dav/addressbooks/7/alice.vcf": true,
	}

	tests := []struct {
		name          string
		preconditions []LockPrecondition
		acl           *ACLGuard
	}{
		{
			name: "request without lock preconditions",
		},
		{
			name: "request with a lock precondition and an ACL guard",
			preconditions: []LockPrecondition{{
				ResourcePath: resourcePath,
				LookupPaths:  []string{resourcePath, "/dav/addressbooks/7"},
			}},
			acl: NewACLGuard([]string{resourcePath, "/dav/addressbooks/7"}, nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New() error = %v", err)
			}
			defer db.Close()

			mock.ExpectBegin()
			expectDAVPathLocks(mock, lockSet)
			if len(tt.preconditions) != 0 {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT token, resource_path, depth, expires_at FROM locks WHERE resource_path = ANY($1) AND expires_at > NOW() ORDER BY created_at`)).
					WithArgs(pq.Array(tt.preconditions[0].LookupPaths)).
					WillReturnRows(sqlmock.NewRows([]string{"token", "resource_path", "depth", "expires_at"}))
			}
			if tt.acl != nil {
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT resource_path, principal_href, is_grant, privilege, ace_order FROM acl_entries WHERE resource_path = ANY($1)`)).
					WithArgs(pq.Array([]string{resourcePath, "/dav/addressbooks/7"})).
					WillReturnRows(sqlmock.NewRows([]string{"resource_path", "principal_href", "is_grant", "privilege", "ace_order"}))
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT 1 FROM address_books WHERE id=$1 FOR KEY SHARE`)).
				WithArgs(addressBookID).
				WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(1))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE resource_path = $1 ORDER BY ace_order, id`)).
				WithArgs(resourcePath).
				WillReturnRows(sqlmock.NewRows([]string{"id", "resource_path", "principal_href", "is_grant", "privilege", "ace_order", "created_at"}))
			mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE resource_path = $1`)).
				WithArgs(resourcePath).
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at) VALUES ($1, $2, $3, $4, $5, $6)`)).
				WithArgs(resourcePath, "/dav/principals/2/", true, "read", 0, sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectExec(regexp.QuoteMeta(`UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
				WithArgs(addressBookID).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(regexp.QuoteMeta(`UPDATE contacts SET last_modified = NOW() WHERE address_book_id = $1 AND object_acl_path = ANY($2)`)).
				WithArgs(addressBookID, pq.Array([]string{resourcePath})).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			err = New(db).SetACLAndState(context.Background(), resourcePath,
				[]ACLEntry{{PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"}},
				ACLResourceExpectation{
					CollectionKind: "addressbook",
					CollectionID:   addressBookID,
					ACL:            tt.acl,
				}, tt.preconditions)
			if err != nil {
				t.Fatalf("SetACLAndState() error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sql expectations: %v", err)
			}
		})
	}
}

// Revoking a share can remove grants from many members of one collection. Their
// sync state is touched in one statement per table, not one pair per member.
func TestRevokePrincipalGrantsTouchesMembersInOneBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	defer db.Close()

	const collectionPath = "/dav/calendars/4"
	principals := pq.Array([]string{"/dav/principals/9/", "/dav/principals/9"})
	privileges := pq.Array([]string{"read"})
	mock.ExpectBegin()
	expectDAVPathLocks(mock, map[string]bool{
		"/dav":             false,
		"/dav/calendars":   false,
		"/dav/calendars/4": true,
	})
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT DISTINCT resource_path FROM acl_entries WHERE`)).
		WithArgs(principals, collectionPath, `/dav/calendars/4/%`, privileges).
		WillReturnRows(sqlmock.NewRows([]string{"resource_path"}).
			AddRow("/dav/calendars/4/a").
			AddRow("/dav/calendars/4/b.ics").
			AddRow("/dav/calendars/4/c"))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM acl_entries WHERE`)).
		WithArgs(principals, collectionPath, `/dav/calendars/4/%`, privileges).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = $1`)).
		WithArgs(int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE events SET last_modified = NOW() WHERE calendar_id = $1 AND object_acl_path = ANY($2)`)).
		WithArgs(int64(4), pq.Array([]string{"/dav/calendars/4/a", "/dav/calendars/4/b.ics", "/dav/calendars/4/c"})).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec(`DELETE FROM locks WHERE user_id=\$1`).
		WithArgs(int64(9), collectionPath, `/dav/calendars/4/%`, int64(4)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	if err := (&aclRepo{pool: db}).RevokePrincipalGrants(context.Background(), collectionPath, "/dav/principals/9/", []string{"read"}); err != nil {
		t.Fatalf("RevokePrincipalGrants() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestPrincipalHrefSpellingsAreSymmetric(t *testing.T) {
	for _, href := range []string{"/dav/principals/9", "/dav/principals/9/"} {
		got := principalHrefSpellings(href)
		slices.Sort(got)
		if want := []string{"/dav/principals/9", "/dav/principals/9/"}; !slices.Equal(got, want) {
			t.Fatalf("principalHrefSpellings(%q) = %q, want %q", href, got, want)
		}
	}
}

func TestParseVCardFieldsRejectsInvalidBirthdays(t *testing.T) {
	for _, value := range []string{"--02-30", "--1301", "1990-02-30", "19900230", "not a date", "1990-05-15Tx"} {
		_, _, birthday := parseVCardFields("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:X\r\nBDAY:" + value + "\r\nEND:VCARD\r\n")
		if birthday != nil {
			t.Errorf("BDAY:%s parsed as %v, want no birthday", value, birthday)
		}
	}
}

// An escaped backslash is one character; it must not combine with the letter
// after it into an escaped line break.
func TestParseVCardFieldsUnescapesDisplayNameInOnePass(t *testing.T) {
	for raw, want := range map[string]string{
		`C:\\new`:      `C:\new`,
		`Smith\, John`: "Smith, John",
		`A\;B`:         "A;B",
		`Line\nBreak`:  "Line\nBreak",
		`Line\NBreak`:  "Line\nBreak",
		`trailing\\`:   `trailing\`,
		`\\\\server`:   `\\server`,
	} {
		displayName, _, _ := parseVCardFields("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:" + raw + "\r\nEND:VCARD\r\n")
		if displayName == nil || *displayName != want {
			t.Errorf("FN:%s decoded to %v, want %q", raw, displayName, want)
		}
	}
}

// A lock is on the resource whose state path it is stored at. One stored at
// target.ics is on the member named target.ics, so it binds a write to that
// member and not one to the member named target, even when a caller looks the
// lock up for it.
func TestLockPreconditionsBindOnlyTheLockedPath(t *testing.T) {
	for _, tt := range []struct{ target, sibling string }{
		{target: "/dav/calendars/1/target", sibling: "/dav/calendars/1/target.ics"},
		{target: "/dav/addressbooks/1/alice", sibling: "/dav/addressbooks/1/alice.vcf"},
	} {
		lock := Lock{Token: "opaquelocktoken:sibling", ResourcePath: tt.sibling, Depth: "0", ExpiresAt: time.Now().Add(time.Hour)}
		onTarget := LockPrecondition{ResourcePath: tt.target, LookupPaths: []string{tt.target, tt.sibling}}
		if !LockPreconditionsSatisfied([]LockPrecondition{onTarget}, []Lock{lock}) {
			t.Errorf("a depth-0 lock on %s binds a write to %s", tt.sibling, tt.target)
		}
		onSibling := LockPrecondition{ResourcePath: tt.sibling, LookupPaths: []string{tt.sibling}}
		if LockPreconditionsSatisfied([]LockPrecondition{onSibling}, []Lock{lock}) {
			t.Errorf("a depth-0 lock on %s did not bind a write to it without its token", tt.sibling)
		}
		onSibling.Tokens = []string{lock.Token}
		if !LockPreconditionsSatisfied([]LockPrecondition{onSibling}, []Lock{lock}) {
			t.Errorf("the token of the lock on %s did not satisfy a write to it", tt.sibling)
		}
	}
}

// A writer that hands the store no metadata gets the columns the DAV layer's
// analysis derives: dates read through the object's own VTIMEZONE, and the end
// the reports read the event with.
func TestParseICalFieldsReadsDatesAsTheReportsDo(t *testing.T) {
	const outlookZone = "BEGIN:VTIMEZONE\r\nTZID:Eastern Standard Time\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
		"END:VTIMEZONE\r\n"
	object := func(properties string) string {
		return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + outlookZone + "BEGIN:VEVENT\r\nUID:x\r\n" + properties + "END:VEVENT\r\nEND:VCALENDAR\r\n"
	}
	at := func(value string) *time.Time {
		parsed, err := time.Parse("20060102T150405Z", value)
		if err != nil {
			t.Fatal(err)
		}
		return &parsed
	}
	tests := []struct {
		name               string
		properties         string
		wantStart, wantEnd *time.Time
	}{
		{"zone only the object defines",
			"DTSTART;TZID=\"Eastern Standard Time\":20240304T090000\r\nDTEND;TZID=\"Eastern Standard Time\":20240304T100000\r\n",
			at("20240304T140000Z"), at("20240304T150000Z")},
		{"DATE-TIME DTEND before DTSTART", "DTSTART:20250301T100000Z\r\nDTEND:20250301T080000Z\r\n",
			at("20250301T100000Z"), at("20250301T100000Z")},
		{"DATE DTEND equal to DTSTART", "DTSTART;VALUE=DATE:20250301\r\nDTEND;VALUE=DATE:20250301\r\n",
			at("20250301T000000Z"), at("20250302T000000Z")},
		{"unusable DTEND beside a DURATION", "DTSTART:20250301T100000Z\r\nDTEND:20250301T080000Z\r\nDURATION:PT3H\r\n",
			at("20250301T100000Z"), nil},
		{"no DTEND on a DATE-TIME start", "DTSTART:20250301T100000Z\r\n", at("20250301T100000Z"), at("20250301T100000Z")},
		{"no DTEND on a DATE start", "DTSTART;VALUE=DATE:20250301\r\n", at("20250301T000000Z"), at("20250302T000000Z")},
		{"no DTEND beside a DURATION", "DTSTART;VALUE=DATE:20250301\r\nDURATION:P2D\r\n", at("20250301T000000Z"), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, dtstart, dtend, _ := parseICalFields(object(tt.properties))
			if dtstart == nil || !dtstart.Equal(*tt.wantStart) {
				t.Errorf("dtstart = %v, want %v", dtstart, tt.wantStart)
			}
			if (dtend == nil) != (tt.wantEnd == nil) || dtend != nil && !dtend.Equal(*tt.wantEnd) {
				t.Errorf("dtend = %v, want %v", dtend, tt.wantEnd)
			}
		})
	}
}

// A lock path prefix is read as the byte range text_pattern_ops sorts every
// path starting with it into, which an index serves whatever the plan.
func TestLockPathPrefixRangeHoldsExactlyThePathsStartingWithIt(t *testing.T) {
	for _, prefix := range []string{"/dav/calendars/42/", "/dav/calendars/4%_\\/", "/dav/addressbooks/7/café", "/dav/x~é"} {
		low, high, bounded := lockPathPrefixRange(prefix)
		if !bounded || low != prefix {
			t.Fatalf("lockPathPrefixRange(%q) = %q, %q, %t", prefix, low, high, bounded)
		}
		for _, path := range []string{prefix, prefix + "a", prefix + "ÿÿ", prefix + "\U0010FFFF"} {
			if path < low || path >= high {
				t.Errorf("%q, which starts with %q, lies outside [%q, %q)", path, prefix, low, high)
			}
		}
	}
	if _, _, bounded := lockPathPrefixRange(""); bounded {
		t.Error("the empty prefix has an upper bound")
	}
}

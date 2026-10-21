package ui

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/contacts"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/events"
	"github.com/jw6ventures/calcard/internal/store"
)

func newUIPostgresStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the access-surface comparison")
	}
	schema := fmt.Sprintf("calcard_ui_test_%d", time.Now().UnixNano())
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		admin.Close()
		t.Fatalf("parse CALCARD_TEST_POSTGRES_DSN: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	pool, err := sql.Open("postgres", parsed.String())
	if err != nil {
		admin.Close()
		t.Fatalf("open schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	schemaSQL, err := os.ReadFile(filepath.Join("..", "..", "db.sql"))
	if err != nil {
		t.Fatalf("read db.sql: %v", err)
	}
	if _, err := pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply db.sql: %v", err)
	}
	pool.SetMaxOpenConns(5)
	return store.New(pool)
}

// accessCase is one member, the entries stored at its own state path, and
// whether the sharee may read it. Several members share spellings of one
// name, so each one's entries sit at another's sibling spelling too.
type accessCase struct {
	name     string
	entries  []store.ACLEntry
	readable bool
}

// Every surface that decides whether a principal may read a member decides
// it as DAV does, from the same ACL: DAV's request for the member and its
// collection listing, the REST read and listing, the UI's calendar view and
// dashboard, and the SQL deciders behind the dashboard's recent events and
// the address book listing. Each member is decided by the entries at its own
// state path alone, whatever its siblings' ACLs say, and an entry naming the
// principal by its href without the trailing slash is that principal's.
func TestPostgres_EverySurfaceDecidesAMemberAsDAVDoes(t *testing.T) {
	database := newUIPostgresStore(t)
	ctx := t.Context()
	newUser := func(name string) *store.User {
		t.Helper()
		user, err := database.Users.UpsertOAuthUser(ctx, name, name+"@example.test", name, name)
		if err != nil {
			t.Fatal(err)
		}
		return user
	}
	owner, sharee := newUser("owner"), newUser("sharee")
	shareeHref := acl.PrincipalHref(sharee.ID)
	bareHref := strings.TrimSuffix(shareeHref, "/")
	entry := func(principal string, grant bool, privilege string, position int) store.ACLEntry {
		return store.ACLEntry{PrincipalHref: principal, IsGrant: grant, Privilege: privilege, Position: position}
	}
	davServer := dav.NewDavServer(dav.Options{Store: database})
	serve := func(method, target, body, depth string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		if depth != "" {
			req.Header.Set("Depth", depth)
		}
		req = req.WithContext(auth.WithUser(req.Context(), sharee))
		rr := httptest.NewRecorder()
		davServer.ServeHTTP(rr, req)
		return rr
	}
	listed := func(collectionPath string) map[string]bool {
		t.Helper()
		rr := serve("PROPFIND", collectionPath+"/", `<?xml version="1.0" encoding="utf-8"?><d:propfind xmlns:d="DAV:"><d:prop><d:getetag/></d:prop></d:propfind>`, "1")
		hrefs := map[string]bool{}
		for _, part := range strings.Split(rr.Body.String(), "<d:href>")[1:] {
			href, _, _ := strings.Cut(part, "</d:href>")
			if decoded, err := url.PathUnescape(href); err == nil {
				hrefs[decoded] = true
			}
		}
		return hrefs
	}

	t.Run("calendar", func(t *testing.T) {
		cal, err := database.Calendars.Create(ctx, store.Calendar{UserID: owner.ID, Name: "Surfaces"})
		if err != nil {
			t.Fatal(err)
		}
		collectionPath := fmt.Sprintf("/dav/calendars/%d", cal.ID)
		if err := database.ACLEntries.SetACL(ctx, collectionPath, []store.ACLEntry{entry(shareeHref, true, "read", 0)}); err != nil {
			t.Fatal(err)
		}
		cases := []accessCase{
			{name: "plain", readable: true},
			{name: "X", entries: []store.ACLEntry{entry(shareeHref, false, "read", 0)}, readable: false},
			{name: "X.ics", readable: true},
			{name: "X.ICS", entries: []store.ACLEntry{entry(bareHref, false, "read", 0)}, readable: false},
			{name: "Y.ics", entries: []store.ACLEntry{entry(shareeHref, false, "all", 0)}, readable: false},
			{name: "Y", readable: true},
			{name: "dotted.name", entries: []store.ACLEntry{entry(acl.PrincipalAuthenticated, false, "read", 0), entry(shareeHref, true, "read", 1)}, readable: false},
			{name: "uid@example.com", entries: []store.ACLEntry{entry(shareeHref, true, "read", 0), entry(acl.PrincipalAuthenticated, false, "read", 1)}, readable: true},
			{name: "padded ", entries: []store.ACLEntry{entry(shareeHref, false, "read", 0)}, readable: false},
			{name: "padded", readable: true},
		}
		// Entries at a spelling no member is stored under apply to none.
		if err := database.ACLEntries.SetACL(ctx, collectionPath+"/plain.ics", []store.ACLEntry{entry(shareeHref, false, "read", 0)}); err != nil {
			t.Fatal(err)
		}
		for _, tc := range cases {
			uid := "uid-" + tc.name
			raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Surfaces//EN\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260115T100000Z\r\nSUMMARY:x\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
			if _, err := database.Events.Upsert(ctx, store.Event{CalendarID: cal.ID, UID: uid, ResourceName: tc.name, RawICAL: raw, ETag: uid}); err != nil {
				t.Fatal(err)
			}
			if tc.entries != nil {
				if err := database.ACLEntries.SetACL(ctx, store.MemberStatePath(collectionPath, tc.name), tc.entries); err != nil {
					t.Fatal(err)
				}
			}
		}

		access, err := database.Calendars.GetAccessible(ctx, cal.ID, sharee.ID)
		if err != nil || access == nil {
			t.Fatalf("GetAccessible() = %v, %v", access, err)
		}
		all, err := database.Events.ListForCalendar(ctx, cal.ID)
		if err != nil {
			t.Fatal(err)
		}
		handler := NewHandler(&config.Config{}, database, nil, dav.ValidateCalendarObject, dav.EventInstances)
		calendarView, err := handler.filterReadableCalendarEvents(ctx, sharee, access, all)
		if err != nil {
			t.Fatal(err)
		}
		recent, err := database.Events.ListRecentByUser(ctx, sharee.ID, 1000)
		if err != nil {
			t.Fatal(err)
		}
		service := events.NewService(database, dav.ValidateCalendarObject)
		restList, err := service.ListEvents(ctx, sharee, cal.ID, store.EventFilter{})
		if err != nil {
			t.Fatal(err)
		}
		davListing := listed(collectionPath)
		hasUID := func(list []store.Event, uid string) bool {
			return slices.ContainsFunc(list, func(e store.Event) bool { return e.UID == uid })
		}

		for _, tc := range cases {
			uid := "uid-" + tc.name
			href := collectionPath + "/" + tc.name + ".ics"
			_, restErr := service.GetEvent(ctx, sharee, cal.ID, uid)
			stored := all[slices.IndexFunc(all, func(e store.Event) bool { return e.UID == uid })]
			dashboard, err := handler.canReadCalendarEvent(ctx, sharee, access, stored)
			if err != nil {
				t.Fatal(err)
			}
			surfaces := map[string]bool{
				"DAV GET":           serve(http.MethodGet, (&url.URL{Path: href}).EscapedPath(), "", "").Code == http.StatusOK,
				"DAV listing":       davListing[href],
				"REST get":          restErr == nil,
				"REST list":         hasUID(restList, uid),
				"UI calendar view":  hasUID(calendarView, uid),
				"UI dashboard":      dashboard,
				"SQL recent events": hasUID(recent, uid),
			}
			for surface, readable := range surfaces {
				if readable != tc.readable {
					t.Errorf("%s decides member %q readable %t, want %t", surface, tc.name, readable, tc.readable)
				}
			}
		}
	})

	contactCard := func(uid, name string) string {
		return "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:" + uid + "\r\nFN:" + name + "\r\nEND:VCARD\r\n"
	}
	contactUID := func(name string) string { return "uid-" + strings.ReplaceAll(name, ".", "-") }
	service := contacts.NewService(database)

	t.Run("shared address book", func(t *testing.T) {
		book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: "Shared"})
		if err != nil {
			t.Fatal(err)
		}
		collectionPath := fmt.Sprintf("/dav/addressbooks/%d", book.ID)
		if err := database.ACLEntries.SetACL(ctx, collectionPath, []store.ACLEntry{entry(shareeHref, true, "read", 0)}); err != nil {
			t.Fatal(err)
		}
		cases := []accessCase{
			{name: "dav.VCF", entries: []store.ACLEntry{entry(shareeHref, false, "read", 0)}, readable: false},
			{name: "dav", readable: true},
			{name: "dav.vcf", entries: []store.ACLEntry{entry(bareHref, false, "read", 0)}, readable: false},
			{name: "c", readable: true},
		}
		if err := database.ACLEntries.SetACL(ctx, collectionPath+"/c.vcf", []store.ACLEntry{entry(shareeHref, false, "read", 0)}); err != nil {
			t.Fatal(err)
		}
		for _, tc := range cases {
			uid := contactUID(tc.name)
			if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: uid, ResourceName: tc.name, RawVCard: contactCard(uid, tc.name), ETag: uid}); err != nil {
				t.Fatal(err)
			}
			if tc.entries != nil {
				if err := database.ACLEntries.SetACL(ctx, store.MemberStatePath(collectionPath, tc.name), tc.entries); err != nil {
					t.Fatal(err)
				}
			}
		}
		davListing := listed(collectionPath)
		page, err := service.ListContactsPage(ctx, sharee, *book, 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range cases {
			uid := contactUID(tc.name)
			href := collectionPath + "/" + tc.name + ".vcf"
			_, restErr := service.GetContact(ctx, sharee, book.ID, uid)
			surfaces := map[string]bool{
				"DAV GET":          serve(http.MethodGet, (&url.URL{Path: href}).EscapedPath(), "", "").Code == http.StatusOK,
				"DAV listing":      davListing[href],
				"REST and UI get":  restErr == nil,
				"REST and UI list": slices.ContainsFunc(page.Items, func(c store.Contact) bool { return c.UID == uid }),
			}
			for surface, readable := range surfaces {
				if readable != tc.readable {
					t.Errorf("%s decides contact %q readable %t, want %t", surface, tc.name, readable, tc.readable)
				}
			}
		}
	})

	t.Run("address book shared through its contacts", func(t *testing.T) {
		// Each contact sits alone in a book that grants the sharee nothing, so
		// the SQL listing of accessible books shows the book exactly when the
		// contact's own ACL lets the sharee read it.
		cases := []accessCase{
			{name: "dav.VCF", entries: []store.ACLEntry{entry(shareeHref, true, "read", 0)}, readable: true},
			{name: "dav", readable: false},
			{name: "c.vcf", entries: []store.ACLEntry{entry(bareHref, true, "read", 0)}, readable: true},
			{name: "denied", entries: []store.ACLEntry{entry(shareeHref, false, "read", 0), entry(acl.PrincipalAll, true, "read", 1)}, readable: false},
		}
		siblings := map[string][]store.ACLEntry{
			"dav":     {entry(shareeHref, true, "read", 0)},
			"dav.vcf": {entry(shareeHref, true, "read", 0)},
			"c":       {entry(shareeHref, true, "read", 0)},
		}
		books := map[string]store.AddressBook{}
		for _, tc := range cases {
			book, err := database.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: "Book " + tc.name})
			if err != nil {
				t.Fatal(err)
			}
			books[tc.name] = *book
			collectionPath := fmt.Sprintf("/dav/addressbooks/%d", book.ID)
			uid := contactUID(tc.name)
			if _, err := database.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: uid, ResourceName: tc.name, RawVCard: contactCard(uid, tc.name), ETag: uid}); err != nil {
				t.Fatal(err)
			}
			if tc.entries != nil {
				if err := database.ACLEntries.SetACL(ctx, store.MemberStatePath(collectionPath, tc.name), tc.entries); err != nil {
					t.Fatal(err)
				}
			}
			for name, entries := range siblings {
				if name == tc.name {
					continue
				}
				if err := database.ACLEntries.SetACL(ctx, store.MemberStatePath(collectionPath, name), entries); err != nil {
					t.Fatal(err)
				}
			}
		}
		accessible, err := database.AddressBooks.ListAccessible(ctx, sharee.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range cases {
			book := books[tc.name]
			uid := contactUID(tc.name)
			href := fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", book.ID, tc.name)
			_, restErr := service.GetContact(ctx, sharee, book.ID, uid)
			page, err := service.ListContactsPage(ctx, sharee, book, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			surfaces := map[string]bool{
				"DAV GET":                     serve(http.MethodGet, (&url.URL{Path: href}).EscapedPath(), "", "").Code == http.StatusOK,
				"REST and UI get":             restErr == nil,
				"REST and UI list":            slices.ContainsFunc(page.Items, func(c store.Contact) bool { return c.UID == uid }),
				"SQL accessible-book listing": slices.ContainsFunc(accessible, func(b store.AddressBook) bool { return b.ID == book.ID }),
			}
			for surface, readable := range surfaces {
				if readable != tc.readable {
					t.Errorf("%s decides contact %q readable %t, want %t", surface, tc.name, readable, tc.readable)
				}
			}
		}
	})
}

package dav

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/jw6ventures/jw6-go-utils/database/migration"
	"github.com/lib/pq"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// upgradeFixture is a database a release left behind, upgraded to v1.2.0
// through the migration runner the server starts with, and the WARNING
// notices the upgrade raised.
type upgradeFixture struct {
	t        *testing.T
	pool     *sql.DB
	mu       sync.Mutex
	warnings []string
}

// newReleaseDatabase installs the schema file a release shipped in a fresh
// schema, and, for a release that ran migrations on first start, runs them
// through the runner stamping releaseVersion.
func newReleaseDatabase(t *testing.T, schemaFile, releaseVersion string, shipped []string) *upgradeFixture {
	t.Helper()
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the upgrade tests")
	}
	schema := fmt.Sprintf("calcard_upgrade_test_%d", time.Now().UnixNano())
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
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	connector, err := pq.NewConnector(parsed.String())
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	f := &upgradeFixture{t: t}
	f.pool = sql.OpenDB(pq.ConnectorWithNoticeHandler(connector, func(notice *pq.Error) {
		if notice.Severity == "WARNING" {
			f.mu.Lock()
			f.warnings = append(f.warnings, notice.Message)
			f.mu.Unlock()
		}
	}))
	t.Cleanup(func() {
		f.pool.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	schemaSQL, err := os.ReadFile(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("install %s: %v", schemaFile, err)
	}
	if len(shipped) != 0 {
		dir := t.TempDir()
		for _, name := range shipped {
			contents, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), contents, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		f.migrate(dir, releaseVersion)
	}
	return f
}

func (f *upgradeFixture) migrate(dir, version string) {
	f.t.Helper()
	runner := migration.NewManager(f.pool, dir, version)
	if err := runner.Initialize(); err != nil {
		f.t.Fatalf("initialize migrations for %s: %v", version, err)
	}
	if err := runner.MigrateDatabase(); err != nil {
		f.t.Fatalf("migrate to %s: %v", version, err)
	}
}

func (f *upgradeFixture) exec(query string, args ...any) {
	f.t.Helper()
	if _, err := f.pool.Exec(query, args...); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f *upgradeFixture) id(query string, args ...any) int64 {
	f.t.Helper()
	var id int64
	if err := f.pool.QueryRow(query, args...).Scan(&id); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
	return id
}

// storedACL reads every stored ACL entry with the order an upgrade gives the
// entries at one path: denials first, then by age.
func (f *upgradeFixture) storedACL() []store.ACLEntry {
	f.t.Helper()
	rows, err := f.pool.Query(`SELECT id, resource_path, principal_href, is_grant, privilege, created_at FROM acl_entries`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var entries []store.ACLEntry
	for rows.Next() {
		var entry store.ACLEntry
		if err := rows.Scan(&entry.ID, &entry.ResourcePath, &entry.PrincipalHref, &entry.IsGrant, &entry.Privilege, &entry.CreatedAt); err != nil {
			f.t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		f.t.Fatal(err)
	}
	slices.SortFunc(entries, func(a, b store.ACLEntry) int {
		return cmp.Or(compareGrants(a.IsGrant, b.IsGrant), a.CreatedAt.Compare(b.CreatedAt), cmp.Compare(a.ID, b.ID))
	})
	return entries
}

func compareGrants(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

// aclRows is the stored ACL as comparable rows, for checking that a second
// run of the upgrade changes nothing.
func (f *upgradeFixture) aclRows() []string {
	f.t.Helper()
	rows, err := f.pool.Query(`SELECT resource_path || ' ' || principal_href || ' ' || is_grant::text || ' ' || privilege || ' ' || ace_order::text FROM acl_entries ORDER BY 1`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			f.t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}

// upgradeMember is a stored calendar object or contact, by its collection's
// path and its stored name.
type upgradeMember struct {
	collection, name, ext string
}

func (m upgradeMember) statePath() string { return m.collection + "/" + m.name }

// releasedReaders lists, for each reader of a v1.1.x release or a v1.2.0
// release candidate, the paths whose entries it decided the member's
// privileges from besides its own, given every path that holds entries. Each
// reader is modelled from the release's code, not from the repair: a DAV
// request, which read the member's path less trailing white space with the
// extension removed where it ends the name, and that with the extension
// added; a GET, which read the same for the name less its last extension of
// any kind; a DAV listing, the REST API and the UI, which read the trimmed
// name and that name with the extension removed or added; the v1.1.x SQL
// deciders, which read the name, that less a lower-case .ics, and that with
// .ics; and the release candidates' SQL deciders, which read every path that
// less one .ics or .vcf in any case is the name less its extension in any
// case.
func (m upgradeMember) releasedReaders(storedPaths []string) map[string][]string {
	stem := func(name string) string { return strings.TrimSuffix(name, path.Ext(name)) }
	ownExt := func(name string) bool { return strings.EqualFold(path.Ext(name), m.ext) }
	request := strings.TrimRightFunc(m.name, unicode.IsSpace)
	withExtension := func(base string) []string {
		paths := []string{m.collection + "/" + base}
		if stem(base) != "" {
			paths = append(paths, m.collection+"/"+base+m.ext)
		}
		return paths
	}
	requestBase := request
	if ownExt(request) && stem(request) != "" {
		requestBase = stem(request)
	}
	stored := request
	if stem(request) != "" {
		stored = stem(request)
	}
	if ownExt(stored) && stem(stored) != "" {
		stored = stem(stored)
	}
	listed := strings.TrimSpace(m.name)
	listing := []string{m.collection + "/" + listed}
	switch {
	case !ownExt(listed):
		listing = append(listing, m.collection+"/"+listed+m.ext)
	case stem(listed) != "":
		listing = append(listing, m.collection+"/"+stem(listed))
	default:
		listing = append(listing, m.collection, m.collection+"/")
	}
	readers := map[string][]string{
		"DAV request": withExtension(requestBase),
		"DAV GET":     withExtension(stored),
		"DAV listing": listing,
	}
	if m.ext == ".ics" {
		base := strings.TrimSuffix(m.name, ".ics")
		readers["v1.1.x SQL"] = []string{m.collection + "/" + m.name, m.collection + "/" + base, m.collection + "/" + base + ".ics"}
	}
	candidateExtension := regexp.MustCompile(`(?i)\.(ics|vcf)$`)
	key := m.collection + "/" + regexp.MustCompile(`(?i)\`+m.ext+`$`).ReplaceAllString(m.name, "")
	var candidate []string
	for _, storedPath := range storedPaths {
		if candidateExtension.ReplaceAllString(storedPath, "") == key {
			candidate = append(candidate, storedPath)
		}
	}
	readers["release candidate SQL"] = candidate
	return readers
}

// readerDecision is the decision one released reader gave user's privilege
// on the member: every entry at the member's own path and the paths it read,
// a denial among them refusing, then its collection's, then nothing. It
// reports whether the member's entries decided it, and whether they denied.
func readerDecision(entries []store.ACLEntry, member upgradeMember, paths []string, ownerID int64, user *store.User, privilege string) (granted, deniedByMember bool) {
	if user.ID == ownerID {
		return true, false
	}
	principals := acl.ApplicablePrincipals(user)
	read := map[string]bool{member.statePath(): true}
	for _, readPath := range paths {
		read[readPath] = true
	}
	var memberEntries []store.ACLEntry
	for _, entry := range entries {
		if read[entry.ResourcePath] {
			memberEntries = append(memberEntries, entry)
		}
	}
	slices.SortStableFunc(memberEntries, func(a, b store.ACLEntry) int { return compareGrants(a.IsGrant, b.IsGrant) })
	if granted, decided := acl.DecisionForPrivilege(memberEntries, principals, privilege); decided {
		return granted, !granted
	}
	var collectionEntries []store.ACLEntry
	for _, entry := range entries {
		if entry.ResourcePath == member.collection {
			collectionEntries = append(collectionEntries, entry)
		}
	}
	granted, _ = acl.DecisionForPrivilege(collectionEntries, principals, privilege)
	return granted, false
}

var upgradeDecisionPrivileges = []string{"read", "read-free-busy", "write-content", "write-properties", "bind", "unbind", "write", "read-acl", "write-acl"}

// seededUpgrade is the data an upgrade test seeds: who owns what, the members
// with both spellings of their names, and their ACLs.
type seededUpgrade struct {
	owner, sharee, stranger, other *store.User
	calendarID                     int64
	calendarPath, bookPath         string
	members                        []upgradeMember
	// untouched is a member no reader read any other path for.
	untouched upgradeMember
	// narrowed lists, by member state path and principal, where the upgrade
	// deliberately refuses what every reader granted before: a denial stored
	// at a sibling's path, carried ahead of the member's own grant, and grants
	// that applied to a member only through another spelling of its name.
	narrowed map[[2]string]bool
}

// seedBothSpellings stores, in the schema a release left, a calendar holding
// the events X, which a CalDAV client named X.ics, and X.ics, which the UI
// imported, together with names whose sibling spellings hold entries no
// member is stored under, names only one reader aliased, and an address book
// holding the contacts dav and dav.VCF; ACLs at each spelling, denials among
// them; members whose state paths coincide or whose names resolve elsewhere;
// and a lock on X.ics.
func seedBothSpellings(f *upgradeFixture) seededUpgrade {
	newUser := func(name string) *store.User {
		return &store.User{ID: f.id(`INSERT INTO users (oauth_subject, primary_email) VALUES ($1, $1 || '@example.test') RETURNING id`, name)}
	}
	s := seededUpgrade{owner: newUser("owner"), sharee: newUser("sharee"), stranger: newUser("stranger"), other: newUser("other")}
	s.calendarID = f.id(`INSERT INTO calendars (user_id, name) VALUES ($1, 'Release') RETURNING id`, s.owner.ID)
	bookID := f.id(`INSERT INTO address_books (user_id, name) VALUES ($1, 'Release') RETURNING id`, s.owner.ID)
	s.calendarPath, s.bookPath = fmt.Sprintf("/dav/calendars/%d", s.calendarID), fmt.Sprintf("/dav/addressbooks/%d", bookID)
	event := func(uid, name string) {
		raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Upgrade//EN\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260115T100000Z\r\nSUMMARY:x\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		f.exec(`INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag) VALUES ($1, $2, $3, $4, $2)`, s.calendarID, uid, name, raw)
	}
	for _, name := range []string{"X", "X.ics", "Solo.ics", "Plain", "Shout.ICS", "Case", "Case.ICS", "Mixed", "a.b", "Upper", "Lone"} {
		event("uid-"+name, name)
		member := upgradeMember{collection: s.calendarPath, name: name, ext: ".ics"}
		if name == "Lone" {
			s.untouched = member
			continue
		}
		s.members = append(s.members, member)
	}
	// Two members at one state path, and names a release's path handling
	// resolved to another path.
	event("Dup", "")
	event("uid-dup", "Dup")
	event("uid-dot", ".")
	event("uid-query", "q?x")
	for _, name := range []string{"dav", "dav.VCF"} {
		uid := "uid-" + strings.ReplaceAll(name, ".", "-")
		f.exec(`INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag) VALUES ($1, $2, $3, $4, $2)`,
			bookID, uid, name, "BEGIN:VCARD\r\nVERSION:3.0\r\nUID:"+uid+"\r\nFN:"+name+"\r\nEND:VCARD\r\n")
		s.members = append(s.members, upgradeMember{collection: s.bookPath, name: name, ext: ".vcf"})
	}
	sharee, stranger, other := acl.PrincipalHref(s.sharee.ID), acl.PrincipalHref(s.stranger.ID), acl.PrincipalHref(s.other.ID)
	age := 0
	ace := func(resourcePath, principal string, grant bool, privilege string) {
		age++
		f.exec(`INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, created_at) VALUES ($1, $2, $3, $4, NOW() - make_interval(days => 100 - $5))`,
			resourcePath, principal, grant, privilege, age)
	}
	ace(s.calendarPath, sharee, true, "read")
	ace(s.calendarPath, stranger, true, "read")
	ace(s.calendarPath, other, true, "read")
	// X's own ACL, which X.ics read too.
	ace(s.calendarPath+"/X", stranger, false, "read")
	// X.ics's own ACL, which X read too. Its grant to the stranger answered
	// ahead of X's denial for X.ics, and X's denial ahead of it for X; its
	// grant to the other user stays X.ics's alone.
	ace(s.calendarPath+"/X.ics", stranger, true, "read")
	ace(s.calendarPath+"/X.ics", sharee, false, "write-content")
	ace(s.calendarPath+"/X.ics", strings.TrimSuffix(other, "/"), false, "read")
	ace(s.calendarPath+"/X.ics", other, true, "write-content")
	// Entries no member is stored under, each read by one member alone.
	ace(s.calendarPath+"/Solo", sharee, false, "read")
	ace(s.calendarPath+"/Solo", other, true, "write")
	ace(s.calendarPath+"/Plain.ics", other, true, "write-content")
	ace(s.calendarPath+"/Shout.ics", sharee, true, "write")
	ace(s.calendarPath+"/Shout.ics", stranger, false, "read")
	// Read by one reader alone: the v1.1.x SQL deciders read Shout.ICS.ics
	// for Shout.ICS, and the release candidates' read Case.ICS, a member's
	// own path, and Mixed.Ics for Case and Mixed.
	ace(s.calendarPath+"/Shout.ICS.ics", sharee, false, "write-content")
	ace(s.calendarPath+"/Case.ICS", stranger, false, "read")
	ace(s.calendarPath+"/Mixed.Ics", other, false, "read")
	ace(s.calendarPath+"/Mixed.ics", sharee, false, "write-content")
	ace(s.calendarPath+"/Mixed.ics", sharee, false, "bind")
	// Grants one reader other than a DAV request read: a GET of a.b read a,
	// and the release candidates' SQL deciders, which never ran on a v1.1.9
	// database, read Upper.ICS for Upper.
	ace(s.calendarPath+"/a", sharee, true, "all")
	ace(s.calendarPath+"/Upper.ICS", sharee, true, "write-content")
	// An entry no member is stored under or read.
	ace(s.calendarPath+"/Gone", sharee, false, "read")
	ace(s.bookPath, sharee, true, "read")
	ace(s.bookPath, stranger, true, "read")
	// dav's own ACL, which dav.VCF read too, and entries at dav.vcf, which
	// both read and no contact is stored under.
	ace(s.bookPath+"/dav", sharee, false, "read")
	ace(s.bookPath+"/dav.vcf", sharee, true, "write")
	ace(s.bookPath+"/dav.vcf", stranger, false, "read")
	f.exec(`INSERT INTO locks (token, resource_path, user_id, depth, expires_at) VALUES ('opaquelocktoken:upgrade', $1, $2, '0', NOW() + INTERVAL '1 hour')`,
		s.calendarPath+"/X.ics", s.sharee.ID)
	// State at the path of the member ".", which DAV requests cleaned to the
	// collection while the SQL deciders read it as stored: the member's own
	// denial, a grant only those deciders read, and a lock below the
	// collection that would refuse the owner's DELETE of it.
	ace(s.calendarPath+"/.", stranger, false, "read")
	ace(s.calendarPath+"/.", stranger, true, "write")
	f.exec(`INSERT INTO locks (token, resource_path, user_id, depth, expires_at) VALUES ('opaquelocktoken:dot', $1, $2, '0', NOW() + INTERVAL '1 hour')`,
		s.calendarPath+"/.", s.owner.ID)

	s.narrowed = map[[2]string]bool{
		// X's denial now answers ahead of X.ics's own grant to the stranger.
		{s.calendarPath + "/X.ics", stranger}: true,
		// X.ics's grant to the other user no longer reaches X.
		{s.calendarPath + "/X", other}: true,
		// dav.vcf's grant reached both contacts, so neither keeps it.
		{s.bookPath + "/dav", sharee}:     true,
		{s.bookPath + "/dav.VCF", sharee}: true,
	}
	return s
}

// The v1.2.0 upgrade gives every member its own ACL at its state path. It
// carries to a member the entries any released reader read for it at another
// path that it may keep -- every denial, ahead of its own entries, and grants
// no other member read -- and leaves no principal more access to any member
// than a reader gave it: a denial any reader applied still applies, nothing is
// granted that no reader granted, and what every reader granted is still
// granted, except where a denial or grant reached a member only through
// another spelling of its name. It keeps every entry it removes or withholds,
// stamps the members whose decisions changed and moves their collection once,
// runs once, reports what it carried, withheld and left, and leaves nothing
// aliased.
func TestPostgres_UpgradeGivesEachMemberItsOwnACL(t *testing.T) {
	for _, release := range []struct {
		name       string
		schemaFile string
		version    string
		shipped    []string
		deadProps  bool
	}{
		{name: "v1.1.9", schemaFile: filepath.Join("..", "store", "testdata", "schema_v1.1.9_release.sql"), version: "v1.1.9",
			shipped: []string{"v1.0.11.sql", "v1.0.12.sql", "v1.1.3.sql", "v1.1.4.sql"}},
		{name: "v1.1.8 tag (v1.2.0-rc6)", schemaFile: filepath.Join("..", "store", "testdata", "schema_v1.1.8_tag.sql"), deadProps: true},
	} {
		t.Run(release.name, func(t *testing.T) {
			f := newReleaseDatabase(t, release.schemaFile, release.version, release.shipped)
			s := seedBothSpellings(f)
			if release.deadProps {
				for _, seed := range [][2]string{{s.calendarPath + "/X", "X's"}, {s.calendarPath + "/X.ics", "X.ics's"}, {s.calendarPath + "/Solo", "nobody's"}, {s.calendarPath + "/.", "the collection's, cleaned"}} {
					f.exec(`INSERT INTO dav_dead_properties (resource_path, namespace_uri, local_name, inner_xml) VALUES ($1, 'urn:x', 'tag', $2)`, seed[0], seed[1])
				}
			}
			before := f.storedACL()
			storedPaths := map[string]bool{}
			for _, entry := range before {
				storedPaths[entry.ResourcePath] = true
			}
			var paths []string
			for storedPath := range storedPaths {
				paths = append(paths, storedPath)
			}
			var ctagBefore int64
			var syncToken time.Time
			if err := f.pool.QueryRow(`UPDATE calendars SET updated_at = clock_timestamp() WHERE id = $1 RETURNING ctag, updated_at`, s.calendarID).Scan(&ctagBefore, &syncToken); err != nil {
				t.Fatal(err)
			}

			f.migrate(filepath.Join("..", "..", "migrations"), "v1.2.0")

			database := store.New(f.pool)
			h := NewDavServer(Options{Store: database})
			ctx := context.Background()
			after := f.storedACL()
			for _, member := range s.members {
				requestPath := requestPathForCanonical(member.statePath())
				readers := member.releasedReaders(paths)
				for _, user := range []*store.User{s.sharee, s.stranger, s.other} {
					for _, privilege := range upgradeDecisionPrivileges {
						is, err := h.checkACLPrivilege(ctx, user, requestPath, privilege)
						if err != nil {
							t.Fatal(err)
						}
						allGranted, requestGranted := true, false
						for reader, readPaths := range readers {
							granted, denied := readerDecision(before, member, readPaths, s.owner.ID, user, privilege)
							allGranted = allGranted && granted
							if reader == "DAV request" {
								requestGranted = granted
							}
							if denied && is {
								t.Errorf("%s: user %d keeps %s on %s, which the %s reader denied", release.name, user.ID, privilege, member.name, reader)
							}
						}
						switch {
						case is && !requestGranted:
							t.Errorf("%s: user %d gained %s on %s, which a DAV request was not granted", release.name, user.ID, privilege, member.name)
						case allGranted && !is && !s.narrowed[[2]string{member.statePath(), acl.PrincipalHref(user.ID)}]:
							t.Errorf("%s: user %d lost %s on %s, which every reader granted", release.name, user.ID, privilege, member.name)
						}
					}
				}
			}

			// The narrowings are the ones the repair reports.
			for _, check := range []struct {
				member    string
				user      *store.User
				privilege string
				want      bool
			}{
				{s.calendarPath + "/X.ics", s.stranger, "read", false},
				{s.calendarPath + "/X.ics", s.sharee, "write-content", false},
				{s.calendarPath + "/X.ics", s.other, "write-content", true},
				{s.calendarPath + "/X", s.sharee, "write-content", false},
				{s.calendarPath + "/X", s.stranger, "read", false},
				{s.calendarPath + "/X", s.other, "write-content", false},
				{s.calendarPath + "/Solo.ics", s.sharee, "read", false},
				{s.calendarPath + "/Solo.ics", s.other, "write", true},
				{s.calendarPath + "/Plain", s.other, "write-content", true},
				{s.calendarPath + "/Shout.ICS", s.sharee, "write", false},
				{s.calendarPath + "/Shout.ICS", s.sharee, "bind", true},
				{s.calendarPath + "/Shout.ICS", s.stranger, "read", false},
				{s.calendarPath + "/Case", s.stranger, "read", false},
				{s.calendarPath + "/Mixed", s.other, "read", false},
				{s.calendarPath + "/Mixed", s.sharee, "bind", false},
				{s.calendarPath + "/a.b", s.sharee, "write-content", false},
				{s.calendarPath + "/a.b", s.sharee, "read", true},
				{s.calendarPath + "/Upper", s.sharee, "write-content", false},
				{s.bookPath + "/dav", s.sharee, "read", false},
				{s.bookPath + "/dav", s.stranger, "read", false},
				{s.bookPath + "/dav", s.sharee, "write", false},
				{s.bookPath + "/dav.VCF", s.sharee, "read", false},
				{s.bookPath + "/dav.VCF", s.sharee, "write", false},
			} {
				if got, err := h.checkACLPrivilege(ctx, check.user, requestPathForCanonical(check.member), check.privilege); err != nil || got != check.want {
					t.Errorf("after the upgrade user %d %s on %s = %t (%v), want %t", check.user.ID, check.privilege, check.member, got, err, check.want)
				}
			}

			// Copied denials keep the order of the readers that read them,
			// then their own, ahead of the member's entries.
			if got, want := f.memberACL(s.calendarPath+"/Mixed"), []string{
				"-3 deny " + acl.PrincipalHref(s.sharee.ID) + " write-content",
				"-2 deny " + acl.PrincipalHref(s.sharee.ID) + " bind",
				"-1 deny " + acl.PrincipalHref(s.other.ID) + " read",
			}; !slices.Equal(got, want) {
				t.Errorf("Mixed's ACL is %q, want %q", got, want)
			}

			// Every remaining member entry is a member's own -- the member "."
			// among them -- but for the one no member is stored under or read,
			// which stays and is reported.
			own := map[string]bool{s.calendarPath: true, s.bookPath: true, s.calendarPath + "/Gone": true, s.calendarPath + "/.": true}
			for _, member := range s.members {
				own[member.statePath()] = true
			}
			for _, entry := range after {
				if !own[entry.ResourcePath] {
					t.Errorf("the upgrade left an entry at %s, a spelling no member owns: %#v", entry.ResourcePath, entry)
				}
			}

			// Each entry removed, and each grant withheld from a member that
			// read it, is kept with the reason and the members that read it.
			kept := f.removedEntries()
			for _, want := range []string{
				s.calendarPath + "/Solo deny read stored=false readers=[" + s.calendarPath + "/Solo.ics] denial copied to every member that read it, then removed",
				s.calendarPath + "/Solo grant write stored=false readers=[" + s.calendarPath + "/Solo.ics] grant carried to the one member whose DAV requests read it, then removed",
				s.calendarPath + "/Mixed.Ics deny read stored=false readers=[" + s.calendarPath + "/Mixed] denial copied to every member that read it, then removed",
				s.calendarPath + "/X.ics grant write-content stored=true readers=[" + s.calendarPath + "/X] grant withheld from the members that read it at another spelling of their names; the member stored here keeps it",
				s.bookPath + "/dav.vcf grant write stored=false readers=[" + s.bookPath + "/dav " + s.bookPath + "/dav.VCF] grant withheld from the several members that read it, then removed",
				s.calendarPath + "/a grant all stored=false readers=[" + s.calendarPath + "/a.b] grant withheld from the member that read it, whose DAV requests did not, then removed",
				s.calendarPath + "/Upper.ICS grant write-content stored=false readers=[" + s.calendarPath + "/Upper] grant withheld from the member that read it, whose DAV requests did not, then removed",
				s.calendarPath + "/. grant write stored=false readers=[] grant stored at a path that resolves to another, which no DAV request read, then removed",
			} {
				if !slices.Contains(kept, want) {
					t.Errorf("acl_entries_v120_removed does not keep %q:\n%s", want, strings.Join(kept, "\n"))
				}
			}
			removed := 0
			for _, row := range kept {
				if strings.Contains(row, " stored=false ") {
					removed++
				}
			}
			if removed != 14 {
				t.Errorf("acl_entries_v120_removed keeps %d removed entries, but the upgrade removed 14", removed)
			}

			warnings := strings.Join(f.warnings, "\n")
			for _, want := range []string{
				"copied 13 deny and 3 grant ACL entries from other spellings of their names to 9 members",
				"removed 13 ACL entries stored under no member's name: 7 denials copied to every member that read them, 3 grants carried to the one member whose DAV requests read each, and 3 grants no member keeps; acl_entries_v120_removed keeps each",
				"6 ACL grants no longer apply",
				s.calendarPath + "/X.ics -> " + s.calendarPath + "/X",
				"left 1 ACL entries at member paths no member is stored under (first 50 paths: " + s.calendarPath + "/Gone)",
				"1 active locks now cover only the name they were taken on",
				"1 state paths are each the path of more than one member, one stored without a name at its UID and one stored under that name, which share their ACL entries, locks and dead properties (first 50: " + s.calendarPath + "/Dup)",
				"2 members have stored names that resolve to another path",
				"(first 50: " + s.calendarPath + "/., " + s.calendarPath + "/q?x)",
				"removed 1 locks, 1 ACL grants and " + map[bool]string{true: "1", false: "0"}[release.deadProps] + " dead properties stored at member paths that resolve to another path, which no DAV request read; the denials there stay, and acl_entries_v120_removed keeps the grants (first 50 paths: " + s.calendarPath + "/.)",
			} {
				if !strings.Contains(warnings, want) {
					t.Errorf("the upgrade's warnings do not report %q:\n%s", want, warnings)
				}
			}
			// At the member "."'s own path its denial stays, and the SQL
			// deciders still apply it; its grant, lock and dead property go.
			var dotLocks, dotGrants, dotDenials, dotProperties int
			if err := f.pool.QueryRow(`SELECT (SELECT count(*) FROM locks WHERE resource_path = $1),
       (SELECT count(*) FILTER (WHERE is_grant) FROM acl_entries WHERE resource_path = $1),
       (SELECT count(*) FILTER (WHERE NOT is_grant) FROM acl_entries WHERE resource_path = $1),
       (SELECT count(*) FROM dav_dead_properties WHERE resource_path = $1)`, s.calendarPath+"/.").Scan(&dotLocks, &dotGrants, &dotDenials, &dotProperties); err != nil {
				t.Fatal(err)
			}
			if dotLocks != 0 || dotGrants != 0 || dotDenials != 1 || dotProperties != 0 {
				t.Errorf("at %s/. the upgrade left %d locks, %d grants, %d denials and %d dead properties, want only the denial", s.calendarPath, dotLocks, dotGrants, dotDenials, dotProperties)
			}
			strangerRecent, err := database.Events.ListRecentByUser(ctx, s.stranger.ID, 100)
			if err != nil {
				t.Fatal(err)
			}
			if slices.ContainsFunc(strangerRecent, func(e store.Event) bool { return e.ResourceName == "." }) {
				t.Errorf("the SQL decider lists the member \".\" to the stranger its own denial refuses")
			}
			if release.deadProps && !strings.Contains(warnings, "left 1 dead properties at member paths no member is stored under (first 50 paths: "+s.calendarPath+"/Solo)") {
				t.Errorf("the upgrade does not report the dead property no member reads:\n%s", warnings)
			}

			// Every member whose decisions the repair changed is stamped --
			// one that read entries at another path, or whose own entries a
			// reader passed over -- and the calendar moves once, so a client
			// syncing from before the upgrade reads each again; a member
			// nothing aliased is not.
			var ctagAfter int64
			if err := f.pool.QueryRow(`SELECT ctag FROM calendars WHERE id = $1`, s.calendarID).Scan(&ctagAfter); err != nil {
				t.Fatal(err)
			}
			if ctagAfter != ctagBefore+1 {
				t.Errorf("the calendar's CTag moved from %d to %d, want one move", ctagBefore, ctagAfter)
			}
			changes, err := database.Events.ListChangesAfter(ctx, s.calendarID, store.SyncCursorAt(syncToken), true, 1000)
			if err != nil {
				t.Fatal(err)
			}
			var synced []string
			for _, event := range changes.Members {
				synced = append(synced, event.ResourceName)
			}
			for _, member := range s.members {
				if member.collection == s.calendarPath && !slices.Contains(synced, member.name) {
					t.Errorf("a sync from before the upgrade does not report %s, whose ACL the repair changed: %q", member.name, synced)
				}
			}
			if slices.Contains(synced, s.untouched.name) {
				t.Errorf("a sync from before the upgrade reports %s, which no reader aliased", s.untouched.name)
			}

			// A second run changes nothing and reports nothing.
			rows := f.aclRows()
			f.warnings = nil
			script, err := os.ReadFile(filepath.Join("..", "..", "migrations", "v1.2.0.sql"))
			if err != nil {
				t.Fatal(err)
			}
			f.exec(string(script))
			if again := f.aclRows(); !slices.Equal(rows, again) {
				t.Errorf("a second run changed the ACL:\n%q\nthen\n%q", rows, again)
			}
			if len(f.warnings) != 0 {
				t.Errorf("a second run reported %q", f.warnings)
			}
			if again := f.removedEntries(); !slices.Equal(kept, again) {
				t.Errorf("a second run changed acl_entries_v120_removed:\n%q\nthen\n%q", kept, again)
			}

			// Nothing aliases any more: the SQL decider joins a member's own
			// path, writing X's ACL leaves X.ics's, and the lock X.ics holds
			// leaves its owner free to write X.
			var expression string
			if err := f.pool.QueryRow(`SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE a.attrelid = 'events'::regclass AND a.attname = 'object_acl_path'`).Scan(&expression); err != nil || strings.Contains(expression, "regexp_replace") {
				t.Errorf("events.object_acl_path is generated as %q (%v)", expression, err)
			}
			recent, err := database.Events.ListRecentByUser(ctx, s.stranger.ID, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range s.members {
				if member.collection != s.calendarPath {
					continue
				}
				readable, err := h.checkACLPrivilege(ctx, s.stranger, requestPathForCanonical(member.statePath()), "read")
				if err != nil {
					t.Fatal(err)
				}
				listed := slices.ContainsFunc(recent, func(e store.Event) bool { return e.ResourceName == member.name })
				if listed != readable {
					t.Errorf("the SQL decider lists %s for the stranger %t, DAV decides %t", member.name, listed, readable)
				}
			}
			icsBefore, err := database.ACLEntries.ListByResource(ctx, s.calendarPath+"/X.ics")
			if err != nil {
				t.Fatal(err)
			}
			if err := database.ACLEntries.SetACL(ctx, s.calendarPath+"/X", nil); err != nil {
				t.Fatal(err)
			}
			if icsAfter, err := database.ACLEntries.ListByResource(ctx, s.calendarPath+"/X.ics"); err != nil || len(icsAfter) != len(icsBefore) {
				t.Errorf("clearing X's ACL changed X.ics's from %d to %d entries (%v)", len(icsBefore), len(icsAfter), err)
			}
			owner := &store.User{ID: s.owner.ID}
			put := httptest.NewRequest(http.MethodPut, s.calendarPath+"/X.ics", strings.NewReader(buildCalendarObject(buildVEvent("uid-X", "SUMMARY:edited"))))
			put.Header.Set("Content-Type", "text/calendar")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, put.WithContext(auth.WithUser(put.Context(), owner)))
			if rr.Code != http.StatusNoContent {
				t.Errorf("owner PUT of X beside the lock on X.ics = %d: %s", rr.Code, rr.Body.String())
			}
			if release.deadProps {
				for name, want := range map[string]string{"X": "X's", "X.ics": "X.ics's", "Solo.ics": ""} {
					props, err := database.DeadProperties.ListByResources(ctx, []string{s.calendarPath + "/" + name})
					if err != nil {
						t.Fatal(err)
					}
					got := ""
					if len(props) == 1 {
						got = props[0].InnerXML
					}
					if got != want {
						t.Errorf("dead property of %s = %q, want %q", name, got, want)
					}
				}
			}
		})
	}
}

// memberACL is one path's ACL as the repair ordered it.
func (f *upgradeFixture) memberACL(resourcePath string) []string {
	f.t.Helper()
	rows, err := f.pool.Query(`SELECT ace_order::text || ' ' || CASE WHEN is_grant THEN 'grant' ELSE 'deny' END || ' ' || principal_href || ' ' || privilege FROM acl_entries WHERE resource_path = $1 ORDER BY ace_order, id`, resourcePath)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			f.t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}

// removedEntries reads acl_entries_v120_removed, or nothing where the upgrade
// had no entry to keep and created no table.
func (f *upgradeFixture) removedEntries() []string {
	f.t.Helper()
	var exists bool
	if err := f.pool.QueryRow(`SELECT to_regclass('acl_entries_v120_removed') IS NOT NULL`).Scan(&exists); err != nil {
		f.t.Fatal(err)
	}
	if !exists {
		return nil
	}
	rows, err := f.pool.Query(`
SELECT resource_path || ' ' || CASE WHEN is_grant THEN 'grant' ELSE 'deny' END || ' ' || privilege || ' stored=' || still_stored::text
       || ' readers=[' || array_to_string(read_by, ' ') || '] ' || reason
FROM acl_entries_v120_removed ORDER BY 1`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			f.t.Fatal(err)
		}
		result = append(result, row)
	}
	return result
}

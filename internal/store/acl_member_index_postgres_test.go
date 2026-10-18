package store

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// An object-level ACL decision reads the entries of the one resource it
// decides, at its state path, from an index leading with resource_path. A
// principal that holds entries on many resources is the case that matters: an
// index leading with the principal reads all of them.
func TestPostgres_ObjectACLDecisionReadsItsResourceFromAnIndex(t *testing.T) {
	pool := newPostgresPool(t)
	ctx := context.Background()
	if _, err := pool.ExecContext(ctx, `
INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order)
SELECT '/dav/calendars/1/member-' || n || CASE WHEN n % 2 = 0 THEN '.ics' ELSE '' END, '/dav/principals/2/', n % 3 <> 0, 'read', 0
FROM generate_series(1, 20000) AS n`); err != nil {
		t.Fatalf("seed ACL entries: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `ANALYZE acl_entries`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	plan := explainStatement(t, pool, `SELECT `+aclOrderedBooleanExpr("a.resource_path = '/dav/calendars/1/member-30'", "2", "read", "all"), true)
	usesIndex := false
	walkPlan(plan, func(node map[string]any) {
		if node["Node Type"] == "Seq Scan" {
			t.Errorf("the decision scans %v: %v", node["Relation Name"], plan)
		}
		if name, _ := node["Index Name"].(string); strings.HasPrefix(name, "idx_acl_resource") {
			usesIndex = true
		}
		if rows, _ := node["Actual Rows"].(float64); node["Relation Name"] == "acl_entries" && rows > 10 {
			t.Errorf("the decision reads %v entries for one resource: %v", rows, plan)
		}
	})
	if !usesIndex {
		t.Errorf("the decision does not use an index leading with resource_path: %v", plan)
	}
}

// Setting a member's ACL tells sync and CTag clients of that member alone: the
// one whose state path is the ACL's path exactly, so padding or a dot segment
// in the path names no other member, and a member stored without a name is
// reached through its UID.
func TestPostgres_SetACLTouchesTheMemberAtExactlyItsStatePath(t *testing.T) {
	pool := newPostgresPool(t)
	ctx := context.Background()
	s := New(pool)
	var userID, calendarID int64
	if err := pool.QueryRowContext(ctx, `INSERT INTO users (oauth_subject, primary_email) VALUES ('touch', 'touch@example.test') RETURNING id`).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := pool.QueryRowContext(ctx, `INSERT INTO calendars (user_id, name) VALUES ($1, 'Touch') RETURNING id`, userID).Scan(&calendarID); err != nil {
		t.Fatalf("seed calendar: %v", err)
	}
	members := map[string]string{"v": "uid-v", "v ": "uid-padded", "": "nameless", "w": "uid-w"}
	for name, uid := range members {
		body := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//touch//EN\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTAMP:20260101T000000Z\r\nDTSTART:20260115T100000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
		if _, err := pool.ExecContext(ctx, `INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, last_modified) VALUES ($1, $2, $3, $4, 'e', NOW())`,
			calendarID, uid, name, body); err != nil {
			t.Fatalf("seed %q: %v", name, err)
		}
	}
	collection := "/dav/calendars/" + strconv.FormatInt(calendarID, 10)
	for _, test := range []struct {
		path    string
		touched string
	}{
		{path: collection + "/v ", touched: "uid-padded"},
		{path: collection + "/nameless", touched: "nameless"},
		{path: collection + "/./w", touched: ""},
	} {
		stamps := func() map[string]time.Time {
			t.Helper()
			rows, err := pool.QueryContext(ctx, `SELECT uid, last_modified FROM events WHERE calendar_id = $1`, calendarID)
			if err != nil {
				t.Fatalf("read stamps: %v", err)
			}
			defer rows.Close()
			stamped := map[string]time.Time{}
			for rows.Next() {
				var uid string
				var at time.Time
				if err := rows.Scan(&uid, &at); err != nil {
					t.Fatalf("scan: %v", err)
				}
				stamped[uid] = at
			}
			return stamped
		}
		before := stamps()
		if err := s.ACLEntries.SetACL(ctx, test.path, []ACLEntry{{ResourcePath: test.path, PrincipalHref: "/dav/principals/99/", IsGrant: true, Privilege: "read"}}); err != nil {
			t.Fatalf("SetACL(%q): %v", test.path, err)
		}
		var touched []string
		for uid, at := range stamps() {
			if !at.Equal(before[uid]) {
				touched = append(touched, uid)
			}
		}
		slices.Sort(touched)
		want := []string(nil)
		if test.touched != "" {
			want = []string{test.touched}
		}
		if !slices.Equal(touched, want) {
			t.Errorf("SetACL(%q) touched %q, want %q", test.path, touched, want)
		}
	}
}

// The locks beneath a collection are read by path prefix on every collection
// DELETE, MOVE and overwrite, inside the transaction holding the collection.
// Under a linguistic collation the default btree on resource_path cannot serve
// a prefix LIKE, so the prefix scan reads an index that compares byte-wise.
func TestPostgres_LockPrefixScanReadsAnIndex(t *testing.T) {
	pool := newPostgresPool(t)
	ctx := context.Background()
	var userID int64
	if err := pool.QueryRowContext(ctx, `INSERT INTO users (oauth_subject, primary_email) VALUES ('locks', 'locks@example.test') RETURNING id`).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `
INSERT INTO locks (token, resource_path, user_id, depth, expires_at)
SELECT 'opaquelocktoken:' || n, '/dav/calendars/' || (n % 500) || '/member-' || n, $1, '0', NOW() + INTERVAL '1 hour'
FROM generate_series(1, 20000) AS n`, userID); err != nil {
		t.Fatalf("seed locks: %v", err)
	}
	if _, err := pool.ExecContext(ctx, `ANALYZE locks`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// The queries run with their bounds as parameters, so they have to use the
	// index under a generic plan as well as a custom one.
	low, high, bounded := lockPathPrefixRange("/dav/calendars/42/")
	if !bounded {
		t.Fatal("a collection's path prefix has no upper bound")
	}
	for _, query := range []string{lockPrefixQuery, lockDescendantScopeQuery} {
		for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
			tx, err := pool.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `SET LOCAL plan_cache_mode = `+mode); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `PREPARE prefix_scan(text, text) AS `+query); err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := tx.QueryRowContext(ctx, `EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, FORMAT JSON) EXECUTE prefix_scan('`+low+`', '`+high+`')`).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `DEALLOCATE prefix_scan`); err != nil {
				t.Fatal(err)
			}
			tx.Rollback()
			var plans []struct {
				Plan map[string]any `json:"Plan"`
			}
			if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
				t.Fatalf("decode plan: %v\n%s", err, raw)
			}
			usesIndex := false
			walkPlan(plans[0].Plan, func(node map[string]any) {
				if node["Node Type"] == "Seq Scan" {
					t.Errorf("%s: the prefix scan reads all of %v: %s", mode, node["Relation Name"], raw)
				}
				if name, _ := node["Index Name"].(string); name == "idx_locks_resource_path_pattern" {
					usesIndex = true
				}
			})
			if !usesIndex {
				t.Errorf("%s: %.60s... does not read idx_locks_resource_path_pattern: %s", mode, query, raw)
			}
		}
	}

}

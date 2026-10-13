package store

import (
	"context"
	"testing"
)

// An object-level ACL decision reads the entries of the one resource it
// decides, by resource_path_norm, from an index. A principal that holds entries
// on many resources is the case that matters: an index leading with the
// principal reads all of them.
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

	plan := explainStatement(t, pool, `SELECT `+aclOrderedBooleanExpr("a.resource_path_norm = '/dav/calendars/1/member-30'", "2", "read", "all"), true)
	usesIndex := false
	walkPlan(plan, func(node map[string]any) {
		if node["Node Type"] == "Seq Scan" {
			t.Errorf("the decision scans %v: %v", node["Relation Name"], plan)
		}
		if node["Index Name"] == "idx_acl_resource_path_norm" {
			usesIndex = true
		}
		if rows, _ := node["Actual Rows"].(float64); node["Relation Name"] == "acl_entries" && rows > 10 {
			t.Errorf("the decision reads %v entries for one resource: %v", rows, plan)
		}
	})
	if !usesIndex {
		t.Errorf("the decision does not use idx_acl_resource_path_norm: %v", plan)
	}
}

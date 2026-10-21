package store

import (
	"context"

	"github.com/lib/pq"
)

// ListByPrincipals lists, in one query, the ACL entries naming any of
// principalHrefs, ordered as ListByPrincipal orders one principal's.
func (r *aclRepo) ListByPrincipals(ctx context.Context, principalHrefs []string) ([]ACLEntry, error) {
	if len(principalHrefs) == 0 {
		return []ACLEntry{}, nil
	}
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries WHERE principal_href = ANY($1) ORDER BY resource_path, ace_order, id`
	defer observeDB(ctx, "acl.list_by_principals")()
	rows, err := r.pool.QueryContext(ctx, q, pq.Array(principalHrefs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanACLEntries(rows)
}

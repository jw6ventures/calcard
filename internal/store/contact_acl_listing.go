package store

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	"github.com/lib/pq"
)

// ContactName is the identity of a contact without its card: what an access
// decision over the contact reads.
type ContactName struct {
	ID           int64
	UID          string
	ResourceName string
}

// ContactSelection picks contacts of a book by ID: the ones listed, or, with
// Except set, every one but those.
type ContactSelection struct {
	IDs    []int64
	Except bool
}

func (sel ContactSelection) includes(id int64) bool {
	return slices.Contains(sel.IDs, id) != sel.Except
}

// ContactListing reads an address book's ACL, its contacts' names and a page of
// its contacts from one snapshot, so the decisions a page is cut by and the
// page itself see the same state.
type ContactListing struct {
	store *Store
	tx    *sql.Tx
}

// ReadContactListing runs read over one read-only REPEATABLE READ
// transaction. A store without a database reads its repositories.
func (s *Store) ReadContactListing(ctx context.Context, read func(*ContactListing) error) error {
	if s == nil || s.pool == nil {
		return read(&ContactListing{store: s})
	}
	tx, err := s.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := read(&ContactListing{store: s, tx: tx}); err != nil {
		return err
	}
	return tx.Commit()
}

// CollectionACLEntries returns the ACL entries naming one of principalHrefs, in
// any stored spelling of it, on the collection at collectionPath and on every
// resource beneath it, ordered by resource path and ACL order.
func (l *ContactListing) CollectionACLEntries(ctx context.Context, collectionPath string, principalHrefs []string) ([]ACLEntry, error) {
	var principals []string
	for _, principalHref := range principalHrefs {
		principals = append(principals, principalHrefSpellings(principalHref)...)
	}
	slices.Sort(principals)
	principals = slices.Compact(principals)
	collectionPath = strings.TrimSuffix(collectionPath, "/")
	memberPrefix := collectionPath + "/"
	if l.tx == nil {
		return l.collectionACLEntriesViaRepository(ctx, collectionPath, memberPrefix, principals)
	}
	// The members are the paths from the prefix up to, not including, the
	// prefix with its closing slash raised to the next character. The pattern
	// operators compare byte-wise, so the range holds exactly the paths that
	// start with the prefix whatever the database's collation, and
	// idx_acl_resource_path_pattern serves it.
	const q = `SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at FROM acl_entries
WHERE (resource_path = $2 OR (resource_path ~>=~ $3 AND resource_path ~<~ $4))
  AND principal_href = ANY($1)
ORDER BY resource_path, ace_order, id`
	defer observeDB(ctx, "acl.list_collection_entries")()
	rows, err := l.tx.QueryContext(ctx, q, pq.Array(principals), collectionPath, memberPrefix, collectionPath+"0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanACLEntries(rows)
}

func (l *ContactListing) collectionACLEntriesViaRepository(ctx context.Context, collectionPath, memberPrefix string, principals []string) ([]ACLEntry, error) {
	if l.store == nil || l.store.ACLEntries == nil {
		return nil, nil
	}
	var result []ACLEntry
	for _, principal := range principals {
		entries, err := l.store.ACLEntries.ListByPrincipal(ctx, principal)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.ResourcePath == collectionPath || strings.HasPrefix(entry.ResourcePath, memberPrefix) {
				result = append(result, entry)
			}
		}
	}
	return result, nil
}

// ContactNamesForACLPaths returns the identity of every contact in the address
// book whose ACL path, its resource path less a trailing .vcf in any case, is
// one of aclPaths, and of every contact with no resource name, whose ACL path
// its UID gives instead. A store without a database returns every contact.
func (l *ContactListing) ContactNamesForACLPaths(ctx context.Context, addressBookID int64, aclPaths []string) ([]ContactName, error) {
	if l.tx == nil {
		contacts, err := l.allContacts(ctx, addressBookID)
		if err != nil {
			return nil, err
		}
		names := make([]ContactName, 0, len(contacts))
		for _, c := range contacts {
			names = append(names, ContactName{ID: c.ID, UID: c.UID, ResourceName: c.ResourceName})
		}
		return names, nil
	}
	const q = `SELECT id, uid, resource_name FROM contacts
WHERE address_book_id=$1 AND (object_acl_path = ANY($2) OR resource_name = '')`
	defer observeDB(ctx, "contacts.list_names_for_acl_paths")()
	rows, err := l.tx.QueryContext(ctx, q, addressBookID, pq.Array(aclPaths))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []ContactName
	for rows.Next() {
		var name ContactName
		if err := rows.Scan(&name.ID, &name.UID, &name.ResourceName); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// Page is ListForBookPaginated over the contacts sel picks: one page of them
// in the same order, with how many there are. A page of no more than zero
// contacts is empty, and still carries the count.
func (l *ContactListing) Page(ctx context.Context, addressBookID int64, sel ContactSelection, limit, offset int) (*PaginatedResult[Contact], error) {
	limit, offset = max(limit, 0), max(offset, 0)
	result := &PaginatedResult[Contact]{Items: []Contact{}, Limit: limit, Offset: offset}
	if l.tx == nil {
		contacts, err := l.allContacts(ctx, addressBookID)
		if err != nil {
			return nil, err
		}
		for _, c := range contacts {
			if !sel.includes(c.ID) {
				continue
			}
			if result.TotalCount >= offset && len(result.Items) < limit {
				result.Items = append(result.Items, c)
			}
			result.TotalCount++
		}
		return result, nil
	}
	selected := `id = ANY($2)`
	if sel.Except {
		selected = `NOT (id = ANY($2))`
	}
	countQ := `SELECT COUNT(*) FROM contacts WHERE address_book_id=$1 AND ` + selected
	pageQ := `SELECT ` + contactColumns + ` FROM contacts WHERE address_book_id=$1 AND ` + selected +
		` ORDER BY LOWER(COALESCE(display_name, '')) ASC, id ASC LIMIT $3 OFFSET $4`
	ids := pq.Array(sel.IDs)
	if sel.IDs == nil {
		ids = pq.Array([]int64{})
	}
	defer observeDB(ctx, "contacts.list_page_selected")()
	if err := l.tx.QueryRowContext(ctx, countQ, addressBookID, ids).Scan(&result.TotalCount); err != nil {
		return nil, err
	}
	rows, err := l.tx.QueryContext(ctx, pageQ, addressBookID, ids, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		c, err := scanContact(rows.Scan)
		if err != nil {
			return nil, err
		}
		result.Items = append(result.Items, c)
	}
	return result, rows.Err()
}

func (l *ContactListing) allContacts(ctx context.Context, addressBookID int64) ([]Contact, error) {
	if l.store == nil || l.store.Contacts == nil {
		return nil, ErrNotFound
	}
	return l.store.Contacts.ListForBookFiltered(ctx, addressBookID, ContactFilter{})
}

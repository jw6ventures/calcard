package store

import (
	"context"
	"database/sql"
	"errors"
)

// ContactObjectWrite is one PUT of a contact resource, carrying the conditional
// and state preconditions the store verifies under lock so the write cannot be
// applied to a resource other than the one the handler authorized.
type ContactObjectWrite struct {
	AddressBookID           int64
	UID                     string
	ResourceName            string
	RawVCard                string
	ETag                    string
	Precondition            CalendarObjectPrecondition
	ExpectedState           DAVResourceState
	ExpectedAddressBookCTag *int64
	StatePath               string
	LockPreconditions       []LockPrecondition
}

// ContactObjectWriteResult reports the stored contact and whether the write
// created it, which is the difference between a 201 and a 204.
type ContactObjectWriteResult struct {
	Contact  *Contact
	Created  bool
	Conflict *Contact
}

// ContactObjectWriter is the seam a non-PostgreSQL store implements to answer
// PutContactObject itself.
type ContactObjectWriter interface {
	PutContactObject(context.Context, ContactObjectWrite) (*ContactObjectWriteResult, error)
}

// PutContactObject writes a contact and its DAV state in one transaction,
// resolving the target row, its UID owner and the conditional-header
// requirement under lock so concurrent writers cannot interleave between the
// check and the write.
func (s *Store) PutContactObject(ctx context.Context, write ContactObjectWrite) (*ContactObjectWriteResult, error) {
	if s == nil || s.Contacts == nil {
		return nil, ErrNotFound
	}
	if write.ResourceName == "" {
		write.ResourceName = write.UID
	}
	if s.pool == nil {
		if s.ContactObjects != nil {
			return s.ContactObjects.PutContactObject(ctx, write)
		}
		return s.putContactObjectViaRepository(ctx, write)
	}

	tx, err := s.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := validateLockPreconditionsTx(ctx, tx, write.LockPreconditions); err != nil {
		return nil, err
	}
	if err := validateCollectionCTagsTx(ctx, tx, "address_books",
		collectionCTagExpectation{id: write.AddressBookID, ctag: write.ExpectedAddressBookCTag}); err != nil {
		return nil, err
	}
	if err := acquireDAVObjectIdentityLocks(ctx, tx, "contact-object", write.AddressBookID,
		DAVResourceState{UID: write.UID, ResourceName: write.ResourceName}); err != nil {
		return nil, err
	}

	result, err := putContactObjectTx(ctx, tx, write)
	if err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func putContactObjectTx(ctx context.Context, tx *sql.Tx, write ContactObjectWrite) (*ContactObjectWriteResult, error) {
	existing, err := selectContactTx(ctx, tx, `resource_name`, write.AddressBookID, write.ResourceName)
	if err != nil {
		return nil, err
	}
	if !contactDAVStateMatches(write.ExpectedState, existing) {
		return nil, ErrResourceStateChanged
	}
	if existing != nil && existing.UID != write.UID {
		return &ContactObjectWriteResult{Conflict: existing}, ErrUIDConflict
	}
	byUID, err := selectContactTx(ctx, tx, `uid`, write.AddressBookID, write.UID)
	if err != nil {
		return nil, err
	}
	if byUID != nil && storedContactResourceName(*byUID) != write.ResourceName {
		return &ContactObjectWriteResult{Conflict: byUID}, ErrUIDConflict
	}
	if !contactPreconditionSatisfied(write.Precondition, existing) {
		return nil, ErrPreconditionFailed
	}

	displayName, primaryEmail, birthday := parseVCardFields(write.RawVCard)
	if existing == nil {
		if err := clearCreatedObjectDAVStateTx(ctx, tx, write.StatePath); err != nil {
			return nil, err
		}
		const insert = `
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT DO NOTHING
RETURNING ` + contactDAVColumns
		created, err := scanContact(tx.QueryRowContext(ctx, insert, write.AddressBookID, write.UID, write.ResourceName,
			write.RawVCard, write.ETag, displayName, primaryEmail, birthday).Scan)
		if errors.Is(err, sql.ErrNoRows) {
			return contactObjectConflictAfterWrite(ctx, tx, write)
		}
		if err != nil {
			return nil, err
		}
		return &ContactObjectWriteResult{Contact: &created, Created: true}, nil
	}

	const update = `
UPDATE contacts SET uid=$2, raw_vcard=$3, etag=$4, display_name=$5, primary_email=$6, birthday=$7, last_modified=NOW()
WHERE id=$1
RETURNING ` + contactDAVColumns
	updated, err := scanContact(tx.QueryRowContext(ctx, update, existing.ID, write.UID, write.RawVCard,
		write.ETag, displayName, primaryEmail, birthday).Scan)
	if err != nil {
		if isContactIdentityConflict(err) {
			return contactObjectConflictAfterWrite(ctx, tx, write)
		}
		return nil, err
	}
	return &ContactObjectWriteResult{Contact: &updated}, nil
}

func contactObjectConflictAfterWrite(ctx context.Context, tx *sql.Tx, write ContactObjectWrite) (*ContactObjectWriteResult, error) {
	existing, err := selectContactTx(ctx, tx, `resource_name`, write.AddressBookID, write.ResourceName)
	if err != nil {
		return nil, err
	}
	byUID, err := selectContactTx(ctx, tx, `uid`, write.AddressBookID, write.UID)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.UID != write.UID {
		return &ContactObjectWriteResult{Conflict: existing}, ErrUIDConflict
	}
	if byUID != nil && storedContactResourceName(*byUID) != write.ResourceName {
		return &ContactObjectWriteResult{Conflict: byUID}, ErrUIDConflict
	}
	return nil, ErrResourceStateChanged
}

func contactPreconditionSatisfied(precondition CalendarObjectPrecondition, existing *Contact) bool {
	if precondition.IfMatch != nil {
		if existing == nil || !precondition.IfMatch.Any && !containsString(precondition.IfMatch.ETags, existing.ETag) {
			return false
		}
	}
	if precondition.IfNoneMatch != nil && existing != nil &&
		(precondition.IfNoneMatch.Any || containsString(precondition.IfNoneMatch.ETags, existing.ETag)) {
		return false
	}
	return true
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func clearCreatedObjectDAVStateTx(ctx context.Context, tx execContext, statePath string) error {
	for _, candidate := range davStatePaths(statePath) {
		if _, err := tx.ExecContext(ctx, `DELETE FROM acl_entries WHERE resource_path=$1`, candidate); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM dav_dead_properties WHERE resource_path=$1`, candidate); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) putContactObjectViaRepository(ctx context.Context, write ContactObjectWrite) (*ContactObjectWriteResult, error) {
	if err := validateLockPreconditionsFallback(ctx, s.Locks, write.LockPreconditions); err != nil {
		return nil, err
	}
	existing, err := s.Contacts.GetByResourceName(ctx, write.AddressBookID, write.ResourceName)
	if err != nil {
		return nil, err
	}
	if !contactDAVStateMatches(write.ExpectedState, existing) {
		return nil, ErrResourceStateChanged
	}
	if existing != nil && existing.UID != write.UID {
		return &ContactObjectWriteResult{Conflict: existing}, ErrUIDConflict
	}
	byUID, err := s.Contacts.GetByUID(ctx, write.AddressBookID, write.UID)
	if err != nil {
		return nil, err
	}
	if byUID != nil && storedContactResourceName(*byUID) != write.ResourceName {
		return &ContactObjectWriteResult{Conflict: byUID}, ErrUIDConflict
	}
	if !contactPreconditionSatisfied(write.Precondition, existing) {
		return nil, ErrPreconditionFailed
	}
	if existing == nil {
		for _, candidate := range davStatePaths(write.StatePath) {
			if s.ACLEntries != nil {
				if err := s.ACLEntries.Delete(ctx, candidate); err != nil {
					return nil, err
				}
			}
			if err := s.clearDeadPropertiesFallback(ctx, candidate); err != nil {
				return nil, err
			}
		}
	}
	contact, err := s.Contacts.Upsert(ctx, Contact{AddressBookID: write.AddressBookID, UID: write.UID,
		ResourceName: write.ResourceName, RawVCard: write.RawVCard, ETag: write.ETag})
	if err != nil {
		return nil, err
	}
	return &ContactObjectWriteResult{Contact: contact, Created: existing == nil}, nil
}

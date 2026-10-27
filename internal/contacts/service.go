// Package contacts provides the business logic for the address book / contact
// REST API. Address books are owned by a single user but may be shared with
// other users through the same ACL system calendars use (store.ACLEntries,
// keyed on the DAV resource path /dav/addressbooks/{id}). Access control here
// mirrors internal/events: the owner always has full access, while sharees are
// granted privileges (read / write) via ACL entries. See sharing.go.
package contacts

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/httpcond"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
	"github.com/jw6ventures/calcard/internal/vcard"
)

// MaxBodyBytes bounds the size of a contact write payload.
const MaxBodyBytes int64 = 10 * 1024 * 1024

var (
	ErrNotFound           = errors.New("not found")
	ErrForbidden          = errors.New("forbidden")
	ErrBadRequest         = errors.New("bad request")
	ErrConflict           = errors.New("conflict")
	ErrPreconditionFailed = errors.New("precondition failed")

	// ErrLocked refuses a write that DAV would refuse for want of a lock token.
	// A UI or REST request cannot submit one, so any active lock covering the
	// contact, or the address book where the write changes its membership,
	// refuses the write, whoever holds the lock.
	ErrLocked = errors.New("locked: a WebDAV lock covers the contact or its address book")

	// ErrChangedConcurrently is a change to a contact given up because the
	// contact or its access kept changing between each attempt's read and its
	// write. It is a conflict.
	ErrChangedConcurrently = fmt.Errorf("%w: the contact or its access kept changing", ErrConflict)

	// ErrDestinationForbidden refuses a move for want of DAV:bind on the
	// destination address book, or of a destination book the user can reach.
	// It is ErrForbidden.
	ErrDestinationForbidden = fmt.Errorf("%w: the destination address book does not admit the contact", ErrForbidden)

	// ErrUnwritableName refuses a write that would keep a contact under a name
	// no href carries, such as "." as a 1.1.x release could store it. Such a
	// contact can be read, deleted, or moved over CardDAV to another name. It
	// is ErrBadRequest.
	ErrUnwritableName = fmt.Errorf("%w: the contact is stored under a name that cannot be kept; it can be read, deleted or moved to another name", ErrBadRequest)
)

// FieldError is a refused structured payload field. Field is the
// StructuredInput JSON name; it unwraps to ErrBadRequest.
type FieldError struct {
	Field  string
	Reason string
}

// FieldError reasons.
const (
	ReasonRequired          = "is required"
	ReasonControlCharacters = "must not contain control characters"
	ReasonUIDCharacters     = "may contain only letters, digits and - . _ ~ @ : + ="
	ReasonUIDTooLong        = "must be at most 1024 characters long"
)

func (e *FieldError) Error() string {
	return ErrBadRequest.Error() + ": " + e.Field + " " + e.Reason
}

func (e *FieldError) Unwrap() error { return ErrBadRequest }

// Service exposes address book and contact operations for API callers.
type Service struct {
	store *store.Store
}

// NewService builds a contacts Service backed by the given store.
func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

// StructuredInput is the JSON form of a contact, assembled into a vCard.
type StructuredInput struct {
	UID         string `json:"uid"`
	DisplayName string `json:"displayName"`
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	Email       string `json:"email"`
	Phone       string `json:"phone"`
	Birthday    string `json:"birthday"`
	Notes       string `json:"notes"`
	Company     string `json:"company"`
}

// UpsertInput carries either a structured contact or a raw vCard body.
type UpsertInput struct {
	Structured  *StructuredInput
	RawVCard    string
	IfMatch     string
	IfNoneMatch string
}

// ListAddressBooks returns the address books the user can access: those they
// own plus any shared with them via ACL. See ListAccessibleAddressBooks for the
// access metadata (shared/editor) variant.
func (s *Service) ListAddressBooks(ctx context.Context, user *store.User) ([]store.AddressBook, error) {
	accessible, err := s.ListAccessibleAddressBooks(ctx, user)
	if err != nil {
		return nil, err
	}
	books := make([]store.AddressBook, 0, len(accessible))
	for _, a := range accessible {
		books = append(books, a.AddressBook)
	}
	return books, nil
}

// GetAddressBook returns a single accessible address book, or ErrNotFound.
func (s *Service) GetAddressBook(ctx context.Context, user *store.User, bookID int64) (*store.AddressBook, error) {
	return s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "read")
}

// AddressBookAccessFor reports how the user reaches the given book. The caller
// must already have confirmed access (e.g. via GetAddressBook).
func (s *Service) AddressBookAccessFor(ctx context.Context, user *store.User, book store.AddressBook) (AddressBookAccess, error) {
	access := AddressBookAccess{AddressBook: book}
	if user != nil && book.UserID == user.ID {
		access.Editor = true
		return access, nil
	}
	access.Shared = true
	granted, _, err := s.privilegeDecision(ctx, user, book.ID, "", "write-content")
	if err != nil {
		return access, err
	}
	access.Editor = granted
	return access, nil
}

// ListContacts returns contacts in an accessible address book matching the
// filter, limited to those the user may read.
func (s *Service) ListContacts(ctx context.Context, user *store.User, bookID int64, filter store.ContactFilter) ([]store.Contact, error) {
	book, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "read")
	if err != nil {
		return nil, err
	}
	if user != nil && book.UserID == user.ID {
		return s.store.Contacts.ListForBookFiltered(ctx, bookID, filter)
	}
	var selection store.ContactSelection
	err = s.store.ReadContactListing(ctx, func(listing *store.ContactListing) error {
		readable, err := newReadDecider(ctx, listing, user, *book)
		if err != nil {
			return err
		}
		selection, err = readable.selection(ctx, listing)
		return err
	})
	if err != nil {
		return nil, err
	}
	if selection.Except && len(selection.IDs) == 0 {
		return s.store.Contacts.ListForBookFiltered(ctx, bookID, filter)
	}
	if !selection.Except && len(selection.IDs) == 0 {
		return []store.Contact{}, nil
	}

	// A contact's own ACL can hide it, so the filter's page is cut after the
	// contacts the user may not read are left out.
	dbFilter := filter
	dbFilter.Limit = 0
	dbFilter.Offset = 0
	contacts, err := s.store.Contacts.ListForBookFiltered(ctx, bookID, dbFilter)
	if err != nil {
		return nil, err
	}
	apart := make(map[int64]bool, len(selection.IDs))
	for _, id := range selection.IDs {
		apart[id] = true
	}
	visible := make([]store.Contact, 0, len(contacts))
	skip := max(filter.Offset, 0)
	for _, c := range contacts {
		if filter.Limit > 0 && len(visible) == filter.Limit {
			break
		}
		if apart[c.ID] == selection.Except {
			continue
		}
		if skip > 0 {
			skip--
			continue
		}
		visible = append(visible, c)
	}
	return visible, nil
}

// ListContactsPage returns one page of the contacts in book that the user may
// read, in the store's display-name order, together with how many such
// contacts there are. Each contact is decided as GetContact decides it, and
// the decisions and the page are read from one snapshot. A page of no more
// than zero contacts is empty, and still carries the count.
func (s *Service) ListContactsPage(ctx context.Context, user *store.User, book store.AddressBook, limit, offset int) (*store.PaginatedResult[store.Contact], error) {
	limit, offset = max(limit, 0), max(offset, 0)
	if user != nil && book.UserID == user.ID {
		return s.store.Contacts.ListForBookPaginated(ctx, book.ID, limit, offset)
	}
	var page *store.PaginatedResult[store.Contact]
	err := s.store.ReadContactListing(ctx, func(listing *store.ContactListing) error {
		readable, err := newReadDecider(ctx, listing, user, book)
		if err != nil {
			return err
		}
		selection, err := readable.selection(ctx, listing)
		if err != nil {
			return err
		}
		page, err = listing.Page(ctx, book.ID, selection, limit, offset)
		return err
	})
	return page, err
}

// readDecider decides which contacts of one book a user who does not own it
// may read, from the ACL entries naming the user on the book and on the book's
// contacts, read in one query. A contact's own ACL decides ahead of the
// book's, as GetContact decides it.
type readDecider struct {
	user          *store.User
	book          store.AddressBook
	entriesByPath map[string][]store.ACLEntry
}

func newReadDecider(ctx context.Context, listing *store.ContactListing, user *store.User, book store.AddressBook) (*readDecider, error) {
	d := &readDecider{user: user, book: book, entriesByPath: map[string][]store.ACLEntry{}}
	if user == nil {
		return d, nil
	}
	entries, err := listing.CollectionACLEntries(ctx, addressBookACLCollectionPath(book.ID), acl.PrincipalHrefs(user))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		d.entriesByPath[entry.ResourcePath] = append(d.entriesByPath[entry.ResourcePath], entry)
	}
	for resourcePath := range d.entriesByPath {
		acl.SortEntries(d.entriesByPath[resourcePath])
	}
	return d, nil
}

func (d *readDecider) canRead(resourceName string) bool {
	return d.user != nil && canReadContactFromEntries(d.user, d.book.ID, d.book.UserID, resourceName, d.entriesByPath)
}

// selection picks the contacts of the book the user may read. The book's ACL
// decides every contact alike except those whose own ACL decides otherwise,
// and only a contact some entry naming the user sits on can be one of those;
// so only such contacts are read, by the paths the entries sit on, and decided
// one by one.
func (d *readDecider) selection(ctx context.Context, listing *store.ContactListing) (store.ContactSelection, error) {
	collection := d.canRead("")
	selection := store.ContactSelection{Except: collection}
	memberPrefix := addressBookACLCollectionPath(d.book.ID) + "/"
	var aclPaths []string
	for resourcePath := range d.entriesByPath {
		if !strings.HasPrefix(resourcePath, memberPrefix) {
			continue
		}
		aclPaths = append(aclPaths, resourcePath)
	}
	if len(aclPaths) == 0 {
		return selection, nil
	}
	names, err := listing.ContactNamesForACLPaths(ctx, d.book.ID, aclPaths)
	if err != nil {
		return store.ContactSelection{}, err
	}
	for _, name := range names {
		resourceName := name.ResourceName
		if resourceName == "" {
			resourceName = name.UID
		}
		if d.canRead(resourceName) != collection {
			selection.IDs = append(selection.IDs, name.ID)
		}
	}
	return selection, nil
}

// GetContact returns a single readable contact in an accessible address book.
// A contact the user may not read is answered as a contact that is not there,
// as a CardDAV GET of it is, so the answer does not disclose that it exists.
func (s *Service) GetContact(ctx context.Context, user *store.User, bookID int64, uid string) (*store.Contact, error) {
	c, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, err
	}
	resourceName := ""
	if c != nil {
		resourceName = contactResourceName(*c)
	}
	if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, resourceName, "read"); err != nil {
		if errors.Is(err, ErrForbidden) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if c == nil {
		return nil, ErrNotFound
	}
	return c, nil
}

// CreateContact creates a new contact. It fails with ErrConflict if one with the
// same UID already exists.
// The write is conditional on no contact existing under its UID or resource
// name, so a contact another client creates concurrently is not overwritten.
// A write that loses to a concurrent change of the contact or of the ACL is
// decided again, which reports that contact as a conflict and a withdrawn
// grant as forbidden.
func (s *Service) CreateContact(ctx context.Context, user *store.User, bookID int64, input UpsertInput) (*store.Contact, bool, error) {
	body, uid, err := normalizeVCardPayload(input, "", "")
	if err != nil {
		if _, accessErr := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "bind"); accessErr != nil {
			return nil, false, accessErr
		}
		return nil, false, err
	}
	for attempt := 1; ; attempt++ {
		c, err := s.createContactOnce(ctx, user, bookID, uid, body, input)
		if !errors.Is(err, store.ErrResourceStateChanged) {
			return c, err == nil, err
		}
		if attempt == maxUpdateAttempts {
			return nil, false, ErrChangedConcurrently
		}
	}
}

func (s *Service) createContactOnce(ctx context.Context, user *store.User, bookID int64, uid, body string, input UpsertInput) (*store.Contact, error) {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, ErrNotFound
	}
	write, err := s.createWrite(ctx, user, book, uid, body, input)
	if err != nil {
		return nil, err
	}
	return s.putContact(ctx, write)
}

// createWrite decides the creation of the contact uid in book and returns the
// write that carries it out.
func (s *Service) createWrite(ctx context.Context, user *store.User, book *store.AddressBook, uid, body string, input UpsertInput) (store.ContactObjectWrite, error) {
	resourceName := newContactResourceName(uid)
	guard, err := s.writeGuard(ctx, user, aclScope{bookID: book.ID, resourceName: resourceName, book: book})
	if err != nil {
		return store.ContactObjectWrite{}, err
	}
	if err := s.requireBookPrivilege(ctx, user, book, "", "bind"); err != nil {
		return store.ContactObjectWrite{}, err
	}
	statePath := addressBookACLResourcePaths(book.ID, resourceName)[0]
	lockPreconditions := []store.LockPrecondition{
		store.NewLockPrecondition(statePath),
		store.NewLockPrecondition(addressBookACLCollectionPath(book.ID)),
	}
	if err := s.lockedAheadOfPreconditions(ctx, input, lockPreconditions); err != nil {
		return store.ContactObjectWrite{}, err
	}
	existing, err := s.store.Contacts.GetByUID(ctx, book.ID, uid)
	if err != nil {
		return store.ContactObjectWrite{}, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return store.ContactObjectWrite{}, ErrPreconditionFailed
	}
	if existing != nil {
		return store.ContactObjectWrite{}, ErrConflict
	}
	return store.ContactObjectWrite{
		AddressBookID:     book.ID,
		UID:               uid,
		ResourceName:      resourceName,
		RawVCard:          body,
		ETag:              utils.GenerateETag(body),
		ExpectedState:     store.DAVResourceState{ACL: guard},
		StatePath:         statePath,
		LockPreconditions: lockPreconditions,
	}, nil
}

// lockedAheadOfPreconditions refuses a write that carries If-Match or
// If-None-Match with ErrLocked when a lock refuses it, before the preconditions
// are decided: DAV answers 423 ahead of 412. The write checks the locks again
// in its transaction, which decides.
func (s *Service) lockedAheadOfPreconditions(ctx context.Context, input UpsertInput, lockPreconditions []store.LockPrecondition) error {
	if input.IfMatch == "" && input.IfNoneMatch == "" {
		return nil
	}
	return contactWriteError(s.store.CheckLocks(ctx, lockPreconditions))
}

// putContact stores write and answers its refusals in the service's errors.
func (s *Service) putContact(ctx context.Context, write store.ContactObjectWrite) (*store.Contact, error) {
	result, err := s.store.PutContactObject(ctx, write)
	if err != nil {
		return nil, contactWriteError(err)
	}
	return result.Contact, nil
}

// contactWriteError is the service's error for a refusal by the store of a
// write to contacts.
func contactWriteError(err error) error {
	switch {
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrConflict):
		return ErrConflict
	case errors.Is(err, store.ErrPreconditionFailed):
		return ErrPreconditionFailed
	case errors.Is(err, store.ErrLockConflict):
		return ErrLocked
	}
	return err
}

// aclScope is what one access decision reads the ACL of: a book, and the
// contact in it when resourceName is set. book is the book when the caller has
// already read it.
type aclScope struct {
	bookID       int64
	resourceName string
	book         *store.AddressBook
}

// pinnedACL is the ACL entries a guard pinned, by resource path.
type pinnedACL map[string][]store.ACLEntry

// entries returns the pinned entries on paths, in ACL order.
func (p pinnedACL) entries(paths ...string) []store.ACLEntry {
	var result []store.ACLEntry
	for _, resourcePath := range paths {
		result = append(result, p[resourcePath]...)
	}
	acl.SortEntries(result)
	return result
}

// pinACL pins the ACL entries a non-owner's write is decided on: in each scope,
// the book's and the contact's own. The store refuses the write if they change
// before it commits, and the caller re-decides; the entries are returned for
// the decision to be made from. An owner's access does not come from the ACL,
// so a book the user owns adds nothing.
func (s *Service) pinACL(ctx context.Context, user *store.User, scopes ...aclScope) (*store.ACLGuard, pinnedACL, error) {
	if s.store.ACLEntries == nil || s.store.AddressBooks == nil {
		return nil, nil, nil
	}
	var paths []string
	for _, scope := range scopes {
		book := scope.book
		if book == nil {
			var err error
			if book, err = s.store.AddressBooks.GetByID(ctx, scope.bookID); err != nil {
				return nil, nil, err
			}
		}
		if book == nil || (user != nil && book.UserID == user.ID) {
			continue
		}
		paths = append(paths, addressBookACLCollectionPath(book.ID))
		paths = append(paths, addressBookACLResourcePaths(book.ID, scope.resourceName)...)
	}
	if len(paths) == 0 {
		return nil, nil, nil
	}
	entries, err := s.store.ACLEntries.ListByResources(ctx, paths)
	if err != nil {
		return nil, nil, err
	}
	pinned := pinnedACL{}
	for _, entry := range entries {
		pinned[entry.ResourcePath] = append(pinned[entry.ResourcePath], entry)
	}
	return store.NewACLGuard(paths, entries), pinned, nil
}

// writeGuard is pinACL for a write decided by reading the ACL afresh.
func (s *Service) writeGuard(ctx context.Context, user *store.User, scopes ...aclScope) (*store.ACLGuard, error) {
	guard, _, err := s.pinACL(ctx, user, scopes...)
	return guard, err
}

// maxUpdateAttempts bounds how often a write is re-read and re-decided when
// another writer changes the contact or its ACL between the read and the
// write.
const maxUpdateAttempts = 3

// UpdateContact replaces an existing contact identified by uid. The write is
// conditional on the version it was built from: a structured edit is merged
// into that version, so a concurrent write is re-read and the edit re-applied
// rather than overwritten. A caller's own If-Match or If-None-Match is checked
// again against the version each attempt reads, so it fails only when the
// contact itself changed, not when only the ACL did. A user refused the write
// who may not read the contact either is answered as for a UID that is not
// there, as a CardDAV PUT over the contact is, so the refusal does not
// disclose that it exists.
func (s *Service) UpdateContact(ctx context.Context, user *store.User, bookID int64, uid string, input UpsertInput) (*store.Contact, bool, error) {
	for attempt := 1; ; attempt++ {
		c, err := s.updateContactOnce(ctx, user, bookID, uid, input)
		if !errors.Is(err, store.ErrResourceStateChanged) {
			return c, false, err
		}
		if attempt == maxUpdateAttempts {
			return nil, false, ErrChangedConcurrently
		}
	}
}

func (s *Service) updateContactOnce(ctx context.Context, user *store.User, bookID int64, uid string, input UpsertInput) (*store.Contact, error) {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, ErrNotFound
	}
	write, err := s.updateWrite(ctx, user, book, uid, input)
	if err != nil {
		return nil, err
	}
	return s.putContact(ctx, write)
}

// updateWrite decides the replacement of the contact uid of book and returns
// the write that carries it out. It is ErrNotFound for a UID the book does not
// hold, unless the user's refusal of write-content on the book is the answer.
func (s *Service) updateWrite(ctx context.Context, user *store.User, book *store.AddressBook, uid string, input UpsertInput) (store.ContactObjectWrite, error) {
	existing, err := s.store.Contacts.GetByUID(ctx, book.ID, uid)
	if err != nil {
		return store.ContactObjectWrite{}, err
	}
	if existing == nil {
		return store.ContactObjectWrite{}, s.missingContactUpdate(ctx, user, book)
	}
	resourceName := contactResourceName(*existing)
	guard, err := s.writeGuard(ctx, user, aclScope{bookID: book.ID, resourceName: resourceName, book: book})
	if err != nil {
		return store.ContactObjectWrite{}, err
	}
	if err := s.requireBookPrivilege(ctx, user, book, resourceName, "write-content"); err != nil {
		if !errors.Is(err, ErrForbidden) {
			return store.ContactObjectWrite{}, err
		}
		readErr := s.requireBookPrivilege(ctx, user, book, resourceName, "read")
		if readErr == nil {
			return store.ContactObjectWrite{}, err
		}
		if !errors.Is(readErr, ErrForbidden) && !errors.Is(readErr, ErrNotFound) {
			return store.ContactObjectWrite{}, readErr
		}
		return store.ContactObjectWrite{}, s.missingContactUpdate(ctx, user, book)
	}
	statePath := addressBookACLResourcePaths(book.ID, resourceName)[0]
	lockPreconditions := []store.LockPrecondition{store.NewLockPrecondition(statePath)}
	if err := s.lockedAheadOfPreconditions(ctx, input, lockPreconditions); err != nil {
		return store.ContactObjectWrite{}, err
	}
	if err := writableContactName(resourceName); err != nil {
		return store.ContactObjectWrite{}, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return store.ContactObjectWrite{}, ErrPreconditionFailed
	}
	body, normalizedUID, err := normalizeVCardPayload(input, uid, existing.RawVCard)
	if err != nil {
		return store.ContactObjectWrite{}, err
	}
	if normalizedUID != uid {
		return store.ContactObjectWrite{}, fmt.Errorf("%w: uid mismatch", ErrBadRequest)
	}
	expected := store.ContactDAVResourceState(existing)
	expected.ACL = guard
	return store.ContactObjectWrite{
		AddressBookID:     book.ID,
		UID:               uid,
		ResourceName:      resourceName,
		RawVCard:          body,
		ETag:              utils.GenerateETag(body),
		ExpectedState:     expected,
		StatePath:         statePath,
		LockPreconditions: lockPreconditions,
	}, nil
}

// writableContactName refuses a write that would keep a contact under
// resourceName when store.ValidMemberName refuses it, as a 1.1.x release could
// store one: its state path names the book, or the book's parent, once
// cleaned. Such a contact can still be read, deleted, or moved over CardDAV to
// another name.
func writableContactName(resourceName string) error {
	if !store.ValidMemberName(resourceName) {
		return fmt.Errorf("%w: %q", ErrUnwritableName, resourceName)
	}
	return nil
}

// missingContactUpdate is the answer to an update of a UID book does not hold:
// the book's refusal of write-content, or ErrNotFound.
func (s *Service) missingContactUpdate(ctx context.Context, user *store.User, book *store.AddressBook) error {
	if err := s.requireBookPrivilege(ctx, user, book, "", "write-content"); err != nil {
		return err
	}
	return ErrNotFound
}

// DeleteContact removes a contact, honoring If-Match/If-None-Match
// preconditions. It is decided and answered as a CardDAV DELETE of the contact
// is: see requireDeletion. A contact that is not there, or that the user is
// answered about as if it were not, is ErrNotFound whatever the preconditions,
// which RFC 9110 section 13.2.1 has a server ignore when its answer without
// them would be neither 2xx nor 412. A delete that loses to a concurrent
// change of the contact or of the ACL is decided again, and the caller's
// precondition is checked again against the contact as it then reads.
func (s *Service) DeleteContact(ctx context.Context, user *store.User, bookID int64, uid, ifMatch, ifNoneMatch string) error {
	for attempt := 1; ; attempt++ {
		err := s.deleteContactOnce(ctx, user, bookID, uid, ifMatch, ifNoneMatch)
		if !errors.Is(err, store.ErrResourceStateChanged) {
			return err
		}
		if attempt == maxUpdateAttempts {
			return ErrChangedConcurrently
		}
	}
}

func (s *Service) deleteContactOnce(ctx context.Context, user *store.User, bookID int64, uid, ifMatch, ifNoneMatch string) error {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return err
	}
	if book == nil {
		return ErrNotFound
	}
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return err
	}
	resourceName := ""
	if existing != nil {
		resourceName = contactResourceName(*existing)
	}
	guard, pinned, err := s.pinACL(ctx, user, aclScope{bookID: bookID, resourceName: resourceName, book: book})
	if err != nil {
		return err
	}
	if err := requireDeletion(user, book, resourceName, existing != nil, pinned); err != nil {
		return err
	}
	if existing == nil {
		return ErrNotFound
	}
	resourcePaths := addressBookACLResourcePaths(bookID, resourceName)
	if len(resourcePaths) == 0 {
		return ErrNotFound
	}
	lockPreconditions := []store.LockPrecondition{
		store.NewLockPrecondition(resourcePaths[0]),
		store.NewLockPrecondition(addressBookACLCollectionPath(bookID)),
	}
	if err := s.lockedAheadOfPreconditions(ctx, UpsertInput{IfMatch: ifMatch, IfNoneMatch: ifNoneMatch}, lockPreconditions); err != nil {
		return err
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return ErrPreconditionFailed
	}
	expected := store.ContactDAVResourceState(existing)
	expected.ACL = guard
	return contactWriteError(s.store.DeleteContactAndState(ctx, bookID, expected, resourcePaths[0], lockPreconditions))
}

// MoveContact moves a contact into another address book under the resource
// name it has. It is decided and answered as a CardDAV MOVE of the contact
// is: the source side as requireMoveOut decides it, then DAV:bind on the
// destination book, whose refusal, as the lack of a destination book the user
// can reach, is ErrDestinationForbidden. The contact's dead properties and its
// own ACL go with it to its new path, as RFC 3744 section 7.3 has a MOVE keep
// the ACL, and locks on the path it leaves are dropped. A contact already in
// the destination book under the same UID or resource name, including one
// written while the move runs, is a conflict and is never replaced. A move
// that loses to a concurrent change of the contact or of the ACL is decided
// again.
func (s *Service) MoveContact(ctx context.Context, user *store.User, fromBookID, toBookID int64, uid string) error {
	for attempt := 1; ; attempt++ {
		err := s.moveContactOnce(ctx, user, fromBookID, toBookID, uid)
		if !errors.Is(err, store.ErrResourceStateChanged) {
			return err
		}
		if attempt == maxUpdateAttempts {
			return ErrChangedConcurrently
		}
	}
}

func (s *Service) moveContactOnce(ctx context.Context, user *store.User, fromBookID, toBookID int64, uid string) error {
	fromBook, err := s.store.AddressBooks.GetByID(ctx, fromBookID)
	if err != nil {
		return err
	}
	if fromBook == nil {
		return ErrNotFound
	}
	toBook := fromBook
	if toBookID != fromBookID {
		if toBook, err = s.store.AddressBooks.GetByID(ctx, toBookID); err != nil {
			return err
		}
	}
	existing, err := s.store.Contacts.GetByUID(ctx, fromBookID, uid)
	if err != nil {
		return err
	}
	resourceName := ""
	if existing != nil {
		resourceName = contactResourceName(*existing)
	}
	scopes := []aclScope{{bookID: fromBookID, resourceName: resourceName, book: fromBook}}
	if toBook != nil && toBook != fromBook {
		scopes = append(scopes, aclScope{bookID: toBookID, book: toBook})
	}
	guard, pinned, err := s.pinACL(ctx, user, scopes...)
	if err != nil {
		return err
	}
	if err := requireMoveOut(user, fromBook, resourceName, pinned); err != nil {
		return err
	}
	if existing == nil {
		return ErrNotFound
	}
	if toBook == nil {
		return ErrDestinationForbidden
	}
	if err := requireBind(user, toBook, pinned); err != nil {
		return err
	}
	if fromBookID == toBookID {
		return nil
	}
	if err := writableContactName(resourceName); err != nil {
		return err
	}
	fromPaths := addressBookACLResourcePaths(fromBookID, resourceName)
	toPaths := addressBookACLResourcePaths(toBookID, resourceName)
	if len(fromPaths) == 0 || len(toPaths) == 0 {
		return ErrNotFound
	}
	if taken, err := s.store.Contacts.GetByUID(ctx, toBookID, uid); err != nil {
		return err
	} else if taken != nil {
		return ErrConflict
	}
	if taken, err := s.store.Contacts.GetByResourceName(ctx, toBookID, resourceName); err != nil {
		return err
	} else if taken != nil {
		return ErrConflict
	}
	// The destination is expected empty, so a contact written there after the
	// checks above fails the move with ErrResourceStateChanged and the next
	// attempt reports it; ErrConflict is a contact holding the UID under
	// another resource name.
	lockPreconditions := []store.LockPrecondition{
		store.NewLockPrecondition(fromPaths[0]),
		store.NewLockPrecondition(addressBookACLCollectionPath(fromBookID)),
		store.NewLockPrecondition(toPaths[0]),
		store.NewLockPrecondition(addressBookACLCollectionPath(toBookID)),
	}
	return contactWriteError(s.store.MoveContactAndState(ctx, fromBookID, toBookID, uid, resourceName, fromPaths[0], toPaths[0], "",
		store.ContactTransferExpectation{Source: store.ContactDAVResourceState(existing), ACL: guard}, lockPreconditions))
}

// memberPrivileges are the privileges any one of which, held on a contact,
// lets a CardDAV DELETE or MOVE of it go on to the book's DAV:unbind check.
var memberPrivileges = []string{"read", "write", "write-content", "write-properties", "bind", "unbind"}

// removalACL is what deciding the removal of one contact from a book reads:
// the ACL entries on the contact and on the book, and the principals the user
// counts as. RFC 3744 section 3.10 makes DAV:unbind a privilege on the
// collection, so the book's ACL alone decides it; the contact's own ACL
// decides, ahead of the book's, only whether the user holds any privilege on
// the contact.
type removalACL struct {
	member, book []store.ACLEntry
	principals   map[string]struct{}
}

func newRemovalACL(user *store.User, book *store.AddressBook, resourceName string, pinned pinnedACL) *removalACL {
	return &removalACL{
		member:     pinned.entries(addressBookACLResourcePaths(book.ID, resourceName)...),
		book:       pinned.entries(addressBookACLCollectionPath(book.ID)),
		principals: acl.ApplicablePrincipalsFor(user, acl.ResourcePrincipals{OwnerHref: acl.PrincipalHref(book.UserID)}),
	}
}

// holding reports what the user holds of memberPrivileges on the contact, or,
// with atMember false, on the book itself.
func (a *removalACL) holding(atMember bool) acl.Holding {
	denied := false
	for _, privilege := range memberPrivileges {
		layers := [][]store.ACLEntry{a.book}
		if atMember {
			layers = [][]store.ACLEntry{a.member, a.book}
		}
		granted, decided := acl.DecisionThroughLayers(a.principals, privilege, layers...)
		if granted {
			return acl.HoldsSome
		}
		denied = denied || decided
	}
	if denied {
		return acl.HoldsDenied
	}
	return acl.HoldsNothing
}

func (a *removalACL) granted(privilege string) bool {
	granted, _ := acl.DecisionForPrivilege(a.book, a.principals, privilege)
	return granted
}

// requireDeletion decides and answers a delete of the contact at resourceName,
// which exists says is there, as a CardDAV DELETE of it is decided and
// answered, from the ACL entries pinned for the delete. A user with no
// privilege on the contact and none denied is told nothing is there. A user
// denied every privilege on a contact that is there cannot read it, and would
// learn from a refusal its own ACL caused that it exists, so is answered as for
// a contact that is not there: the book's refusal, or ErrNotFound. A user
// holding some privilege on the contact knows it is there, and is refused for
// want of DAV:unbind on the book.
func requireDeletion(user *store.User, book *store.AddressBook, resourceName string, exists bool, pinned pinnedACL) error {
	if user != nil && book.UserID == user.ID {
		return nil
	}
	if user == nil {
		return ErrNotFound
	}
	decision := newRemovalACL(user, book, resourceName, pinned)
	switch acl.DecideRemoval(decision.holding(true), decision.holding(false), decision.granted("unbind"), exists) {
	case acl.RemovalAllowed:
		return nil
	case acl.RemovalAbsent:
		return ErrNotFound
	default:
		return ErrForbidden
	}
}

// requireMoveOut decides and answers the source side of a move of the contact
// at resourceName, as a CardDAV MOVE of it is, from the ACL entries pinned for
// the move: a user holding no privilege on the contact is told nothing is
// there, and one holding some is refused for want of DAV:unbind on the book.
func requireMoveOut(user *store.User, book *store.AddressBook, resourceName string, pinned pinnedACL) error {
	if user != nil && book.UserID == user.ID {
		return nil
	}
	if user == nil {
		return ErrNotFound
	}
	decision := newRemovalACL(user, book, resourceName, pinned)
	if decision.holding(true) != acl.HoldsSome {
		return ErrNotFound
	}
	if !decision.granted("unbind") {
		return ErrForbidden
	}
	return nil
}

// requireBind decides the destination side of a move, as a CardDAV MOVE does,
// from the ACL entries pinned for the move: DAV:bind on the book.
func requireBind(user *store.User, book *store.AddressBook, pinned pinnedACL) error {
	if user != nil && book.UserID == user.ID {
		return nil
	}
	if user == nil || !newRemovalACL(user, book, "", pinned).granted("bind") {
		return ErrDestinationForbidden
	}
	return nil
}

func (s *Service) requireOwnedBook(ctx context.Context, user *store.User, bookID int64) (*store.AddressBook, error) {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	// Treat a missing or unowned book identically so the API does not leak the
	// existence of other users' address books.
	if book == nil || user == nil || book.UserID != user.ID {
		return nil, ErrNotFound
	}
	return book, nil
}

// loadAddressBookWithPrivilege returns the address book if the user owns it or
// holds privilege on the given contact (when resourceName is set) or on the
// collection. A user with no applicable ACL entry gets ErrNotFound so the book
// stays hidden; a user who is a sharee but lacks the privilege gets ErrForbidden.
func (s *Service) loadAddressBookWithPrivilege(ctx context.Context, user *store.User, bookID int64, resourceName, privilege string) (*store.AddressBook, error) {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if book == nil {
		return nil, ErrNotFound
	}
	if err := s.requireBookPrivilege(ctx, user, book, resourceName, privilege); err != nil {
		return nil, err
	}
	return book, nil
}

// requireBookPrivilege is loadAddressBookWithPrivilege's decision over a book
// the caller has read.
func (s *Service) requireBookPrivilege(ctx context.Context, user *store.User, book *store.AddressBook, resourceName, privilege string) error {
	if user != nil && book.UserID == user.ID {
		return nil
	}
	granted, applicable, err := s.privilegeDecision(ctx, user, book.ID, resourceName, privilege)
	if err != nil {
		return err
	}
	if granted {
		return nil
	}
	if applicable {
		return ErrForbidden
	}
	return ErrNotFound
}

// ListAccessibleAddressBooks returns the books the user owns plus any shared
// with them via ACL, each annotated with how the user reaches it.
func (s *Service) ListAccessibleAddressBooks(ctx context.Context, user *store.User) ([]AddressBookAccess, error) {
	owned, err := s.store.AddressBooks.ListByUser(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	result := make([]AddressBookAccess, 0, len(owned))
	seen := make(map[int64]struct{}, len(owned))
	for _, book := range owned {
		result = append(result, AddressBookAccess{AddressBook: book, Editor: true})
		seen[book.ID] = struct{}{}
	}

	if s.store.ACLEntries == nil {
		return result, nil
	}

	entries, err := s.store.ACLEntries.ListByPrincipals(ctx, acl.PrincipalHrefs(user))
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		bookID, ok := addressBookIDFromCollectionPath(entry.ResourcePath)
		if !ok {
			continue
		}
		if _, ok := seen[bookID]; ok {
			continue
		}
		seen[bookID] = struct{}{}
		book, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "read")
		if err != nil {
			if err == ErrNotFound || err == ErrForbidden {
				continue
			}
			return nil, err
		}
		access, err := s.AddressBookAccessFor(ctx, user, *book)
		if err != nil {
			return nil, err
		}
		result = append(result, access)
	}
	return result, nil
}

// addressBookIDFromCollectionPath extracts the book ID from an ACL resource
// path that addresses an address book collection (/dav/addressbooks/{id}).
func addressBookIDFromCollectionPath(resourcePath string) (int64, bool) {
	const prefix = "/dav/addressbooks/"
	trimmed := strings.TrimSpace(resourcePath)
	if !strings.HasPrefix(trimmed, prefix) {
		return 0, false
	}
	segment, _, _ := strings.Cut(strings.TrimPrefix(trimmed, prefix), "/")
	id, err := strconv.ParseInt(segment, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// normalizeVCardPayload turns a write payload into the card to store. A
// structured edit is merged into stored, the card it replaces, when there is
// one.
func normalizeVCardPayload(input UpsertInput, expectedUID, stored string) (string, string, error) {
	if strings.TrimSpace(input.RawVCard) != "" {
		body := ensureCRLF(strings.TrimSpace(input.RawVCard))
		if err := vcard.CheckOctets(body); err != nil {
			return "", "", fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		structure, err := vcard.Inspect(body, false)
		if err != nil && vcard.Version(body) == "2.1" {
			converted, convertErr := vcard.Convert21To30(body)
			if convertErr != nil {
				return "", "", fmt.Errorf("%w: vCard 2.1: %w", ErrBadRequest, convertErr)
			}
			body = converted
			structure, err = vcard.Inspect(body, false)
		}
		if err != nil {
			return "", "", fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
		uid := structure.UID
		if uid == "" {
			uid = expectedUID
			if uid == "" {
				uid = utils.GenerateUID()
			}
			body = injectVCardUID(body, uid)
		}
		if expectedUID != "" && uid != expectedUID {
			return "", "", fmt.Errorf("%w: path uid does not match vCard data uid", ErrBadRequest)
		}
		return body, uid, nil
	}

	if input.Structured == nil {
		return "", "", fmt.Errorf("%w: missing contact body", ErrBadRequest)
	}
	return structuredContactBody(input.Structured, expectedUID, stored)
}

// validateStructuredFields refuses control characters in a contact payload,
// since a CR or LF would end the content line the value is written into. Notes
// come from a textarea, so line breaks there are escaped instead.
func validateStructuredFields(input *StructuredInput) error {
	fields := []struct {
		name      string
		value     string
		multiline bool
	}{
		{name: "uid", value: input.UID},
		{name: "displayName", value: input.DisplayName},
		{name: "firstName", value: input.FirstName},
		{name: "lastName", value: input.LastName},
		{name: "email", value: input.Email},
		{name: "phone", value: input.Phone},
		{name: "birthday", value: input.Birthday},
		{name: "company", value: input.Company},
		{name: "notes", value: input.Notes, multiline: true},
	}
	for _, field := range fields {
		for _, c := range []byte(field.value) {
			if (c == '\r' || c == '\n') && field.multiline {
				continue
			}
			if vcard.IsControlOctet(c) {
				return &FieldError{Field: field.name, Reason: ReasonControlCharacters}
			}
		}
	}
	return nil
}

// safeResourceSegment reports whether a UID can name a new contact's resource
// as a single path segment without escaping.
func safeResourceSegment(uid string) bool {
	if !store.ValidMemberName(uid) {
		return false
	}
	for i := 0; i < len(uid); i++ {
		c := uid[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("-._~@:+=", c) >= 0:
		default:
			return false
		}
	}
	return true
}

// newContactResourceName names a new contact's resource after its UID when the
// UID is a safe path segment. A raw card keeps whatever UID its author wrote,
// so an unsafe one gets a generated resource name instead.
func newContactResourceName(uid string) string {
	if safeResourceSegment(uid) {
		return uid
	}
	return utils.GenerateUID()
}

func structuredContactBody(input *StructuredInput, expectedUID, stored string) (string, string, error) {
	if err := validateStructuredFields(input); err != nil {
		return "", "", err
	}

	form := contactForm{
		displayName: strings.TrimSpace(input.DisplayName),
		firstName:   strings.TrimSpace(input.FirstName),
		lastName:    strings.TrimSpace(input.LastName),
		email:       strings.TrimSpace(input.Email),
		phone:       strings.TrimSpace(input.Phone),
		birthday:    strings.TrimSpace(input.Birthday),
		notes:       strings.TrimSpace(input.Notes),
		company:     strings.TrimSpace(input.Company),
	}
	if form.displayName == "" {
		return "", "", &FieldError{Field: "displayName", Reason: ReasonRequired}
	}

	uid := strings.TrimSpace(input.UID)
	// An edit keeps the UID the contact already has, which another client may
	// have written with any characters, so only a new UID is restricted.
	if expectedUID == "" && uid != "" && !safeResourceSegment(uid) {
		return "", "", &FieldError{Field: "uid", Reason: ReasonUIDCharacters}
	}
	if expectedUID == "" && len(uid) > vcard.MaxUIDOctets {
		return "", "", &FieldError{Field: "uid", Reason: ReasonUIDTooLong}
	}
	if expectedUID != "" {
		if uid != "" && uid != expectedUID {
			return "", "", fmt.Errorf("%w: path uid does not match payload uid", ErrBadRequest)
		}
		uid = expectedUID
	}
	if uid == "" {
		uid = utils.GenerateUID()
	}

	if stored != "" {
		body, err := mergeStructuredContact(stored, uid, form, time.Now())
		return body, uid, err
	}

	body, err := utils.BuildVCard(uid, form.displayName, form.firstName, form.lastName, form.email, form.phone, form.birthday, form.notes, form.company)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	return body, uid, nil
}

// injectVCardUID inserts a UID line immediately after the BEGIN:VCARD line.
// body must be CRLF-normalized and begin with BEGIN:VCARD.
func injectVCardUID(body, uid string) string {
	idx := strings.Index(body, "\r\n")
	if idx < 0 {
		return body
	}
	return body[:idx+2] + "UID:" + uid + "\r\n" + body[idx+2:]
}

func ensureCRLF(body string) string {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	body = strings.ReplaceAll(body, "\r", "\n")
	body = strings.ReplaceAll(body, "\n", "\r\n")
	return body
}

// checkConditionalHeaders evaluates a request's If-Match and If-None-Match
// against the contact it writes, which is nil where there is none.
func checkConditionalHeaders(ifMatch, ifNoneMatch string, existing *store.Contact) bool {
	etag := ""
	if existing != nil {
		etag = existing.ETag
	}
	return httpcond.Check(ifMatch, ifNoneMatch, etag, existing != nil)
}

// StatusCode maps a service error to the HTTP status the API should return.
func StatusCode(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	case errors.Is(err, ErrPreconditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrLocked):
		return http.StatusLocked
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

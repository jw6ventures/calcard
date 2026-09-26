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

	// Sharees may hold per-contact grants, so pagination is applied in Go after
	// ACL filtering (mirroring events.Service.ListEvents) to keep page sizes
	// correct.
	limit, offset := filter.Limit, filter.Offset
	dbFilter := filter
	dbFilter.Limit = 0
	dbFilter.Offset = 0
	contacts, err := s.store.Contacts.ListForBookFiltered(ctx, bookID, dbFilter)
	if err != nil {
		return nil, err
	}
	entriesByPath, err := s.prefetchACLEntries(ctx, user, bookID, contacts)
	if err != nil {
		return nil, err
	}
	visible := make([]store.Contact, 0, len(contacts))
	for _, c := range contacts {
		if canReadContactFromEntries(user, bookID, book.UserID, contactResourceName(c), entriesByPath) {
			visible = append(visible, c)
		}
	}
	if offset > 0 {
		if offset >= len(visible) {
			return []store.Contact{}, nil
		}
		visible = visible[offset:]
	}
	if limit > 0 && limit < len(visible) {
		visible = visible[:limit]
	}
	return visible, nil
}

// GetContact returns a single readable contact in an accessible address book.
func (s *Service) GetContact(ctx context.Context, user *store.User, bookID int64, uid string) (*store.Contact, error) {
	c, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, err
	}
	if c == nil {
		// Distinguish a missing contact in an accessible book from a hidden book.
		if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "read"); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, contactResourceName(*c), "read"); err != nil {
		return nil, err
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
	conditional := input.IfMatch != "" || input.IfNoneMatch != ""
	for attempt := 1; ; attempt++ {
		c, err := s.createContactOnce(ctx, user, bookID, uid, body, input)
		if !errors.Is(err, store.ErrResourceStateChanged) {
			return c, err == nil, err
		}
		if conditional {
			return nil, false, ErrPreconditionFailed
		}
		if attempt == maxUpdateAttempts {
			return nil, false, ErrConflict
		}
	}
}

func (s *Service) createContactOnce(ctx context.Context, user *store.User, bookID int64, uid, body string, input UpsertInput) (*store.Contact, error) {
	resourceName := newContactResourceName(uid)
	guard, err := s.writeGuard(ctx, user, bookID, resourceName)
	if err != nil {
		return nil, err
	}
	if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "bind"); err != nil {
		return nil, err
	}
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, ErrPreconditionFailed
	}
	if existing != nil {
		return nil, ErrConflict
	}
	result, err := s.store.PutContactObject(ctx, store.ContactObjectWrite{
		AddressBookID: bookID,
		UID:           uid,
		ResourceName:  resourceName,
		RawVCard:      body,
		ETag:          utils.GenerateETag(body),
		ExpectedState: store.DAVResourceState{ACL: guard},
		StatePath:     addressBookACLResourcePaths(bookID, resourceName)[0],
	})
	switch {
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrConflict):
		return nil, ErrConflict
	case errors.Is(err, store.ErrPreconditionFailed):
		return nil, ErrPreconditionFailed
	case err != nil:
		return nil, err
	}
	return result.Contact, nil
}

// writeGuard pins the ACL entries a non-owner's write to one contact is
// decided on: the book's and the contact's own. The store refuses the write if
// they change before it commits, and the caller re-decides. An owner's access
// does not come from the ACL, so it writes unguarded.
func (s *Service) writeGuard(ctx context.Context, user *store.User, bookID int64, resourceName string) (*store.ACLGuard, error) {
	if s.store.ACLEntries == nil || s.store.AddressBooks == nil {
		return nil, nil
	}
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if book == nil || (user != nil && book.UserID == user.ID) {
		return nil, nil
	}
	paths := append([]string{addressBookACLCollectionPath(bookID)}, addressBookACLResourcePaths(bookID, resourceName)...)
	entries, err := s.store.ACLEntries.ListByResources(ctx, paths)
	if err != nil {
		return nil, err
	}
	return store.NewACLGuard(paths, entries), nil
}

// maxUpdateAttempts bounds how often an update is re-read and re-applied
// when another writer changes the contact between the read and the write.
const maxUpdateAttempts = 3

// UpdateContact replaces an existing contact identified by uid. The write is
// conditional on the version it was built from: a structured edit is merged
// into that version, so a concurrent write is re-read and the edit re-applied
// rather than overwritten. A caller's own If-Match or If-None-Match is honoured
// instead of retrying.
func (s *Service) UpdateContact(ctx context.Context, user *store.User, bookID int64, uid string, input UpsertInput) (*store.Contact, bool, error) {
	conditional := input.IfMatch != "" || input.IfNoneMatch != ""
	for attempt := 1; ; attempt++ {
		c, err := s.updateContactOnce(ctx, user, bookID, uid, input)
		if !errors.Is(err, store.ErrResourceStateChanged) {
			return c, false, err
		}
		if conditional {
			return nil, false, ErrPreconditionFailed
		}
		if attempt == maxUpdateAttempts {
			return nil, false, ErrConflict
		}
	}
}

func (s *Service) updateContactOnce(ctx context.Context, user *store.User, bookID int64, uid string, input UpsertInput) (*store.Contact, error) {
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "write-content"); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	resourceName := contactResourceName(*existing)
	guard, err := s.writeGuard(ctx, user, bookID, resourceName)
	if err != nil {
		return nil, err
	}
	if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, resourceName, "write-content"); err != nil {
		return nil, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, ErrPreconditionFailed
	}
	body, normalizedUID, err := normalizeVCardPayload(input, uid, existing.RawVCard)
	if err != nil {
		return nil, err
	}
	if normalizedUID != uid {
		return nil, fmt.Errorf("%w: uid mismatch", ErrBadRequest)
	}
	expected := store.ContactDAVResourceState(existing)
	expected.ACL = guard
	statePaths := addressBookACLResourcePaths(bookID, resourceName)
	result, err := s.store.PutContactObject(ctx, store.ContactObjectWrite{
		AddressBookID: bookID,
		UID:           uid,
		ResourceName:  resourceName,
		RawVCard:      body,
		ETag:          utils.GenerateETag(body),
		ExpectedState: expected,
		StatePath:     statePaths[0],
	})
	switch {
	case errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrConflict):
		return nil, ErrConflict
	case errors.Is(err, store.ErrPreconditionFailed):
		return nil, ErrPreconditionFailed
	case err != nil:
		return nil, err
	}
	return result.Contact, nil
}

// DeleteContact removes a contact, honoring If-Match/If-None-Match preconditions.
func (s *Service) DeleteContact(ctx context.Context, user *store.User, bookID int64, uid, ifMatch, ifNoneMatch string) error {
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return err
	}
	resourceName := ""
	if existing != nil {
		resourceName = contactResourceName(*existing)
	}
	if _, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, resourceName, "unbind"); err != nil {
		return err
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return ErrPreconditionFailed
	}
	if existing == nil {
		return ErrNotFound
	}
	resourcePaths := addressBookACLResourcePaths(bookID, contactResourceName(*existing))
	if len(resourcePaths) == 0 {
		return ErrNotFound
	}
	err = s.store.DeleteContactAndState(ctx, bookID, store.ContactDAVResourceState(existing), resourcePaths[0], nil)
	if errors.Is(err, store.ErrResourceStateChanged) {
		if ifMatch != "" || ifNoneMatch != "" {
			return ErrPreconditionFailed
		}
		return ErrConflict
	}
	return err
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
	if user != nil && book.UserID == user.ID {
		return book, nil
	}
	granted, applicable, err := s.privilegeDecision(ctx, user, bookID, resourceName, privilege)
	if err != nil {
		return nil, err
	}
	if granted {
		return book, nil
	}
	if applicable {
		return nil, ErrForbidden
	}
	return nil, ErrNotFound
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

	for _, principal := range acl.PrincipalHrefs(user) {
		entries, err := s.store.ACLEntries.ListByPrincipal(ctx, principal)
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
			book, err := s.loadAddressBookWithPrivilege(ctx, user, bookID, "", "read")
			if err != nil {
				if err == ErrNotFound || err == ErrForbidden {
					continue
				}
				return nil, err
			}
			seen[bookID] = struct{}{}
			access, err := s.AddressBookAccessFor(ctx, user, *book)
			if err != nil {
				return nil, err
			}
			result = append(result, access)
		}
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
	if uid == "" || uid == "." || uid == ".." {
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

func checkConditionalHeaders(ifMatch, ifNoneMatch string, existing *store.Contact) bool {
	if ifNoneMatch == "*" {
		return existing == nil
	}
	if ifMatch != "" {
		if existing == nil {
			return false
		}
		return strings.Trim(ifMatch, "\"") == existing.ETag
	}
	if ifNoneMatch != "" {
		if existing == nil {
			return true
		}
		return strings.Trim(ifNoneMatch, "\"") != existing.ETag
	}
	return true
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
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

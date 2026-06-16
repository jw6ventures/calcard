// Package contacts provides the business logic for the address book / contact
// REST API. Address books are owned outright by a single user (no per-resource
// ACL like calendars), so access control here is simple ownership.
package contacts

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// MaxBodyBytes bounds the size of a contact write payload.
const MaxBodyBytes int64 = 10 * 1024 * 1024

var (
	ErrNotFound           = errors.New("not found")
	ErrBadRequest         = errors.New("bad request")
	ErrConflict           = errors.New("conflict")
	ErrPreconditionFailed = errors.New("precondition failed")
)

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

// ListAddressBooks returns the address books owned by the user.
func (s *Service) ListAddressBooks(ctx context.Context, user *store.User) ([]store.AddressBook, error) {
	return s.store.AddressBooks.ListByUser(ctx, user.ID)
}

// GetAddressBook returns a single owned address book, or ErrNotFound.
func (s *Service) GetAddressBook(ctx context.Context, user *store.User, bookID int64) (*store.AddressBook, error) {
	return s.requireOwnedBook(ctx, user, bookID)
}

// ListContacts returns contacts in an owned address book matching the filter.
func (s *Service) ListContacts(ctx context.Context, user *store.User, bookID int64, filter store.ContactFilter) ([]store.Contact, error) {
	if _, err := s.requireOwnedBook(ctx, user, bookID); err != nil {
		return nil, err
	}
	return s.store.Contacts.ListForBookFiltered(ctx, bookID, filter)
}

// GetContact returns a single contact in an owned address book, or ErrNotFound.
func (s *Service) GetContact(ctx context.Context, user *store.User, bookID int64, uid string) (*store.Contact, error) {
	if _, err := s.requireOwnedBook(ctx, user, bookID); err != nil {
		return nil, err
	}
	c, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrNotFound
	}
	return c, nil
}

// CreateContact creates a new contact. It fails with ErrConflict if one with the
// same UID already exists.
func (s *Service) CreateContact(ctx context.Context, user *store.User, bookID int64, input UpsertInput) (*store.Contact, bool, error) {
	if _, err := s.requireOwnedBook(ctx, user, bookID); err != nil {
		return nil, false, err
	}
	body, uid, err := normalizeVCardPayload(input, "")
	if err != nil {
		return nil, false, err
	}
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, false, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}
	if existing != nil {
		return nil, false, ErrConflict
	}
	return s.saveContact(ctx, bookID, uid, uid, body, input.IfMatch, input.IfNoneMatch)
}

// UpdateContact replaces an existing contact identified by uid.
func (s *Service) UpdateContact(ctx context.Context, user *store.User, bookID int64, uid string, input UpsertInput) (*store.Contact, bool, error) {
	if _, err := s.requireOwnedBook(ctx, user, bookID); err != nil {
		return nil, false, err
	}
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, ErrNotFound
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}
	body, normalizedUID, err := normalizeVCardPayload(input, uid)
	if err != nil {
		return nil, false, err
	}
	if normalizedUID != uid {
		return nil, false, fmt.Errorf("%w: uid mismatch", ErrBadRequest)
	}
	resourceName := existing.ResourceName
	if resourceName == "" {
		resourceName = uid
	}
	return s.saveContact(ctx, bookID, uid, resourceName, body, input.IfMatch, input.IfNoneMatch)
}

// DeleteContact removes a contact, honoring If-Match/If-None-Match preconditions.
func (s *Service) DeleteContact(ctx context.Context, user *store.User, bookID int64, uid, ifMatch, ifNoneMatch string) error {
	if _, err := s.requireOwnedBook(ctx, user, bookID); err != nil {
		return err
	}
	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return err
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return ErrPreconditionFailed
	}
	if existing == nil {
		return ErrNotFound
	}
	return s.store.Contacts.DeleteByUID(ctx, bookID, uid)
}

func (s *Service) requireOwnedBook(ctx context.Context, user *store.User, bookID int64) (*store.AddressBook, error) {
	book, err := s.store.AddressBooks.GetByID(ctx, bookID)
	if err != nil {
		return nil, err
	}
	// Treat a missing or unowned book identically so the API does not leak the
	// existence of other users' address books.
	if book == nil || book.UserID != user.ID {
		return nil, ErrNotFound
	}
	return book, nil
}

func (s *Service) saveContact(ctx context.Context, bookID int64, uid, resourceName, body, ifMatch, ifNoneMatch string) (*store.Contact, bool, error) {
	existingByResource, err := s.store.Contacts.GetByResourceName(ctx, bookID, resourceName)
	if err != nil {
		return nil, false, err
	}
	if existingByResource != nil && existingByResource.UID != uid {
		return nil, false, ErrConflict
	}

	existing, err := s.store.Contacts.GetByUID(ctx, bookID, uid)
	if err != nil {
		return nil, false, err
	}
	if existing != nil && existing.ResourceName != "" && existing.ResourceName != resourceName {
		return nil, false, ErrConflict
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}

	etag := utils.GenerateETag(body)
	created := existing == nil
	c, err := s.store.Contacts.Upsert(ctx, store.Contact{
		AddressBookID: bookID,
		UID:           uid,
		ResourceName:  resourceName,
		RawVCard:      body,
		ETag:          etag,
	})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			return nil, false, ErrConflict
		}
		return nil, false, err
	}
	return c, created, nil
}

func normalizeVCardPayload(input UpsertInput, expectedUID string) (string, string, error) {
	if strings.TrimSpace(input.RawVCard) != "" {
		body := ensureCRLF(strings.TrimSpace(input.RawVCard))
		if err := validateVCard(body); err != nil {
			return "", "", err
		}
		uid := utils.ExtractVCardUID(body)
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
	return buildStructuredContact(input.Structured, expectedUID)
}

func buildStructuredContact(input *StructuredInput, expectedUID string) (string, string, error) {
	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		return "", "", fmt.Errorf("%w: displayName is required", ErrBadRequest)
	}

	uid := strings.TrimSpace(input.UID)
	if expectedUID != "" {
		if uid != "" && uid != expectedUID {
			return "", "", fmt.Errorf("%w: path uid does not match payload uid", ErrBadRequest)
		}
		uid = expectedUID
	}
	if uid == "" {
		uid = utils.GenerateUID()
	}

	body := utils.BuildVCard(
		uid,
		displayName,
		strings.TrimSpace(input.FirstName),
		strings.TrimSpace(input.LastName),
		strings.TrimSpace(input.Email),
		strings.TrimSpace(input.Phone),
		strings.TrimSpace(input.Birthday),
		strings.TrimSpace(input.Notes),
		strings.TrimSpace(input.Company),
	)
	return body, uid, nil
}

func validateVCard(body string) error {
	upper := strings.ToUpper(body)
	if !strings.HasPrefix(upper, "BEGIN:VCARD") || !strings.Contains(upper, "END:VCARD") {
		return fmt.Errorf("%w: invalid vCard data", ErrBadRequest)
	}
	return nil
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

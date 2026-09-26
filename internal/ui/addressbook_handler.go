package ui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/contacts"
	httperrors "github.com/jw6ventures/calcard/internal/http/errors"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

type addressBookShareView struct {
	User      store.User
	Editor    bool
	CreatedAt time.Time
}

// AddressBooks displays the address books the user can access, with sharing
// controls for the ones they own.
func (h *Handler) AddressBooks(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	books, err := h.contacts.ListAccessibleAddressBooks(r.Context(), user)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	users, err := h.store.Users.ListActive(r.Context())
	if err != nil {
		http.Error(w, "failed to load users", http.StatusInternalServerError)
		return
	}
	userMap := make(map[int64]store.User, len(users))
	for _, u := range users {
		userMap[u.ID] = u
	}

	type addressBookView struct {
		Access          contacts.AddressBookAccess
		Shares          []addressBookShareView
		ShareCandidates []store.User
	}

	items := make([]addressBookView, 0, len(books))
	for _, book := range books {
		bv := addressBookView{Access: book}
		if !book.Shared {
			shares, err := h.addressBookShareViews(r.Context(), user, book.ID, userMap)
			if err != nil {
				http.Error(w, "failed to load shares", http.StatusInternalServerError)
				return
			}
			bv.Shares = shares
			shared := make(map[int64]struct{}, len(shares))
			for _, s := range shares {
				shared[s.User.ID] = struct{}{}
			}
			for _, candidate := range users {
				if candidate.ID == user.ID {
					continue
				}
				if _, ok := shared[candidate.ID]; ok {
					continue
				}
				bv.ShareCandidates = append(bv.ShareCandidates, candidate)
			}
		}
		items = append(items, bv)
	}

	data := h.withFlash(r, map[string]any{
		"Title":            "Address Books",
		"User":             user,
		"Books":            books,
		"AddressBookViews": items,
	})
	h.render(w, r, "addressbooks.html", data)
}

func (h *Handler) addressBookShareViews(ctx context.Context, owner *store.User, bookID int64, userMap map[int64]store.User) ([]addressBookShareView, error) {
	shares, err := h.contacts.ListAddressBookShares(ctx, owner, bookID)
	if err != nil {
		return nil, err
	}
	views := make([]addressBookShareView, 0, len(shares))
	for _, s := range shares {
		u, ok := userMap[s.UserID]
		if !ok {
			continue
		}
		views = append(views, addressBookShareView{User: u, Editor: s.Editor, CreatedAt: s.CreatedAt})
	}
	return views, nil
}

// CreateAddressBook creates a new address book.
func (h *Handler) CreateAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "name is required"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	_, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name})
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "failed to create"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "addressbook_created"})
}

// RenameAddressBook renames an existing address book.
func (h *Handler) RenameAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "name is required"})
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), id)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "rename failed"})
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := h.store.AddressBooks.Rename(r.Context(), user.ID, id, name); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "rename failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "addressbook_renamed"})
}

// DeleteAddressBook deletes an address book.
func (h *Handler) DeleteAddressBook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	if err := h.store.AddressBooks.Delete(r.Context(), user.ID, id); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "delete failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "addressbook_deleted"})
}

// ShareAddressBook shares an address book with another user.
func (h *Handler) ShareAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid address book"})
		return
	}
	targetID, err := strconv.ParseInt(r.FormValue("user_id"), 10, 64)
	if err != nil || targetID == 0 {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid user"})
		return
	}
	editor := strings.EqualFold(strings.TrimSpace(r.FormValue("role")), "editor")
	if err := h.contacts.ShareAddressBook(r.Context(), user, bookID, targetID, editor); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": addressBookShareError(err)})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "addressbook_shared"})
}

// UnshareAddressBook removes a share (owner) or leaves a shared book (sharee).
func (h *Handler) UnshareAddressBook(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid address book"})
		return
	}
	targetID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil || targetID == 0 {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid user"})
		return
	}
	if err := h.contacts.UnshareAddressBook(r.Context(), user, bookID, targetID); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": addressBookShareError(err)})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "addressbook_updated"})
}

// writeContactAccessError maps a contacts.Service error to an HTTP response for
// non-redirecting handlers (hidden books are 404; a sharee lacking a privilege
// is 403).
func (h *Handler) writeContactAccessError(w http.ResponseWriter, err error) {
	switch contacts.StatusCode(err) {
	case http.StatusNotFound:
		http.Error(w, "not found", http.StatusNotFound)
	case http.StatusForbidden:
		http.Error(w, "forbidden", http.StatusForbidden)
	default:
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
	}
}

func addressBookShareError(err error) string {
	switch contacts.StatusCode(err) {
	case http.StatusNotFound:
		return "not found"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusBadRequest:
		return "invalid request"
	default:
		return "failed to update sharing"
	}
}

// ViewAddressBook displays an address book and its contacts.
func (h *Handler) ViewAddressBook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.contacts.GetAddressBook(r.Context(), user, id)
	if err != nil {
		h.writeContactAccessError(w, err)
		return
	}
	access, err := h.contacts.AddressBookAccessFor(r.Context(), user, *book)
	if err != nil {
		http.Error(w, "failed to resolve access", http.StatusInternalServerError)
		return
	}

	// Parse pagination params
	page, limit := h.parsePagination(r)
	offset := (page - 1) * limit

	result, err := h.store.Contacts.ListForBookPaginated(r.Context(), id, limit, offset)
	if err != nil {
		http.Error(w, "failed to load contacts", http.StatusInternalServerError)
		return
	}

	// Build view data with parsed fields
	var contactData []map[string]any
	for _, c := range result.Items {
		displayName := "Unnamed Contact"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}
		var email string
		if c.PrimaryEmail != nil {
			email = *c.PrimaryEmail
		}
		contactData = append(contactData, map[string]any{
			"UID":          c.UID,
			"DisplayName":  displayName,
			"Email":        email,
			"LastModified": c.LastModified,
			"RawVCard":     c.RawVCard,
			"ETag":         c.ETag,
		})
	}

	// Get the address books the user may move contacts into (editor access).
	accessibleBooks, err := h.contacts.ListAccessibleAddressBooks(r.Context(), user)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	allBooks := make([]store.AddressBook, 0, len(accessibleBooks))
	for _, b := range accessibleBooks {
		if b.Editor {
			allBooks = append(allBooks, b.AddressBook)
		}
	}

	totalPages := (result.TotalCount + limit - 1) / limit
	data := h.withFlash(r, map[string]any{
		"Title":           book.Name + " - Address Book",
		"User":            user,
		"AddressBook":     book,
		"CanEdit":         access.Editor,
		"Shared":          access.Shared,
		"AllAddressBooks": allBooks,
		"Contacts":        contactData,
		"Page":            page,
		"Limit":           limit,
		"TotalCount":      result.TotalCount,
		"TotalPages":      totalPages,
		"HasPrev":         page > 1,
		"HasNext":         page < totalPages,
		"PrevPage":        page - 1,
		"NextPage":        page + 1,
	})
	h.render(w, r, "addressbook_view.html", data)
}

// CreateContact creates a new contact in an address book.
func (h *Handler) CreateContact(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	input := contacts.StructuredInput{
		DisplayName: strings.TrimSpace(r.FormValue("display_name")),
		FirstName:   strings.TrimSpace(r.FormValue("first_name")),
		LastName:    strings.TrimSpace(r.FormValue("last_name")),
		Email:       strings.TrimSpace(r.FormValue("email")),
		Phone:       strings.TrimSpace(r.FormValue("phone")),
		Birthday:    strings.TrimSpace(r.FormValue("birthday")),
		Notes:       strings.TrimSpace(r.FormValue("notes")),
		Company:     strings.TrimSpace(r.FormValue("company")),
	}
	// The service does not judge an address's shape, so that check is the
	// form's own.
	if validateContactEmail(input.Email) != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "The email address is not valid."})
		return
	}
	// The service owns the UID, vCard and DAV resource name a contact is
	// stored with, so a contact written here and one written by a CardDAV PUT
	// carry the same identity.
	if _, _, err := h.contacts.CreateContact(r.Context(), user, bookID, contacts.UpsertInput{Structured: &input}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID),
			map[string]string{"error": contactWriteFlashError(r, err, "failed to create contact")})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_created"})
}

// contactWriteFlashError is the flash text for a failed contact write. A
// refusal is described by the error's type, in words for the person filling in
// the form: the errors' own text is written for DAV clients and logs. Anything
// other than a refusal is a fault of this server rather than of the
// submission, so it is logged and answered with the caller's fallback.
func contactWriteFlashError(r *http.Request, err error, fallback string) string {
	const unclassified = "The contact could not be saved because some of its details are not valid."
	var fieldErr *contacts.FieldError
	switch {
	case errors.As(err, &fieldErr):
		if message := contactFieldMessage(fieldErr); message != "" {
			return message
		}
		return unclassified
	case errors.Is(err, utils.ErrInvalidBirthday):
		return "The birthday is not a valid date."
	case errors.Is(err, utils.ErrInvalidUID):
		return "The contact ID cannot be stored."
	case errors.Is(err, utils.ErrControlCharacter):
		return "The contact contains characters it cannot store."
	case errors.Is(err, contacts.ErrCardNotEditable):
		return "This contact is stored in a form the editor cannot update without losing data. Edit it in a CardDAV client instead."
	case errors.Is(err, contacts.ErrPreconditionFailed):
		return "This contact was changed elsewhere since the page loaded. Reload the page to see the current version, then make your changes again."
	case contacts.StatusCode(err) == http.StatusBadRequest:
		return unclassified
	}
	httperrors.LogError(r, fallback, err)
	return fallback
}

// contactFieldLabels names the structured fields the way the form does.
var contactFieldLabels = map[string]string{
	"uid":         "contact ID",
	"displayName": "display name",
	"firstName":   "first name",
	"lastName":    "last name",
	"email":       "email address",
	"phone":       "phone number",
	"birthday":    "birthday",
	"company":     "company",
	"notes":       "notes",
}

// contactFieldMessage words a refused field for the flash, or returns "" for a
// field the form does not know. The submitted value is never quoted back,
// because the flash travels in the redirect URL.
func contactFieldMessage(err *contacts.FieldError) string {
	label, ok := contactFieldLabels[err.Field]
	if !ok {
		return ""
	}
	switch err.Reason {
	case contacts.ReasonRequired:
		return "Enter a " + label + "."
	case contacts.ReasonControlCharacters:
		return "The " + label + " contains characters a contact cannot store."
	case contacts.ReasonUIDCharacters:
		return "The " + label + " " + contacts.ReasonUIDCharacters + "."
	default:
		return "The " + label + " " + strings.TrimSuffix(err.Reason, ".") + "."
	}
}

// storedContactHasEmail reports whether the stored contact's card carries
// email as one of its EMAIL values, as the page read it: the raw value, before
// any unescaping. A contact that cannot be read carries none.
func (h *Handler) storedContactHasEmail(ctx context.Context, user *store.User, bookID int64, uid, email string) bool {
	contact, err := h.contacts.GetContact(ctx, user, bookID, uid)
	if err != nil || contact == nil {
		return false
	}
	for _, line := range utils.UnfoldLines(contact.RawVCard) {
		keyPart, value, ok := ical.SplitContentLine(line)
		if !ok {
			continue
		}
		name, _, _ := strings.Cut(keyPart, ";")
		if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
			name = name[dot+1:]
		}
		if strings.EqualFold(strings.TrimSpace(name), "EMAIL") && strings.TrimSpace(value) == email {
			return true
		}
	}
	return false
}

// validateContactEmail rejects an address no mail system could route. vCard
// EMAIL carries no domain policy (RFC 6350 section 6.4.2), and a CardDAV client
// may store whatever it likes there, so this only catches the shapes that are
// certainly typos -- a missing "@", a missing side of it, an embedded space --
// rather than judging the domain. An empty field means the contact has no
// email, which is not an error.
func validateContactEmail(email string) error {
	if email == "" {
		return nil
	}
	// ParseAddress also accepts the "Name <addr>" form; the field holds a bare
	// address, so anything it had to strip means the value was not one.
	parsed, err := mail.ParseAddress(email)
	if err != nil || parsed.Address != email {
		return errors.New("not a valid email address")
	}
	return nil
}

// requireEditableAddressBook resolves an address book the current user may
// modify (owner or editor share). It writes the appropriate error response and
// returns ok=false when access is denied.
func (h *Handler) requireEditableAddressBook(w http.ResponseWriter, r *http.Request, bookID int64) (*store.AddressBook, bool) {
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.contacts.GetAddressBook(r.Context(), user, bookID)
	if err != nil {
		h.writeContactAccessError(w, err)
		return nil, false
	}
	access, err := h.contacts.AddressBookAccessFor(r.Context(), user, *book)
	if err != nil {
		http.Error(w, "failed to resolve access", http.StatusInternalServerError)
		return nil, false
	}
	if !access.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
		return nil, false
	}
	return book, true
}

// UpdateContact updates an existing contact.
func (h *Handler) UpdateContact(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	uid := resourceUIDParam(r)
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	input := contacts.StructuredInput{
		UID:         uid,
		DisplayName: strings.TrimSpace(r.FormValue("display_name")),
		FirstName:   strings.TrimSpace(r.FormValue("first_name")),
		LastName:    strings.TrimSpace(r.FormValue("last_name")),
		Email:       strings.TrimSpace(r.FormValue("email")),
		Phone:       strings.TrimSpace(r.FormValue("phone")),
		Birthday:    strings.TrimSpace(r.FormValue("birthday")),
		Notes:       strings.TrimSpace(r.FormValue("notes")),
		Company:     strings.TrimSpace(r.FormValue("company")),
	}
	// The service does not judge an address's shape, so that check is the
	// form's own. An address the stored card already carries is sent back
	// unchanged by the form, and is left to whichever client wrote it.
	if validateContactEmail(input.Email) != nil && !h.storedContactHasEmail(r.Context(), user, bookID, uid, input.Email) {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "The email address is not valid."})
		return
	}
	// An edit changes the contact, not its identity: the service carries the
	// stored resource name across the write, which keeps the href the sync
	// reports publish for this contact stable.
	// The form posts the ETag of the card it was built from, so a form built
	// before another client changed the contact is refused rather than
	// writing its stale values over that change.
	update := contacts.UpsertInput{Structured: &input, IfMatch: strings.TrimSpace(r.FormValue("etag"))}
	if _, _, err := h.contacts.UpdateContact(r.Context(), user, bookID, uid, update); err != nil {
		switch contacts.StatusCode(err) {
		case http.StatusNotFound:
			http.Error(w, "contact not found", http.StatusNotFound)
		case http.StatusForbidden:
			http.Error(w, "forbidden", http.StatusForbidden)
		default:
			h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID),
				map[string]string{"error": contactWriteFlashError(r, err, "failed to update contact")})
		}
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_updated"})
}

// DeleteContact removes a contact from an address book.
func (h *Handler) DeleteContact(w http.ResponseWriter, r *http.Request) {
	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	uid := resourceUIDParam(r)
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}

	contact, err := h.store.Contacts.GetByUID(r.Context(), bookID, uid)
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to delete contact"})
		return
	}
	if contact == nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_deleted"})
		return
	}
	resourceName := contact.ResourceName
	if resourceName == "" {
		resourceName = contact.UID
	}
	resourcePath := path.Join("/dav/addressbooks", strconv.FormatInt(bookID, 10), resourceName)
	if err := h.store.DeleteContactAndState(r.Context(), bookID, store.ContactDAVResourceState(contact), resourcePath, nil); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to delete contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_deleted"})
}

// MoveContact moves a contact from one address book to another.
func (h *Handler) MoveContact(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	uid := resourceUIDParam(r)
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	targetBookIDStr := strings.TrimSpace(r.FormValue("target_address_book_id"))
	if targetBookIDStr == "" {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "target address book is required"})
		return
	}

	targetBookID, err := strconv.ParseInt(targetBookIDStr, 10, 64)
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "invalid target address book id"})
		return
	}

	user, _ := auth.UserFromContext(r.Context())

	// Moving = remove from source + create in target, so the user needs editor
	// access (owner or write share) on both books.
	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}
	targetBook, err := h.contacts.GetAddressBook(r.Context(), user, targetBookID)
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "target address book not found"})
		return
	}
	targetAccess, err := h.contacts.AddressBookAccessFor(r.Context(), user, *targetBook)
	if err != nil {
		http.Error(w, "failed to resolve access", http.StatusInternalServerError)
		return
	}
	if !targetAccess.Editor {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "cannot write to target address book"})
		return
	}

	// Verify contact exists in source address book
	contact, err := h.store.Contacts.GetByUID(r.Context(), bookID, uid)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if contact == nil {
		http.Error(w, "contact not found", http.StatusNotFound)
		return
	}

	// Move the contact
	destResourceName := contact.ResourceName
	if destResourceName == "" {
		destResourceName = contact.UID
	}
	if existingByUID, err := h.store.Contacts.GetByUID(r.Context(), targetBookID, uid); err != nil {
		http.Error(w, "failed to check target address book", http.StatusInternalServerError)
		return
	} else if existingByUID != nil && targetBookID != bookID {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "contact already exists in target address book"})
		return
	}
	if existingByName, err := h.store.Contacts.GetByResourceName(r.Context(), targetBookID, destResourceName); err != nil {
		http.Error(w, "failed to check target address book", http.StatusInternalServerError)
		return
	} else if existingByName != nil && (targetBookID != bookID || existingByName.UID != contact.UID) {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "contact already exists in target address book"})
		return
	}

	if err := h.store.Contacts.MoveToAddressBook(r.Context(), bookID, targetBookID, uid, destResourceName); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to move contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", targetBookID), map[string]string{"status": "contact_moved"})
}

// ImportAddressBook imports contacts from a VCF file into an address book.
func (h *Handler) ImportAddressBook(w http.ResponseWriter, r *http.Request) {
	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}

	// Parse multipart form
	if err := r.ParseMultipartForm(10 << 20); err != nil { // 10 MB max
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "invalid file upload"})
		return
	}

	file, _, err := r.FormFile("file")
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "no file uploaded"})
		return
	}
	defer file.Close()

	// Read file content
	contentBytes, err := io.ReadAll(file)
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to read file"})
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	target := fmt.Sprintf("/addressbooks/%d", bookID)
	result, err := h.contacts.ImportVCards(r.Context(), user, bookID, string(contentBytes))
	if err != nil {
		switch contacts.StatusCode(err) {
		case http.StatusBadRequest:
			h.redirect(w, r, target, map[string]string{"error": "no contacts found in file"})
		case http.StatusNotFound, http.StatusForbidden:
			h.writeContactAccessError(w, err)
		default:
			httperrors.LogError(r, "import contacts", err)
			h.redirect(w, r, target, map[string]string{"error": "failed to import contacts"})
		}
		return
	}

	flash := map[string]string{"error": importSkipFlash(result.Skipped)}
	if result.Imported > 0 {
		flash["status"] = fmt.Sprintf("Imported %d %s.", result.Imported, plural(result.Imported, "contact", "contacts"))
	}
	h.redirect(w, r, target, flash)
}

// importSkipMessage words a skipped card's reason from its code alone. The
// flash travels in the redirect URL and a skip's Reason may quote the file, so
// nothing from it is repeated.
func importSkipMessage(skip contacts.ImportSkip) string {
	switch skip.Code {
	case contacts.ImportSkipMissingFN:
		return "it has no name"
	case contacts.ImportSkipUnsupportedCharset:
		return "it uses a character set this server cannot read"
	case contacts.ImportSkipMalformed:
		return "it is not a well-formed vCard"
	case contacts.ImportSkipDuplicateUID:
		return "another contact already uses this UID"
	case contacts.ImportSkipForbidden:
		return "you may not change this contact"
	case contacts.ImportSkipChanged:
		return "the contact changed while it was being imported"
	default:
		return "not a vCard this server can read"
	}
}

// maxImportSkipsShown bounds the skipped cards the flash names.
const maxImportSkipsShown = 3

// importSkipFlash is the flash for the cards an import skipped, or "" when it
// skipped none. Cards are named by position, never by UID, which is file
// content.
func importSkipFlash(skipped []contacts.ImportSkip) string {
	if len(skipped) == 0 {
		return ""
	}
	parts := make([]string, 0, maxImportSkipsShown+1)
	for i, skip := range skipped {
		if i == maxImportSkipsShown {
			parts = append(parts, fmt.Sprintf("and %d more", len(skipped)-maxImportSkipsShown))
			break
		}
		parts = append(parts, fmt.Sprintf("card %d (%s)", skip.Card, importSkipMessage(skip)))
	}
	return fmt.Sprintf("%d %s skipped: %s.", len(skipped), plural(len(skipped), "card", "cards"), strings.Join(parts, ", "))
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

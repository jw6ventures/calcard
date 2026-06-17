package ui

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/contacts"
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
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "created"})
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
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "renamed"})
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
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "deleted"})
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
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "shared"})
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
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "updated"})
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

	displayName := strings.TrimSpace(r.FormValue("display_name"))
	if displayName == "" {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "name is required"})
		return
	}

	firstName := strings.TrimSpace(r.FormValue("first_name"))
	lastName := strings.TrimSpace(r.FormValue("last_name"))
	email := strings.TrimSpace(r.FormValue("email"))
	phone := strings.TrimSpace(r.FormValue("phone"))
	birthday := strings.TrimSpace(r.FormValue("birthday"))
	notes := strings.TrimSpace(r.FormValue("notes"))
	company := strings.TrimSpace(r.FormValue("company"))

	uid := utils.GenerateUID()
	vcard := utils.BuildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes, company)
	etag := utils.GenerateETag(vcard)

	if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{
		AddressBookID: bookID,
		UID:           uid,
		RawVCard:      vcard,
		ETag:          etag,
	}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to create contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_created"})
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

	uid := chi.URLParam(r, "uid")
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}

	existing, err := h.store.Contacts.GetByUID(r.Context(), bookID, uid)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if existing == nil {
		http.Error(w, "contact not found", http.StatusNotFound)
		return
	}

	displayName := strings.TrimSpace(r.FormValue("display_name"))
	if displayName == "" {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "name is required"})
		return
	}

	firstName := strings.TrimSpace(r.FormValue("first_name"))
	lastName := strings.TrimSpace(r.FormValue("last_name"))
	email := strings.TrimSpace(r.FormValue("email"))
	phone := strings.TrimSpace(r.FormValue("phone"))
	birthday := strings.TrimSpace(r.FormValue("birthday"))
	notes := strings.TrimSpace(r.FormValue("notes"))
	company := strings.TrimSpace(r.FormValue("company"))

	vcard := utils.BuildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes, company)
	etag := utils.GenerateETag(vcard)

	if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{
		AddressBookID: bookID,
		UID:           uid,
		RawVCard:      vcard,
		ETag:          etag,
	}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to update contact"})
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

	uid := chi.URLParam(r, "uid")
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	if _, ok := h.requireEditableAddressBook(w, r, bookID); !ok {
		return
	}

	if err := h.store.Contacts.DeleteByUID(r.Context(), bookID, uid); err != nil {
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

	uid := chi.URLParam(r, "uid")
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

	// Parse VCF file
	vcards, err := utils.ParseVCFFile(string(contentBytes))
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to parse VCF file"})
		return
	}

	if len(vcards) == 0 {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "no contacts found in file"})
		return
	}

	// Import each vCard
	imported := 0
	for _, vcard := range vcards {
		// Extract UID or generate one if missing
		uid := utils.ExtractVCardUID(vcard)
		if uid == "" {
			uid = utils.GenerateUID()
			// Inject UID into vCard if missing
			vcard = strings.Replace(vcard, "BEGIN:VCARD\r\n", fmt.Sprintf("BEGIN:VCARD\r\nUID:%s\r\n", uid), 1)
		}

		etag := utils.GenerateETag(vcard)

		if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{
			AddressBookID: bookID,
			UID:           uid,
			RawVCard:      vcard,
			ETag:          etag,
		}); err != nil {
			// Continue importing other contacts even if one fails
			continue
		}
		imported++
	}

	if imported == 0 {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to import any contacts"})
		return
	}

	statusMsg := fmt.Sprintf("imported %d contact(s)", imported)
	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": statusMsg})
}

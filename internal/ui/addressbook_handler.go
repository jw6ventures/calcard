package ui

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/store"
	"gitea.jw6.us/james/calcard/internal/ui/utils"
	"github.com/go-chi/chi/v5"
)

// AddressBooks displays the user's address books.
func (h *Handler) AddressBooks(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	books, err := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	data := h.withFlash(r, map[string]any{
		"Title": "Address Books",
		"User":  user,
		"Books": books,
	})
	h.render(w, "addressbooks.html", data)
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

// ViewAddressBook displays an address book and its contacts.
func (h *Handler) ViewAddressBook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
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

	totalPages := (result.TotalCount + limit - 1) / limit
	data := h.withFlash(r, map[string]any{
		"Title":       book.Name + " - Address Book",
		"User":        user,
		"AddressBook": book,
		"Contacts":    contactData,
		"Page":        page,
		"Limit":       limit,
		"TotalCount":  result.TotalCount,
		"TotalPages":  totalPages,
		"HasPrev":     page > 1,
		"HasNext":     page < totalPages,
		"PrevPage":    page - 1,
		"NextPage":    page + 1,
	})
	h.render(w, "addressbook_view.html", data)
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

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
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

	uid := utils.GenerateUID()
	vcard := utils.BuildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes)
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

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
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

	vcard := utils.BuildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes)
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

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := h.store.Contacts.DeleteByUID(r.Context(), bookID, uid); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to delete contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_deleted"})
}

// ImportAddressBook imports contacts from a VCF file into an address book.
func (h *Handler) ImportAddressBook(w http.ResponseWriter, r *http.Request) {
	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
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

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/contacts"
	"github.com/jw6ventures/calcard/internal/store"
)

const maxContactSearchLen = 256

const maxContactLimit = 1000

type contactWriteRequest struct {
	InputMode  string                    `json:"inputMode"`
	RawVCard   string                    `json:"rawVcard"`
	Structured *contacts.StructuredInput `json:"structured"`
}

type addressBookResponse struct {
	ID          int64   `json:"id"`
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	Shared      bool    `json:"shared"`
	ReadOnly    bool    `json:"readOnly"`
}

type addressBookShareResponse struct {
	UserID int64  `json:"userId"`
	Email  string `json:"email"`
	Role   string `json:"role"`
}

type shareAddressBookRequest struct {
	UserID int64  `json:"userId"`
	Role   string `json:"role"`
}

type contactResponse struct {
	UID           string  `json:"uid"`
	AddressBookID int64   `json:"addressBookId"`
	ResourceName  string  `json:"resourceName"`
	DisplayName   *string `json:"displayName,omitempty"`
	Email         *string `json:"email,omitempty"`
	Birthday      *string `json:"birthday,omitempty"`
	ETag          string  `json:"etag"`
	LastModified  string  `json:"lastModified"`
	RawVCard      string  `json:"rawVcard"`
}

func (h *Handler) ListAddressBooks(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	books, err := h.contacts.ListAccessibleAddressBooks(r.Context(), user)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	resp := make([]addressBookResponse, 0, len(books))
	for _, b := range books {
		resp = append(resp, toAddressBookAccessResponse(b))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) GetAddressBook(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return
	}
	book, err := h.contacts.GetAddressBook(r.Context(), user, bookID)
	if err != nil {
		writeContactError(w, err)
		return
	}
	access, err := h.contacts.AddressBookAccessFor(r.Context(), user, *book)
	if err != nil {
		http.Error(w, "failed to resolve access", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, toAddressBookAccessResponse(access))
}

func (h *Handler) ListContacts(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return
	}
	filter, err := parseContactFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, err := h.contacts.ListContacts(r.Context(), user, bookID, filter)
	if err != nil {
		writeContactError(w, err)
		return
	}
	resp := make([]contactResponse, 0, len(items))
	for _, c := range items {
		resp = append(resp, toContactResponse(c))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) GetContact(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, uid, ok := parseBookIDAndUID(w, r)
	if !ok {
		return
	}
	c, err := h.contacts.GetContact(r.Context(), user, bookID, uid)
	if err != nil {
		writeContactError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+c.ETag+`"`)
	writeJSON(w, http.StatusOK, toContactResponse(*c))
}

func (h *Handler) CreateContact(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return
	}
	input, err := decodeContactInput(r)
	if err != nil {
		writeContactError(w, err)
		return
	}
	c, created, err := h.contacts.CreateContact(r.Context(), user, bookID, input)
	if err != nil {
		writeContactError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("ETag", `"`+c.ETag+`"`)
	writeJSON(w, status, toContactResponse(*c))
}

func (h *Handler) UpdateContact(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, uid, ok := parseBookIDAndUID(w, r)
	if !ok {
		return
	}
	input, err := decodeContactInput(r)
	if err != nil {
		writeContactError(w, err)
		return
	}
	c, _, err := h.contacts.UpdateContact(r.Context(), user, bookID, uid, input)
	if err != nil {
		writeContactError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+c.ETag+`"`)
	writeJSON(w, http.StatusOK, toContactResponse(*c))
}

func (h *Handler) DeleteContact(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, uid, ok := parseBookIDAndUID(w, r)
	if !ok {
		return
	}
	err := h.contacts.DeleteContact(r.Context(), user, bookID, uid, r.Header.Get("If-Match"), r.Header.Get("If-None-Match"))
	if err != nil {
		writeContactError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListAddressBookShares returns the principals an owned address book is shared with.
func (h *Handler) ListAddressBookShares(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return
	}
	shares, err := h.contacts.ListAddressBookShares(r.Context(), user, bookID)
	if err != nil {
		writeContactError(w, err)
		return
	}
	resp := make([]addressBookShareResponse, 0, len(shares))
	for _, s := range shares {
		email := ""
		if u, err := h.store.Users.GetByID(r.Context(), s.UserID); err == nil && u != nil {
			email = u.PrimaryEmail
		}
		resp = append(resp, addressBookShareResponse{UserID: s.UserID, Email: email, Role: shareRole(s.Editor)})
	}
	writeJSON(w, http.StatusOK, resp)
}

// ShareAddressBook grants another user read-only or editor access to an owned book.
func (h *Handler) ShareAddressBook(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return
	}
	var req shareAddressBookRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.contacts.ShareAddressBook(r.Context(), user, bookID, req.UserID, roleIsEditor(req.Role)); err != nil {
		writeContactError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// UnshareAddressBook revokes a share (owner) or leaves a shared book (sharee).
func (h *Handler) UnshareAddressBook(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return
	}
	targetID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil || targetID == 0 {
		http.Error(w, "invalid user id", http.StatusBadRequest)
		return
	}
	if err := h.contacts.UnshareAddressBook(r.Context(), user, bookID, targetID); err != nil {
		writeContactError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func roleIsEditor(role string) bool {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "editor", "write", "read-write", "readwrite":
		return true
	default:
		return false
	}
}

func shareRole(editor bool) string {
	if editor {
		return "editor"
	}
	return "read"
}

func decodeContactInput(r *http.Request) (contacts.UpsertInput, error) {
	input := contacts.UpsertInput{
		IfMatch:     r.Header.Get("If-Match"),
		IfNoneMatch: r.Header.Get("If-None-Match"),
	}
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "text/vcard") || strings.HasPrefix(contentType, "text/x-vcard") || strings.HasPrefix(contentType, "application/vcard") {
		body, err := io.ReadAll(io.LimitReader(r.Body, contacts.MaxBodyBytes+1))
		if err != nil {
			return input, err
		}
		if int64(len(body)) > contacts.MaxBodyBytes {
			return input, fmtContactBadRequest(errors.New("request body too large"))
		}
		input.RawVCard = string(body)
		return input, nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, contacts.MaxBodyBytes+1))
	if err != nil {
		return input, err
	}
	if int64(len(body)) > contacts.MaxBodyBytes {
		return input, fmtContactBadRequest(errors.New("request body too large"))
	}
	if len(body) == 0 {
		return input, fmtContactBadRequest(errors.New("missing request body"))
	}
	var req contactWriteRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return input, fmtContactBadRequest(err)
	}
	switch req.InputMode {
	case "", "structured":
		input.Structured = req.Structured
	case "raw_vcard":
		input.RawVCard = req.RawVCard
	default:
		return input, fmtContactBadRequest(errors.New("invalid inputMode"))
	}
	if req.InputMode == "raw_vcard" && strings.TrimSpace(input.RawVCard) == "" {
		return input, fmtContactBadRequest(errors.New("rawVcard is required"))
	}
	if req.InputMode != "raw_vcard" && req.Structured == nil {
		return input, fmtContactBadRequest(errors.New("structured is required"))
	}
	return input, nil
}

func parseAddressBookID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return 0, false
	}
	return id, true
}

func parseBookIDAndUID(w http.ResponseWriter, r *http.Request) (int64, string, bool) {
	bookID, ok := parseAddressBookID(w, r)
	if !ok {
		return 0, "", false
	}
	rawUID := chi.URLParam(r, "uid")
	uid, err := url.PathUnescape(rawUID)
	if err != nil || uid == "" {
		uid = rawUID
	}
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return 0, "", false
	}
	return bookID, uid, true
}

// parseContactFilter builds a store.ContactFilter from the request query string.
// Supported params: name, email, q (matches either field), limit, offset.
func parseContactFilter(r *http.Request) (store.ContactFilter, error) {
	q := r.URL.Query()
	var f store.ContactFilter

	for name, dst := range map[string]*string{
		"name":  &f.Name,
		"email": &f.Email,
		"q":     &f.Query,
	} {
		v := strings.TrimSpace(q.Get(name))
		if len(v) > maxContactSearchLen {
			return f, fmt.Errorf("%s is too long (max %d characters)", name, maxContactSearchLen)
		}
		*dst = v
	}

	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 0 {
			return f, errors.New("invalid limit")
		}
		if limit > maxContactLimit {
			limit = maxContactLimit
		}
		f.Limit = limit
	}
	if raw := q.Get("offset"); raw != "" {
		offset, err := strconv.Atoi(raw)
		if err != nil || offset < 0 {
			return f, errors.New("invalid offset")
		}
		f.Offset = offset
	}

	return f, nil
}

func toAddressBookResponse(b store.AddressBook) addressBookResponse {
	return addressBookResponse{
		ID:          b.ID,
		Name:        b.Name,
		Description: b.Description,
	}
}

func toAddressBookAccessResponse(a contacts.AddressBookAccess) addressBookResponse {
	resp := toAddressBookResponse(a.AddressBook)
	resp.Shared = a.Shared
	resp.ReadOnly = !a.Editor
	return resp
}

func toContactResponse(c store.Contact) contactResponse {
	var birthday *string
	if c.Birthday != nil {
		v := c.Birthday.Format("2006-01-02")
		birthday = &v
	}
	return contactResponse{
		UID:           c.UID,
		AddressBookID: c.AddressBookID,
		ResourceName:  c.ResourceName,
		DisplayName:   c.DisplayName,
		Email:         c.PrimaryEmail,
		Birthday:      birthday,
		ETag:          c.ETag,
		LastModified:  c.LastModified.UTC().Format(time.RFC3339),
		RawVCard:      c.RawVCard,
	}
}

func writeContactError(w http.ResponseWriter, err error) {
	status := contacts.StatusCode(err)
	if status == http.StatusInternalServerError {
		http.Error(w, "internal server error", status)
		return
	}
	http.Error(w, err.Error(), status)
}

func fmtContactBadRequest(err error) error {
	return errors.Join(contacts.ErrBadRequest, err)
}

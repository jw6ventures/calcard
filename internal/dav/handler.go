package dav

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
)

// Handler serves WebDAV/CalDAV/CardDAV requests.
type Handler struct {
	cfg   *config.Config
	store *store.Store
}

var errInvalidSyncToken = errors.New("invalid sync token")

// maxDAVBodyBytes is the maximum body size for DAV requests. Preventing DOS attacks.
const maxDAVBodyBytes int64 = 10 * 1024 * 1024

func NewHandler(cfg *config.Config, store *store.Store) *Handler {
	return &Handler{cfg: cfg, store: store}
}

func (h *Handler) Options(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "OPTIONS, HEAD, GET, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT")
	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.Header().Set("Accept-Patch", "application/xml")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Head(w http.ResponseWriter, r *http.Request) {
	h.Get(w, r)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean(r.URL.Path)
	if !strings.HasPrefix(cleanPath, "/dav") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	if calendarID, uid, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		if _, err := h.loadCalendar(r.Context(), user, calendarID); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to load calendar", http.StatusInternalServerError)
			return
		}
		event, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if event == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/calendar")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", event.ETag))
		_, _ = w.Write([]byte(event.RawICAL))
		return
	}

	if addressBookID, uid, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			if err == store.ErrNotFound {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, "failed to load address book", http.StatusInternalServerError)
			return
		}
		contact, err := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
		if err != nil {
			http.Error(w, "failed to load contact", http.StatusInternalServerError)
			return
		}
		if contact == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/vcard")
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", contact.ETag))
		_, _ = w.Write([]byte(contact.RawVCard))
		return
	}

	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Propfind(w http.ResponseWriter, r *http.Request) {
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if depth == "" {
		depth = "1"
	}

	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	responses, err := h.buildPropfindResponses(r.Context(), r.URL.Path, depth, user)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, http.ErrNotSupported) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	payload := multistatus{
		XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
		XmlnsCS:  "http://calendarserver.org/ns/",
		Response: responses,
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_ = xml.NewEncoder(w).Encode(payload)
}

func (h *Handler) Proppatch(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Mkcol(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		http.Error(w, "collection name required", http.StatusBadRequest)
		return
	}
	if _, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name}); err != nil {
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Mkcalendar(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		http.Error(w, "calendar name required", http.StatusBadRequest)
		return
	}
	if _, err := h.store.Calendars.Create(r.Context(), store.Calendar{UserID: user.ID, Name: name}); err != nil {
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if r.ContentLength > maxDAVBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxDAVBodyBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	etag := fmt.Sprintf("%x", sha256.Sum256(body))

	if calendarID, uid, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		cal, err := h.loadCalendar(r.Context(), user, calendarID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
			return
		}
		if !cal.Editor {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		existing, _ := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if _, err := h.store.Events.Upsert(r.Context(), store.Event{CalendarID: calendarID, UID: uid, RawICAL: string(body), ETag: etag}); err != nil {
			http.Error(w, "failed to save event", http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
		if existing == nil {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	if addressBookID, uid, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}
		existing, _ := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
		if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{AddressBookID: addressBookID, UID: uid, RawVCard: string(body), ETag: etag}); err != nil {
			http.Error(w, "failed to save contact", http.StatusInternalServerError)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
		if existing == nil {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
		return
	}

	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if calendarID, uid, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		cal, err := h.loadCalendar(r.Context(), user, calendarID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "not found", status)
			return
		}
		if !cal.Editor {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if err := h.store.Events.DeleteByUID(r.Context(), calendarID, uid); err != nil {
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if addressBookID, uid, matched := parseResourcePath(cleanPath, "/dav/addressbooks"); matched {
		if _, err := h.loadAddressBook(r.Context(), user, addressBookID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "not found", status)
			return
		}
		if err := h.store.Contacts.DeleteByUID(r.Context(), addressBookID, uid); err != nil {
			http.Error(w, "failed to delete", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "unsupported path", http.StatusBadRequest)
}

func (h *Handler) Report(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	if r.ContentLength > maxDAVBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxDAVBodyBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	var report reportRequest
	if err := xml.Unmarshal(body, &report); err != nil {
		http.Error(w, "invalid REPORT body", http.StatusBadRequest)
		return
	}

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
		if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
			http.Error(w, "invalid calendar path", http.StatusBadRequest)
			return
		}
		calID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid calendar id", http.StatusBadRequest)
			return
		}
		cal, err := h.loadCalendar(r.Context(), user, calID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
			return
		}
		responses, syncToken, err := h.calendarReportResponses(r.Context(), cal, h.principalURL(user), cleanPath, report)
		if err != nil {
			if errors.Is(err, errInvalidSyncToken) {
				http.Error(w, "invalid sync token", http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		payload := multistatus{
			XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:    "DAV:",
			XmlnsC:    "urn:ietf:params:xml:ns:caldav",
			XmlnsA:    "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:   "http://calendarserver.org/ns/",
			SyncToken: syncToken,
			Response:  responses,
		}
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
		if len(parts) < 2 || parts[1] == "" {
			http.Error(w, "invalid address book path", http.StatusBadRequest)
			return
		}
		bookID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid address book id", http.StatusBadRequest)
			return
		}
		book, err := h.loadAddressBook(r.Context(), user, bookID)
		if err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}
		responses, syncToken, err := h.addressBookReportResponses(r.Context(), book, h.principalURL(user), cleanPath, report)
		if err != nil {
			if errors.Is(err, errInvalidSyncToken) {
				http.Error(w, "invalid sync token", http.StatusForbidden)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		payload := multistatus{
			XMLName:   xml.Name{Space: "DAV:", Local: "multistatus"},
			XmlnsD:    "DAV:",
			XmlnsC:    "urn:ietf:params:xml:ns:caldav",
			XmlnsA:    "urn:ietf:params:xml:ns:carddav",
			XmlnsCS:   "http://calendarserver.org/ns/",
			SyncToken: syncToken,
			Response:  responses,
		}
		w.WriteHeader(http.StatusMultiStatus)
		_ = xml.NewEncoder(w).Encode(payload)
		return
	}

	http.Error(w, "REPORT not supported for path", http.StatusBadRequest)
}

// buildPropfindResponses constructs DAV multistatus responses for a path.
func (h *Handler) buildPropfindResponses(ctx context.Context, reqPath, depth string, user *store.User) ([]response, error) {
	cleanPath := path.Clean(reqPath)
	if !strings.HasPrefix(cleanPath, "/dav") {
		return nil, http.ErrNotSupported
	}

	// Ensure trailing slash on collections for predictable href values.
	ensureCollectionHref := func(p string) string {
		if !strings.HasSuffix(p, "/") {
			return p + "/"
		}
		return p
	}

	switch {
	case cleanPath == "/dav" || cleanPath == "/dav/":
		href := ensureCollectionHref(cleanPath)
		principalHref := h.principalURL(user)
		res := []response{rootCollectionResponse(href, user, principalHref)}
		if depth == "1" {
			res = append(res,
				collectionResponse(ensureCollectionHref("/dav/calendars"), "Calendars"),
				collectionResponse(ensureCollectionHref("/dav/addressbooks"), "Address Books"),
				principalResponse(ensureCollectionHref(principalHref), user),
			)
		}
		return res, nil
	case strings.HasPrefix(cleanPath, "/dav/principals"):
		return h.principalResponses(cleanPath, depth, user, ensureCollectionHref)
	case strings.HasPrefix(cleanPath, "/dav/calendars"):
		return h.calendarResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
	case strings.HasPrefix(cleanPath, "/dav/addressbooks"):
		return h.addressBookResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
	default:
		return nil, http.ErrNotSupported
	}
}

func (h *Handler) calendarResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/calendars")
		res := []response{collectionResponse(base, "Calendars")}
		if depth == "1" {
			cals, err := h.store.Calendars.ListAccessible(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)
			for _, c := range cals {
				href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(c.ID)))
				ctag := fmt.Sprintf("%d", c.CTag)
				syncToken := buildSyncToken("cal", c.ID, c.UpdatedAt)
				res = append(res, calendarCollectionResponse(href, c.Name, c.Description, c.Timezone, principalHref, syncToken, ctag))
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	calID, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil {
		return nil, http.ErrNotSupported
	}
	cal, err := h.loadCalendar(ctx, user, calID)
	if err != nil {
		return nil, err
	}
	href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(cal.ID)))
	ctag := fmt.Sprintf("%d", cal.CTag)
	syncToken := buildSyncToken("cal", cal.ID, cal.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{calendarCollectionResponse(href, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, ctag)}
	if depth == "1" {
		events, err := h.store.Events.ListForCalendar(ctx, cal.ID)
		if err != nil {
			return nil, err
		}
		base := ensureCollectionHref(href)
		res = append(res, calendarResourceResponses(base, events)...)
	}
	return res, nil
}

func (h *Handler) addressBookResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if relPath == "" {
		base := ensureCollectionHref("/dav/addressbooks")
		res := []response{collectionResponse(base, "Address Books")}
		if depth == "1" {
			books, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			principalHref := h.principalURL(user)
			for _, b := range books {
				href := ensureCollectionHref(path.Join("/dav/addressbooks", fmt.Sprint(b.ID)))
				ctag := fmt.Sprintf("%d", b.CTag)
				syncToken := buildSyncToken("card", b.ID, b.UpdatedAt)
				res = append(res, addressBookCollectionResponse(href, b.Name, b.Description, principalHref, syncToken, ctag))
			}
		}
		return res, nil
	}

	segments := strings.Split(relPath, "/")
	bookID, err := strconv.ParseInt(segments[0], 10, 64)
	if err != nil {
		return nil, http.ErrNotSupported
	}
	book, err := h.loadAddressBook(ctx, user, bookID)
	if err != nil {
		return nil, err
	}
	href := ensureCollectionHref(path.Join("/dav/addressbooks", fmt.Sprint(book.ID)))
	ctag := fmt.Sprintf("%d", book.CTag)
	syncToken := buildSyncToken("card", book.ID, book.UpdatedAt)
	principalHref := h.principalURL(user)
	res := []response{addressBookCollectionResponse(href, book.Name, book.Description, principalHref, syncToken, ctag)}
	if depth == "1" {
		contacts, err := h.store.Contacts.ListForBook(ctx, book.ID)
		if err != nil {
			return nil, err
		}
		base := ensureCollectionHref(href)
		res = append(res, addressBookResourceResponses(base, contacts)...)
	}
	return res, nil
}

func (h *Handler) loadCalendar(ctx context.Context, user *store.User, id int64) (*store.CalendarAccess, error) {
	cal, err := h.store.Calendars.GetAccessible(ctx, id, user.ID)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		return nil, store.ErrNotFound
	}
	return cal, nil
}

func (h *Handler) loadAddressBook(ctx context.Context, user *store.User, id int64) (*store.AddressBook, error) {
	book, err := h.store.AddressBooks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if book == nil || book.UserID != user.ID {
		return nil, store.ErrNotFound
	}
	return book, nil
}

// parseResourcePath extracts the numeric collection ID and resource UID from a DAV resource path.
// The returned boolean indicates whether the path matched the expected prefix and contained both parts.
func parseResourcePath(rawPath, prefix string) (int64, string, bool) {
	cleanPath := normalizeDAVHref(rawPath)
	if cleanPath == "" || !strings.HasPrefix(cleanPath, prefix) {
		return 0, "", false
	}
	trimmed := strings.TrimPrefix(cleanPath, prefix)
	trimmed = strings.TrimPrefix(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return 0, "", false
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", false
	}
	uid := strings.TrimSuffix(parts[1], path.Ext(parts[1]))
	if uid == "" {
		return 0, "", false
	}
	return id, uid, true
}

func normalizeDAVHref(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if u, err := url.Parse(trimmed); err == nil {
		if u.Path != "" {
			trimmed = u.Path
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + strings.TrimPrefix(cleaned, "/")
	}
	return cleaned
}

func resolveDAVHref(basePath, rawHref string) string {
	trimmed := strings.TrimSpace(rawHref)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		if u, err := url.Parse(trimmed); err == nil {
			return normalizeDAVHref(u.Path)
		}
		return ""
	}
	if strings.HasPrefix(trimmed, "/") {
		return normalizeDAVHref(trimmed)
	}
	if u, err := url.Parse(trimmed); err == nil && u.Path != "" {
		if strings.HasPrefix(u.Path, "/") {
			return normalizeDAVHref(u.Path)
		}
		trimmed = u.Path
	}
	base := normalizeDAVHref(basePath)
	if base == "" {
		base = "/"
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return normalizeDAVHref(path.Join(base, trimmed))
}

const syncTokenPrefix = "urn:calcard-sync"

type syncTokenInfo struct {
	Kind      string
	ID        int64
	Timestamp time.Time
}

func buildSyncToken(kind string, id int64, ts time.Time) string {
	nanos := int64(0)
	if !ts.IsZero() {
		nanos = ts.UTC().UnixNano()
	}
	return fmt.Sprintf("%s:%s:%d:%d", syncTokenPrefix, kind, id, nanos)
}

func parseSyncToken(token string) (syncTokenInfo, error) {
	if token == "" || !strings.HasPrefix(token, syncTokenPrefix+":") {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	parts := strings.Split(token[len(syncTokenPrefix)+1:], ":")
	if len(parts) != 3 {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	nanos, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return syncTokenInfo{}, errInvalidSyncToken
	}
	info := syncTokenInfo{Kind: parts[0], ID: id}
	if nanos > 0 {
		info.Timestamp = time.Unix(0, nanos).UTC()
	}
	return info, nil
}

func (h *Handler) calendarSyncTokenValue(ctx context.Context, cal *store.CalendarAccess) (string, time.Time) {
	return buildSyncToken("cal", cal.ID, cal.UpdatedAt), cal.UpdatedAt
}

func (h *Handler) addressBookSyncTokenValue(ctx context.Context, book *store.AddressBook) (string, time.Time) {
	return buildSyncToken("card", book.ID, book.UpdatedAt), book.UpdatedAt
}

type multistatus struct {
	XMLName   xml.Name   `xml:"d:multistatus"`
	XmlnsD    string     `xml:"xmlns:d,attr"`
	XmlnsC    string     `xml:"xmlns:cal,attr"`
	XmlnsA    string     `xml:"xmlns:card,attr"`
	XmlnsCS   string     `xml:"xmlns:cs,attr,omitempty"`
	SyncToken string     `xml:"d:sync-token,omitempty"`
	Response  []response `xml:"d:response"`
}

type response struct {
	Href     string     `xml:"d:href"`
	Propstat []propstat `xml:"d:propstat,omitempty"`
	Status   string     `xml:"d:status,omitempty"`
}

type propstat struct {
	Prop   prop   `xml:"d:prop"`
	Status string `xml:"d:status"`
}

type prop struct {
	DisplayName          string              `xml:"d:displayname,omitempty"`
	ResourceType         resourceType        `xml:"d:resourcetype"`
	GetETag              string              `xml:"d:getetag,omitempty"`
	GetContentType       string              `xml:"d:getcontenttype,omitempty"`
	CalendarData         cdataString         `xml:"cal:calendar-data,omitempty"`
	AddressData          cdataString         `xml:"card:address-data,omitempty"`
	CalendarDescription  string              `xml:"cal:calendar-description,omitempty"`
	CalendarTimezone     string              `xml:"cal:calendar-timezone,omitempty"`
	AddressBookDesc      string              `xml:"card:addressbook-description,omitempty"`
	SyncToken            string              `xml:"d:sync-token,omitempty"`
	CTag                 string              `xml:"cs:getctag,omitempty"`
	CurrentUserPrincipal *hrefProp           `xml:"d:current-user-principal,omitempty"`
	PrincipalURL         *hrefProp           `xml:"d:principal-URL,omitempty"`
	CalendarHomeSet      *hrefListProp       `xml:"cal:calendar-home-set,omitempty"`
	AddressbookHomeSet   *hrefListProp       `xml:"card:addressbook-home-set,omitempty"`
	SupportedReportSet   *supportedReportSet `xml:"d:supported-report-set,omitempty"`
}

// cdataString wraps string content in CDATA for raw XML output.
// This preserves special characters like CRLF in iCalendar/vCard data.
type cdataString string

func (c cdataString) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if c == "" {
		return nil
	}
	return e.EncodeElement(struct {
		S string `xml:",cdata"`
	}{S: string(c)}, start)
}

type resourceType struct {
	Collection  *struct{} `xml:"d:collection,omitempty"`
	Calendar    *struct{} `xml:"cal:calendar,omitempty"`
	AddressBook *struct{} `xml:"card:addressbook,omitempty"`
	Principal   *struct{} `xml:"d:principal,omitempty"`
}

type reportRequest struct {
	XMLName   xml.Name
	Hrefs     []string `xml:"DAV: href"`
	SyncToken string   `xml:"DAV: sync-token"`
}

func collectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}})},
	}
}

func calendarCollectionResponse(href, name string, description, timezone *string, principalHref, syncToken, ctag string) response {
	resp := response{
		Href:     href,
		Propstat: []propstat{statusOKPropWithExtras(name, resourceType{Collection: &struct{}{}, Calendar: &struct{}{}}, principalHref, true, false)},
	}
	if syncToken != "" {
		resp.Propstat[0].Prop.SyncToken = syncToken
	}
	if ctag != "" {
		resp.Propstat[0].Prop.CTag = ctag
	}
	if description != nil && *description != "" {
		resp.Propstat[0].Prop.CalendarDescription = *description
	}
	if timezone != nil && *timezone != "" {
		resp.Propstat[0].Prop.CalendarTimezone = *timezone
	}
	return resp
}

func addressBookCollectionResponse(href, name string, description *string, principalHref, syncToken, ctag string) response {
	resp := response{
		Href:     href,
		Propstat: []propstat{statusOKPropWithExtras(name, resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}}, principalHref, false, true)},
	}
	if syncToken != "" {
		resp.Propstat[0].Prop.SyncToken = syncToken
	}
	if ctag != "" {
		resp.Propstat[0].Prop.CTag = ctag
	}
	if description != nil && *description != "" {
		resp.Propstat[0].Prop.AddressBookDesc = *description
	}
	return resp
}

func statusOKProp(name string, rtype resourceType) propstat {
	return propstat{
		Prop: prop{
			DisplayName:  name,
			ResourceType: rtype,
		},
		Status: "HTTP/1.1 200 OK",
	}
}

func statusOKPropWithExtras(name string, rtype resourceType, principalHref string, includeCalendarHome, includeAddressHome bool) propstat {
	p := prop{
		DisplayName:          name,
		ResourceType:         rtype,
		CurrentUserPrincipal: &hrefProp{Href: principalHref},
	}
	if includeCalendarHome {
		p.CalendarHomeSet = &hrefListProp{Href: []string{"/dav/calendars/"}}
		p.SupportedReportSet = calendarSupportedReports()
	}
	if includeAddressHome {
		p.AddressbookHomeSet = &hrefListProp{Href: []string{"/dav/addressbooks/"}}
		p.SupportedReportSet = addressbookSupportedReports()
	}
	if !includeCalendarHome && !includeAddressHome {
		p.SupportedReportSet = combinedSupportedReports()
	}
	return propstat{Prop: p, Status: "HTTP/1.1 200 OK"}
}

func etagProp(etag, data string, calendar bool) propstat {
	propVal := prop{GetETag: fmt.Sprintf("\"%s\"", etag)}
	if calendar {
		propVal.CalendarData = cdataString(data)
		propVal.GetContentType = "text/calendar; charset=utf-8"
	} else {
		propVal.AddressData = cdataString(data)
		propVal.GetContentType = "text/vcard; charset=utf-8"
	}
	return propstat{Prop: propVal, Status: "HTTP/1.1 200 OK"}
}

func resourceResponse(href string, ps propstat) response {
	return response{Href: href, Propstat: []propstat{ps}}
}

// deletedResponse returns a response indicating the resource was deleted (for sync-collection).
func deletedResponse(href string) response {
	return response{Href: href, Status: "HTTP/1.1 404 Not Found"}
}

type hrefProp struct {
	Href string `xml:"d:href"`
}

type hrefListProp struct {
	Href []string `xml:"d:href"`
}

type supportedReportSet struct {
	Reports []supportedReport `xml:"d:supported-report"`
}

type supportedReport struct {
	Report reportType `xml:"d:report"`
}

type reportType struct {
	CalendarMultiGet    *struct{} `xml:"cal:calendar-multiget,omitempty"`
	CalendarQuery       *struct{} `xml:"cal:calendar-query,omitempty"`
	AddressbookMultiGet *struct{} `xml:"card:addressbook-multiget,omitempty"`
	AddressbookQuery    *struct{} `xml:"card:addressbook-query,omitempty"`
	SyncCollection      *struct{} `xml:"d:sync-collection,omitempty"`
}

func calendarSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
		},
	}
}

func addressbookSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
		},
	}
}

func combinedSupportedReports() *supportedReportSet {
	return &supportedReportSet{
		Reports: []supportedReport{
			{Report: reportType{CalendarMultiGet: &struct{}{}}},
			{Report: reportType{CalendarQuery: &struct{}{}}},
			{Report: reportType{AddressbookMultiGet: &struct{}{}}},
			{Report: reportType{AddressbookQuery: &struct{}{}}},
			{Report: reportType{SyncCollection: &struct{}{}}},
		},
	}
}

func (h *Handler) principalURL(user *store.User) string {
	return fmt.Sprintf("/dav/principals/%d/", user.ID)
}

func (h *Handler) principalResponses(cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	relPath := strings.Trim(strings.TrimPrefix(cleanPath, "/dav/principals"), "/")
	principalHref := ensureCollectionHref(h.principalURL(user))

	// Only the authenticated user's principal is exposed.
	if relPath == "" {
		res := []response{collectionResponse(ensureCollectionHref("/dav/principals"), "Principals")}
		if depth == "1" {
			res = append(res, principalResponse(principalHref, user))
		}
		return res, nil
	}

	if relPath != fmt.Sprint(user.ID) && relPath != fmt.Sprint(user.ID)+"/" {
		return nil, store.ErrNotFound
	}

	return []response{principalResponse(principalHref, user)}, nil
}

func principalResponse(href string, user *store.User) response {
	p := prop{
		DisplayName:          user.PrimaryEmail,
		ResourceType:         resourceType{Principal: &struct{}{}},
		PrincipalURL:         &hrefProp{Href: href},
		CurrentUserPrincipal: &hrefProp{Href: href},
		CalendarHomeSet:      &hrefListProp{Href: []string{"/dav/calendars/"}},
		AddressbookHomeSet:   &hrefListProp{Href: []string{"/dav/addressbooks/"}},
		SupportedReportSet:   combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: "HTTP/1.1 200 OK"}}}
}

func rootCollectionResponse(href string, user *store.User, principalHref string) response {
	p := prop{
		DisplayName:          "CalCard DAV",
		ResourceType:         resourceType{Collection: &struct{}{}},
		CurrentUserPrincipal: &hrefProp{Href: principalHref},
		SupportedReportSet:   combinedSupportedReports(),
	}
	return response{Href: href, Propstat: []propstat{{Prop: p, Status: "HTTP/1.1 200 OK"}}}
}

func (h *Handler) calendarReportResponses(ctx context.Context, cal *store.CalendarAccess, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.calendarMultiGet(ctx, cal.ID, report.Hrefs, cleanPath)
		return res, "", err
	case "calendar-query":
		res, err := h.calendarQuery(ctx, cal.ID, cleanPath)
		return res, "", err
	case "sync-collection":
		return h.calendarSyncCollection(ctx, cal, principalHref, cleanPath, report)
	default:
		// Fallback: return all events to keep clients moving even if they send unsupported report types.
		res, err := h.calendarQuery(ctx, cal.ID, cleanPath)
		return res, "", err
	}
}

func (h *Handler) addressBookReportResponses(ctx context.Context, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	switch report.XMLName.Local {
	case "addressbook-multiget":
		res, err := h.addressBookMultiGet(ctx, book.ID, report.Hrefs, cleanPath)
		return res, "", err
	case "addressbook-query":
		res, err := h.addressBookQuery(ctx, book.ID, cleanPath)
		return res, "", err
	case "sync-collection":
		return h.addressBookSyncCollection(ctx, book, principalHref, cleanPath, report)
	default:
		res, err := h.addressBookQuery(ctx, book.ID, cleanPath)
		return res, "", err
	}
}

func (h *Handler) calendarQuery(ctx context.Context, calID int64, cleanPath string) ([]response, error) {
	events, err := h.store.Events.ListForCalendar(ctx, calID)
	if err != nil {
		return nil, fmt.Errorf("failed to list events")
	}
	return calendarResourceResponses(cleanPath, events), nil
}

func (h *Handler) calendarMultiGet(ctx context.Context, calID int64, hrefs []string, cleanPath string) ([]response, error) {
	if len(hrefs) == 0 {
		return h.calendarQuery(ctx, calID, cleanPath)
	}
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		id, uid, ok := parseResourcePath(cleanHref, "/dav/calendars")
		if !ok || id != calID {
			continue
		}
		ev, err := h.store.Events.GetByUID(ctx, calID, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch event")
		}
		if ev == nil {
			continue
		}
		responses = append(responses, resourceResponse(cleanHref, etagProp(ev.ETag, ev.RawICAL, true)))
	}
	return responses, nil
}

func (h *Handler) addressBookQuery(ctx context.Context, bookID int64, cleanPath string) ([]response, error) {
	contacts, err := h.store.Contacts.ListForBook(ctx, bookID)
	if err != nil {
		return nil, fmt.Errorf("failed to list contacts")
	}
	return addressBookResourceResponses(cleanPath, contacts), nil
}

func (h *Handler) addressBookMultiGet(ctx context.Context, bookID int64, hrefs []string, cleanPath string) ([]response, error) {
	if len(hrefs) == 0 {
		return h.addressBookQuery(ctx, bookID, cleanPath)
	}
	var responses []response
	for _, href := range hrefs {
		cleanHref := resolveDAVHref(cleanPath, href)
		if cleanHref == "" {
			continue
		}
		id, uid, ok := parseResourcePath(cleanHref, "/dav/addressbooks")
		if !ok || id != bookID {
			continue
		}
		c, err := h.store.Contacts.GetByUID(ctx, bookID, uid)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch contact")
		}
		if c == nil {
			continue
		}
		responses = append(responses, resourceResponse(cleanHref, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses, nil
}

func calendarResourceResponses(base string, events []store.Event) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, ev := range events {
		href := baseHref + ev.UID + ".ics"
		responses = append(responses, resourceResponse(href, etagProp(ev.ETag, ev.RawICAL, true)))
	}
	return responses
}

func addressBookResourceResponses(base string, contacts []store.Contact) []response {
	baseHref := strings.TrimSuffix(base, "/") + "/"
	var responses []response
	for _, c := range contacts {
		href := baseHref + c.UID + ".vcf"
		responses = append(responses, resourceResponse(href, etagProp(c.ETag, c.RawVCard, false)))
	}
	return responses
}

func (h *Handler) calendarSyncCollection(ctx context.Context, cal *store.CalendarAccess, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	syncToken, _ := h.calendarSyncTokenValue(ctx, cal)
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "cal" || info.ID != cal.ID {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var events []store.Event
	var err error
	if since.IsZero() {
		events, err = h.store.Events.ListForCalendar(ctx, cal.ID)
	} else {
		events, err = h.store.Events.ListModifiedSince(ctx, cal.ID, since)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to list events")
	}

	responses := []response{
		calendarCollectionResponse(collectionHref, cal.Name, cal.Description, cal.Timezone, principalHref, syncToken, fmt.Sprintf("%d", cal.CTag)),
	}
	responses = append(responses, calendarResourceResponses(collectionHref, events)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "event", cal.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted events")
		}
		for _, d := range deleted {
			href := collectionHref + d.UID + ".ics"
			responses = append(responses, deletedResponse(href))
		}
	}

	return responses, syncToken, nil
}

func (h *Handler) addressBookSyncCollection(ctx context.Context, book *store.AddressBook, principalHref, cleanPath string, report reportRequest) ([]response, string, error) {
	syncToken, _ := h.addressBookSyncTokenValue(ctx, book)
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"

	var since time.Time
	if report.SyncToken != "" {
		info, err := parseSyncToken(report.SyncToken)
		if err != nil || info.Kind != "card" || info.ID != book.ID {
			return nil, "", errInvalidSyncToken
		}
		since = info.Timestamp
	}

	var contacts []store.Contact
	var err error
	if since.IsZero() {
		contacts, err = h.store.Contacts.ListForBook(ctx, book.ID)
	} else {
		contacts, err = h.store.Contacts.ListModifiedSince(ctx, book.ID, since)
	}
	if err != nil {
		return nil, "", fmt.Errorf("failed to list contacts")
	}

	responses := []response{
		addressBookCollectionResponse(collectionHref, book.Name, book.Description, principalHref, syncToken, fmt.Sprintf("%d", book.CTag)),
	}
	responses = append(responses, addressBookResourceResponses(collectionHref, contacts)...)

	// Include deleted resources if this is an incremental sync
	if !since.IsZero() {
		deleted, err := h.store.DeletedResources.ListDeletedSince(ctx, "contact", book.ID, since)
		if err != nil {
			return nil, "", fmt.Errorf("failed to list deleted contacts")
		}
		for _, d := range deleted {
			href := collectionHref + d.UID + ".vcf"
			responses = append(responses, deletedResponse(href))
		}
	}

	return responses, syncToken, nil
}

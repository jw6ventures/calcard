package dav

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/example/calcard/internal/auth"
	"github.com/example/calcard/internal/config"
	"github.com/example/calcard/internal/store"
)

// Handler serves WebDAV/CalDAV/CardDAV requests.
type Handler struct {
	cfg   *config.Config
	store *store.Store
}

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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	payload := multistatus{
		XMLName:  xml.Name{Space: "DAV:", Local: "multistatus"},
		XmlnsD:   "DAV:",
		XmlnsC:   "urn:ietf:params:xml:ns:caldav",
		XmlnsA:   "urn:ietf:params:xml:ns:carddav",
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	etag := fmt.Sprintf("%x", sha256.Sum256(body))

	if calendarID, uid, matched := parseResourcePath(cleanPath, "/dav/calendars"); matched {
		if _, err := h.loadCalendar(r.Context(), user, calendarID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
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
		if _, err := h.loadCalendar(r.Context(), user, calendarID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "not found", status)
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

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
		if len(parts) < 2 || parts[1] == "" {
			http.Error(w, "invalid calendar path", http.StatusBadRequest)
			return
		}
		calID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "invalid calendar id", http.StatusBadRequest)
			return
		}
		if _, err := h.loadCalendar(r.Context(), user, calID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "calendar not found", status)
			return
		}
		events, err := h.store.Events.ListForCalendar(r.Context(), calID)
		if err != nil {
			http.Error(w, "failed to list events", http.StatusInternalServerError)
			return
		}
		base := strings.TrimSuffix(cleanPath, "/") + "/"
		var responses []response
		for _, ev := range events {
			href := base + ev.UID + ".ics"
			responses = append(responses, response{Href: href, Propstat: []propstat{etagProp(ev.ETag, ev.RawICAL, true)}})
		}

		payload := multistatus{XMLName: xml.Name{Space: "DAV:", Local: "multistatus"}, XmlnsD: "DAV:", XmlnsC: "urn:ietf:params:xml:ns:caldav", XmlnsA: "urn:ietf:params:xml:ns:carddav", Response: responses}
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
		if _, err := h.loadAddressBook(r.Context(), user, bookID); err != nil {
			status := http.StatusInternalServerError
			if err == store.ErrNotFound {
				status = http.StatusNotFound
			}
			http.Error(w, "address book not found", status)
			return
		}
		contacts, err := h.store.Contacts.ListForBook(r.Context(), bookID)
		if err != nil {
			http.Error(w, "failed to list contacts", http.StatusInternalServerError)
			return
		}
		base := strings.TrimSuffix(cleanPath, "/") + "/"
		var responses []response
		for _, c := range contacts {
			href := base + c.UID + ".vcf"
			responses = append(responses, response{Href: href, Propstat: []propstat{etagProp(c.ETag, c.RawVCard, false)}})
		}

		payload := multistatus{XMLName: xml.Name{Space: "DAV:", Local: "multistatus"}, XmlnsD: "DAV:", XmlnsC: "urn:ietf:params:xml:ns:caldav", XmlnsA: "urn:ietf:params:xml:ns:carddav", Response: responses}
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
		res := []response{collectionResponse(href, "CalCard DAV")}
		if depth == "1" || depth == "0" {
			res = append(res,
				collectionResponse(ensureCollectionHref("/dav/calendars"), "Calendars"),
				collectionResponse(ensureCollectionHref("/dav/addressbooks"), "Address Books"),
			)
		}
		return res, nil
	case strings.HasPrefix(cleanPath, "/dav/calendars"):
		return h.calendarResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
	case strings.HasPrefix(cleanPath, "/dav/addressbooks"):
		return h.addressBookResponses(ctx, cleanPath, depth, user, ensureCollectionHref)
	default:
		return nil, http.ErrNotSupported
	}
}

func (h *Handler) calendarResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	segments := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	switch {
	case len(segments) == 0 || segments[0] == "":
		base := ensureCollectionHref("/dav/calendars")
		res := []response{collectionResponse(base, "Calendars")}
		if depth == "1" || depth == "0" {
			cals, err := h.store.Calendars.ListByUser(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			for _, c := range cals {
				href := ensureCollectionHref(path.Join("/dav/calendars", fmt.Sprint(c.ID)))
				res = append(res, calendarCollectionResponse(href, c.Name))
			}
		}
		return res, nil
	case len(segments) >= 1 && segments[0] != "":
		href := ensureCollectionHref(cleanPath)
		return []response{calendarCollectionResponse(href, segments[0])}, nil
	default:
		return nil, http.ErrNotSupported
	}
}

func (h *Handler) addressBookResponses(ctx context.Context, cleanPath, depth string, user *store.User, ensureCollectionHref func(string) string) ([]response, error) {
	segments := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	switch {
	case len(segments) == 0 || segments[0] == "":
		base := ensureCollectionHref("/dav/addressbooks")
		res := []response{collectionResponse(base, "Address Books")}
		if depth == "1" || depth == "0" {
			books, err := h.store.AddressBooks.ListByUser(ctx, user.ID)
			if err != nil {
				return nil, err
			}
			for _, b := range books {
				href := ensureCollectionHref(path.Join("/dav/addressbooks", fmt.Sprint(b.ID)))
				res = append(res, addressBookCollectionResponse(href, b.Name))
			}
		}
		return res, nil
	case len(segments) >= 1 && segments[0] != "":
		href := ensureCollectionHref(cleanPath)
		return []response{addressBookCollectionResponse(href, segments[0])}, nil
	default:
		return nil, http.ErrNotSupported
	}
}

func (h *Handler) loadCalendar(ctx context.Context, user *store.User, id int64) (*store.Calendar, error) {
	cal, err := h.store.Calendars.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if cal == nil || cal.UserID != user.ID {
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
func parseResourcePath(cleanPath, prefix string) (int64, string, bool) {
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

type multistatus struct {
	XMLName  xml.Name   `xml:"d:multistatus"`
	XmlnsD   string     `xml:"xmlns:d,attr"`
	XmlnsC   string     `xml:"xmlns:cal,attr"`
	XmlnsA   string     `xml:"xmlns:card,attr"`
	Response []response `xml:"d:response"`
}

type response struct {
	Href     string     `xml:"d:href"`
	Propstat []propstat `xml:"d:propstat"`
}

type propstat struct {
	Prop   prop   `xml:"d:prop"`
	Status string `xml:"d:status"`
}

type prop struct {
	DisplayName  string       `xml:"d:displayname"`
	ResourceType resourceType `xml:"d:resourcetype"`
	GetETag      string       `xml:"d:getetag,omitempty"`
	CalendarData string       `xml:"cal:calendar-data,omitempty"`
	AddressData  string       `xml:"card:address-data,omitempty"`
}

type resourceType struct {
	Collection  *struct{} `xml:"d:collection,omitempty"`
	Calendar    *struct{} `xml:"cal:calendar,omitempty"`
	AddressBook *struct{} `xml:"card:addressbook,omitempty"`
}

func collectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}})},
	}
}

func calendarCollectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}, Calendar: &struct{}{}})},
	}
}

func addressBookCollectionResponse(href, name string) response {
	return response{
		Href:     href,
		Propstat: []propstat{statusOKProp(name, resourceType{Collection: &struct{}{}, AddressBook: &struct{}{}})},
	}
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

func etagProp(etag, data string, calendar bool) propstat {
	propVal := prop{GetETag: fmt.Sprintf("\"%s\"", etag)}
	if calendar {
		propVal.CalendarData = data
	} else {
		propVal.AddressData = data
	}
	return propstat{Prop: propVal, Status: "HTTP/1.1 200 OK"}
}

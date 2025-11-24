package dav

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"path"
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
	w.Header().Set("Allow", "OPTIONS, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT")
	w.Header().Set("DAV", "1, 2, calendar-access, addressbook")
	w.Header().Set("Accept-Patch", "application/xml")
	w.WriteHeader(http.StatusNoContent)
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
	// TODO: handle property updates like displayname.
	http.Error(w, "PROPPATCH not implemented", http.StatusNotImplemented)
}

func (h *Handler) Mkcol(w http.ResponseWriter, r *http.Request) {
	// TODO: create collections (address books) when appropriate.
	http.Error(w, "MKCOL not implemented", http.StatusNotImplemented)
}

func (h *Handler) Mkcalendar(w http.ResponseWriter, r *http.Request) {
	// TODO: create CalDAV calendar collections.
	http.Error(w, "MKCALENDAR not implemented", http.StatusNotImplemented)
}

func (h *Handler) Put(w http.ResponseWriter, r *http.Request) {
	// TODO: store or update calendar events / contacts based on path.
	http.Error(w, "PUT not implemented", http.StatusNotImplemented)
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	// TODO: delete resources or collections.
	http.Error(w, "DELETE not implemented", http.StatusNotImplemented)
}

func (h *Handler) Report(w http.ResponseWriter, r *http.Request) {
	// TODO: handle calendar-query, calendar-multiget, addressbook-query, addressbook-multiget.
	http.Error(w, "REPORT not implemented", http.StatusNotImplemented)
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
		if depth == "1" {
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
		if depth == "1" {
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
		if depth == "1" {
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

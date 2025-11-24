package dav

import (
	"net/http"

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
	// TODO: set DAV headers and allowed methods.
	w.Header().Set("Allow", "OPTIONS, PROPFIND, PROPPATCH, MKCOL, MKCALENDAR, PUT, DELETE, REPORT")
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Propfind(w http.ResponseWriter, r *http.Request) {
	// TODO: parse depth header and respond with XML multi-status.
	http.Error(w, "PROPFIND not implemented", http.StatusNotImplemented)
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

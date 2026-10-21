package ui

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/http/csrf"
	"github.com/jw6ventures/calcard/internal/http/errors"
)

const defaultPageSize = 50

// resourceUIDParam returns the {uid} route parameter with percent-encoding
// resolved exactly once.
//
// Which of the two path forms chi matched on decides whether there is anything
// to resolve. chi routes on r.URL.RawPath when net/url set it -- which happens
// only when the request spelled a segment differently from how the decoded path
// re-encodes, as "a%40b.com" does for "@" -- and on the already-decoded
// r.URL.Path otherwise. Unescaping in the second case decodes the value a
// second time, so a UID carrying a literal percent ("literal%41", requested as
// "literal%2541") would come back as "literalA": a different resource, and one
// the caller never named.
func resourceUIDParam(r *http.Request) string {
	raw := chi.URLParam(r, "uid")
	if r.URL.RawPath == "" {
		return raw
	}
	decoded, err := url.PathUnescape(raw)
	if err != nil || decoded == "" {
		return raw
	}
	return decoded
}

// parsePagination extracts page and limit from query parameters.
func (h *Handler) parsePagination(r *http.Request) (page, limit int) {
	page = 1
	limit = defaultPageSize

	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}
	return
}

// flashMessages maps the status keys handlers redirect with to the sentence the
// page shows. A key missing here is rendered as itself, so every key a handler
// emits needs an entry; TestFlashMessageResolvesEveryHandlerStatusKey enforces
// that. Keys name their resource because a redirect target is reached from more
// than one handler, and "Created." alone does not say what was created.
var flashMessages = map[string]string{
	"addressbook_created":  "Address book created.",
	"addressbook_deleted":  "Address book deleted.",
	"addressbook_renamed":  "Address book renamed.",
	"addressbook_shared":   "Address book shared.",
	"addressbook_updated":  "Address book updated.",
	"app_password_created": "App password created. Copy it now -- it is not shown again.",
	"calendar_created":     "Calendar created.",
	"calendar_deleted":     "Calendar deleted.",
	"calendar_renamed":     "Calendar renamed.",
	"calendar_shared":      "Calendar shared.",
	"calendar_updated":     "Calendar updated.",
	"contact_created":      "Contact created.",
	"contact_deleted":      "Contact deleted.",
	"contact_moved":        "Contact moved.",
	"contact_updated":      "Contact updated.",
	"event_created":        "Event created.",
	"event_deleted":        "Event deleted.",
	"event_updated":        "Event updated.",
	"occurrence_deleted":   "Occurrence deleted.",
	"sessions_revoked":     "All other sessions revoked.",
	"session_revoked":      "Session revoked.",
}

// flashMessage resolves a status key to its sentence. The import handlers build
// their message from a count rather than picking a key, so an unmapped value is
// passed through as prose.
func flashMessage(status string) string {
	if message, ok := flashMessages[status]; ok {
		return message
	}
	return sentenceCase(status)
}

// sentenceCase renders a message the way the flash table already spells its
// entries, so a handler-supplied string and a mapped key do not sit next to
// each other in different styles. It assumes the message opens with an ordinary
// word; a message that must keep a lowercase first letter belongs in
// flashMessages rather than here.
func sentenceCase(message string) string {
	if message == "" {
		return ""
	}
	first, width := utf8.DecodeRuneInString(message)
	message = string(unicode.ToUpper(first)) + message[width:]
	if last, _ := utf8.DecodeLastRuneInString(message); !strings.ContainsRune(".!?", last) {
		message += "."
	}
	return message
}

// withFlash adds flash messages and CSRF token to template data.
func (h *Handler) withFlash(r *http.Request, data map[string]any) map[string]any {
	q := r.URL.Query()
	if status := q.Get("status"); status != "" {
		data["FlashMessage"] = flashMessage(status)
	}
	if err := q.Get("error"); err != "" {
		data["FlashError"] = sentenceCase(err)
	}
	if token := q.Get("token"); token != "" {
		data["PlainToken"] = token
	}
	if csrfToken := csrf.TokenFromContext(r.Context()); csrfToken != "" {
		data["CSRFToken"] = csrfToken
	}
	return data
}

// redirect redirects to a path with query parameters.
func (h *Handler) redirect(w http.ResponseWriter, r *http.Request, path string, params map[string]string) {
	q := url.Values{}
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	location := path
	if encoded := q.Encode(); encoded != "" {
		location += "?" + encoded
	}
	http.Redirect(w, r, location, http.StatusFound)
}

// render executes a template and writes the response.
func (h *Handler) render(w http.ResponseWriter, r *http.Request, name string, data any) {
	tmpl, ok := h.templates[name]
	if !ok {
		errors.InternalError(w, r, fmt.Errorf("template not found"), fmt.Sprintf("template %q not found", name))
		return
	}

	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		errors.InternalError(w, r, err, fmt.Sprintf("template render error for %q", name))
	}
}

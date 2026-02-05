package ui

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/jw6ventures/calcard/internal/http/csrf"
	"github.com/jw6ventures/calcard/internal/http/errors"
)

const defaultPageSize = 50

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

// withFlash adds flash messages and CSRF token to template data.
func (h *Handler) withFlash(r *http.Request, data map[string]any) map[string]any {
	q := r.URL.Query()
	if status := q.Get("status"); status != "" {
		data["FlashMessage"] = status
	}
	if err := q.Get("error"); err != "" {
		data["FlashError"] = err
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

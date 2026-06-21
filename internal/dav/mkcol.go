package dav

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/lib/pq"
)

func (h *DavServer) Mkcol(w http.ResponseWriter, r *http.Request) {
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.logger().Trace("Mkcol", "MKCOL %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLocks(w, r, "resource is locked", cleanPath, path.Dir(cleanPath)) {
		return
	}
	pendingLockPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve collection path", http.StatusInternalServerError)
		return
	}
	if !strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if len(parts) > 2 || (len(parts) == 2 && parts[0] != "" && parts[1] != "") {
		http.Error(w, "nested address book collections not allowed", http.StatusForbidden)
		return
	}
	name := strings.TrimSpace(parts[len(parts)-1])
	if name == "" {
		http.Error(w, "collection name required", http.StatusBadRequest)
		return
	}
	if _, err := strconv.ParseInt(name, 10, 64); err == nil {
		http.Error(w, "collection name must be non-numeric", http.StatusBadRequest)
		return
	}
	description := (*string)(nil)
	if r.Body != nil && r.Body != http.NoBody {
		body, err := readDAVBody(w, r, maxDAVBodyBytes)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "failed to read body", http.StatusBadRequest)
			}
			return
		}
		var mkReq mkcalendarRequest
		if len(body) > 0 {
			if err := safeUnmarshalXML(body, &mkReq); err != nil {
				http.Error(w, "invalid MKCOL body", http.StatusBadRequest)
				return
			}
			if mkReq.Set != nil {
				if mkReq.Set.Prop.DisplayName != nil && strings.TrimSpace(*mkReq.Set.Prop.DisplayName) != "" {
					name = strings.TrimSpace(*mkReq.Set.Prop.DisplayName)
				}
				description = mkReq.Set.Prop.AddressBookDesc
			}
		}
	}
	if _, err := strconv.ParseInt(name, 10, 64); err == nil {
		http.Error(w, "collection name must be non-numeric", http.StatusBadRequest)
		return
	}
	created, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name, Description: description})
	if err != nil {
		if errors.Is(err, store.ErrConflict) {
			http.Error(w, "address book already exists", http.StatusConflict)
			return
		}
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	if created != nil {
		location := path.Join("/dav/addressbooks", fmt.Sprint(created.ID)) + "/"
		if err := h.rebindCollectionLocks(r.Context(), pendingLockPath, strings.TrimSuffix(location, "/")); err != nil {
			if deleteErr := h.store.AddressBooks.Delete(r.Context(), user.ID, created.ID); deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) {
				log.Printf("failed to roll back address book %d after lock rebind failure: %v", created.ID, deleteErr)
			}
			http.Error(w, "failed to rebind collection locks", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", location)
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *DavServer) Mkcalendar(w http.ResponseWriter, r *http.Request) {
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.logger().Trace("Mkcalendar", "MKCALENDAR %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLocks(w, r, "resource is locked", cleanPath, path.Dir(cleanPath)) {
		return
	}
	pendingLockPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve collection path", http.StatusInternalServerError)
		return
	}
	if !strings.HasPrefix(cleanPath, "/dav/calendars/") {
		http.Error(w, "unsupported path", http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")

	if len(parts) > 2 || (len(parts) == 2 && parts[0] != "" && parts[1] != "") {
		http.Error(w, "nested calendar collections not allowed", http.StatusForbidden)
		return
	}

	pathName := strings.TrimSpace(parts[len(parts)-1])
	if pathName == "" {
		http.Error(w, "calendar name required", http.StatusBadRequest)
		return
	}
	if _, err := strconv.ParseInt(pathName, 10, 64); err == nil {
		http.Error(w, "calendar name must be non-numeric", http.StatusBadRequest)
		return
	}

	var mkReq mkcalendarRequest
	if r.Body != http.NoBody {
		body, err := readDAVBody(w, r, maxDAVBodyBytes)
		if err != nil {
			if errors.Is(err, errRequestTooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "failed to read body", http.StatusBadRequest)
			}
			return
		}
		if err := safeUnmarshalXML(body, &mkReq); err != nil {
			http.Error(w, "invalid MKCALENDAR body", http.StatusBadRequest)
			return
		}
	}

	name := pathName
	var description *string
	var timezone *string
	var color *string
	if mkReq.Set != nil {
		if mkReq.Set.Prop.DisplayName != nil {
			trimmed := strings.TrimSpace(*mkReq.Set.Prop.DisplayName)
			if trimmed != "" {
				name = trimmed
			}
		}
		description = mkReq.Set.Prop.CalendarDescription
		timezone = mkReq.Set.Prop.CalendarTimezone
		if mkReq.Set.Prop.CalendarColor != nil {
			color, err = store.NormalizeCalendarColor(*mkReq.Set.Prop.CalendarColor)
			if err != nil {
				http.Error(w, "invalid calendar color", http.StatusBadRequest)
				return
			}
		}
	}

	cals, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to check calendars", http.StatusInternalServerError)
		return
	}
	// Normalize slug for consistent case-insensitive comparison
	normalizedPathName := strings.ToLower(pathName)
	for _, cal := range cals {
		if cal.Slug != nil && *cal.Slug == normalizedPathName {
			http.Error(w, "calendar already exists", http.StatusConflict)
			return
		}
		if strings.EqualFold(cal.Name, pathName) {
			http.Error(w, "calendar already exists", http.StatusConflict)
			return
		}
	}
	// Use pre-normalized slug to match database constraint (LOWER(slug))
	slug := normalizedPathName
	// Validate slug for path safety (prevent path traversal, injection)
	if !isValidCalendarSlug(slug) {
		http.Error(w, "invalid calendar name: must contain only lowercase letters, numbers, and hyphens", http.StatusBadRequest)
		return
	}
	created, err := h.store.Calendars.Create(r.Context(), store.Calendar{
		UserID:      user.ID,
		Name:        name,
		Slug:        &slug,
		Description: description,
		Timezone:    timezone,
		Color:       color,
	})
	if err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			http.Error(w, "calendar already exists", http.StatusConflict)
			return
		}
		http.Error(w, "failed to create", http.StatusInternalServerError)
		return
	}
	location := path.Join("/dav/calendars", fmt.Sprint(created.ID)) + "/"
	if err := h.rebindCollectionLocks(r.Context(), pendingLockPath, strings.TrimSuffix(location, "/")); err != nil {
		if deleteErr := h.store.Calendars.Delete(r.Context(), user.ID, created.ID); deleteErr != nil && !errors.Is(deleteErr, store.ErrNotFound) {
			log.Printf("failed to roll back calendar %d after lock rebind failure: %v", created.ID, deleteErr)
		}
		http.Error(w, "failed to rebind collection locks", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Location", location)
	w.WriteHeader(http.StatusCreated)
}

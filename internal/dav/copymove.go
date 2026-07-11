package dav

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func parseDestinationHeader(r *http.Request) (string, bool, error) {
	dest := r.Header.Get("Destination")
	if dest == "" {
		return "", false, fmt.Errorf("missing Destination header")
	}

	u, err := url.Parse(dest)
	if err != nil {
		return "", false, fmt.Errorf("invalid Destination URL")
	}

	destPath := path.Clean(u.Path)
	if !strings.HasPrefix(destPath, "/dav/") {
		return "", false, fmt.Errorf("destination outside DAV namespace")
	}

	overwrite := true
	if r.Header.Get("Overwrite") == "F" {
		overwrite = false
	}

	return destPath, overwrite, nil
}

// prefetchCopyMoveLocks fetches, in one query, every lock that could apply to
// the source, destination, or their parent collections, and installs the batch
// index so the 4-6 requireLock checks a COPY/MOVE performs read from it
// instead of issuing one lock query each. On prefetch failure it returns the
// request unchanged and the individual checks fall back to direct queries.
func (h *DavServer) prefetchCopyMoveLocks(r *http.Request, srcPath, destPath string) *http.Request {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return r
	}
	seen := make(map[string]struct{})
	var union []string
	for _, p := range []string{srcPath, destPath, path.Dir(srcPath), path.Dir(destPath)} {
		if p == "" || p == "." || p == "/" {
			continue
		}
		target := h.resolveLockTarget(r, p)
		for _, lookupPath := range target.lookupPaths {
			if _, ok := seen[lookupPath]; ok {
				continue
			}
			seen[lookupPath] = struct{}{}
			union = append(union, lookupPath)
		}
	}
	if len(union) == 0 {
		return r
	}
	locks, err := h.store.Locks.ListByResources(r.Context(), union)
	if err != nil {
		return r
	}
	byPath := make(map[string][]store.Lock, len(locks))
	for i := range locks {
		key := normalizeDAVHref(locks[i].ResourcePath)
		byPath[key] = append(byPath[key], locks[i])
	}
	return r.WithContext(withLockBatchIndex(r.Context(), &lockBatchIndex{byPath: byPath}))
}

// writeSourceResolutionError maps COPY/MOVE source-path resolution failures to
// the same statuses PUT/DELETE/GET use, instead of flattening them all to 404.
func writeSourceResolutionError(w http.ResponseWriter, err error) {
	if errors.Is(err, errAmbiguousCalendar) || errors.Is(err, errAmbiguousAddressBook) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	}
	http.Error(w, "failed to resolve source", http.StatusInternalServerError)
}

func (h *DavServer) Copy(w http.ResponseWriter, r *http.Request) {
	r = ensureRequestCaches(r)
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.logger().Trace("Copy", "COPY %s -> %s", r.URL.Path, r.Header.Get("Destination"))
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	srcPath := path.Clean(r.URL.Path)
	destPath, overwrite, err := parseDestinationHeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Check locks on source and destination
	r = h.prefetchCopyMoveLocks(r, srcPath, destPath)
	if !h.requireLock(w, r, srcPath, "source is locked") {
		return
	}
	if !h.requireLock(w, r, destPath, "destination is locked") {
		return
	}

	// Handle calendar event copy
	if srcCalID, srcUID, srcMatched, err := h.parseCalendarResourcePath(r.Context(), user, srcPath); err != nil {
		writeSourceResolutionError(w, err)
		return
	} else if srcMatched && srcUID != "" {
		h.copyCalendarEvent(w, r, user, srcCalID, srcUID, destPath, overwrite)
		return
	}

	// Handle contact copy
	if srcBookID, srcUID, srcMatched, err := h.parseAddressBookResourcePath(r.Context(), user, srcPath); err != nil {
		writeSourceResolutionError(w, err)
		return
	} else if srcMatched && srcUID != "" {
		h.copyContact(w, r, user, srcBookID, srcUID, destPath, overwrite)
		return
	}

	http.Error(w, "unsupported copy source", http.StatusForbidden)
}

func (h *DavServer) copyCalendarEvent(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool) {
	_, err := h.loadCalendarWithPrivilege(r.Context(), user, srcCalID, srcPath(r), "read")
	if err != nil {
		status := http.StatusNotFound
		if errors.Is(err, errForbidden) {
			status = http.StatusForbidden
		}
		http.Error(w, "source not found", status)
		return
	}

	src, err := h.store.Events.GetByResourceName(r.Context(), srcCalID, srcUID)
	if err != nil || src == nil {
		http.Error(w, "source event not found", http.StatusNotFound)
		return
	}
	destCalID, destResourceName, destMatched, err := h.parseCalendarResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}

	if !h.requireLock(w, r, path.Dir(destPath), "destination is locked") {
		return
	}

	existing, err := h.store.Events.GetByResourceName(r.Context(), destCalID, destResourceName)
	if err != nil {
		http.Error(w, "failed to load destination event", http.StatusInternalServerError)
		return
	}
	sameResource := srcCalID == destCalID && eventResourceName(*src) == destResourceName
	if sameResource {
		if !overwrite {
			http.Error(w, "destination exists", http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, src.ETag))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	existingByUID, err := h.store.Events.GetByUID(r.Context(), destCalID, src.UID)
	if err != nil {
		http.Error(w, "failed to load destination event", http.StatusInternalServerError)
		return
	}
	if existingByUID != nil {
		sameSource := destCalID == srcCalID && existingByUID.UID == src.UID && eventResourceName(*existingByUID) == eventResourceName(*src)
		if !sameSource && eventResourceName(*existingByUID) != destResourceName {
			writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
			return
		}
	}
	if srcCalID == destCalID {
		writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
		return
	}
	if existing != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	loadPrivilege := "bind"
	if existing != nil && existing.UID == src.UID {
		loadPrivilege = "write-content"
	}
	destCal, err := h.loadCalendarWithPrivilege(r.Context(), user, destCalID, destPath, loadPrivilege)
	if err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if err := h.requireCalendarDestinationWritePrivileges(r.Context(), user, destCal, destPath, existing, src.UID); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if existing != nil {
		if err := h.deleteDAVResourceState(r.Context(), user, destPath); err != nil {
			http.Error(w, "failed to clear destination state", http.StatusInternalServerError)
			return
		}
	}
	etag := newCopyETag(src.RawICAL, destCalID)

	_, err = h.store.Events.CopyToCalendar(r.Context(), srcCalID, destCalID, src.UID, destResourceName, etag)
	if err != nil {
		if err == store.ErrConflict {
			writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
			return
		}
		http.Error(w, "failed to copy event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	if existing != nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *DavServer) copyContact(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool) {
	destBookID, destResourceName, destMatched, err := h.parseAddressBookResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}

	srcBook, err := h.getAddressBook(r.Context(), srcBookID)
	if err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, srcBook, path.Clean(r.URL.Path), "read"); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	src, err := h.store.Contacts.GetByResourceName(r.Context(), srcBookID, srcUID)
	if err != nil {
		http.Error(w, "failed to load source contact", http.StatusInternalServerError)
		return
	}
	if src == nil {
		http.Error(w, "source contact not found", http.StatusNotFound)
		return
	}

	destBook, err := h.getAddressBook(r.Context(), destBookID)
	if err != nil {
		http.Error(w, "destination not found", http.StatusNotFound)
		return
	}

	existingByName, err := h.store.Contacts.GetByResourceName(r.Context(), destBookID, destResourceName)
	if err != nil {
		http.Error(w, "failed to load destination contact", http.StatusInternalServerError)
		return
	}
	if err := h.requireAddressBookDestinationWritePrivileges(r.Context(), user, destBook, destPath, existingByName, src.UID); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if !h.requireLock(w, r, path.Dir(destPath), "destination is locked") {
		return
	}
	if int64(len(src.RawVCard)) > maxDAVBodyBytes {
		writeCardDAVPrecondition(w, http.StatusRequestEntityTooLarge, "max-resource-size")
		return
	}
	sameResource := srcBookID == destBookID && contactResourceName(*src) == destResourceName
	if sameResource {
		if !overwrite {
			http.Error(w, "destination exists", http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, src.ETag))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if srcBookID == destBookID {
		conflictHref := fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", srcBookID, contactResourceName(*src))
		writeCardDAVUIDConflict(w, conflictHref)
		return
	}
	if existingByName != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	existingByUID, err := h.store.Contacts.GetByUID(r.Context(), destBookID, src.UID)
	if err != nil {
		http.Error(w, "failed to load destination contact", http.StatusInternalServerError)
		return
	}
	if existingByUID != nil {
		sameSource := destBookID == srcBookID && existingByUID.UID == src.UID && contactResourceName(*existingByUID) == contactResourceName(*src)
		if !sameSource && contactResourceName(*existingByUID) != destResourceName {
			conflictHref := fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", destBookID, contactResourceName(*existingByUID))
			writeCardDAVUIDConflict(w, conflictHref)
			return
		}
	}
	etag := newCopyETag(src.RawVCard, destBookID)

	if existingByName != nil {
		if err := h.deleteDAVResourceState(r.Context(), user, destPath); err != nil {
			http.Error(w, "failed to clear destination state", http.StatusInternalServerError)
			return
		}
	} else {
		if err := h.deleteDAVACLState(r.Context(), user, destPath); err != nil {
			http.Error(w, "failed to reset destination ACL state", http.StatusInternalServerError)
			return
		}
	}

	_, err = h.store.Contacts.CopyToAddressBook(r.Context(), srcBookID, destBookID, src.UID, destResourceName, etag)
	if err != nil {
		http.Error(w, "failed to copy contact", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	if existingByName != nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *DavServer) Move(w http.ResponseWriter, r *http.Request) {
	r = ensureRequestCaches(r)
	if h.handleRegisteredMethod(w, r) {
		return
	}
	h.logger().Trace("Move", "MOVE %s -> %s", r.URL.Path, r.Header.Get("Destination"))
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	srcPath := path.Clean(r.URL.Path)
	destPath, overwrite, err := parseDestinationHeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Check locks on both source and destination
	r = h.prefetchCopyMoveLocks(r, srcPath, destPath)
	if !h.requireLock(w, r, srcPath, "source is locked") {
		return
	}
	if !h.requireLock(w, r, destPath, "destination is locked") {
		return
	}

	// Handle calendar event move
	if srcCalID, srcUID, srcMatched, err := h.parseCalendarResourcePath(r.Context(), user, srcPath); err != nil {
		writeSourceResolutionError(w, err)
		return
	} else if srcMatched && srcUID != "" {
		h.moveCalendarEvent(w, r, user, srcCalID, srcUID, destPath, overwrite)
		return
	}

	// Handle contact move
	if srcBookID, srcUID, srcMatched, err := h.parseAddressBookResourcePath(r.Context(), user, srcPath); err != nil {
		writeSourceResolutionError(w, err)
		return
	} else if srcMatched && srcUID != "" {
		h.moveContact(w, r, user, srcBookID, srcUID, destPath, overwrite)
		return
	}

	http.Error(w, "unsupported move source", http.StatusForbidden)
}

func (h *DavServer) moveCalendarEvent(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool) {
	srcCal, err := h.loadCalendarWithPrivilege(r.Context(), user, srcCalID, srcPath(r), "read")
	if err != nil {
		status := http.StatusInternalServerError
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		if errors.Is(err, errForbidden) {
			status = http.StatusForbidden
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if err := h.requireCalendarPrivilege(r.Context(), user, &srcCal.Calendar, srcPath(r), "unbind"); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if !h.requireLock(w, r, path.Dir(path.Clean(r.URL.Path)), "source is locked") {
		return
	}
	src, err := h.store.Events.GetByResourceName(r.Context(), srcCalID, srcUID)
	if err != nil || src == nil {
		http.Error(w, "source event not found", http.StatusNotFound)
		return
	}

	destCalID, destResourceName, destMatched, err := h.parseCalendarResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}

	if !h.requireLock(w, r, path.Dir(destPath), "destination is locked") {
		return
	}

	existing, err := h.store.Events.GetByResourceName(r.Context(), destCalID, destResourceName)
	if err != nil {
		http.Error(w, "failed to load destination event", http.StatusInternalServerError)
		return
	}
	sameResource := srcCalID == destCalID && eventResourceName(*src) == destResourceName
	if sameResource {
		if !overwrite {
			http.Error(w, "destination exists", http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	existingByUID, err := h.store.Events.GetByUID(r.Context(), destCalID, src.UID)
	if err != nil {
		http.Error(w, "failed to load destination event", http.StatusInternalServerError)
		return
	}
	if existingByUID != nil {
		sameSource := destCalID == srcCalID && existingByUID.UID == src.UID && eventResourceName(*existingByUID) == eventResourceName(*src)
		if !sameSource && eventResourceName(*existingByUID) != destResourceName {
			writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
			return
		}
	}
	if existing != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	loadPrivilege := "bind"
	if existing != nil && existing.UID == src.UID {
		loadPrivilege = "write-content"
	}
	destCal, err := h.loadCalendarWithPrivilege(r.Context(), user, destCalID, destPath, loadPrivilege)
	if err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if err := h.requireCalendarDestinationWritePrivileges(r.Context(), user, destCal, destPath, existing, src.UID); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}

	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state paths", http.StatusInternalServerError)
		return
	}
	replacedUID := ""
	if existing != nil {
		replacedUID = existing.UID
	}
	markLockBatchIndexStale(r.Context())
	defer invalidateACLEntryCache(r.Context())
	if err := h.store.MoveEventAndState(r.Context(), srcCalID, destCalID, src.UID, destResourceName, fromStatePath, toStatePath, replacedUID); err != nil {
		if err == store.ErrConflict {
			writeCalDAVError(w, http.StatusConflict, "no-uid-conflict")
			return
		}
		http.Error(w, "failed to move event", http.StatusInternalServerError)
		return
	}

	if existing != nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *DavServer) moveContact(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool) {
	destBookID, destResourceName, destMatched, err := h.parseAddressBookResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}

	srcBook, err := h.getAddressBook(r.Context(), srcBookID)
	if err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, srcBook, path.Clean(r.URL.Path), "unbind"); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if !h.requireLock(w, r, path.Dir(path.Clean(r.URL.Path)), "source is locked") {
		return
	}
	src, err := h.store.Contacts.GetByResourceName(r.Context(), srcBookID, srcUID)
	if err != nil {
		http.Error(w, "failed to load source contact", http.StatusInternalServerError)
		return
	}
	if src == nil {
		http.Error(w, "source contact not found", http.StatusNotFound)
		return
	}

	destBook, err := h.getAddressBook(r.Context(), destBookID)
	if err != nil {
		http.Error(w, "destination not found", http.StatusNotFound)
		return
	}

	existingByName, err := h.store.Contacts.GetByResourceName(r.Context(), destBookID, destResourceName)
	if err != nil {
		http.Error(w, "failed to load destination contact", http.StatusInternalServerError)
		return
	}
	if err := h.requireAddressBookDestinationWritePrivileges(r.Context(), user, destBook, destPath, existingByName, src.UID); err != nil {
		status := http.StatusForbidden
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if !h.requireLock(w, r, path.Dir(destPath), "destination is locked") {
		return
	}
	if int64(len(src.RawVCard)) > maxDAVBodyBytes {
		writeCardDAVPrecondition(w, http.StatusRequestEntityTooLarge, "max-resource-size")
		return
	}
	sameResource := srcBookID == destBookID && contactResourceName(*src) == destResourceName
	if sameResource {
		if !overwrite {
			http.Error(w, "destination exists", http.StatusPreconditionFailed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if existingByName != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	existingByUID, err := h.store.Contacts.GetByUID(r.Context(), destBookID, src.UID)
	if err != nil {
		http.Error(w, "failed to load destination contact", http.StatusInternalServerError)
		return
	}
	if existingByUID != nil {
		sameSource := destBookID == srcBookID && existingByUID.UID == src.UID && contactResourceName(*existingByUID) == contactResourceName(*src)
		if !sameSource && contactResourceName(*existingByUID) != destResourceName {
			conflictHref := fmt.Sprintf("/dav/addressbooks/%d/%s.vcf", destBookID, contactResourceName(*existingByUID))
			writeCardDAVUIDConflict(w, conflictHref)
			return
		}
	}

	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state paths", http.StatusInternalServerError)
		return
	}
	replacedUID := ""
	if existingByName != nil {
		replacedUID = existingByName.UID
	}
	markLockBatchIndexStale(r.Context())
	defer invalidateACLEntryCache(r.Context())
	if err := h.store.MoveContactAndState(r.Context(), srcBookID, destBookID, src.UID, destResourceName, fromStatePath, toStatePath, replacedUID); err != nil {
		http.Error(w, "failed to move contact", http.StatusInternalServerError)
		return
	}

	if existingByName != nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *DavServer) requireAddressBookDestinationWritePrivileges(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string, existing *store.Contact, sourceUID string) error {
	if existing == nil {
		return h.requireAddressBookPrivilege(ctx, user, book, cleanPath, "bind")
	}
	if existing.UID == sourceUID {
		return h.requireAddressBookPrivilege(ctx, user, book, cleanPath, "write-content")
	}
	for _, privilege := range []string{"unbind", "bind"} {
		if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, privilege); err != nil {
			return err
		}
	}
	return nil
}

func (h *DavServer) requireCalendarDestinationWritePrivileges(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, existing *store.Event, sourceUID string) error {
	if existing == nil {
		return h.requireCalendarPrivilege(ctx, user, &cal.Calendar, cleanPath, "bind")
	}
	if existing.UID == sourceUID {
		return h.requireCalendarPrivilege(ctx, user, &cal.Calendar, cleanPath, "write-content")
	}
	for _, privilege := range []string{"unbind", "bind"} {
		if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, cleanPath, privilege); err != nil {
			return err
		}
	}
	return nil
}

func srcPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	return path.Clean(r.URL.Path)
}

func newCopyETag(raw string, destinationID int64) string {
	entropy := make([]byte, 16)
	if _, err := rand.Read(entropy); err != nil {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", raw, destinationID, store.Now().UnixNano()))))
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(raw+fmt.Sprint(destinationID)+hex.EncodeToString(entropy))))
}

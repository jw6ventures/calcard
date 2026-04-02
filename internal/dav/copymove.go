package dav

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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

func (h *Handler) Copy(w http.ResponseWriter, r *http.Request) {
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
	if !h.requireLock(w, r, srcPath, "source is locked") {
		return
	}
	if !h.requireLock(w, r, destPath, "destination is locked") {
		return
	}

	// Handle calendar event copy
	if srcCalID, srcUID, srcMatched, err := h.parseCalendarResourcePath(r.Context(), user, srcPath); err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	} else if srcMatched && srcUID != "" {
		h.copyCalendarEvent(w, r, user, srcCalID, srcUID, destPath, overwrite)
		return
	}

	// Handle contact copy
	if srcBookID, srcUID, srcMatched, err := h.parseAddressBookResourcePath(r.Context(), user, srcPath); err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	} else if srcMatched && srcUID != "" {
		h.copyContact(w, r, user, srcBookID, srcUID, destPath, overwrite)
		return
	}

	http.Error(w, "unsupported copy source", http.StatusForbidden)
}

func (h *Handler) copyCalendarEvent(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool) {
	if _, err := h.loadCalendar(r.Context(), user, srcCalID); err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
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

	cal, err := h.loadCalendar(r.Context(), user, destCalID)
	if err != nil || !cal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
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
	etag := newCopyETag(src.RawICAL, destCalID)

	_, err = h.store.Events.CopyToCalendar(r.Context(), srcCalID, destCalID, src.UID, destResourceName, etag)
	if err != nil {
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

func (h *Handler) copyContact(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool) {
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

	if existingByName == nil {
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

func (h *Handler) Move(w http.ResponseWriter, r *http.Request) {
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
	if !h.requireLock(w, r, srcPath, "source is locked") {
		return
	}
	if !h.requireLock(w, r, destPath, "destination is locked") {
		return
	}

	// Handle calendar event move
	if srcCalID, srcUID, srcMatched, err := h.parseCalendarResourcePath(r.Context(), user, srcPath); err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	} else if srcMatched && srcUID != "" {
		h.moveCalendarEvent(w, r, user, srcCalID, srcUID, destPath, overwrite)
		return
	}

	// Handle contact move
	if srcBookID, srcUID, srcMatched, err := h.parseAddressBookResourcePath(r.Context(), user, srcPath); err != nil {
		http.Error(w, "source not found", http.StatusNotFound)
		return
	} else if srcMatched && srcUID != "" {
		h.moveContact(w, r, user, srcBookID, srcUID, destPath, overwrite)
		return
	}

	http.Error(w, "unsupported move source", http.StatusForbidden)
}

func (h *Handler) moveCalendarEvent(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool) {
	srcCal, err := h.loadCalendar(r.Context(), user, srcCalID)
	if err != nil {
		status := http.StatusInternalServerError
		if err == store.ErrNotFound {
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if !srcCal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
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

	destCal, err := h.loadCalendar(r.Context(), user, destCalID)
	if err != nil || !destCal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
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

	if err := h.store.Events.MoveToCalendar(r.Context(), srcCalID, destCalID, src.UID, destResourceName); err != nil {
		http.Error(w, "failed to move event", http.StatusInternalServerError)
		return
	}
	if err := h.rebindMovedDAVResourceState(r.Context(), user, srcPath(r), destPath, existing != nil); err != nil {
		if rollbackErr := h.rollbackCalendarMove(r.Context(), destCalID, destResourceName, *src, existing); rollbackErr != nil {
			http.Error(w, "failed to roll back move after state rebind failure", http.StatusInternalServerError)
			return
		}
		http.Error(w, "failed to rebind resource state", http.StatusInternalServerError)
		return
	}
	if err := h.clearOverwrittenEventTombstone(r.Context(), destCalID, existing, destResourceName); err != nil {
		if rollbackErr := h.rollbackCalendarMove(r.Context(), destCalID, destResourceName, *src, existing); rollbackErr != nil {
			http.Error(w, "failed to roll back move after tombstone cleanup failure", http.StatusInternalServerError)
			return
		}
		http.Error(w, "failed to finalize move", http.StatusInternalServerError)
		return
	}

	if existing != nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *Handler) moveContact(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool) {
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

	if err := h.store.Contacts.MoveToAddressBook(r.Context(), srcBookID, destBookID, src.UID, destResourceName); err != nil {
		http.Error(w, "failed to move contact", http.StatusInternalServerError)
		return
	}
	if err := h.rebindMovedDAVResourceState(r.Context(), user, srcPath(r), destPath, existingByName != nil); err != nil {
		if rollbackErr := h.rollbackContactMove(r.Context(), destBookID, destResourceName, *src, existingByName); rollbackErr != nil {
			http.Error(w, "failed to roll back move after state rebind failure", http.StatusInternalServerError)
			return
		}
		http.Error(w, "failed to rebind resource state", http.StatusInternalServerError)
		return
	}
	if err := h.clearOverwrittenContactTombstone(r.Context(), destBookID, existingByName, destResourceName); err != nil {
		if rollbackErr := h.rollbackContactMove(r.Context(), destBookID, destResourceName, *src, existingByName); rollbackErr != nil {
			http.Error(w, "failed to roll back move after tombstone cleanup failure", http.StatusInternalServerError)
			return
		}
		http.Error(w, "failed to finalize move", http.StatusInternalServerError)
		return
	}

	if existingByName != nil {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *Handler) requireAddressBookDestinationWritePrivileges(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string, existing *store.Contact, sourceUID string) error {
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

func srcPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return ""
	}
	return path.Clean(r.URL.Path)
}

func (h *Handler) rollbackCalendarMove(ctx context.Context, currentCalendarID int64, currentResourceName string, src store.Event, replaced *store.Event) error {
	if err := h.store.Events.MoveToCalendar(ctx, currentCalendarID, src.CalendarID, src.UID, eventResourceName(src)); err != nil {
		return err
	}
	if replaced == nil {
		return h.cleanupRollbackEventTombstones(ctx, currentCalendarID, currentResourceName, src, nil)
	}
	restore := *replaced
	if _, err := h.store.Events.Upsert(ctx, restore); err != nil {
		return err
	}
	return h.cleanupRollbackEventTombstones(ctx, currentCalendarID, currentResourceName, src, replaced)
}

func (h *Handler) rollbackContactMove(ctx context.Context, currentAddressBookID int64, currentResourceName string, src store.Contact, replaced *store.Contact) error {
	if err := h.store.Contacts.MoveToAddressBook(ctx, currentAddressBookID, src.AddressBookID, src.UID, contactResourceName(src)); err != nil {
		return err
	}
	if replaced == nil {
		return h.cleanupRollbackContactTombstones(ctx, currentAddressBookID, currentResourceName, src, nil)
	}
	restore := *replaced
	if _, err := h.store.Contacts.Upsert(ctx, restore); err != nil {
		return err
	}
	return h.cleanupRollbackContactTombstones(ctx, currentAddressBookID, currentResourceName, src, replaced)
}

func newCopyETag(raw string, destinationID int64) string {
	entropy := make([]byte, 16)
	if _, err := rand.Read(entropy); err != nil {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", raw, destinationID, store.Now().UnixNano()))))
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(raw+fmt.Sprint(destinationID)+hex.EncodeToString(entropy))))
}

func (h *Handler) clearOverwrittenEventTombstone(ctx context.Context, calendarID int64, replaced *store.Event, resourceName string) error {
	if replaced == nil || h == nil || h.store == nil || h.store.DeletedResources == nil {
		return nil
	}
	return h.store.DeletedResources.DeleteByIdentity(ctx, "event", calendarID, replaced.UID, resourceName)
}

func (h *Handler) clearOverwrittenContactTombstone(ctx context.Context, addressBookID int64, replaced *store.Contact, resourceName string) error {
	if replaced == nil || h == nil || h.store == nil || h.store.DeletedResources == nil {
		return nil
	}
	return h.store.DeletedResources.DeleteByIdentity(ctx, "contact", addressBookID, replaced.UID, resourceName)
}

func (h *Handler) cleanupRollbackEventTombstones(ctx context.Context, currentCalendarID int64, currentResourceName string, src store.Event, replaced *store.Event) error {
	if h.store == nil || h.store.DeletedResources == nil {
		return nil
	}
	for _, tombstone := range []struct {
		collectionID int64
		uid          string
		resourceName string
	}{
		{collectionID: src.CalendarID, uid: src.UID, resourceName: eventResourceName(src)},
		{collectionID: currentCalendarID, uid: src.UID, resourceName: currentResourceName},
	} {
		if err := h.store.DeletedResources.DeleteByIdentity(ctx, "event", tombstone.collectionID, tombstone.uid, tombstone.resourceName); err != nil {
			return err
		}
	}
	if replaced == nil {
		return nil
	}
	return h.store.DeletedResources.DeleteByIdentity(ctx, "event", currentCalendarID, replaced.UID, eventResourceName(*replaced))
}

func (h *Handler) cleanupRollbackContactTombstones(ctx context.Context, currentAddressBookID int64, currentResourceName string, src store.Contact, replaced *store.Contact) error {
	if h.store == nil || h.store.DeletedResources == nil {
		return nil
	}
	for _, tombstone := range []struct {
		collectionID int64
		uid          string
		resourceName string
	}{
		{collectionID: src.AddressBookID, uid: src.UID, resourceName: contactResourceName(src)},
		{collectionID: currentAddressBookID, uid: src.UID, resourceName: currentResourceName},
	} {
		if err := h.store.DeletedResources.DeleteByIdentity(ctx, "contact", tombstone.collectionID, tombstone.uid, tombstone.resourceName); err != nil {
			return err
		}
	}
	if replaced == nil {
		return nil
	}
	return h.store.DeletedResources.DeleteByIdentity(ctx, "contact", currentAddressBookID, replaced.UID, contactResourceName(*replaced))
}

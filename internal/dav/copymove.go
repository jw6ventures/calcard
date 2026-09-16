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

var errCrossAuthorityDestination = errors.New("destination is on another server")

func parseDestinationHeader(r *http.Request) (string, bool, error) {
	dest := r.Header.Get("Destination")
	if dest == "" {
		return "", false, fmt.Errorf("missing Destination header")
	}

	u, err := url.ParseRequestURI(dest)
	if err != nil {
		return "", false, fmt.Errorf("invalid Destination URL")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", false, fmt.Errorf("invalid Destination URL")
	}
	// The authority is checked whether or not a scheme is present. A
	// protocol-relative "//authority/path" names another server just as an
	// absolute URL does, and RFC 4918 §9.8.6 makes a destination on another
	// server a 502 rather than something to answer locally. url.ParseRequestURI
	// leaves that form entirely in Path, so the authority is split off here
	// instead of being read from Host.
	if u.Scheme != "" && u.Scheme != "http" && u.Scheme != "https" {
		return "", false, errCrossAuthorityDestination
	}
	authority, rawPath := u.Host, u.EscapedPath()
	if authority == "" && strings.HasPrefix(rawPath, "//") {
		var rest string
		authority, rest, _ = strings.Cut(strings.TrimPrefix(rawPath, "//"), "/")
		rawPath = "/" + rest
	}
	if authority != "" && !strings.EqualFold(authority, r.Host) {
		return "", false, errCrossAuthorityDestination
	}
	for _, segment := range strings.Split(rawPath, "/") {
		if segment == "." || segment == ".." || strings.Contains(strings.ToLower(segment), "%2f") || strings.Contains(strings.ToLower(segment), "%5c") {
			return "", false, fmt.Errorf("invalid Destination URL")
		}
	}

	unescapedPath, err := url.PathUnescape(rawPath)
	if err != nil {
		return "", false, fmt.Errorf("invalid Destination URL")
	}
	destPath := path.Clean(unescapedPath)
	if !strings.HasPrefix(destPath, "/dav/") {
		return "", false, fmt.Errorf("destination outside DAV namespace")
	}

	overwrite := true
	switch overwriteHeader := strings.TrimSpace(r.Header.Get("Overwrite")); overwriteHeader {
	case "", "T":
	case "F":
		overwrite = false
	default:
		return "", false, fmt.Errorf("invalid Overwrite header")
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

func (h *DavServer) copy(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Copy", "COPY %s -> %s", r.URL.Path, r.Header.Get("Destination"))
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	srcPath := path.Clean(r.URL.Path)
	destPath, overwrite, err := parseDestinationHeader(r)
	if err != nil {
		if errors.Is(err, errCrossAuthorityDestination) {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if isBirthdayCalendarPath(r.Context(), destPath) {
		http.Error(w, "birthday calendar is read-only", http.StatusForbidden)
		return
	}
	if target := parsedDAVTarget(r.Context(), srcPath); target.Valid && target.Domain == davPathCalendar && !target.Resource && target.CollectionSegment != "" {
		depth, err := calendarCollectionCopyDepth(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.copyCalendarCollection(w, r, user, target.CollectionSegment, srcPath, destPath, depth, overwrite)
		return
	}

	// Prefetch lock state now; source and destination handlers evaluate it only
	// after resolving and authorizing the resources involved.
	r = h.prefetchCopyMoveLocks(r, srcPath, destPath)

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
	h.copyCalendarEventWithRetry(w, r, user, srcCalID, srcUID, destPath, overwrite, maxResourceStateRetries)
}

func (h *DavServer) copyCalendarEventWithRetry(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool, retries int) {
	srcCal, err := h.loadCalendarWithPrivilege(r.Context(), user, srcCalID, srcPath(r), "read")
	if err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, srcPath(r), "read"))
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

	existing, err := h.store.Events.GetByResourceName(r.Context(), destCalID, destResourceName)
	if err != nil {
		http.Error(w, "failed to load destination event", http.StatusInternalServerError)
		return
	}
	loadPrivilege := "bind"
	loadPrivilegePath := path.Dir(destPath)
	if existing != nil {
		loadPrivilege = "write-content"
		loadPrivilegePath = destPath
	}
	destCal, err := h.loadCalendarWithPrivilege(r.Context(), user, destCalID, loadPrivilegePath, loadPrivilege)
	if err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, loadPrivilegePath, loadPrivilege))
		return
	}
	if err := h.requireCalendarDestinationWritePrivileges(r.Context(), user, destCal, destPath, existing); err != nil {
		_ = writePrivilegeRequirementError(w, err)
		return
	}
	if !h.requireLocks(w, r, "destination is locked", destPath, path.Dir(destPath)) {
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
			writeCalDAVUIDConflict(w, calendarObjectConflictHref(destCalID, existingByUID))
			return
		}
	}
	if srcCalID == destCalID {
		// A copy inside one collection would give the source's UID to a second
		// resource, which §4.1 forbids; the source is the resource already
		// holding it.
		writeCalDAVUIDConflict(w, calendarObjectConflictHref(srcCalID, src))
		return
	}
	if existing != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	validated, fault := validateCalendarObjectForStorage(src.RawICAL, destCal)
	if fault != nil {
		writeCalendarObjectFault(w, fault)
		return
	}
	etag := newCopyETag(src.RawICAL, destCalID)
	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state paths", http.StatusInternalServerError)
		return
	}
	defer invalidateDAVRequestState(r.Context())
	result, err := h.store.TransferCalendarObject(r.Context(), store.CalendarObjectTransfer{
		Operation:               store.CalendarObjectCopy,
		SourceCalendarID:        srcCalID,
		SourceUID:               src.UID,
		SourceResourceName:      eventResourceName(*src),
		ExpectedSourceETag:      src.ETag,
		ExpectedSourceRaw:       src.RawICAL,
		ExpectedSourceCTag:      &srcCal.CTag,
		DestinationCalendarID:   destCalID,
		DestinationResourceName: destResourceName,
		ExpectedDestination:     calendarObjectTransferState(existing),
		ExpectedDestinationCTag: &destCal.CTag,
		Overwrite:               overwrite,
		RawICAL:                 src.RawICAL,
		ETag:                    etag,
		Metadata:                &validated.Analysis.Metadata,
		SourceStatePath:         fromStatePath,
		DestinationStatePath:    toStatePath,
		LockPreconditions:       h.lockPreconditions(r, destPath, path.Dir(destPath)),
	})
	if err != nil {
		if errors.Is(err, store.ErrResourceStateChanged) && retries > 0 {
			invalidateDAVRequestState(r.Context())
			h.copyCalendarEventWithRetry(w, r, user, srcCalID, srcUID, destPath, overwrite, retries-1)
			return
		}
		if h.writeCalendarTransferError(w, r, err, result, destCalID, destResourceName, src.UID) {
			return
		}
		http.Error(w, "failed to copy event", http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, etag))
	if result != nil && !result.Created {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func (h *DavServer) copyContact(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool) {
	h.copyContactWithRetry(w, r, user, srcBookID, srcUID, destPath, overwrite, maxResourceStateRetries)
}

func (h *DavServer) copyContactWithRetry(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool, retries int) {
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
		_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, path.Clean(r.URL.Path), "read"))
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

	if !h.checkConditionalHeadersContact(r, src) {
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
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
	if err := h.requireAddressBookDestinationWritePrivileges(r.Context(), user, destBook, destPath, existingByName); err != nil {
		_ = writePrivilegeRequirementError(w, err)
		return
	}
	if !h.requireLocks(w, r, "destination is locked", destPath, path.Dir(destPath)) {
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
		conflictHref := addressObjectConflictHref(srcBookID, contactResourceName(*src))
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
			conflictHref := addressObjectConflictHref(destBookID, contactResourceName(*existingByUID))
			writeCardDAVUIDConflict(w, conflictHref)
			return
		}
	}
	etag := newCopyETag(src.RawVCard, destBookID)

	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state paths", http.StatusInternalServerError)
		return
	}
	replacedUID := ""
	if existingByName != nil {
		replacedUID = existingByName.UID
	}
	expected := store.ContactTransferExpectation{
		Source:                     store.ContactDAVResourceState(src),
		Destination:                store.ContactDAVResourceState(existingByName),
		SourceAddressBookCTag:      &srcBook.CTag,
		DestinationAddressBookCTag: &destBook.CTag,
		Overwrite:                  overwrite,
	}
	defer invalidateDAVRequestState(r.Context())
	_, err = h.store.CopyContactAndState(r.Context(), srcBookID, destBookID, src.UID, destResourceName, etag, fromStatePath, toStatePath, replacedUID,
		expected, h.lockPreconditions(r, destPath, path.Dir(destPath)))
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is locked", http.StatusLocked)
			return
		}
		if errors.Is(err, store.ErrConflict) {
			h.writeContactCopyMoveConflict(w, r, destBookID, destResourceName, src.UID)
			return
		}
		if errors.Is(err, store.ErrResourceStateChanged) {
			if retries > 0 {
				invalidateDAVRequestState(r.Context())
				h.copyContactWithRetry(w, r, user, srcBookID, srcUID, destPath, overwrite, retries-1)
				return
			}
			http.Error(w, "resource changed; retry the request", http.StatusConflict)
			return
		}
		if errors.Is(err, store.ErrPreconditionFailed) {
			http.Error(w, "resource state changed", http.StatusPreconditionFailed)
			return
		}
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

func (h *DavServer) move(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Move", "MOVE %s -> %s", r.URL.Path, r.Header.Get("Destination"))
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	srcPath := path.Clean(r.URL.Path)
	destPath, overwrite, err := parseDestinationHeader(r)
	if err != nil {
		if errors.Is(err, errCrossAuthorityDestination) {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if isBirthdayCalendarPath(r.Context(), destPath) {
		http.Error(w, "birthday calendar is read-only", http.StatusForbidden)
		return
	}
	if target := parsedDAVTarget(r.Context(), srcPath); target.Valid && target.Domain == davPathCalendar && !target.Resource && target.CollectionSegment != "" {
		if err := calendarCollectionMoveDepth(r); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.moveCalendarCollection(w, r, user, target.CollectionSegment, srcPath, destPath, overwrite)
		return
	}

	// Prefetch lock state now; source and destination handlers evaluate it only
	// after resolving and authorizing the resources involved.
	r = h.prefetchCopyMoveLocks(r, srcPath, destPath)

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

func calendarCollectionCopyDepth(r *http.Request) (string, error) {
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if depth == "" {
		return "infinity", nil
	}
	if depth != "0" && !strings.EqualFold(depth, "infinity") {
		return "", fmt.Errorf("calendar collection COPY Depth must be 0 or infinity")
	}
	return strings.ToLower(depth), nil
}

func calendarCollectionMoveDepth(r *http.Request) error {
	depth := strings.TrimSpace(r.Header.Get("Depth"))
	if depth != "" && !strings.EqualFold(depth, "infinity") {
		return fmt.Errorf("calendar collection MOVE Depth must be infinity")
	}
	return nil
}

func (h *DavServer) moveCalendarEvent(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool) {
	h.moveCalendarEventWithRetry(w, r, user, srcCalID, srcUID, destPath, overwrite, maxResourceStateRetries)
}

func (h *DavServer) moveCalendarEventWithRetry(w http.ResponseWriter, r *http.Request, user *store.User, srcCalID int64, srcUID, destPath string, overwrite bool, retries int) {
	sourceParent := path.Dir(srcPath(r))
	srcAccess, err := h.loadCalendarWithAnyPrivilege(r.Context(), user, srcCalID, path.Clean(r.URL.Path))
	if err != nil {
		if errors.Is(err, errForbidden) {
			http.Error(w, "source not found", http.StatusNotFound)
			return
		}
		writeSourceResolutionError(w, err)
		return
	}
	srcCal := &srcAccess.Calendar
	if err := h.requireCalendarPrivilege(r.Context(), user, srcCal, sourceParent, "unbind"); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, sourceParent, "unbind"))
		return
	}
	src, err := h.store.Events.GetByResourceName(r.Context(), srcCalID, srcUID)
	if err != nil || src == nil {
		http.Error(w, "source event not found", http.StatusNotFound)
		return
	}
	if !h.requireLocks(w, r, "source is locked", path.Clean(r.URL.Path), sourceParent) {
		return
	}

	destCalID, destResourceName, destMatched, err := h.parseCalendarResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}

	existing, err := h.store.Events.GetByResourceName(r.Context(), destCalID, destResourceName)
	if err != nil {
		http.Error(w, "failed to load destination event", http.StatusInternalServerError)
		return
	}
	loadPrivilege := "bind"
	loadPrivilegePath := path.Dir(destPath)
	if existing != nil {
		loadPrivilege = "unbind"
	}
	destCal, err := h.loadCalendarWithPrivilege(r.Context(), user, destCalID, loadPrivilegePath, loadPrivilege)
	if err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, loadPrivilegePath, loadPrivilege))
		return
	}
	if err := h.requireCalendarDestinationWritePrivileges(r.Context(), user, destCal, destPath, existing); err != nil {
		_ = writePrivilegeRequirementError(w, err)
		return
	}
	if !h.requireLocks(w, r, "destination is locked", destPath, path.Dir(destPath)) {
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
			writeCalDAVUIDConflict(w, calendarObjectConflictHref(destCalID, existingByUID))
			return
		}
	}
	if existing != nil && !overwrite {
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return
	}
	validated, fault := validateCalendarObjectForStorage(src.RawICAL, destCal)
	if fault != nil {
		writeCalendarObjectFault(w, fault)
		return
	}

	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		http.Error(w, "failed to resolve resource state paths", http.StatusInternalServerError)
		return
	}
	defer invalidateDAVRequestState(r.Context())
	result, err := h.store.TransferCalendarObject(r.Context(), store.CalendarObjectTransfer{
		Operation:               store.CalendarObjectMove,
		SourceCalendarID:        srcCalID,
		SourceUID:               src.UID,
		SourceResourceName:      eventResourceName(*src),
		ExpectedSourceETag:      src.ETag,
		ExpectedSourceRaw:       src.RawICAL,
		ExpectedSourceCTag:      &srcAccess.CTag,
		DestinationCalendarID:   destCalID,
		DestinationResourceName: destResourceName,
		ExpectedDestination:     calendarObjectTransferState(existing),
		ExpectedDestinationCTag: &destCal.CTag,
		Overwrite:               overwrite,
		RawICAL:                 src.RawICAL,
		ETag:                    src.ETag,
		Metadata:                &validated.Analysis.Metadata,
		SourceStatePath:         fromStatePath,
		DestinationStatePath:    toStatePath,
		LockPreconditions:       h.lockPreconditions(r, srcPath(r), path.Dir(srcPath(r)), destPath, path.Dir(destPath)),
	})
	if err != nil {
		if errors.Is(err, store.ErrResourceStateChanged) && retries > 0 {
			invalidateDAVRequestState(r.Context())
			h.moveCalendarEventWithRetry(w, r, user, srcCalID, srcUID, destPath, overwrite, retries-1)
			return
		}
		if h.writeCalendarTransferError(w, r, err, result, destCalID, destResourceName, src.UID) {
			return
		}
		http.Error(w, "failed to move event", http.StatusInternalServerError)
		return
	}

	if result != nil && !result.Created {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.Header().Set("Location", destPath)
		w.WriteHeader(http.StatusCreated)
	}
}

func calendarObjectTransferState(event *store.Event) store.CalendarObjectTransferState {
	if event == nil {
		return store.CalendarObjectTransferState{}
	}
	return store.CalendarObjectTransferState{Exists: true, UID: event.UID, ETag: event.ETag}
}

func (h *DavServer) writeCalendarTransferError(w http.ResponseWriter, r *http.Request, err error, result *store.CalendarObjectTransferResult, calendarID int64, resourceName, uid string) bool {
	switch {
	case errors.Is(err, store.ErrUIDConflict):
		if result != nil && result.Conflict != nil {
			writeCalDAVUIDConflict(w, calendarObjectConflictHref(calendarID, result.Conflict))
			return true
		}
		h.writeCalendarCopyMoveConflict(w, r, calendarID, resourceName, uid)
		return true
	case errors.Is(err, store.ErrPreconditionFailed):
		http.Error(w, "destination exists", http.StatusPreconditionFailed)
		return true
	case errors.Is(err, store.ErrResourceStateChanged):
		http.Error(w, "resource changed while transferring; retry the request", http.StatusConflict)
		return true
	case errors.Is(err, store.ErrLockConflict):
		http.Error(w, "resource is locked", http.StatusLocked)
		return true
	case errors.Is(err, store.ErrConflict):
		h.writeCalendarCopyMoveConflict(w, r, calendarID, resourceName, uid)
		return true
	default:
		return false
	}
}

func (h *DavServer) writeCalendarCopyMoveConflict(w http.ResponseWriter, r *http.Request, calendarID int64, resourceName, uid string) {
	existing, resourceErr := h.store.Events.GetByResourceName(r.Context(), calendarID, resourceName)
	byUID, uidErr := h.store.Events.GetByUID(r.Context(), calendarID, uid)
	if resourceErr != nil || uidErr != nil {
		http.Error(w, "failed to load conflicting event", http.StatusInternalServerError)
		return
	}
	var conflict *store.Event
	if existing != nil && existing.UID != uid {
		conflict = existing
	} else if byUID != nil && eventResourceName(*byUID) != resourceName {
		conflict = byUID
	}
	if conflict == nil {
		http.Error(w, "failed to resolve event conflict", http.StatusInternalServerError)
		return
	}
	writeCalDAVUIDConflict(w, calendarObjectConflictHref(calendarID, conflict))
}

func (h *DavServer) moveContact(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool) {
	h.moveContactWithRetry(w, r, user, srcBookID, srcUID, destPath, overwrite, maxResourceStateRetries)
}

func (h *DavServer) moveContactWithRetry(w http.ResponseWriter, r *http.Request, user *store.User, srcBookID int64, srcUID, destPath string, overwrite bool, retries int) {
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
	sourcePath := path.Clean(r.URL.Path)
	sourceParent := path.Dir(sourcePath)
	if err := h.requireAnyAddressBookPrivilege(r.Context(), user, srcBook, sourcePath); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, sourceParent, "unbind"))
		return
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, srcBook, sourceParent, "unbind"); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, sourceParent, "unbind"))
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
	if !h.requireLocks(w, r, "source is locked", sourcePath, sourceParent) {
		return
	}

	if !h.checkConditionalHeadersContact(r, src) {
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
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
	if err := h.requireAddressBookDestinationWritePrivileges(r.Context(), user, destBook, destPath, existingByName); err != nil {
		_ = writePrivilegeRequirementError(w, err)
		return
	}
	if !h.requireLocks(w, r, "destination is locked", destPath, path.Dir(destPath)) {
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
			conflictHref := addressObjectConflictHref(destBookID, contactResourceName(*existingByUID))
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
	expected := store.ContactTransferExpectation{
		Source:                     store.ContactDAVResourceState(src),
		Destination:                store.ContactDAVResourceState(existingByName),
		SourceAddressBookCTag:      &srcBook.CTag,
		DestinationAddressBookCTag: &destBook.CTag,
		Overwrite:                  overwrite,
	}
	defer invalidateDAVRequestState(r.Context())
	if err := h.store.MoveContactAndState(r.Context(), srcBookID, destBookID, src.UID, destResourceName, fromStatePath, toStatePath, replacedUID,
		expected, h.lockPreconditions(r, srcPath(r), path.Dir(srcPath(r)), destPath, path.Dir(destPath))); err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is locked", http.StatusLocked)
			return
		}
		if errors.Is(err, store.ErrConflict) {
			h.writeContactCopyMoveConflict(w, r, destBookID, destResourceName, src.UID)
			return
		}
		if errors.Is(err, store.ErrResourceStateChanged) {
			if retries > 0 {
				invalidateDAVRequestState(r.Context())
				h.moveContactWithRetry(w, r, user, srcBookID, srcUID, destPath, overwrite, retries-1)
				return
			}
			http.Error(w, "resource changed; retry the request", http.StatusConflict)
			return
		}
		if errors.Is(err, store.ErrPreconditionFailed) {
			http.Error(w, "resource state changed", http.StatusPreconditionFailed)
			return
		}
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

func (h *DavServer) writeContactCopyMoveConflict(w http.ResponseWriter, r *http.Request, addressBookID int64, resourceName, uid string) {
	existing, resourceErr := h.store.Contacts.GetByResourceName(r.Context(), addressBookID, resourceName)
	byUID, uidErr := h.store.Contacts.GetByUID(r.Context(), addressBookID, uid)
	if resourceErr != nil || uidErr != nil {
		http.Error(w, "failed to load conflicting contact", http.StatusInternalServerError)
		return
	}
	var conflict *store.Contact
	if existing != nil && existing.UID != uid {
		conflict = existing
	} else if byUID != nil && contactResourceName(*byUID) != resourceName {
		conflict = byUID
	}
	if conflict == nil {
		writeCardDAVUIDConflict(w, addressObjectConflictHref(addressBookID, resourceName))
		return
	}
	writeCardDAVUIDConflict(w, addressObjectConflictHref(addressBookID, contactResourceName(*conflict)))
}

func (h *DavServer) requireAddressBookDestinationWritePrivileges(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string, existing *store.Contact) error {
	parentPath := path.Dir(cleanPath)
	if err := h.requireAddressBookPrivilege(ctx, user, book, parentPath, "bind"); err != nil {
		return requirePrivilegeAt(err, parentPath, "bind")
	}
	if existing == nil {
		return nil
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, parentPath, "unbind"); err != nil {
		return requirePrivilegeAt(err, parentPath, "unbind")
	}
	return nil
}

func (h *DavServer) requireCalendarDestinationWritePrivileges(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string, existing *store.Event) error {
	parentPath := path.Dir(cleanPath)
	if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, parentPath, "bind"); err != nil {
		return requirePrivilegeAt(err, parentPath, "bind")
	}
	if existing == nil {
		return nil
	}
	if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, parentPath, "unbind"); err != nil {
		return requirePrivilegeAt(err, parentPath, "unbind")
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
		return fmt.Sprintf("%x", sha256.Sum256(fmt.Appendf(nil, "%s:%d:%d", raw, destinationID, store.Now().UnixNano())))
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(raw+fmt.Sprint(destinationID)+hex.EncodeToString(entropy))))
}

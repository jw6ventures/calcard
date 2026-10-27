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

func (h *DavServer) parseDestinationHeader(r *http.Request) (string, bool, error) {
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
	// absolute URL does, and RFC 4918 §9.8.5 makes a destination on another
	// server a 502 rather than something to answer locally. url.ParseRequestURI
	// leaves that form entirely in Path, so the authority is split off here
	// instead of being read from Host. An empty authority names no server and
	// is malformed rather than foreign.
	scheme, rawAuthority, rawPath := u.Scheme, u.Host, u.EscapedPath()
	hasAuthority := scheme != ""
	if scheme == "" && strings.HasPrefix(rawPath, "//") {
		var rest string
		rawAuthority, rest, _ = strings.Cut(strings.TrimPrefix(rawPath, "//"), "/")
		rawPath = "/" + rest
		hasAuthority = true
	}
	if hasAuthority {
		lowerScheme := strings.ToLower(scheme)
		if lowerScheme != "" && lowerScheme != "http" && lowerScheme != "https" {
			return "", false, errCrossAuthorityDestination
		}
		if rawAuthority == "" {
			return "", false, fmt.Errorf("invalid Destination URL")
		}
		if !h.isLocalOrigin(r, scheme, rawAuthority) {
			return "", false, errCrossAuthorityDestination
		}
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
	switch overwriteHeader := strings.ToUpper(strings.TrimSpace(r.Header.Get("Overwrite"))); overwriteHeader {
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
		target, err := h.resolveLockTarget(r, p)
		if err != nil {
			return r
		}
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
	return r.WithContext(withLockBatchIndex(r.Context(), newLockBatchIndex(union, locks)))
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
	writeServerFailure(w, err, "failed to resolve source")
}

func (h *DavServer) copy(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Copy", "COPY %s -> %s", r.URL.Path, r.Header.Get("Destination"))
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	srcPath := path.Clean(r.URL.Path)
	destPath, overwrite, err := h.parseDestinationHeader(r)
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

	if srcCalID, srcUID, srcMatched, err := h.parseCalendarResourcePath(r.Context(), user, srcPath); err != nil {
		writeSourceResolutionError(w, err)
		return
	} else if srcMatched && srcUID != "" {
		h.copyCalendarEvent(w, r, user, srcCalID, srcUID, destPath, overwrite)
		return
	}

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
	aclGuard, err := h.aclGuard(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate ACL")
		return
	}
	if _, err := h.loadCalendarWithPrivilege(r.Context(), user, srcCalID, srcPath(r), "read"); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, srcPath(r), "read"))
		return
	}

	src, err := h.store.Events.GetByResourceName(r.Context(), srcCalID, srcUID)
	if err != nil {
		writeServerFailure(w, err, "failed to load source event")
		return
	}
	if src == nil {
		http.Error(w, "source event not found", http.StatusNotFound)
		return
	}
	if !h.checkConditionalHeaders(r, src) {
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	}
	destCalID, destResourceName, destMatched, err := h.parseCalendarResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}
	if destinationNameRefused(destResourceName, eventResourceName(*src)) {
		writeCalDAVError(w, http.StatusForbidden, "valid-calendar-object-resource")
		return
	}

	existing, err := h.store.Events.GetByResourceName(r.Context(), destCalID, destResourceName)
	if err != nil {
		writeServerFailure(w, err, "failed to load destination event")
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
		h.writeCalendarDestinationRefusal(w, r, user, destCalID, destPath, existing, requirePrivilegeAt(err, loadPrivilegePath, loadPrivilege))
		return
	}
	// A COPY replaces an existing destination's properties with the source's
	// as well as its body, and RFC 3744 Appendix B has it need DAV:read on the
	// destination too.
	if existing != nil {
		for _, privilege := range []string{"write-properties", "read"} {
			if err := h.requireCalendarPrivilege(r.Context(), user, &destCal.Calendar, destPath, privilege); err != nil {
				h.writeCalendarDestinationRefusal(w, r, user, destCalID, destPath, existing, requirePrivilegeAt(err, destPath, privilege))
				return
			}
		}
	}
	if err := h.requireCalendarDestinationWritePrivileges(r.Context(), user, destCal, destPath, existing); err != nil {
		h.writeCalendarDestinationRefusal(w, r, user, destCalID, destPath, existing, err)
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
		writeServerFailure(w, err, "failed to load destination event")
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
	validated, fault := validateTransferredCalendarObject(src.RawICAL, destCal, nil)
	if fault != nil {
		writeCalendarObjectFault(w, fault)
		return
	}
	etag := newCopyETag(validated.Body, destCalID)
	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		writeServerFailure(w, err, "failed to resolve resource state paths")
		return
	}
	lockPreconditions, ok := h.writeLockPreconditions(w, r, destPath, path.Dir(destPath))
	if !ok {
		return
	}
	defer invalidateDAVRequestState(r.Context())
	result, err := h.store.TransferCalendarObject(r.Context(), store.CalendarObjectTransfer{
		Operation:               store.CalendarObjectCopy,
		SourceCalendarID:        srcCalID,
		SourceUID:               src.UID,
		SourceResourceName:      eventResourceName(*src),
		ExpectedSourceETag:      src.ETag,
		DestinationCalendarID:   destCalID,
		DestinationResourceName: destResourceName,
		ExpectedDestination:     calendarObjectTransferState(existing),
		Overwrite:               overwrite,
		ExpectedACL:             aclGuard,
		RawICAL:                 validated.Body,
		ETag:                    etag,
		Metadata:                &validated.Analysis.Metadata,
		SourceStatePath:         fromStatePath,
		DestinationStatePath:    toStatePath,
		LockPreconditions:       lockPreconditions,
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
		writeServerFailure(w, err, "failed to copy event")
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
	aclGuard, err := h.aclGuard(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate ACL")
		return
	}
	srcBook, err := h.getAddressBook(r.Context(), srcBookID)
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "source not found", http.StatusNotFound)
		return
	}
	if err := h.requireAddressBookPrivilege(r.Context(), user, srcBook, path.Clean(r.URL.Path), "read"); err != nil {
		_ = writePrivilegeRequirementError(w, requirePrivatePrivilegeAt(err, path.Clean(r.URL.Path), "read"))
		return
	}
	src, err := h.store.Contacts.GetByResourceName(r.Context(), srcBookID, srcUID)
	if err != nil {
		writeServerFailure(w, err, "failed to load source contact")
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
	destBookID, destResourceName, destMatched, err := h.parseAddressBookResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}
	if destinationNameRefused(destResourceName, contactResourceName(*src)) {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}
	destBook, err := h.getAddressBook(r.Context(), destBookID)
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "destination not found", http.StatusNotFound)
		return
	}

	existingByName, err := h.store.Contacts.GetByResourceName(r.Context(), destBookID, destResourceName)
	if err != nil {
		writeServerFailure(w, err, "failed to load destination contact")
		return
	}
	if !h.authorizeAddressBookDestination(w, r, user, destBook, destPath, existingByName,
		privilegeAt{"write-content", destPath}, privilegeAt{"write-properties", destPath}, privilegeAt{"read", destPath}) {
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
		writeServerFailure(w, err, "failed to load destination contact")
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
		writeServerFailure(w, err, "failed to resolve resource state paths")
		return
	}
	replacedUID := ""
	if existingByName != nil {
		replacedUID = existingByName.UID
	}
	expected := store.ContactTransferExpectation{
		Source:      store.ContactDAVResourceState(src),
		Destination: store.ContactDAVResourceState(existingByName),
		ACL:         aclGuard,
		Overwrite:   overwrite,
	}
	lockPreconditions, ok := h.writeLockPreconditions(w, r, destPath, path.Dir(destPath))
	if !ok {
		return
	}
	defer invalidateDAVRequestState(r.Context())
	_, err = h.store.CopyContactAndState(r.Context(), srcBookID, destBookID, src.UID, destResourceName, etag, fromStatePath, toStatePath, replacedUID,
		expected, lockPreconditions)
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
		writeServerFailure(w, err, "failed to copy contact")
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
	destPath, overwrite, err := h.parseDestinationHeader(r)
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

	if srcCalID, srcUID, srcMatched, err := h.parseCalendarResourcePath(r.Context(), user, srcPath); err != nil {
		writeSourceResolutionError(w, err)
		return
	} else if srcMatched && srcUID != "" {
		h.moveCalendarEvent(w, r, user, srcCalID, srcUID, destPath, overwrite)
		return
	}

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
	aclGuard, err := h.aclGuard(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate ACL")
		return
	}
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
	if err != nil {
		writeServerFailure(w, err, "failed to load source event")
		return
	}
	if src == nil {
		http.Error(w, "source event not found", http.StatusNotFound)
		return
	}
	if !h.requireLocks(w, r, "source is locked", path.Clean(r.URL.Path), sourceParent) {
		return
	}
	if !h.checkConditionalHeaders(r, src) {
		http.Error(w, "precondition failed", http.StatusPreconditionFailed)
		return
	}

	destCalID, destResourceName, destMatched, err := h.parseCalendarResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}
	if destinationNameRefused(destResourceName, eventResourceName(*src)) {
		writeCalDAVError(w, http.StatusForbidden, "valid-calendar-object-resource")
		return
	}

	existing, err := h.store.Events.GetByResourceName(r.Context(), destCalID, destResourceName)
	if err != nil {
		writeServerFailure(w, err, "failed to load destination event")
		return
	}
	loadPrivilege := "bind"
	loadPrivilegePath := path.Dir(destPath)
	if existing != nil {
		loadPrivilege = "unbind"
	}
	destCal, err := h.loadCalendarWithPrivilege(r.Context(), user, destCalID, loadPrivilegePath, loadPrivilege)
	if err != nil {
		h.writeCalendarDestinationRefusal(w, r, user, destCalID, destPath, existing, requirePrivilegeAt(err, loadPrivilegePath, loadPrivilege))
		return
	}
	if err := h.requireCalendarDestinationWritePrivileges(r.Context(), user, destCal, destPath, existing); err != nil {
		h.writeCalendarDestinationRefusal(w, r, user, destCalID, destPath, existing, err)
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
		writeServerFailure(w, err, "failed to load destination event")
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
	validated, fault := validateTransferredCalendarObject(src.RawICAL, destCal, nil)
	if fault != nil {
		writeCalendarObjectFault(w, fault)
		return
	}

	fromStatePath, toStatePath, err := h.moveStatePaths(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		writeServerFailure(w, err, "failed to resolve resource state paths")
		return
	}
	lockPreconditions, ok := h.writeLockPreconditions(w, r, srcPath(r), path.Dir(srcPath(r)), destPath, path.Dir(destPath))
	if !ok {
		return
	}
	defer invalidateDAVRequestState(r.Context())
	result, err := h.store.TransferCalendarObject(r.Context(), store.CalendarObjectTransfer{
		Operation:               store.CalendarObjectMove,
		SourceCalendarID:        srcCalID,
		SourceUID:               src.UID,
		SourceResourceName:      eventResourceName(*src),
		ExpectedSourceETag:      src.ETag,
		DestinationCalendarID:   destCalID,
		DestinationResourceName: destResourceName,
		ExpectedDestination:     calendarObjectTransferState(existing),
		Overwrite:               overwrite,
		ExpectedACL:             aclGuard,
		RawICAL:                 validated.Body,
		Metadata:                &validated.Analysis.Metadata,
		SourceStatePath:         fromStatePath,
		DestinationStatePath:    toStatePath,
		LockPreconditions:       lockPreconditions,
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
		writeServerFailure(w, err, "failed to move event")
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
		writeServerFailure(w, errors.Join(resourceErr, uidErr), "failed to load conflicting event")
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
	aclGuard, err := h.aclGuard(r.Context(), user, srcPath(r), destPath)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate ACL")
		return
	}
	srcBook, err := h.getAddressBook(r.Context(), srcBookID)
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
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
		writeServerFailure(w, err, "failed to load source contact")
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
	destBookID, destResourceName, destMatched, err := h.parseAddressBookResourcePath(r.Context(), user, destPath)
	if err != nil || !destMatched {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "invalid destination", http.StatusForbidden)
		return
	}
	if destinationNameRefused(destResourceName, contactResourceName(*src)) {
		writeCardDAVPrecondition(w, http.StatusBadRequest, "valid-address-data")
		return
	}
	destBook, err := h.getAddressBook(r.Context(), destBookID)
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "destination not found", http.StatusNotFound)
		return
	}

	existingByName, err := h.store.Contacts.GetByResourceName(r.Context(), destBookID, destResourceName)
	if err != nil {
		writeServerFailure(w, err, "failed to load destination contact")
		return
	}
	if !h.authorizeAddressBookDestination(w, r, user, destBook, destPath, existingByName, privilegeAt{"unbind", path.Dir(destPath)}) {
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
		writeServerFailure(w, err, "failed to load destination contact")
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
		writeServerFailure(w, err, "failed to resolve resource state paths")
		return
	}
	replacedUID := ""
	if existingByName != nil {
		replacedUID = existingByName.UID
	}
	expected := store.ContactTransferExpectation{
		Source:      store.ContactDAVResourceState(src),
		Destination: store.ContactDAVResourceState(existingByName),
		ACL:         aclGuard,
		Overwrite:   overwrite,
	}
	lockPreconditions, ok := h.writeLockPreconditions(w, r, srcPath(r), path.Dir(srcPath(r)), destPath, path.Dir(destPath))
	if !ok {
		return
	}
	defer invalidateDAVRequestState(r.Context())
	if err := h.store.MoveContactAndState(r.Context(), srcBookID, destBookID, src.UID, destResourceName, fromStatePath, toStatePath, replacedUID,
		expected, lockPreconditions); err != nil {
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
		writeServerFailure(w, err, "failed to move contact")
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
		writeServerFailure(w, errors.Join(resourceErr, uidErr), "failed to load conflicting contact")
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

// writeCalendarDestinationRefusal answers a COPY or MOVE refused on its
// destination. A principal that may not read an existing destination is
// answered as for an absent one, as a PUT to it would be.
func (h *DavServer) writeCalendarDestinationRefusal(w http.ResponseWriter, r *http.Request, user *store.User, calendarID int64, destPath string, existing *store.Event, refusal error) {
	if existing != nil {
		refusal = h.existingObjectWriteRefusal(r.Context(), user, destPath, refusal, func() error {
			return h.calendarMemberBindRefusal(r.Context(), user, calendarID, destPath)
		})
	}
	_ = writePrivilegeRequirementError(w, refusal)
}

// privilegeAt names a privilege and the path it is needed on.
type privilegeAt struct {
	privilege, path string
}

// authorizeAddressBookDestination checks the privileges RFC 3744 Appendix B
// makes a COPY or MOVE need on its destination in book, as calendars check
// them, and answers a refusal: DAV:bind on the collection for an absent
// destination, and for an existing one first overwrite, which is
// DAV:write-content, DAV:write-properties and DAV:read on the destination for
// a COPY and DAV:unbind on the collection for a MOVE, then DAV:bind and
// DAV:unbind on the collection. A principal that may not read an existing destination is
// answered as for an absent one.
func (h *DavServer) authorizeAddressBookDestination(w http.ResponseWriter, r *http.Request, user *store.User, book *store.AddressBook, destPath string, existing *store.Contact, overwrite ...privilegeAt) bool {
	needed := []privilegeAt{{"bind", path.Dir(destPath)}}
	if existing != nil {
		needed = overwrite
	}
	var refusal error
	for _, need := range needed {
		if err := h.requireAddressBookPrivilege(r.Context(), user, book, need.path, need.privilege); err != nil {
			refusal = requirePrivilegeAt(err, need.path, need.privilege)
			break
		}
	}
	if refusal == nil {
		refusal = h.requireAddressBookDestinationWritePrivileges(r.Context(), user, book, destPath, existing)
	}
	if refusal == nil {
		return true
	}
	if existing != nil {
		refusal = h.existingObjectWriteRefusal(r.Context(), user, destPath, refusal, func() error {
			return h.addressBookMemberBindRefusal(r.Context(), user, book, destPath)
		})
	}
	_ = writePrivilegeRequirementError(w, refusal)
	return false
}

// addressBookMemberBindRefusal is the refusal a write creating the member at
// cleanPath meets: DAV:bind on its address book, or nil when user holds it.
func (h *DavServer) addressBookMemberBindRefusal(ctx context.Context, user *store.User, book *store.AddressBook, cleanPath string) error {
	parentPath := path.Dir(cleanPath)
	if err := h.requireAddressBookPrivilege(ctx, user, book, parentPath, "bind"); err != nil {
		return requirePrivilegeAt(err, parentPath, "bind")
	}
	return nil
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

// destinationNameRefused reports whether a COPY or MOVE of the object stored
// as sourceName is refused the destination name destName, as a PUT of that
// name is. A name the object is already stored under is kept whatever its
// length, since its index entry fits, but not one store.ValidMemberName
// refuses.
func destinationNameRefused(destName, sourceName string) bool {
	if !store.ValidMemberName(destName) {
		return true
	}
	return len(destName) > store.MaxIdentifierOctets && destName != sourceName
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

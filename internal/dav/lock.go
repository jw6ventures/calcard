package dav

import (
	"context"
	"crypto/rand"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

const (
	defaultLockTimeout = 3600      // 1 hour
	maxLockTimeout     = 86400 * 7 // 1 week
)

// conditionLockTokenMatchesRequestURI is the RFC 4918 precondition for a lock
// token that names no lock covering the Request-URI. The status depends on the
// method, and the two statements in the specification differ: §16 and §9.11.1
// give UNLOCK 409, while §9.10.6 gives a LOCK refresh 412 for the same
// condition. Each call site carries the status its own section names.
const conditionLockTokenMatchesRequestURI = "lock-token-matches-request-uri"

func generateLockToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate lock token: %w", err)
	}
	return fmt.Sprintf("opaquelocktoken:%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func parseLockTimeout(header string) int {
	if header == "" {
		return defaultLockTimeout
	}
	for _, part := range strings.Split(header, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "Second-") {
			if secs, err := strconv.Atoi(strings.TrimPrefix(part, "Second-")); err == nil && secs > 0 {
				if secs > maxLockTimeout {
					return maxLockTimeout
				}
				return secs
			}
		}
		if part == "Infinite" {
			return maxLockTimeout
		}
	}
	return defaultLockTimeout
}

func parseLockDepth(header string) (string, error) {
	depth := strings.ToLower(strings.TrimSpace(header))
	if depth == "" {
		return "infinity", nil
	}
	switch depth {
	case "0", "infinity":
		return depth, nil
	default:
		return "", fmt.Errorf("invalid lock depth")
	}
}

type ifHeaderState struct {
	tagged   map[string][]string
	untagged []string
}

// parseIfHeaderState collects the lock tokens an If header submits. tagPath
// turns a resource tag into the decoded path it names on this server, and
// reports false for a tag that names nothing here. RFC 4918 §10.4.2 applies a
// tagged list only to the resource its tag names, so the lists after such a
// tag are dropped rather than read as untagged ones.
func parseIfHeaderState(header string, tagPath func(string) (string, bool)) ifHeaderState {
	state := ifHeaderState{tagged: make(map[string][]string)}
	header = strings.TrimSpace(header)
	if header == "" {
		return state
	}

	addToken := func(dst []string, token string) []string {
		token = strings.TrimSpace(token)
		if token == "" {
			return dst
		}
		for _, existing := range dst {
			if existing == token {
				return dst
			}
		}
		return append(dst, token)
	}

	extractTokens := func(segment string) []string {
		var tokens []string
		for len(segment) > 0 {
			start := strings.IndexByte(segment, '<')
			if start < 0 {
				break
			}
			prefix := strings.TrimSpace(segment[:start])
			segment = segment[start+1:]
			end := strings.IndexByte(segment, '>')
			if end < 0 {
				break
			}
			if !strings.EqualFold(prefix, "Not") {
				tokens = addToken(tokens, segment[:end])
			}
			segment = segment[end+1:]
		}
		return tokens
	}

	tagged, currentTag := false, ""
	for i := 0; i < len(header); {
		switch header[i] {
		case ' ', '\t', '\r', '\n':
			i++
		case '<':
			end := strings.IndexByte(header[i+1:], '>')
			if end < 0 {
				return state
			}
			token := header[i+1 : i+1+end]
			i += end + 2
			j := i
			for j < len(header) && strings.ContainsRune(" \t\r\n", rune(header[j])) {
				j++
			}
			if j < len(header) && header[j] == '(' {
				tagged = true
				currentTag, _ = tagPath(token)
			}
			i = j
		case '(':
			end := strings.IndexByte(header[i+1:], ')')
			if end < 0 {
				return state
			}
			tokens := extractTokens(header[i+1 : i+1+end])
			switch {
			case !tagged:
				for _, token := range tokens {
					state.untagged = addToken(state.untagged, token)
				}
			case currentTag != "":
				for _, token := range tokens {
					state.tagged[currentTag] = addToken(state.tagged[currentTag], token)
				}
			}
			i += end + 2
		default:
			i++
		}
	}

	return state
}

// ifResourceTagPath resolves an If header resource tag to the path it names,
// so a tag naming another server names no resource here. A relative tag
// resolves against the Request-URI as a DAV:href does, but the tag's path alone
// names the resource; the Request-URI's query has no bearing on it.
func (h *DavServer) ifResourceTagPath(r *http.Request) func(string) (string, bool) {
	return func(tag string) (string, bool) {
		resolved, ok := h.resolveLocalReference(tag, davHrefReferenceBase(r.URL.Path), r)
		if !ok {
			return "", false
		}
		return normalizeDAVHref(resolved.EscapedPath()), true
	}
}

func (h *DavServer) parseIfHeader(r *http.Request) ifHeaderState {
	return parseIfHeaderState(r.Header.Get("If"), h.ifResourceTagPath(r))
}

// lockTokensForPaths lists the tokens submitted for any of resourcePaths, then
// the untagged ones, each once.
func (state ifHeaderState) lockTokensForPaths(resourcePaths ...string) []string {
	seen := make(map[string]struct{})
	var tokens []string
	for _, resourcePath := range resourcePaths {
		resourcePath = cleanDAVPath(resourcePath)
		if resourcePath == "" {
			continue
		}
		if tagged := state.tagged[resourcePath]; len(tagged) > 0 {
			for _, token := range tagged {
				if _, ok := seen[token]; ok {
					continue
				}
				seen[token] = struct{}{}
				tokens = append(tokens, token)
			}
		}
	}
	for _, token := range state.untagged {
		if _, ok := seen[token]; ok {
			continue
		}
		seen[token] = struct{}{}
		tokens = append(tokens, token)
	}
	return tokens
}

func (state ifHeaderState) firstLockToken(resourcePaths ...string) string {
	tokens := state.lockTokensForPaths(resourcePaths...)
	if len(tokens) == 0 {
		return ""
	}
	return tokens[0]
}

func (h *DavServer) lock(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Lock", "LOCK %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousAddressBook) || errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to resolve path", http.StatusInternalServerError)
		return
	}
	depth, err := parseLockDepth(r.Header.Get("Depth"))
	if err != nil {
		http.Error(w, "invalid Depth header", http.StatusBadRequest)
		return
	}
	targetExists, targetState, err := h.lockTargetState(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to resolve lock target", http.StatusInternalServerError)
		return
	}
	aclGuard, err := h.aclGuard(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to authorize lock", http.StatusInternalServerError)
		return
	}
	allowed, err := h.canLockPath(r.Context(), user, cleanPath, targetExists)
	if err != nil {
		http.Error(w, "failed to authorize lock", http.StatusInternalServerError)
		return
	}
	if !allowed {
		_ = writePrivilegeRequirementError(w, h.lockRefusal(r.Context(), user, cleanPath, targetExists))
		return
	}
	target := parsedDAVTarget(r.Context(), cleanPath)
	createsMember := !targetExists && target.Valid && target.Resource
	timeout := parseLockTimeout(r.Header.Get("Timeout"))
	expiresAt := time.Now().Add(time.Duration(timeout) * time.Second)

	// Check for lock refresh (If header with existing token)
	if ifToken := h.parseIfHeader(r).firstLockToken(cleanPath, canonicalPath); ifToken != "" {
		existing, err := h.store.Locks.GetByToken(r.Context(), ifToken)
		if err != nil || existing == nil {
			// RFC 4918 §9.10.6 gives a lock refresh whose token has disappeared
			// or is invalid the same precondition as one out of scope, at 412.
			writeDAVError(w, http.StatusPreconditionFailed, conditionLockTokenMatchesRequestURI)
			return
		}
		if existing.UserID != user.ID {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if !sameLockRoot(existing.ResourcePath, canonicalPath) {
			writeDAVError(w, http.StatusPreconditionFailed, conditionLockTokenMatchesRequestURI)
			return
		}
		defer invalidateDAVRequestState(r.Context())
		refreshed, err := h.store.Locks.Refresh(r.Context(), ifToken, timeout, expiresAt)
		if err != nil {
			http.Error(w, "failed to refresh lock", http.StatusInternalServerError)
			return
		}
		writeLockResponse(w, refreshed, http.StatusOK)
		return
	}

	// Parse lock request body
	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	if len(body) == 0 {
		http.Error(w, "lock request body required", http.StatusBadRequest)
		return
	}

	var info lockInfo
	if err := safeUnmarshalXML(body, &info); err != nil {
		http.Error(w, "invalid lock request", http.StatusBadRequest)
		return
	}
	if info.LockType.Write == nil || (info.LockScope.Exclusive == nil && info.LockScope.Shared == nil) {
		http.Error(w, "invalid lock request", http.StatusBadRequest)
		return
	}

	scope := "exclusive"
	if info.LockScope.Shared != nil {
		scope = "shared"
	}
	lockType := "write"

	ownerInfo := ""
	if info.Owner != nil {
		if info.Owner.Href != "" {
			ownerInfo = info.Owner.Href
		} else {
			ownerInfo = info.Owner.Text
		}
	}

	token, err := generateLockToken()
	if err != nil {
		http.Error(w, "failed to generate lock token", http.StatusInternalServerError)
		return
	}
	newLock := store.Lock{
		Token:          token,
		ResourcePath:   canonicalPath,
		UserID:         user.ID,
		LockScope:      scope,
		LockType:       lockType,
		Depth:          depth,
		OwnerInfo:      ownerInfo,
		TimeoutSeconds: timeout,
		ExpiresAt:      expiresAt,
	}
	expectedExists := targetExists
	newLock.ExpectedTargetExists = &expectedExists
	if err := h.populateLockExpectation(r.Context(), user, cleanPath, targetState, &newLock); err != nil {
		http.Error(w, "failed to resolve lock target state", http.StatusInternalServerError)
		return
	}
	// RFC 4918 §9.10.4 has a LOCK of an unmapped URL create the resource, which
	// changes its collection's membership and so needs the collection's lock
	// (§7.5), held by this principal (§6.4), as a PUT creating it does. The
	// store re-checks the collection's locks as it creates this one.
	var membershipPreconditions []store.LockPrecondition
	if createsMember {
		if !h.requireLock(w, r, path.Dir(cleanPath), "resource is locked") {
			return
		}
		membershipPreconditions = h.lockPreconditions(r, path.Dir(cleanPath))
	}

	status := http.StatusOK
	if !targetExists {
		status = http.StatusCreated
	}

	defer invalidateDAVRequestState(r.Context())
	created, err := h.store.CreateLockAndState(r.Context(), newLock, aclGuard, membershipPreconditions)
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is already locked", http.StatusLocked)
			return
		}
		if errors.Is(err, store.ErrResourceStateChanged) {
			http.Error(w, "resource state changed", http.StatusPreconditionFailed)
			return
		}
		http.Error(w, "failed to create lock", http.StatusInternalServerError)
		return
	}
	if created.ResourcePath != canonicalPath {
		status = http.StatusOK
	}

	w.Header().Set("Lock-Token", "<"+token+">")
	writeLockResponse(w, created, status)
}

// populateLockExpectation records on lock the collection cleanPath is in and,
// for an object resource, resourceState: the state lockTargetState read when
// deciding the target's existence, so the lock is created only if that state
// still holds.
func (h *DavServer) populateLockExpectation(ctx context.Context, user *store.User, cleanPath string, resourceState *store.DAVResourceState, lock *store.Lock) error {
	if h == nil || h.store == nil || lock == nil {
		return nil
	}
	target := parsedDAVTarget(ctx, cleanPath)
	if !target.Valid || target.CollectionSegment == "" {
		return nil
	}
	switch target.Domain {
	case davPathCalendar:
		calendarID, ok, err := h.resolveCalendarID(ctx, user, target.CollectionSegment)
		if errors.Is(err, store.ErrNotFound) || !ok {
			return nil
		}
		if err != nil || calendarID == birthdayCalendarID {
			return err
		}
		if _, err := h.getCalendar(ctx, calendarID); err != nil {
			return err
		}
		lock.ExpectedCollection = "calendar"
		lock.ExpectedCollectionID = calendarID
		if target.Resource {
			lock.ExpectedResourceState = resourceState
		}
	case davPathAddressBook:
		addressBookID, ok, err := h.resolveAddressBookID(ctx, user, target.CollectionSegment)
		if errors.Is(err, store.ErrNotFound) || !ok {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := h.getAddressBook(ctx, addressBookID); err != nil {
			return err
		}
		lock.ExpectedCollection = "addressbook"
		lock.ExpectedCollectionID = addressBookID
		if target.Resource {
			lock.ExpectedResourceState = resourceState
		}
	}
	return nil
}

// canLockPath decides a LOCK on cleanPath. targetExists is lockTargetState's
// answer for the same path, so the privilege checked and the one a refusal
// names both follow from one resolution of the resource the path names.
func (h *DavServer) canLockPath(ctx context.Context, user *store.User, cleanPath string, targetExists bool) (bool, error) {
	switch {
	case strings.HasPrefix(cleanPath, "/dav/addressbooks/"):
		return h.canLockAddressBookPath(ctx, user, cleanPath, targetExists)
	case strings.HasPrefix(cleanPath, "/dav/calendars/"):
		return h.canLockCalendarPath(ctx, user, cleanPath, targetExists)
	default:
		return false, nil
	}
}

// objectLockPrivilege names the privilege RFC 3744 Appendix B makes a LOCK on
// an object resource need, and the path it is checked on: DAV:write-content on
// an existing object and DAV:bind on the parent of an absent one.
func objectLockPrivilege(cleanPath string, targetExists bool) (privilegePath, privilege string) {
	if targetExists {
		return cleanPath, "write-content"
	}
	return path.Dir(cleanPath), "bind"
}

// lockRefusal is the refusal a LOCK canLockPath refused answers with. Like a
// PUT's, it names the resource by the request's own path.
func (h *DavServer) lockRefusal(ctx context.Context, user *store.User, cleanPath string, targetExists bool) error {
	absentRefusal := requirePrivilegeAt(errPrivilegeNotGranted, path.Dir(cleanPath), "bind")
	if !targetExists {
		return absentRefusal
	}
	refusal := requirePrivilegeAt(errPrivilegeNotGranted, cleanPath, "write-content")
	if target := parsedDAVTarget(ctx, cleanPath); !target.Valid || !target.Resource {
		return refusal
	}
	return h.existingObjectWriteRefusal(ctx, user, cleanPath, refusal, func() error {
		lockableIfAbsent, err := h.canLockPath(ctx, user, cleanPath, false)
		if err != nil {
			return requirePrivilegeAt(err, path.Dir(cleanPath), "bind")
		}
		if lockableIfAbsent {
			return nil
		}
		return absentRefusal
	})
}

func (h *DavServer) lockTargetExists(ctx context.Context, user *store.User, cleanPath string) (bool, error) {
	exists, _, err := h.lockTargetState(ctx, user, cleanPath)
	return exists, err
}

// lockTargetState reports whether the resource cleanPath names exists and, for
// an object resource, the state a lock taken on it expects, both from one read.
func (h *DavServer) lockTargetState(ctx context.Context, user *store.User, cleanPath string) (bool, *store.DAVResourceState, error) {
	switch {
	case strings.HasPrefix(cleanPath, "/dav/addressbooks/"):
		if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(ctx, user, cleanPath); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil, nil
			}
			return false, nil, err
		} else if matched {
			if h == nil || h.store == nil || h.store.Contacts == nil {
				return true, nil, nil
			}
			existing, err := h.store.Contacts.GetByResourceName(ctx, addressBookID, resourceName)
			if err != nil {
				return false, nil, err
			}
			state := store.ContactDAVResourceState(existing)
			if existing == nil {
				state.ResourceName = resourceName
			}
			return existing != nil, &state, nil
		}

		segment := singleCollectionSegment(cleanPath, "/dav/addressbooks/")
		if segment == "" {
			return false, nil, nil
		}
		_, ok, err := h.resolveAddressBookID(ctx, user, segment)
		if errors.Is(err, store.ErrNotFound) {
			return false, nil, nil
		}
		return ok, nil, err
	case strings.HasPrefix(cleanPath, "/dav/calendars/"):
		if calendarID, resourceName, matched, err := h.parseCalendarResourcePath(ctx, user, cleanPath); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil, nil
			}
			return false, nil, err
		} else if matched {
			if h == nil || h.store == nil || h.store.Events == nil {
				return true, nil, nil
			}
			existing, err := h.store.Events.GetByResourceName(ctx, calendarID, resourceName)
			if err != nil {
				return false, nil, err
			}
			state := store.EventDAVResourceState(existing)
			if existing == nil {
				state.ResourceName = resourceName
			}
			return existing != nil, &state, nil
		}

		segment := singleCollectionSegment(cleanPath, "/dav/calendars/")
		if segment == "" {
			return false, nil, nil
		}
		_, ok, err := h.resolveCalendarID(ctx, user, segment)
		if errors.Is(err, store.ErrNotFound) {
			return false, nil, nil
		}
		return ok, nil, err
	default:
		return true, nil, nil
	}
}

func (h *DavServer) canLockAddressBookPath(ctx context.Context, user *store.User, cleanPath string, targetExists bool) (bool, error) {
	if addressBookID, _, matched, err := h.parseAddressBookResourcePath(ctx, user, cleanPath); err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errAmbiguousAddressBook) {
			return false, nil
		}
		return false, err
	} else if matched {
		book, err := h.getAddressBook(ctx, addressBookID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return false, nil
			}
			return false, err
		}
		privilegePath, privilege := objectLockPrivilege(cleanPath, targetExists)
		if err := h.requireAddressBookPrivilege(ctx, user, book, privilegePath, privilege); err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	segment := singleCollectionSegment(cleanPath, "/dav/addressbooks/")
	if segment == "" {
		return false, nil
	}
	addressBookID, ok, err := h.resolveAddressBookID(ctx, user, segment)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return true, nil
		}
		if errors.Is(err, errAmbiguousAddressBook) {
			return false, nil
		}
		return false, err
	}
	if !ok {
		return true, nil
	}
	book, err := h.getAddressBook(ctx, addressBookID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, "write-content"); err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h *DavServer) canLockCalendarPath(ctx context.Context, user *store.User, cleanPath string, targetExists bool) (bool, error) {
	if calendarID, _, matched, err := h.parseCalendarResourcePath(ctx, user, cleanPath); err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errAmbiguousCalendar) {
			return false, nil
		}
		return false, err
	} else if matched {
		if calendarID == birthdayCalendarID {
			return false, nil
		}
		cal, err := h.getCalendar(ctx, calendarID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
				return false, nil
			}
			return false, err
		}
		privilegePath, privilege := objectLockPrivilege(cleanPath, targetExists)
		if err := h.requireCalendarPrivilege(ctx, user, cal, privilegePath, privilege); err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	segment := singleCollectionSegment(cleanPath, "/dav/calendars/")
	if segment == "" {
		return false, nil
	}
	calendarID, ok, err := h.resolveCalendarID(ctx, user, segment)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return true, nil
		}
		if errors.Is(err, errAmbiguousCalendar) {
			return false, nil
		}
		return false, err
	}
	if !ok {
		return true, nil
	}
	if calendarID == birthdayCalendarID {
		return false, nil
	}
	cal, err := h.loadCalendar(ctx, user, calendarID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, cleanPath, "write-content"); err != nil {
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func singleCollectionSegment(cleanPath, prefix string) string {
	trimmed := strings.Trim(strings.TrimPrefix(cleanPath, prefix), "/")
	if trimmed == "" || strings.Contains(trimmed, "/") {
		return ""
	}
	return strings.TrimSpace(trimmed)
}

type lockResponseProp struct {
	XMLName       xml.Name           `xml:"d:prop"`
	XmlnsD        string             `xml:"xmlns:d,attr"`
	LockDiscovery *lockDiscoveryProp `xml:"d:lockdiscovery,omitempty"`
}

func writeLockResponse(w http.ResponseWriter, lock *store.Lock, status int) {
	resp := lockResponseProp{
		XmlnsD:        "DAV:",
		LockDiscovery: &lockDiscoveryProp{ActiveLocks: []activeLock{activeLockFromStoreLock(lock, true)}},
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *DavServer) unlock(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Unlock", "UNLOCK %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	tokenHeader := r.Header.Get("Lock-Token")
	if tokenHeader == "" {
		http.Error(w, "missing Lock-Token header", http.StatusBadRequest)
		return
	}

	token := strings.TrimPrefix(strings.TrimSuffix(tokenHeader, ">"), "<")
	cleanPath := path.Clean(r.URL.Path)
	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if errors.Is(err, errAmbiguousAddressBook) || errors.Is(err, errAmbiguousCalendar) {
			http.Error(w, "ambiguous path", http.StatusConflict)
			return
		}
		http.Error(w, "failed to resolve path", http.StatusInternalServerError)
		return
	}
	lock, err := h.store.Locks.GetByToken(r.Context(), token)
	if err != nil || lock == nil {
		writeDAVError(w, http.StatusConflict, conditionLockTokenMatchesRequestURI)
		return
	}
	if !sameLockRoot(lock.ResourcePath, canonicalPath) {
		writeDAVError(w, http.StatusConflict, conditionLockTokenMatchesRequestURI)
		return
	}

	if lock.UserID != user.ID {
		if h.store == nil || h.store.ACLEntries == nil {
			writeNeedPrivileges(w, canonicalPath, "unlock")
			return
		}
		if err := h.requireACLPrivilege(r.Context(), user, requestPathForCanonical(canonicalPath), "unlock"); err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, canonicalPath, "unlock"))
			return
		}
	}

	defer invalidateDAVRequestState(r.Context())
	if err := h.store.Locks.Delete(r.Context(), token); err != nil {
		http.Error(w, "failed to unlock", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// resolvedLockTarget carries one write target's request path, canonical path,
// and the lookup paths whose locks could block a write to it.
type resolvedLockTarget struct {
	requestPath   string
	canonicalPath string
	lookupPaths   []string
}

func (h *DavServer) resolveLockTarget(r *http.Request, resourcePath string) resolvedLockTarget {
	requestPath := cleanDAVPath(resourcePath)
	canonicalPath := requestPath
	if user, ok := auth.UserFromContext(r.Context()); ok {
		if canonical, err := h.canonicalDAVPath(r.Context(), user, requestPath); err == nil && canonical != "" {
			canonicalPath = canonical
		}
	}
	return resolvedLockTarget{requestPath: requestPath, canonicalPath: canonicalPath, lookupPaths: lockLookupPaths(canonicalPath)}
}

// lockAllowsWrite reports whether the write may proceed given the locks that
// could apply to the target: active locks on the resource itself and
// depth-infinity locks on its ancestors block the write unless the request's
// If header carries a matching token.
func (h *DavServer) lockAllowsWrite(r *http.Request, target resolvedLockTarget, locks []store.Lock) bool {
	if len(locks) == 0 {
		return true
	}
	now := time.Now()
	ifHeader := h.parseIfHeader(r)
	// RFC 4918 Section 6.4 requires the authenticated principal to match the
	// lock creator as well as the token to be submitted. Token possession alone
	// cannot carry the decision: DAV:lockdiscovery reports a lock to every
	// principal that can read the resource, so on a shared collection the token
	// is not a secret the creator holds.
	user, _ := auth.UserFromContext(r.Context())

	hasActiveLock := false
	for _, lock := range locks {
		if lock.ExpiresAt.Before(now) {
			continue
		}
		lockPath := normalizeDAVResourceIdentity(lock.ResourcePath)
		// Ancestor locks only apply if they have depth-infinity.
		if lockPath != target.canonicalPath && lock.Depth != "infinity" {
			continue
		}
		hasActiveLock = true
		if user == nil || lock.UserID != user.ID {
			continue
		}
		for _, token := range ifHeader.lockTokensForPaths(target.requestPath, target.canonicalPath, lock.ResourcePath, lockPath) {
			if lock.Token == token {
				return true
			}
		}
	}

	// If there are active locks but none matched the token, block the write.
	return !hasActiveLock
}

// checkLock verifies that a write operation is allowed on a resource.
// Returns true if the operation can proceed, false if the resource is locked
// and the request doesn't include a valid lock token.
func (h *DavServer) checkLock(r *http.Request, resourcePath string) (bool, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return true, nil
	}
	target := h.resolveLockTarget(r, resourcePath)
	locks, err := h.locksForLookupPaths(r.Context(), target.lookupPaths)
	if err != nil {
		return false, err
	}
	return h.lockAllowsWrite(r, target, locks), nil
}

// checkLocks verifies several write targets at once, fetching the union of
// their lock lookup paths in a single query instead of one query per target.
func (h *DavServer) checkLocks(r *http.Request, resourcePaths ...string) (bool, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return true, nil
	}
	seen := make(map[string]struct{}, len(resourcePaths))
	targets := make([]resolvedLockTarget, 0, len(resourcePaths))
	seenLookup := make(map[string]struct{})
	var union []string
	for _, resourcePath := range resourcePaths {
		resourcePath = path.Clean(resourcePath)
		if resourcePath == "." || resourcePath == "/" {
			continue
		}
		if _, ok := seen[resourcePath]; ok {
			continue
		}
		seen[resourcePath] = struct{}{}
		target := h.resolveLockTarget(r, resourcePath)
		targets = append(targets, target)
		for _, p := range target.lookupPaths {
			if _, ok := seenLookup[p]; ok {
				continue
			}
			seenLookup[p] = struct{}{}
			union = append(union, p)
		}
	}
	if len(targets) == 0 {
		return true, nil
	}

	locks, err := h.locksForLookupPaths(r.Context(), union)
	if err != nil {
		return false, err
	}
	byPath := make(map[string][]store.Lock, len(locks))
	for i := range locks {
		key := cleanDAVPath(locks[i].ResourcePath)
		byPath[key] = append(byPath[key], locks[i])
	}
	for _, target := range targets {
		var targetLocks []store.Lock
		for _, p := range target.lookupPaths {
			targetLocks = append(targetLocks, byPath[p]...)
		}
		if !h.lockAllowsWrite(r, target, targetLocks) {
			return false, nil
		}
	}
	return true, nil
}

func (h *DavServer) lockPreconditions(r *http.Request, resourcePaths ...string) []store.LockPrecondition {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return nil
	}
	ifHeader := h.parseIfHeader(r)
	seen := make(map[string]struct{}, len(resourcePaths))
	preconditions := make([]store.LockPrecondition, 0, len(resourcePaths))
	for _, resourcePath := range resourcePaths {
		resourcePath = path.Clean(resourcePath)
		if resourcePath == "." || resourcePath == "/" {
			continue
		}
		if _, ok := seen[resourcePath]; ok {
			continue
		}
		seen[resourcePath] = struct{}{}
		target := h.resolveLockTarget(r, resourcePath)
		tokenPaths := append([]string{target.requestPath, target.canonicalPath}, target.lookupPaths...)
		preconditions = append(preconditions, store.LockPrecondition{
			ResourcePath: target.canonicalPath,
			LookupPaths:  append([]string(nil), target.lookupPaths...),
			Tokens:       ifHeader.lockTokensForPaths(tokenPaths...),
		})
	}
	return preconditions
}

func (h *DavServer) requireLock(w http.ResponseWriter, r *http.Request, resourcePath, lockedMessage string) bool {
	allowed, err := h.checkLock(r, resourcePath)
	if err != nil {
		http.Error(w, "failed to verify lock state", http.StatusInternalServerError)
		return false
	}
	if !allowed {
		http.Error(w, lockedMessage, http.StatusLocked)
		return false
	}
	return true
}

func (h *DavServer) requireLocks(w http.ResponseWriter, r *http.Request, lockedMessage string, resourcePaths ...string) bool {
	allowed, err := h.checkLocks(r, resourcePaths...)
	if err != nil {
		http.Error(w, "failed to verify lock state", http.StatusInternalServerError)
		return false
	}
	if !allowed {
		http.Error(w, lockedMessage, http.StatusLocked)
		return false
	}
	return true
}

func sameLockRoot(lockPath, requestPath string) bool {
	return normalizeDAVResourceIdentity(lockPath) == normalizeDAVResourceIdentity(requestPath)
}

// ancestorPaths returns all parent paths of p, excluding p itself.
// For example, "/dav/addressbooks/5/contact.vcf" returns
// ["/dav/addressbooks/5", "/dav/addressbooks", "/dav"].
func ancestorPaths(p string) []string {
	p = strings.TrimSuffix(p, "/")
	var ancestors []string
	for {
		parent := path.Dir(p)
		if parent == p || parent == "." || parent == "/" {
			break
		}
		ancestors = append(ancestors, parent)
		p = parent
	}
	return ancestors
}

func lockLookupPaths(canonicalPath string) []string {
	seen := map[string]struct{}{}
	var paths []string
	addPath := func(p string) {
		p = cleanDAVPath(p)
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		paths = append(paths, p)
	}

	addPath(canonicalPath)
	for _, legacyPath := range legacyDAVResourcePaths(canonicalPath) {
		addPath(legacyPath)
	}
	for _, ancestor := range ancestorPaths(canonicalPath) {
		addPath(ancestor)
		for _, legacyPath := range legacyDAVResourcePaths(ancestor) {
			addPath(legacyPath)
		}
	}
	return paths
}

func defaultSupportedLock() *supportedLockProp {
	return &supportedLockProp{
		LockEntries: []lockEntry{
			{
				LockScope: activeLockScope{Exclusive: &struct{}{}},
				LockType:  activeLockType{Write: &struct{}{}},
			},
			{
				LockScope: activeLockScope{Shared: &struct{}{}},
				LockType:  activeLockType{Write: &struct{}{}},
			},
		},
	}
}

// revealToken carries whether DAV:locktoken may be reported. RFC 4918 Section
// 15.8 leaves the token optional in DAV:lockdiscovery, and it has to stay out
// of a response to any principal but the creator: the write check treats a
// submitted token as proof the request comes from the lock's owner.
func activeLockFromStoreLock(lock *store.Lock, revealToken bool) activeLock {
	scopeEl := activeLockScope{Exclusive: &struct{}{}}
	if lock.LockScope == "shared" {
		scopeEl = activeLockScope{Shared: &struct{}{}}
	}

	var owner *lockOwnerResp
	if lock.OwnerInfo != "" {
		if strings.HasPrefix(lock.OwnerInfo, "/") || strings.HasPrefix(lock.OwnerInfo, "http") {
			owner = &lockOwnerResp{Href: lock.OwnerInfo}
		} else {
			owner = &lockOwnerResp{Text: lock.OwnerInfo}
		}
	}

	var lockToken *lockTokenProp
	if revealToken {
		lockToken = &lockTokenProp{Href: lock.Token}
	}

	return activeLock{
		LockScope: scopeEl,
		LockType:  activeLockType{Write: &struct{}{}},
		Depth:     lock.Depth,
		Owner:     owner,
		Timeout:   fmt.Sprintf("Second-%d", lock.TimeoutSeconds),
		LockToken: lockToken,
		LockRoot:  &hrefProp{Href: escapeDAVPath(publicDAVLockRoot(lock.ResourcePath))},
	}
}

func publicDAVLockRoot(resourcePath string) string {
	canonicalPath := normalizeDAVResourceIdentity(resourcePath)
	if publicPath, ok := publicPendingCollectionPath(canonicalPath); ok {
		return publicPath
	}
	if legacyPaths := legacyDAVResourcePaths(canonicalPath); len(legacyPaths) > 0 {
		return legacyPaths[0]
	}
	return cleanDAVPath(resourcePath)
}

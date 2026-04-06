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

func generateLockToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		if _, err = rand.Read(b); err != nil {
			return "", fmt.Errorf("generate lock token: %w", err)
		}
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

func parseIfHeaderState(header string) ifHeaderState {
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

	currentTag := ""
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
				currentTag = normalizeDAVHref(token)
			}
			i = j
		case '(':
			end := strings.IndexByte(header[i+1:], ')')
			if end < 0 {
				return state
			}
			tokens := extractTokens(header[i+1 : i+1+end])
			if currentTag != "" {
				for _, token := range tokens {
					state.tagged[currentTag] = addToken(state.tagged[currentTag], token)
				}
			} else {
				for _, token := range tokens {
					state.untagged = addToken(state.untagged, token)
				}
			}
			i += end + 2
		default:
			i++
		}
	}

	return state
}

func ifLockTokensForPaths(header string, resourcePaths ...string) []string {
	state := parseIfHeaderState(header)
	seen := make(map[string]struct{})
	var tokens []string
	for _, resourcePath := range resourcePaths {
		resourcePath = normalizeDAVHref(resourcePath)
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

func firstIfLockToken(header string, resourcePaths ...string) string {
	tokens := ifLockTokensForPaths(header, resourcePaths...)
	if len(tokens) == 0 {
		return ""
	}
	return tokens[0]
}

func (h *Handler) Lock(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	canonicalPath, err := h.canonicalDAVPath(r.Context(), user, cleanPath)
	if err != nil {
		if err == store.ErrNotFound {
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
	timeout := parseLockTimeout(r.Header.Get("Timeout"))
	expiresAt := time.Now().Add(time.Duration(timeout) * time.Second)

	// Check for lock refresh (If header with existing token)
	ifHeader := r.Header.Get("If")
	if ifToken := firstIfLockToken(ifHeader, cleanPath, canonicalPath); ifToken != "" {
		existing, err := h.store.Locks.GetByToken(r.Context(), ifToken)
		if err != nil || existing == nil {
			http.Error(w, "lock token not found", http.StatusPreconditionFailed)
			return
		}
		if existing.UserID != user.ID {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if !sameLockRoot(existing.ResourcePath, canonicalPath) {
			http.Error(w, "lock token does not match request URI", http.StatusPreconditionFailed)
			return
		}
		refreshed, err := h.store.Locks.Refresh(r.Context(), ifToken, timeout, expiresAt)
		if err != nil {
			http.Error(w, "failed to refresh lock", http.StatusInternalServerError)
			return
		}
		writeLockResponse(w, refreshed, http.StatusOK)
		return
	}

	allowed, err := h.canLockPath(r.Context(), user, cleanPath)
	if err != nil {
		http.Error(w, "failed to authorize lock", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "forbidden", http.StatusForbidden)
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

	status := http.StatusOK
	if exists, err := h.lockTargetExists(r.Context(), user, cleanPath); err != nil {
		http.Error(w, "failed to resolve lock target", http.StatusInternalServerError)
		return
	} else if !exists {
		status = http.StatusCreated
	}

	created, err := h.store.Locks.Create(r.Context(), newLock)
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			http.Error(w, "resource is already locked", http.StatusLocked)
			return
		}
		http.Error(w, "failed to create lock", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Lock-Token", "<"+token+">")
	writeLockResponse(w, created, status)
}

func (h *Handler) canLockPath(ctx context.Context, user *store.User, cleanPath string) (bool, error) {
	switch {
	case strings.HasPrefix(cleanPath, "/dav/addressbooks/"):
		return h.canLockAddressBookPath(ctx, user, cleanPath)
	case strings.HasPrefix(cleanPath, "/dav/calendars/"):
		return h.canLockCalendarPath(ctx, user, cleanPath)
	default:
		return false, nil
	}
}

func (h *Handler) lockTargetExists(ctx context.Context, user *store.User, cleanPath string) (bool, error) {
	switch {
	case strings.HasPrefix(cleanPath, "/dav/addressbooks/"):
		if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(ctx, user, cleanPath); err != nil {
			if err == store.ErrNotFound {
				return false, nil
			}
			return false, err
		} else if matched {
			if h == nil || h.store == nil || h.store.Contacts == nil {
				return true, nil
			}
			existing, err := h.store.Contacts.GetByResourceName(ctx, addressBookID, resourceName)
			if err != nil {
				return false, err
			}
			return existing != nil, nil
		}

		segment := singleCollectionSegment(cleanPath, "/dav/addressbooks/")
		if segment == "" {
			return false, nil
		}
		_, ok, err := h.resolveAddressBookID(ctx, user, segment)
		if err == store.ErrNotFound {
			return false, nil
		}
		return ok, err
	case strings.HasPrefix(cleanPath, "/dav/calendars/"):
		if calendarID, resourceName, matched, err := h.parseCalendarResourcePath(ctx, user, cleanPath); err != nil {
			if err == store.ErrNotFound {
				return false, nil
			}
			return false, err
		} else if matched {
			if h == nil || h.store == nil || h.store.Events == nil {
				return true, nil
			}
			existing, err := h.store.Events.GetByResourceName(ctx, calendarID, resourceName)
			if err != nil {
				return false, err
			}
			return existing != nil, nil
		}

		segment := singleCollectionSegment(cleanPath, "/dav/calendars/")
		if segment == "" {
			return false, nil
		}
		_, ok, err := h.resolveCalendarID(ctx, user, segment)
		if err == store.ErrNotFound {
			return false, nil
		}
		return ok, err
	default:
		return true, nil
	}
}

func (h *Handler) canLockAddressBookPath(ctx context.Context, user *store.User, cleanPath string) (bool, error) {
	if addressBookID, resourceName, matched, err := h.parseAddressBookResourcePath(ctx, user, cleanPath); err != nil {
		if err == store.ErrNotFound || errors.Is(err, errAmbiguousAddressBook) {
			return false, nil
		}
		return false, err
	} else if matched {
		book, err := h.getAddressBook(ctx, addressBookID)
		if err != nil {
			if err == store.ErrNotFound {
				return false, nil
			}
			return false, err
		}
		privilege := "bind"
		if h != nil && h.store != nil && h.store.Contacts != nil {
			existing, err := h.store.Contacts.GetByResourceName(ctx, addressBookID, resourceName)
			if err != nil {
				return false, err
			}
			if existing != nil {
				privilege = "write-content"
			}
		}
		if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, privilege); err != nil {
			if err == store.ErrNotFound || errors.Is(err, errForbidden) {
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
		if err == store.ErrNotFound {
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
		if err == store.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, "write"); err != nil {
		if err == store.ErrNotFound || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h *Handler) canLockCalendarPath(ctx context.Context, user *store.User, cleanPath string) (bool, error) {
	if calendarID, _, matched, err := h.parseCalendarResourcePath(ctx, user, cleanPath); err != nil {
		if err == store.ErrNotFound || errors.Is(err, errAmbiguousCalendar) {
			return false, nil
		}
		return false, err
	} else if matched {
		if calendarID == birthdayCalendarID {
			return false, nil
		}
		cal, err := h.getCalendar(ctx, calendarID)
		if err != nil {
			if err == store.ErrNotFound || errors.Is(err, errForbidden) {
				return false, nil
			}
			return false, err
		}
		privilege := "bind"
		if h.store != nil && h.store.Events != nil {
			existing, err := h.store.Events.GetByResourceName(ctx, calendarID, path.Base(normalizeDAVResourceIdentity(cleanPath)))
			if err != nil {
				return false, err
			}
			if existing != nil {
				privilege = "write-content"
			}
		} else {
			privilege = "write-content"
		}
		if err := h.requireCalendarPrivilege(ctx, user, cal, cleanPath, privilege); err != nil {
			if err == store.ErrNotFound || errors.Is(err, errForbidden) {
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
		if err == store.ErrNotFound {
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
		if err == store.ErrNotFound || errors.Is(err, errForbidden) {
			return false, nil
		}
		return false, err
	}
	allowed, err := h.hasAnyCalendarWritePrivilege(ctx, user, cal, cleanPath)
	if err != nil {
		return false, err
	}
	if !allowed {
		return false, nil
	}
	return true, nil
}

func (h *Handler) hasAnyCalendarWritePrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, cleanPath string) (bool, error) {
	for _, privilege := range []string{"write", "bind", "write-content", "write-properties", "unbind"} {
		if err := h.requireCalendarPrivilege(ctx, user, &cal.Calendar, cleanPath, privilege); err == nil {
			return true, nil
		} else if err != store.ErrNotFound && !errors.Is(err, errForbidden) {
			return false, err
		}
	}
	return false, nil
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
		LockDiscovery: &lockDiscoveryProp{ActiveLocks: []activeLock{activeLockFromStoreLock(lock)}},
	}

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(resp)
}

func (h *Handler) Unlock(w http.ResponseWriter, r *http.Request) {
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
		if err == store.ErrNotFound {
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
		http.Error(w, "lock not found", http.StatusConflict)
		return
	}
	if !sameLockRoot(lock.ResourcePath, canonicalPath) {
		http.Error(w, "lock token does not match request URI", http.StatusPreconditionFailed)
		return
	}

	if lock.UserID != user.ID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if err := h.store.Locks.Delete(r.Context(), token); err != nil {
		http.Error(w, "failed to unlock", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// checkLock verifies that a write operation is allowed on a resource.
// Returns true if the operation can proceed, false if the resource is locked
// and the request doesn't include a valid lock token.
func (h *Handler) checkLock(r *http.Request, resourcePath string) (bool, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return true, nil
	}
	requestPath := normalizeDAVHref(resourcePath)
	canonicalPath := requestPath
	if user, ok := auth.UserFromContext(r.Context()); ok {
		if canonical, err := h.canonicalDAVPath(r.Context(), user, requestPath); err == nil && canonical != "" {
			canonicalPath = canonical
		}
	}

	// Batch-fetch locks for the resource and all ancestors in a single query.
	paths := lockLookupPaths(canonicalPath)
	allLocks, err := h.store.Locks.ListByResources(r.Context(), paths)
	if err != nil {
		return false, err
	}
	if len(allLocks) == 0 {
		return true, nil
	}

	// Collect active locks: direct locks on the resource, plus
	// depth-infinity locks on ancestor paths.
	now := time.Now()
	ifHeader := r.Header.Get("If")

	hasActiveLock := false
	for _, lock := range allLocks {
		if lock.ExpiresAt.Before(now) {
			continue
		}
		lockPath := normalizeDAVResourceIdentity(lock.ResourcePath)
		// Ancestor locks only apply if they have depth-infinity.
		if lockPath != canonicalPath && lock.Depth != "infinity" {
			continue
		}
		hasActiveLock = true
		for _, token := range ifLockTokensForPaths(ifHeader, requestPath, canonicalPath, lock.ResourcePath, lockPath) {
			if lock.Token == token {
				return true, nil
			}
		}
	}

	// If there are active locks but none matched the token, block the write.
	return !hasActiveLock, nil
}

func (h *Handler) checkLocks(r *http.Request, resourcePaths ...string) (bool, error) {
	seen := make(map[string]struct{}, len(resourcePaths))
	for _, resourcePath := range resourcePaths {
		resourcePath = path.Clean(resourcePath)
		if resourcePath == "." || resourcePath == "/" {
			continue
		}
		if _, ok := seen[resourcePath]; ok {
			continue
		}
		seen[resourcePath] = struct{}{}
		allowed, err := h.checkLock(r, resourcePath)
		if err != nil {
			return false, err
		}
		if !allowed {
			return false, nil
		}
	}
	return true, nil
}

func (h *Handler) requireLock(w http.ResponseWriter, r *http.Request, resourcePath, lockedMessage string) bool {
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

func (h *Handler) requireLocks(w http.ResponseWriter, r *http.Request, lockedMessage string, resourcePaths ...string) bool {
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

func lockCoversPath(lockPath, depth, requestPath string) bool {
	lockPath = normalizeDAVResourceIdentity(lockPath)
	requestPath = normalizeDAVResourceIdentity(requestPath)
	if lockPath == requestPath {
		return true
	}
	if depth != "infinity" {
		return false
	}
	lockPath = strings.TrimSuffix(lockPath, "/")
	if lockPath == "" {
		return false
	}
	return strings.HasPrefix(requestPath, lockPath+"/")
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
		p = normalizeDAVHref(p)
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

func activeLockFromStoreLock(lock *store.Lock) activeLock {
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

	return activeLock{
		LockScope: scopeEl,
		LockType:  activeLockType{Write: &struct{}{}},
		Depth:     lock.Depth,
		Owner:     owner,
		Timeout:   fmt.Sprintf("Second-%d", lock.TimeoutSeconds),
		LockToken: &lockTokenProp{Href: lock.Token},
		LockRoot:  &hrefProp{Href: publicDAVLockRoot(lock.ResourcePath)},
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
	return normalizeDAVHref(resourcePath)
}

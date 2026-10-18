package dav

import (
	"bytes"
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

// ifHeaderState holds the lock tokens an If header submits, in the order they
// appear, each once. RFC 4918 §7.5 and §10.4.1 have a token submitted by its
// appearing in the header, in whichever list and whatever resource the list's
// tag names, so a COPY or MOVE submits its source's and its destination's
// tokens alike; a tag names the resource a list's conditions are about, not
// the locks its tokens are submitted for. A token that appears only after Not
// is not submitted.
type ifHeaderState struct {
	tokens []string
	seen   map[string]struct{}
}

func (state *ifHeaderState) add(token string) {
	if _, ok := state.seen[token]; ok {
		return
	}
	if state.seen == nil {
		state.seen = make(map[string]struct{})
	}
	state.seen[token] = struct{}{}
	state.tokens = append(state.tokens, token)
}

// parseIfHeaderState collects the lock tokens an If header submits: the state
// token of every condition in its lists that Not does not negate. It reads the
// header by RFC 4918 §10.4's grammar in one pass: a resource tag outside the
// lists is skipped whole, so a tag whose URL holds a parenthesis does not open
// a list; an entity tag is skipped as the quoted string it is, so brackets,
// parentheses or angle brackets in it neither end its list nor read as a token;
// and Not negates only the condition right after it.
func parseIfHeaderState(header string) ifHeaderState {
	var state ifHeaderState
	inList, negated := false, false
	for i := 0; i < len(header); {
		switch c := header[i]; {
		case c == '<':
			end := strings.IndexByte(header[i+1:], '>')
			if end < 0 {
				return state
			}
			if inList {
				if token := strings.TrimSpace(header[i+1 : i+1+end]); token != "" && !negated {
					state.add(token)
				}
				negated = false
			}
			i += end + 2
		case !inList:
			if c == '(' {
				inList, negated = true, false
			}
			i++
		case c == ')':
			inList = false
			i++
		case c == '[':
			end := entityTagConditionEnd(header, i+1)
			if end < 0 {
				return state
			}
			negated = false
			i = end + 1
		case len(header)-i >= 3 && strings.EqualFold(header[i:i+3], "Not"):
			negated = true
			i += 3
		default:
			i++
		}
	}
	return state
}

// entityTagConditionEnd returns the index of the "]" closing the entity tag
// condition whose content starts at start, reading the tag's quoted string
// whole, or -1 when the header ends first.
func entityTagConditionEnd(header string, start int) int {
	i := start
	for i < len(header) && (header[i] == ' ' || header[i] == '\t') {
		i++
	}
	if strings.HasPrefix(header[i:], "W/") {
		i += 2
	}
	if i < len(header) && header[i] == '"' {
		closing := strings.IndexByte(header[i+1:], '"')
		if closing < 0 {
			return -1
		}
		i += closing + 2
	}
	end := strings.IndexByte(header[i:], ']')
	if end < 0 {
		return -1
	}
	return i + end
}

// parseIfHeader reads r's If header once per request: a write checks several
// targets against it, and the header can carry many thousands of tokens.
func (h *DavServer) parseIfHeader(r *http.Request) *ifHeaderState {
	requestState := davRequestStateFromContext(r.Context())
	if parsed := requestState.parsedIfHeader(); parsed != nil {
		return parsed
	}
	parsed := parseIfHeaderState(r.Header.Get("If"))
	requestState.setParsedIfHeader(&parsed)
	return &parsed
}

// submitsLockToken reports whether the If header submits token.
func (state *ifHeaderState) submitsLockToken(token string) bool {
	_, ok := state.seen[token]
	return ok
}

// firstLockToken is the first token the If header submits, or "".
func (state *ifHeaderState) firstLockToken() string {
	if len(state.tokens) == 0 {
		return ""
	}
	return state.tokens[0]
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
	if refusedMemberName(w, target) {
		return
	}
	createsMember := !targetExists && target.Valid && target.Resource
	timeout := parseLockTimeout(r.Header.Get("Timeout"))
	expiresAt := time.Now().Add(time.Duration(timeout) * time.Second)

	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}
	// RFC 4918 §9.10.2 makes a LOCK without a body a refresh of the lock its If
	// header names, and one with a body a request for a new lock.
	if len(bytes.Trim(body, xmlWhitespace)) == 0 {
		h.refreshLock(w, r, user, canonicalPath, timeout, expiresAt)
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
	// Some clients resend the lockinfo body when they refresh. As a leniency
	// RFC 4918 does not ask for, a LOCK with a body that submits the
	// requester's own lock rooted at the Request-URI, and whose lockinfo asks
	// for that lock's scope, refreshes it rather than conflicting with it. The
	// scope is all the lockinfo can differ in: the type is write for every
	// lock, an owner is not something a refresh changes, and §9.10.2 has a
	// refresh ignore the Depth header. Any other LOCK with a body asks for a
	// new lock, and the tokens it submits are for locks it has to get past,
	// such as its collection's when it creates a member.
	own, err := h.ownLockRootedAt(r, user, canonicalPath)
	if err != nil {
		http.Error(w, "failed to resolve lock token", http.StatusInternalServerError)
		return
	}
	if own != nil && own.LockScope == scope {
		h.writeLockRefresh(w, r, own.Token, timeout, expiresAt)
		return
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
		if membershipPreconditions, ok = h.writeLockPreconditions(w, r, path.Dir(cleanPath)); !ok {
			return
		}
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

// refreshLock answers a LOCK without a body: RFC 4918 §9.10.2 makes it a
// refresh of the one lock its If header submits, sent to a resource within
// that lock's scope.
func (h *DavServer) refreshLock(w http.ResponseWriter, r *http.Request, user *store.User, canonicalPath string, timeout int, expiresAt time.Time) {
	token := h.parseIfHeader(r).firstLockToken()
	if token == "" {
		http.Error(w, "a LOCK without a body refreshes a lock, and its If header names no lock token", http.StatusBadRequest)
		return
	}
	existing, err := h.store.Locks.GetByToken(r.Context(), token)
	if err != nil {
		http.Error(w, "failed to resolve lock token", http.StatusInternalServerError)
		return
	}
	if existing == nil || existing.ExpiresAt.Before(time.Now()) {
		// §9.10.6 gives a refresh whose token names no lock the same
		// precondition as one out of scope, at 412.
		writeDAVError(w, http.StatusPreconditionFailed, conditionLockTokenMatchesRequestURI)
		return
	}
	if existing.UserID != user.ID {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !lockScopeIncludes(existing, canonicalPath) {
		writeDAVError(w, http.StatusPreconditionFailed, conditionLockTokenMatchesRequestURI)
		return
	}
	h.writeLockRefresh(w, r, token, timeout, expiresAt)
}

// writeLockRefresh refreshes the lock token names and answers with it, its
// lockroot still the lock's own root. A lock an UNLOCK or its expiry removed
// after it was found names no lock by the time it is refreshed, which §9.10.6
// answers as for a token out of scope.
func (h *DavServer) writeLockRefresh(w http.ResponseWriter, r *http.Request, token string, timeout int, expiresAt time.Time) {
	defer invalidateDAVRequestState(r.Context())
	refreshed, err := h.store.Locks.Refresh(r.Context(), token, timeout, expiresAt)
	if errors.Is(err, store.ErrNotFound) {
		writeDAVError(w, http.StatusPreconditionFailed, conditionLockTokenMatchesRequestURI)
		return
	}
	if err != nil {
		http.Error(w, "failed to refresh lock", http.StatusInternalServerError)
		return
	}
	writeLockResponse(w, refreshed, http.StatusOK)
}

// lockScopeIncludes reports whether the resource at canonicalPath is within
// lock's scope: the lock's root and, for a depth-infinity lock, every
// resource below it (RFC 4918 §6.1, §7.4).
func lockScopeIncludes(lock *store.Lock, canonicalPath string) bool {
	if sameLockRoot(lock.ResourcePath, canonicalPath) {
		return true
	}
	root := strings.TrimSuffix(cleanDAVPath(lock.ResourcePath), "/")
	return lock.Depth == "infinity" && strings.HasPrefix(cleanDAVPath(canonicalPath), root+"/")
}

// ownLockRootedAt returns an unexpired lock of user's rooted at canonicalPath
// whose token r's If header submits, or nil. It reads the locks at
// canonicalPath in one query however many tokens the header carries.
func (h *DavServer) ownLockRootedAt(r *http.Request, user *store.User, canonicalPath string) (*store.Lock, error) {
	locks, err := h.locksForLookupPaths(r.Context(), lockLookupPaths(canonicalPath))
	if err != nil {
		return nil, err
	}
	ifHeader := h.parseIfHeader(r)
	now := time.Now()
	for i := range locks {
		lock := &locks[i]
		if lock.UserID != user.ID || lock.ExpiresAt.Before(now) || !sameLockRoot(lock.ResourcePath, canonicalPath) {
			continue
		}
		if ifHeader.submitsLockToken(lock.Token) {
			return lock, nil
		}
	}
	return nil, nil
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
	// RFC 4918 §9.11: the Request-URI identifies a resource within the scope
	// of the lock, not necessarily its root.
	if !lockScopeIncludes(lock, canonicalPath) {
		writeDAVError(w, http.StatusConflict, conditionLockTokenMatchesRequestURI)
		return
	}

	// Another principal's lock is removed by the DAV:unlock privilege on the
	// resource the lock is on, its root, wherever in its scope the request is
	// sent.
	if lock.UserID != user.ID {
		lockRoot := cleanDAVPath(lock.ResourcePath)
		if h.store == nil || h.store.ACLEntries == nil {
			writeNeedPrivileges(w, lockRoot, "unlock")
			return
		}
		if err := h.requireACLPrivilege(r.Context(), user, requestPathForCanonical(lockRoot), "unlock"); err != nil {
			_ = writePrivilegeRequirementError(w, requirePrivilegeAt(err, lockRoot, "unlock"))
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

// resolvedLockTarget carries one write target's canonical path and the lookup
// paths whose locks could block a write to it.
type resolvedLockTarget struct {
	canonicalPath string
	lookupPaths   []string
}

// resolveLockTarget resolves resourcePath to the canonical path its locks are
// stored at. A path that cannot be resolved is an error: looking its locks up
// under the request's spelling would find none.
func (h *DavServer) resolveLockTarget(r *http.Request, resourcePath string) (resolvedLockTarget, error) {
	canonicalPath := cleanDAVPath(resourcePath)
	if user, ok := auth.UserFromContext(r.Context()); ok {
		canonical, err := h.canonicalDAVPath(r.Context(), user, canonicalPath)
		if err != nil {
			return resolvedLockTarget{}, fmt.Errorf("resolve lock target %q: %w", resourcePath, err)
		}
		if canonical != "" {
			canonicalPath = canonical
		}
	}
	return resolvedLockTarget{canonicalPath: canonicalPath, lookupPaths: lockLookupPaths(canonicalPath)}, nil
}

// resolveLockTargets resolves each distinct write target among resourcePaths
// and returns them with the union of their lookup paths.
func (h *DavServer) resolveLockTargets(r *http.Request, resourcePaths []string) ([]resolvedLockTarget, []string, error) {
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
		target, err := h.resolveLockTarget(r, resourcePath)
		if err != nil {
			return nil, nil, err
		}
		targets = append(targets, target)
		for _, p := range target.lookupPaths {
			if _, ok := seenLookup[p]; ok {
				continue
			}
			seenLookup[p] = struct{}{}
			union = append(union, p)
		}
	}
	return targets, union, nil
}

// lockAllowsWrite reports whether the write may proceed given the locks that
// could apply to the target: active locks on the resource itself and
// depth-infinity locks on its ancestors block the write unless the request's
// If header submits the token of one of them that the requester holds.
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
	targetPath := cleanDAVPath(target.canonicalPath)
	for _, lock := range locks {
		if lock.ExpiresAt.Before(now) {
			continue
		}
		// Ancestor locks only apply if they have depth-infinity.
		if cleanDAVPath(lock.ResourcePath) != targetPath && lock.Depth != "infinity" {
			continue
		}
		hasActiveLock = true
		if user != nil && lock.UserID == user.ID && ifHeader.submitsLockToken(lock.Token) {
			return true
		}
	}

	// If there are active locks but none matched the token, block the write.
	return !hasActiveLock
}

// checkLock verifies that a write operation is allowed on a resource.
// Returns true if the operation can proceed, false if the resource is locked
// and the request doesn't include a valid lock token.
func (h *DavServer) checkLock(r *http.Request, resourcePath string) (bool, error) {
	return h.checkLocks(r, resourcePath)
}

// checkLocks verifies several write targets at once, fetching the union of
// their lock lookup paths in a single query instead of one query per target.
func (h *DavServer) checkLocks(r *http.Request, resourcePaths ...string) (bool, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return true, nil
	}
	targets, union, err := h.resolveLockTargets(r, resourcePaths)
	if err != nil {
		return false, err
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

// lockPreconditions captures the lock conditions of a write to resourcePaths
// for the store to check again in the write's transaction. Every precondition
// carries the same tokens: those the If header submits that name an active
// lock the requester holds on one of the targets' lookup paths. A token naming
// no such lock could satisfy nothing the store finds, since a lock created
// later has a token the request could not have known, so the store is not
// handed the rest of a header that may carry many thousands of them.
func (h *DavServer) lockPreconditions(r *http.Request, resourcePaths ...string) ([]store.LockPrecondition, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return nil, nil
	}
	targets, union, err := h.resolveLockTargets(r, resourcePaths)
	if err != nil || len(targets) == 0 {
		return nil, err
	}
	tokens, err := h.submittedOwnLockTokens(r, union)
	if err != nil {
		return nil, err
	}
	preconditions := make([]store.LockPrecondition, 0, len(targets))
	for _, target := range targets {
		preconditions = append(preconditions, store.LockPrecondition{
			ResourcePath: target.canonicalPath,
			LookupPaths:  target.lookupPaths,
			Tokens:       tokens,
		})
	}
	return preconditions, nil
}

// submittedOwnLockTokens lists the tokens of the active locks at lookupPaths
// that the requester holds and r's If header submits.
func (h *DavServer) submittedOwnLockTokens(r *http.Request, lookupPaths []string) ([]string, error) {
	user, ok := auth.UserFromContext(r.Context())
	ifHeader := h.parseIfHeader(r)
	if !ok || len(ifHeader.tokens) == 0 {
		return nil, nil
	}
	locks, err := h.locksForLookupPaths(r.Context(), lookupPaths)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	seen := make(map[string]struct{})
	var tokens []string
	for i := range locks {
		lock := &locks[i]
		if lock.UserID != user.ID || lock.ExpiresAt.Before(now) || !ifHeader.submitsLockToken(lock.Token) {
			continue
		}
		if _, ok := seen[lock.Token]; ok {
			continue
		}
		seen[lock.Token] = struct{}{}
		tokens = append(tokens, lock.Token)
	}
	return tokens, nil
}

// lockedPathsBelow lists, as request paths, the resources below the collection
// at the canonical collectionPath that hold an active lock, members and
// unmapped URLs alike. A request that removes or replaces the collection's
// members removes a lock-null resource with them, so it must satisfy that
// resource's lock as it does a member's (RFC 4918 §9.6.1, §9.9.2, §7.3).
func (h *DavServer) lockedPathsBelow(ctx context.Context, collectionPath string) ([]string, error) {
	if h == nil || h.store == nil || h.store.Locks == nil {
		return nil, nil
	}
	locks, err := h.store.Locks.ListByResourcePrefix(ctx, strings.TrimSuffix(collectionPath, "/")+"/")
	if err != nil {
		return nil, err
	}
	now := time.Now()
	paths := make([]string, 0, len(locks))
	for i := range locks {
		if !locks[i].ExpiresAt.Before(now) {
			paths = append(paths, requestPathForCanonical(locks[i].ResourcePath))
		}
	}
	return paths, nil
}

// writeLockStateError answers a lock check that could not be made: 404 or 409
// for a target whose collection is missing or ambiguous, as LOCK answers such
// a path, and 500 for any other failure.
func writeLockStateError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, errAmbiguousAddressBook), errors.Is(err, errAmbiguousCalendar):
		http.Error(w, "ambiguous path", http.StatusConflict)
	default:
		http.Error(w, "failed to verify lock state", http.StatusInternalServerError)
	}
}

// writeLockPreconditions is lockPreconditions for a handler: it answers the
// failure and reports false when the preconditions cannot be captured.
func (h *DavServer) writeLockPreconditions(w http.ResponseWriter, r *http.Request, resourcePaths ...string) ([]store.LockPrecondition, bool) {
	preconditions, err := h.lockPreconditions(r, resourcePaths...)
	if err != nil {
		writeLockStateError(w, err)
		return nil, false
	}
	return preconditions, true
}

func (h *DavServer) requireLock(w http.ResponseWriter, r *http.Request, resourcePath, lockedMessage string) bool {
	allowed, err := h.checkLock(r, resourcePath)
	if err != nil {
		writeLockStateError(w, err)
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
		writeLockStateError(w, err)
		return false
	}
	if !allowed {
		http.Error(w, lockedMessage, http.StatusLocked)
		return false
	}
	return true
}

// sameLockRoot reports whether a lock stored at lockPath is rooted at the
// canonical requestPath: whether both are one resource's state path.
func sameLockRoot(lockPath, requestPath string) bool {
	return cleanDAVPath(lockPath) == cleanDAVPath(requestPath)
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
	for _, ancestor := range ancestorPaths(canonicalPath) {
		addPath(ancestor)
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

// publicDAVLockRoot is the path a client names the resource a lock stored at
// resourcePath is on by: a pending collection's public path, a member's state
// path with the extension a request strips, and any other path as it is.
func publicDAVLockRoot(resourcePath string) string {
	canonicalPath := cleanDAVPath(resourcePath)
	if publicPath, ok := publicPendingCollectionPath(canonicalPath); ok {
		return publicPath
	}
	return requestPathForCanonical(canonicalPath)
}

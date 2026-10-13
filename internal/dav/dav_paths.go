package dav

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/jw6ventures/calcard/internal/store"
)

var errPrivilegeNotGranted = fmt.Errorf("privilege not granted: %w", store.ErrNotFound)

// Collection URL prefixes for the default DAV modules. Used by the shared
// path/ACL helpers below so calendar and address-book code stays in lockstep.
const (
	calendarPrefix    = "/dav/calendars"
	addressBookPrefix = "/dav/addressbooks"
)

// collectionPathForPrefix reduces a resource path to its owning collection path
// (e.g. "/dav/calendars/12/foo.ics" -> "/dav/calendars/12"). Paths outside the
// prefix are returned normalized but otherwise unchanged.
func collectionPathForPrefix(cleanPath, prefix string) string {
	cleanPath = cleanDAVPath(cleanPath)
	if !strings.HasPrefix(cleanPath, prefix+"/") {
		return cleanPath
	}
	trimmed := strings.TrimPrefix(cleanPath, prefix+"/")
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return prefix
	}
	return path.Join(prefix, parts[0])
}

// collectionResourcePath builds the canonical path of a collection by ID.
func collectionResourcePath(prefix string, id int64) string {
	return path.Join(prefix, fmt.Sprint(id))
}

// objectRequestPath is the decoded path of the href naming the object
// resourceName in collection id. Privilege checks canonicalize the path they
// are given, which strips one extension from its last segment, so they must be
// given this path rather than the object's canonical path: from that one they
// would strip the object's own extension, as from "uid@example.com", and
// decide for another object.
func objectRequestPath(prefix string, id int64, resourceName, ext string) string {
	return collectionResourcePath(prefix, id) + "/" + resourceName + ext
}

// requestPathForCanonical is the decoded path a request names the resource at
// canonicalPath by, so that canonicalizing it gives canonicalPath back: an
// object's path with its extension, and any other path unchanged. An object is
// any member of a calendar or address book, a name such as ".hidden" with no
// stem before its dot included.
func requestPathForCanonical(canonicalPath string) string {
	for _, candidate := range []struct{ prefix, ext string }{{calendarPrefix, ".ics"}, {addressBookPrefix, ".vcf"}} {
		rest, ok := strings.CutPrefix(canonicalPath, candidate.prefix+"/")
		if !ok {
			continue
		}
		if collection, name, member := strings.Cut(rest, "/"); member && collection != "" && name != "" && !strings.Contains(name, "/") {
			return canonicalPath + candidate.ext
		}
	}
	return canonicalPath
}

// objectACLPaths lists, in decision order, the stored spellings whose ACL
// entries make up the ACL of the member resourceName of the canonical
// collectionPath. The store's ACL writes replace exactly these.
func objectACLPaths(collectionPath, resourceName string) []string {
	return store.ObjectACLPaths(collectionPath, resourceName)
}

// orderACLEntries puts entries read from the spellings paths of one
// resource's ACL in the order a decision reads them: by the spelling's place
// in paths, so the resource's own path comes first, then by each spelling's
// ACE order. The order then depends on what the spellings hold, not on the
// order their rows were saved in.
func orderACLEntries(entries []store.ACLEntry, paths []string) {
	slices.SortStableFunc(entries, func(a, b store.ACLEntry) int {
		return cmp.Or(
			cmp.Compare(spellingRank(paths, a.ResourcePath), spellingRank(paths, b.ResourcePath)),
			cmp.Compare(a.Position, b.Position),
			cmp.Compare(a.ID, b.ID),
		)
	})
}

func spellingRank(paths []string, resourcePath string) int {
	if rank := slices.Index(paths, resourcePath); rank >= 0 {
		return rank
	}
	return len(paths)
}

// requirePrivilegeDecision turns the (allowed, denied) result of a privilege
// decision into the standard error contract shared by the require*Privilege
// helpers: nil when allowed, errForbidden when explicitly denied, and
// errPrivilegeNotGranted otherwise. That sentinel still matches
// store.ErrNotFound so existing privacy-preserving callers fail closed, while
// method handlers can distinguish an absent resource from a missing privilege.
func requirePrivilegeDecision(allowed, denied bool, err error) error {
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	if denied {
		return errForbidden
	}
	return errPrivilegeNotGranted
}

func isPrivilegeNotGranted(err error) bool {
	return errors.Is(err, errPrivilegeNotGranted)
}

// isPrivilegeRefusal reports whether err refuses a privilege, as opposed to
// failing to resolve the resource or to evaluate the privilege.
func isPrivilegeRefusal(err error) bool {
	return errors.Is(err, errForbidden) || isPrivilegeNotGranted(err)
}

// existingObjectWriteRefusal returns the answer to a request refused with
// refusal on the existing object at cleanPath. A principal that may not read
// the object is given answerAsAbsent's answer, the one the same request gets
// when no object is at cleanPath, so the refusal does not disclose that the
// object exists. answerAsAbsent returns nil when that request would succeed;
// then, as when the principal may read the object, its existence is no secret
// from the principal, and refusal stands. So does a refusal that already
// answers as for a missing resource.
func (h *DavServer) existingObjectWriteRefusal(ctx context.Context, user *store.User, cleanPath string, refusal error, answerAsAbsent func() error) error {
	if !refusalNamesPrivilege(refusal) {
		return refusal
	}
	readable, err := h.canReadObjectAt(ctx, user, cleanPath)
	if err != nil {
		return requirePrivilegeAt(err, cleanPath, "read")
	}
	if readable {
		return refusal
	}
	if absent := answerAsAbsent(); absent != nil {
		return absent
	}
	return refusal
}

// canReadObjectAt reports whether user may read the calendar or address object
// resource cleanPath names, as GET decides it.
func (h *DavServer) canReadObjectAt(ctx context.Context, user *store.User, cleanPath string) (bool, error) {
	err := h.requireObjectRead(ctx, user, cleanPath)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, store.ErrNotFound), errors.Is(err, errForbidden),
		errors.Is(err, errAmbiguousCalendar), errors.Is(err, errAmbiguousAddressBook):
		return false, nil
	default:
		return false, err
	}
}

func (h *DavServer) requireObjectRead(ctx context.Context, user *store.User, cleanPath string) error {
	if calendarID, resourceName, matched, err := h.parseCalendarResourcePath(ctx, user, cleanPath); err != nil {
		return err
	} else if matched {
		_, err := h.loadCalendarForObjectRead(ctx, user, calendarID, resourceName, cleanPath)
		return err
	}
	if addressBookID, _, matched, err := h.parseAddressBookResourcePath(ctx, user, cleanPath); err != nil {
		return err
	} else if matched {
		_, err := h.loadAddressBookWithPrivilege(ctx, user, addressBookID, cleanPath, "read")
		return err
	}
	return store.ErrNotFound
}

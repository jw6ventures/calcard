package dav

import (
	"context"

	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) rebindCollectionLocks(ctx context.Context, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	if h == nil || h.store == nil || h.store.Locks == nil {
		return nil
	}
	markLockBatchIndexStale(ctx)
	return h.store.Locks.MoveResourcePath(ctx, fromPath, toPath)
}

// moveStatePaths resolves the canonical DAV state paths a MOVE rebinds
// lock/ACL state between; the store performs the rebind inside the move
// transaction.
func (h *DavServer) moveStatePaths(ctx context.Context, user *store.User, fromPath, toPath string) (string, string, error) {
	fromCanonical, err := h.canonicalDAVPath(ctx, user, fromPath)
	if err != nil {
		return "", "", err
	}
	toCanonical, err := h.canonicalDAVPath(ctx, user, toPath)
	if err != nil {
		return "", "", err
	}
	return fromCanonical, toCanonical, nil
}

func (h *DavServer) deleteDAVACLState(ctx context.Context, user *store.User, resourcePath string) error {
	canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath)
	if err != nil {
		return err
	}
	if canonicalPath == "" || h == nil || h.store == nil || h.store.ACLEntries == nil {
		return nil
	}
	defer invalidateACLEntryCache(ctx)
	for _, statePath := range davStatePaths(canonicalPath) {
		if err := h.store.ACLEntries.Delete(ctx, statePath); err != nil {
			return err
		}
	}
	return nil
}

func (h *DavServer) deleteDAVResourceState(ctx context.Context, user *store.User, resourcePath string) error {
	canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath)
	if err != nil {
		return err
	}
	if canonicalPath == "" {
		return nil
	}
	if h == nil || h.store == nil {
		return nil
	}
	defer invalidateACLEntryCache(ctx)
	if h.store.Locks != nil {
		markLockBatchIndexStale(ctx)
	}
	for _, statePath := range davStatePaths(canonicalPath) {
		if h.store.Locks != nil {
			if err := h.store.Locks.DeleteByResourcePath(ctx, statePath); err != nil {
				return err
			}
		}
		if h.store.ACLEntries != nil {
			if err := h.store.ACLEntries.Delete(ctx, statePath); err != nil {
				return err
			}
		}
	}
	return nil
}

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
	defer invalidateDAVRequestState(ctx)
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

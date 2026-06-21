package dav

import (
	"context"
	"fmt"

	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) rebindCollectionLocks(ctx context.Context, fromPath, toPath string) error {
	if fromPath == "" || toPath == "" || fromPath == toPath {
		return nil
	}
	if h == nil || h.store == nil || h.store.Locks == nil {
		return nil
	}
	return h.store.Locks.MoveResourcePath(ctx, fromPath, toPath)
}

func (h *DavServer) moveDAVResourceState(ctx context.Context, user *store.User, fromPath, toPath string) error {
	fromCanonical, err := h.canonicalDAVPath(ctx, user, fromPath)
	if err != nil {
		return err
	}
	toCanonical, err := h.canonicalDAVPath(ctx, user, toPath)
	if err != nil {
		return err
	}
	if fromCanonical == "" || toCanonical == "" || fromCanonical == toCanonical {
		return nil
	}
	if h == nil || h.store == nil {
		return nil
	}
	movedACL := false
	if h.store.ACLEntries != nil {
		if err := h.store.ACLEntries.MoveResourcePath(ctx, fromCanonical, toCanonical); err != nil {
			return err
		}
		movedACL = true
	}
	if h.store.Locks != nil {
		if err := h.store.Locks.MoveResourcePath(ctx, fromCanonical, toCanonical); err != nil {
			if movedACL && h.store.ACLEntries != nil {
				if rollbackErr := h.store.ACLEntries.MoveResourcePath(ctx, toCanonical, fromCanonical); rollbackErr != nil {
					return fmt.Errorf("move locks: %w (acl rollback failed: %v)", err, rollbackErr)
				}
			}
			return err
		}
	}
	return nil
}

func (h *DavServer) rebindMovedDAVResourceState(ctx context.Context, user *store.User, fromPath, toPath string, overwrite bool) error {
	return h.moveDAVResourceState(ctx, user, fromPath, toPath)
}

func (h *DavServer) deleteDAVACLState(ctx context.Context, user *store.User, resourcePath string) error {
	canonicalPath, err := h.canonicalDAVPath(ctx, user, resourcePath)
	if err != nil {
		return err
	}
	if canonicalPath == "" || h == nil || h.store == nil || h.store.ACLEntries == nil {
		return nil
	}
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

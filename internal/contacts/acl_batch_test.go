package contacts

import (
	"context"

	"github.com/jw6ventures/calcard/internal/store"
)

func (f *fakeACL) ListByResources(_ context.Context, paths []string) ([]store.ACLEntry, error) {
	return filterTestACLEntries(f.entries, paths, nil), nil
}

func (f *fakeACL) ListByResourcesAndPrincipals(_ context.Context, paths, principals []string) ([]store.ACLEntry, error) {
	return filterTestACLEntries(f.entries, paths, principals), nil
}

func filterTestACLEntries(entries []store.ACLEntry, paths, principals []string) []store.ACLEntry {
	pathSet := make(map[string]struct{}, len(paths))
	for _, value := range paths {
		pathSet[value] = struct{}{}
	}
	principalSet := make(map[string]struct{}, len(principals))
	for _, value := range principals {
		principalSet[value] = struct{}{}
	}
	var result []store.ACLEntry
	for _, entry := range entries {
		if _, ok := pathSet[entry.ResourcePath]; !ok {
			continue
		}
		if len(principalSet) != 0 {
			if _, ok := principalSet[entry.PrincipalHref]; !ok {
				continue
			}
		}
		result = append(result, entry)
	}
	return result
}

func (f *fakeAB) ListAccessible(ctx context.Context, userID int64) ([]store.AddressBook, error) {
	return f.ListByUser(ctx, userID)
}

func (f *fakeContacts) ListForBookPageAfter(ctx context.Context, bookID, afterID int64, limit int) ([]store.Contact, error) {
	contacts, err := f.ListForBook(ctx, bookID)
	if err != nil || len(contacts) <= limit {
		return contacts, err
	}
	return contacts[:limit], nil
}

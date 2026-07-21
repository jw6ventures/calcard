package api

import (
	"context"

	"github.com/jw6ventures/calcard/internal/store"
)

func (f *fakeACLRepo) ListByResources(_ context.Context, paths []string) ([]store.ACLEntry, error) {
	return filterBatchEntries(f.entries, paths, nil), nil
}

func (f *fakeACLRepo) ListByResourcesAndPrincipals(_ context.Context, paths, principals []string) ([]store.ACLEntry, error) {
	return filterBatchEntries(f.entries, paths, principals), nil
}

func filterBatchEntries(entries []store.ACLEntry, paths, principals []string) []store.ACLEntry {
	pathSet := make(map[string]bool, len(paths))
	for _, value := range paths {
		pathSet[value] = true
	}
	principalSet := make(map[string]bool, len(principals))
	for _, value := range principals {
		principalSet[value] = true
	}
	var result []store.ACLEntry
	for _, entry := range entries {
		if !pathSet[entry.ResourcePath] || (len(principalSet) != 0 && !principalSet[entry.PrincipalHref]) {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func (f *fakeAddressBookRepo) ListAccessible(ctx context.Context, userID int64) ([]store.AddressBook, error) {
	return f.ListByUser(ctx, userID)
}

func (f *fakeContactRepo) ListForBookPageAfter(ctx context.Context, bookID, afterID int64, limit int) ([]store.Contact, error) {
	contacts, err := f.ListForBook(ctx, bookID)
	if err != nil || len(contacts) <= limit {
		return contacts, err
	}
	return contacts[:limit], nil
}

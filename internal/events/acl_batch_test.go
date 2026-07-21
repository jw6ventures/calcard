package events

import (
	"context"

	"github.com/jw6ventures/calcard/internal/store"
)

func (f *fakeACLRepo) ListByResources(_ context.Context, paths []string) ([]store.ACLEntry, error) {
	return eventTestBatchEntries(f.entries, paths, nil), nil
}

func (f *fakeACLRepo) ListByResourcesAndPrincipals(_ context.Context, paths, principals []string) ([]store.ACLEntry, error) {
	return eventTestBatchEntries(f.entries, paths, principals), nil
}

func eventTestBatchEntries(entries []store.ACLEntry, paths, principals []string) []store.ACLEntry {
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

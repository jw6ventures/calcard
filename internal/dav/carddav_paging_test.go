package dav

import (
	"context"
	"fmt"
	"testing"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func pagedContact(id int64, name string) *store.Contact {
	uid := fmt.Sprintf("contact-%03d", id)
	return &store.Contact{
		ID:            id,
		AddressBookID: 5,
		UID:           uid,
		ResourceName:  uid,
		RawVCard:      buildVCard("3.0", "UID:"+uid, "FN:"+name),
		ETag:          "etag-" + uid,
	}
}

func TestAddressBookQueryStopsAfterClientLimitLookahead(t *testing.T) {
	repo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	for id := int64(1); id <= 600; id++ {
		contact := pagedContact(id, "Visible")
		repo.contacts[repo.key(5, contact.UID)] = contact
	}
	h := &DavServer{store: &store.Store{Contacts: repo}}
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}

	responses, err := h.addressBookQuery(context.Background(), &store.User{ID: 1}, book, "/dav/addressbooks/5/", nil, nil, nil, &addressbookLimit{NResults: 1})
	if err != nil {
		t.Fatalf("addressBookQuery() error = %v", err)
	}
	if repo.pageLookupCount != 1 {
		t.Fatalf("page lookups = %d, want 1", repo.pageLookupCount)
	}
	if len(responses) != 2 || responses[1].Error == nil || responses[1].Error.NumberOfMatchesWithinLimits == nil {
		t.Fatalf("limited responses = %#v", responses)
	}
}

func TestAddressBookQueryAppliesAuthoritativeFilterAcrossPages(t *testing.T) {
	repo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	for id := int64(1); id <= 300; id++ {
		name := "No Match"
		if id == 270 {
			name = "Target Person"
		}
		contact := pagedContact(id, name)
		repo.contacts[repo.key(5, contact.UID)] = contact
	}
	h := &DavServer{store: &store.Store{Contacts: repo}}
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}
	filter := &cardFilter{PropFilter: []cardPropFilter{{Name: "FN", TextMatch: &textMatch{Text: "Target Person", MatchType: "equals"}}}}

	responses, err := h.addressBookQuery(context.Background(), &store.User{ID: 1}, book, "/dav/addressbooks/5/", filter, nil, nil, nil)
	if err != nil {
		t.Fatalf("addressBookQuery() error = %v", err)
	}
	if repo.pageLookupCount != 2 {
		t.Fatalf("page lookups = %d, want 2", repo.pageLookupCount)
	}
	if len(responses) != 1 || responses[0].Href != "/dav/addressbooks/5/contact-270.vcf" {
		t.Fatalf("filtered responses = %#v", responses)
	}
}

func TestAddressBookQuerySkipsDeniedMatchesAcrossPages(t *testing.T) {
	repo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	for id := int64(1); id <= 300; id++ {
		name := "No Match"
		if id == 250 || id == 270 {
			name = "Target Person"
		}
		contact := pagedContact(id, name)
		repo.contacts[repo.key(5, contact.UID)] = contact
	}
	acls := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/addressbooks/5", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/addressbooks/5/contact-250", PrincipalHref: "/dav/principals/1/", IsGrant: false, Privilege: "read"},
	}}
	h := &DavServer{store: &store.Store{Contacts: repo, ACLEntries: acls}}
	book := &store.AddressBook{ID: 5, UserID: 9, Name: "Shared Contacts"}
	filter := &cardFilter{PropFilter: []cardPropFilter{{Name: "FN", TextMatch: &textMatch{Text: "Target Person", MatchType: "equals"}}}}

	responses, err := h.addressBookQuery(context.Background(), &store.User{ID: 1}, book, "/dav/addressbooks/5/", filter, nil, nil, nil)
	if err != nil {
		t.Fatalf("addressBookQuery() error = %v", err)
	}
	if len(responses) != 1 || responses[0].Href != "/dav/addressbooks/5/contact-270.vcf" {
		t.Fatalf("filtered responses = %#v", responses)
	}
	if repo.pageLookupCount != 2 || acls.listScopedACLCalls != 2 {
		t.Fatalf("page calls = %d, scoped ACL calls = %d; want 2 each", repo.pageLookupCount, acls.listScopedACLCalls)
	}
}

func TestAddressBookQueryHardResponseCapUsesTopLevelLimitSignal(t *testing.T) {
	repo := &fakeContactRepo{contacts: map[string]*store.Contact{}}
	for id := int64(1); id <= 3; id++ {
		contact := pagedContact(id, "Visible")
		repo.contacts[repo.key(5, contact.UID)] = contact
	}
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 1
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := &DavServer{store: &store.Store{Contacts: repo}, cfg: cfg}
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts"}

	responses, err := h.addressBookQuery(context.Background(), &store.User{ID: 1}, book, "/dav/addressbooks/5/", nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("addressBookQuery() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("hard-cap build responses = %d, want max+1 signal", len(responses))
	}
	if repo.pageLookupCount != 1 {
		t.Fatalf("page lookups = %d, want 1", repo.pageLookupCount)
	}
}

package dav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

type fakeDeadPropertyRepo struct {
	properties map[string]map[string]store.DeadProperty
	listCalls  int
}

func TestCopyMoveDeleteDeadPropertyLifecycle(t *testing.T) {
	events := &fakeEventRepo{events: map[string]*store.Event{
		"1:event": {CalendarID: 1, UID: "event", ResourceName: "event", RawICAL: "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", ETag: "source"},
		"2:old":   {CalendarID: 2, UID: "old", ResourceName: "copied", ETag: "destination"},
	}}
	dead := &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/calendars/1/event": {
			"urn:test\x00note": {ResourcePath: "/dav/calendars/1/event", NamespaceURI: "urn:test", LocalName: "note", InnerXML: "source"},
		},
		"/dav/calendars/2/copied": {
			"urn:test\x00note": {ResourcePath: "/dav/calendars/2/copied", NamespaceURI: "urn:test", LocalName: "note", InnerXML: "destination"},
		},
	}}
	locks := &fakeLockRepo{locks: map[string]*store.Lock{
		"destination": {Token: "destination", ResourcePath: "/dav/calendars/2/copied", ExpiresAt: time.Now().Add(time.Hour)},
		"legacy":      {Token: "legacy", ResourcePath: "/dav/calendars/2/copied.ics", ExpiresAt: time.Now().Add(time.Hour)},
	}}
	acls := &fakeACLRepo{entries: []store.ACLEntry{
		{ResourcePath: "/dav/calendars/1/event", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2/copied", PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "read"},
		{ResourcePath: "/dav/calendars/2/copied.ics", PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "write"},
	}}
	st := &store.Store{Events: events, Locks: locks, ACLEntries: acls, DeadProperties: dead}

	_, err := st.CopyEventAndState(context.Background(), 1, 2, "event", "copied", "new-etag", "/dav/calendars/1/event", "/dav/calendars/2/copied", "old")
	if err != nil {
		t.Fatalf("CopyEventAndState() error = %v", err)
	}
	if got := dead.properties["/dav/calendars/2/copied"]["urn:test\x00note"].InnerXML; got != "source" {
		t.Fatalf("copied dead property = %q, want source", got)
	}
	if len(dead.properties["/dav/calendars/1/event"]) != 1 {
		t.Fatalf("COPY changed source dead properties: %#v", dead.properties)
	}
	if len(locks.locks) != 0 {
		t.Fatalf("COPY retained destination locks: %#v", locks.locks)
	}
	for _, entry := range acls.entries {
		if entry.ResourcePath == "/dav/calendars/2/copied" || entry.ResourcePath == "/dav/calendars/2/copied.ics" {
			t.Fatalf("COPY retained destination ACL: %#v", acls.entries)
		}
	}
	dead.properties["/dav/calendars/2/copied.ics"] = map[string]store.DeadProperty{
		"urn:test\x00legacy": {ResourcePath: "/dav/calendars/2/copied.ics", NamespaceURI: "urn:test", LocalName: "legacy", InnerXML: "legacy-source"},
	}
	dead.properties["/dav/calendars/3/moved.ics"] = map[string]store.DeadProperty{
		"urn:test\x00old": {ResourcePath: "/dav/calendars/3/moved.ics", NamespaceURI: "urn:test", LocalName: "old", InnerXML: "old-destination"},
	}
	locks.locks["source-canonical"] = &store.Lock{Token: "source-canonical", ResourcePath: "/dav/calendars/2/copied", ExpiresAt: time.Now().Add(time.Hour)}
	locks.locks["source-legacy"] = &store.Lock{Token: "source-legacy", ResourcePath: "/dav/calendars/2/copied.ics", ExpiresAt: time.Now().Add(time.Hour)}
	locks.locks["old-destination"] = &store.Lock{Token: "old-destination", ResourcePath: "/dav/calendars/3/moved.ics", ExpiresAt: time.Now().Add(time.Hour)}
	acls.entries = append(acls.entries,
		store.ACLEntry{ResourcePath: "/dav/calendars/2/copied", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read"},
		store.ACLEntry{ResourcePath: "/dav/calendars/2/copied.ics", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "write"},
		store.ACLEntry{ResourcePath: "/dav/calendars/3/moved.ics", PrincipalHref: "/dav/principals/3/", IsGrant: true, Privilege: "read"},
	)

	if err := st.MoveEventAndState(context.Background(), 2, 3, "event", "moved", "/dav/calendars/2/copied", "/dav/calendars/3/moved", ""); err != nil {
		t.Fatalf("MoveEventAndState() error = %v", err)
	}
	if len(dead.properties["/dav/calendars/2/copied"]) != 0 || dead.properties["/dav/calendars/3/moved"]["urn:test\x00note"].InnerXML != "source" {
		t.Fatalf("MOVE dead properties = %#v", dead.properties)
	}
	if len(dead.properties["/dav/calendars/2/copied.ics"]) != 0 || dead.properties["/dav/calendars/3/moved.ics"]["urn:test\x00legacy"].InnerXML != "legacy-source" {
		t.Fatalf("MOVE legacy dead properties = %#v", dead.properties)
	}
	if _, ok := locks.locks["old-destination"]; ok || locks.locks["source-canonical"].ResourcePath != "/dav/calendars/3/moved" || locks.locks["source-legacy"].ResourcePath != "/dav/calendars/3/moved.ics" {
		t.Fatalf("MOVE lock state = %#v", locks.locks)
	}
	for _, entry := range acls.entries {
		if entry.ResourcePath == "/dav/calendars/2/copied" || entry.ResourcePath == "/dav/calendars/2/copied.ics" || entry.PrincipalHref == "/dav/principals/3/" {
			t.Fatalf("MOVE ACL state = %#v", acls.entries)
		}
	}

	if err := st.DeleteEventAndState(context.Background(), 3, "event", "/dav/calendars/3/moved"); err != nil {
		t.Fatalf("DeleteEventAndState() error = %v", err)
	}
	if len(dead.properties["/dav/calendars/3/moved"]) != 0 {
		t.Fatalf("DELETE retained dead properties: %#v", dead.properties)
	}
}

func TestReportDecorationBatchesLockACLAndDeadPropertyQueries(t *testing.T) {
	user := &store.User{ID: 1}
	dead := &fakeDeadPropertyRepo{}
	locks := &fakeLockRepo{}
	acls := &fakeACLRepo{}
	h := &DavServer{store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 1, Name: "Work"}}},
		Locks:          locks,
		ACLEntries:     acls,
		DeadProperties: dead,
	}}
	events := []store.Event{
		{CalendarID: 1, UID: "one", ResourceName: "one", ETag: "one"},
		{CalendarID: 1, UID: "two", ResourceName: "two", ETag: "two"},
	}
	requested := &reportProp{propertySelection: propertySelection{LockDiscovery: &struct{}{}, ACLProp: &struct{}{}}}
	ctx := withDAVRequestState(auth.WithUser(context.Background(), user))

	responses, err := h.calendarResourceReportResponses(ctx, user, "/dav/calendars/1/", events, requested, nil)
	if err != nil {
		t.Fatalf("calendarResourceReportResponses() error = %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses = %#v", responses)
	}
	if locks.listByResourcesCalls != 1 || acls.listByResourcesCalls != 1 || dead.listCalls != 1 {
		t.Fatalf("batch calls: locks=%d ACLs=%d dead=%d", locks.listByResourcesCalls, acls.listByResourcesCalls, dead.listCalls)
	}
}

func TestPropnameSkipsValueOnlyLockAndACLQueries(t *testing.T) {
	user := &store.User{ID: 1}
	dead := &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/calendars/1": {
			"urn:test\x00note": {ResourcePath: "/dav/calendars/1", NamespaceURI: "urn:test", LocalName: "note", InnerXML: "value"},
		},
	}}
	locks := &fakeLockRepo{}
	acls := &fakeACLRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{accessible: []store.CalendarAccess{{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work"}, Editor: true}}},
		Events:         &fakeEventRepo{},
		Locks:          locks,
		ACLEntries:     acls,
		DeadProperties: dead,
	}})
	request := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(`<D:propfind xmlns:D="DAV:"><D:propname/></D:propfind>`))
	request.Header.Set("Depth", "0")
	request = request.WithContext(auth.WithUser(request.Context(), user))
	response := httptest.NewRecorder()

	h.ServeHTTP(response, request)

	if response.Code != http.StatusMultiStatus || !strings.Contains(response.Body.String(), "note") {
		t.Fatalf("PROPFIND propname = %d: %s", response.Code, response.Body.String())
	}
	for _, unwanted := range []string{"<cal:calendar-data", "<card:address-data"} {
		if strings.Contains(response.Body.String(), unwanted) {
			t.Fatalf("PROPFIND propname advertised REPORT-only %s: %s", unwanted, response.Body.String())
		}
	}
	if locks.listByResourcesCalls != 0 || acls.listByResourcesCalls != 0 || acls.listByResourceCalls != 0 {
		t.Fatalf("propname ran value-only queries: locks=%d ACL batches=%d ACL singles=%d", locks.listByResourcesCalls, acls.listByResourcesCalls, acls.listByResourceCalls)
	}
	if dead.listCalls != 1 {
		t.Fatalf("dead-property name batch calls = %d, want 1", dead.listCalls)
	}
}

func TestPropfindAddressDataIsAlwaysNotFoundWithoutConversionChecks(t *testing.T) {
	user := &store.User{ID: 1}
	h := NewDavServer(Options{Store: &store.Store{
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{5: {ID: 5, UserID: 1, Name: "Contacts"}}},
		Contacts: &fakeContactRepo{contacts: map[string]*store.Contact{
			"5:alice": {AddressBookID: 5, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice")},
		}},
	}})
	body := `<D:propfind xmlns:D="DAV:" xmlns:A="urn:ietf:params:xml:ns:carddav"><D:prop><A:address-data content-type="application/unsupported" version="99.0"/></D:prop></D:propfind>`
	request := httptest.NewRequest("PROPFIND", "/dav/addressbooks/5/alice.vcf", strings.NewReader(body))
	request.Header.Set("Depth", "0")
	request = request.WithContext(auth.WithUser(request.Context(), user))
	response := httptest.NewRecorder()

	h.ServeHTTP(response, request)

	if response.Code != http.StatusMultiStatus || !strings.Contains(response.Body.String(), httpStatusNotFound) {
		t.Fatalf("PROPFIND address-data = %d, want 207/404 propstat: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "supported-address-data-conversion") || strings.Contains(response.Body.String(), "BEGIN:VCARD") {
		t.Fatalf("PROPFIND processed REPORT-only address-data: %s", response.Body.String())
	}
}

func (f *fakeDeadPropertyRepo) ListByResources(_ context.Context, paths []string) ([]store.DeadProperty, error) {
	f.listCalls++
	var result []store.DeadProperty
	for _, resourcePath := range paths {
		for _, property := range f.properties[resourcePath] {
			result = append(result, property)
		}
	}
	return result, nil
}

func (f *fakeDeadPropertyRepo) Apply(_ context.Context, resourcePath string, mutations []store.DeadPropertyMutation) error {
	if f.properties == nil {
		f.properties = make(map[string]map[string]store.DeadProperty)
	}
	if f.properties[resourcePath] == nil {
		f.properties[resourcePath] = make(map[string]store.DeadProperty)
	}
	for _, mutation := range mutations {
		key := mutation.NamespaceURI + "\x00" + mutation.LocalName
		if mutation.Remove {
			delete(f.properties[resourcePath], key)
			continue
		}
		f.properties[resourcePath][key] = store.DeadProperty{
			ResourcePath: resourcePath,
			NamespaceURI: mutation.NamespaceURI,
			LocalName:    mutation.LocalName,
			InnerXML:     mutation.InnerXML,
		}
	}
	return nil
}

func TestProppatchPersistsNamespacedDeadPropertyAndPropfindReturnsIt(t *testing.T) {
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work"}
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
		Events:         &fakeEventRepo{events: map[string]*store.Event{}},
		DeadProperties: dead,
	}})
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}

	patchBody := `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:example:custom"><D:set><D:prop><X:meta><X:child>value</X:child></X:meta></D:prop></D:set></D:propertyupdate>`
	patchRequest := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(patchBody))
	patchRequest = patchRequest.WithContext(auth.WithUser(patchRequest.Context(), user))
	patchResponse := httptest.NewRecorder()
	h.ServeHTTP(patchResponse, patchRequest)
	if patchResponse.Code != http.StatusMultiStatus {
		t.Fatalf("PROPPATCH status = %d: %s", patchResponse.Code, patchResponse.Body.String())
	}

	findBody := `<D:propfind xmlns:D="DAV:" xmlns:X="urn:example:custom"><D:prop><X:meta/></D:prop></D:propfind>`
	findRequest := httptest.NewRequest("PROPFIND", "/dav/calendars/1", strings.NewReader(findBody))
	findRequest.Header.Set("Depth", "0")
	findRequest = findRequest.WithContext(auth.WithUser(findRequest.Context(), user))
	findResponse := httptest.NewRecorder()
	h.ServeHTTP(findResponse, findRequest)
	if findResponse.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d: %s", findResponse.Code, findResponse.Body.String())
	}
	responseBody := findResponse.Body.String()
	for _, want := range []string{"urn:example:custom", "meta", "child", "value", "200 OK"} {
		if !strings.Contains(responseBody, want) {
			t.Fatalf("PROPFIND response missing %q: %s", want, responseBody)
		}
	}

	allpropRequest := httptest.NewRequest("PROPFIND", "/dav/calendars/1", strings.NewReader(`<D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`))
	allpropRequest.Header.Set("Depth", "0")
	allpropRequest = allpropRequest.WithContext(auth.WithUser(allpropRequest.Context(), user))
	allpropResponse := httptest.NewRecorder()
	h.ServeHTTP(allpropResponse, allpropRequest)
	if allpropResponse.Code != http.StatusMultiStatus || !strings.Contains(allpropResponse.Body.String(), "<meta") || !strings.Contains(allpropResponse.Body.String(), "value") {
		t.Fatalf("PROPFIND allprop omitted dead property: %d: %s", allpropResponse.Code, allpropResponse.Body.String())
	}
}

func TestCalendarReportsResolveDeadPropertiesLikePropfind(t *testing.T) {
	user := &store.User{ID: 1}
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work", UpdatedAt: store.Now()}
	event := &store.Event{
		CalendarID:   1,
		UID:          "event",
		ResourceName: "event",
		RawICAL:      "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
		ETag:         "etag",
		LastModified: store.Now(),
	}
	dead := &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/calendars/1/event": {
			"urn:test\x00note": {ResourcePath: "/dav/calendars/1/event", NamespaceURI: "urn:test", LocalName: "note", InnerXML: "report-value"},
		},
	}}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
		Events:         &fakeEventRepo{events: map[string]*store.Event{"1:event": event}},
		DeadProperties: dead,
	}})

	tests := []struct {
		name string
		body string
	}{
		{
			name: "calendar query",
			body: `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:" xmlns:X="urn:test"><D:prop><X:note/></D:prop></C:calendar-query>`,
		},
		{
			name: "calendar multiget",
			body: `<C:calendar-multiget xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:" xmlns:X="urn:test"><D:prop><X:note/></D:prop><D:href>/dav/calendars/1/event.ics</D:href></C:calendar-multiget>`,
		},
		{
			name: "sync collection",
			body: `<D:sync-collection xmlns:D="DAV:" xmlns:X="urn:test"><D:sync-token/><D:prop><X:note/></D:prop></D:sync-collection>`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(tt.body))
			request = request.WithContext(auth.WithUser(request.Context(), user))
			response := httptest.NewRecorder()

			h.ServeHTTP(response, request)

			if response.Code != http.StatusMultiStatus {
				t.Fatalf("REPORT status = %d: %s", response.Code, response.Body.String())
			}
			objectResponse := davResponseForHref(t, response.Body.String(), "/dav/calendars/1/event.ics")
			for _, want := range []string{"urn:test", "note", "report-value", httpStatusOK} {
				if !strings.Contains(objectResponse, want) {
					t.Fatalf("REPORT response missing %q: %s", want, objectResponse)
				}
			}
		})
	}
}

func TestAddressBookReportsResolveDeadPropertiesLikePropfind(t *testing.T) {
	user := &store.User{ID: 1}
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: store.Now()}
	contact := &store.Contact{
		AddressBookID: 5,
		UID:           "alice",
		ResourceName:  "alice",
		RawVCard:      buildVCard("3.0", "UID:alice", "FN:Alice"),
		ETag:          "etag",
		LastModified:  store.Now(),
	}
	dead := &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/addressbooks/5/alice": {
			"urn:test\x00note": {ResourcePath: "/dav/addressbooks/5/alice", NamespaceURI: "urn:test", LocalName: "note", InnerXML: "report-value"},
		},
	}}
	h := NewDavServer(Options{Store: &store.Store{
		AddressBooks:   &fakeAddressBookRepo{books: map[int64]*store.AddressBook{5: book}},
		Contacts:       &fakeContactRepo{contacts: map[string]*store.Contact{"5:alice": contact}},
		DeadProperties: dead,
	}})

	tests := []struct {
		name  string
		depth string
		body  string
	}{
		{
			name:  "addressbook query",
			depth: "1",
			body:  `<A:addressbook-query xmlns:A="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:" xmlns:X="urn:test"><D:prop><X:note/></D:prop><A:filter/></A:addressbook-query>`,
		},
		{
			name:  "addressbook multiget",
			depth: "0",
			body:  `<A:addressbook-multiget xmlns:A="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:" xmlns:X="urn:test"><D:prop><X:note/></D:prop><D:href>/dav/addressbooks/5/alice.vcf</D:href></A:addressbook-multiget>`,
		},
		{
			name: "sync collection",
			body: `<D:sync-collection xmlns:D="DAV:" xmlns:X="urn:test"><D:sync-token/><D:prop><X:note/></D:prop></D:sync-collection>`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(tt.body))
			if tt.depth != "" {
				request.Header.Set("Depth", tt.depth)
			}
			request = request.WithContext(auth.WithUser(request.Context(), user))
			response := httptest.NewRecorder()

			h.ServeHTTP(response, request)

			if response.Code != http.StatusMultiStatus {
				t.Fatalf("REPORT status = %d: %s", response.Code, response.Body.String())
			}
			objectResponse := davResponseForHref(t, response.Body.String(), "/dav/addressbooks/5/alice.vcf")
			for _, want := range []string{"urn:test", "note", "report-value", httpStatusOK} {
				if !strings.Contains(objectResponse, want) {
					t.Fatalf("REPORT response missing %q: %s", want, objectResponse)
				}
			}
		})
	}
}

func TestObjectProppatchDoesNotMutateParentAndRollsBackDeadProperty(t *testing.T) {
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work"}
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", ResourceName: "event"},
		}},
		DeadProperties: dead,
	}})
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	body := `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:example:custom"><D:set><D:prop><X:meta>value</X:meta><D:displayname>Wrong parent</D:displayname></D:prop></D:set></D:propertyupdate>`
	request := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/event.ics", strings.NewReader(body))
	request = request.WithContext(auth.WithUser(request.Context(), user))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)

	if response.Code != http.StatusMultiStatus || !strings.Contains(response.Body.String(), "403 Forbidden") || !strings.Contains(response.Body.String(), "424 Failed Dependency") {
		t.Fatalf("PROPPATCH response = %d: %s", response.Code, response.Body.String())
	}
	if calendar.Name != "Work" {
		t.Fatalf("object PROPPATCH changed parent display name to %q", calendar.Name)
	}
	if len(dead.properties) != 0 {
		t.Fatalf("failed atomic PROPPATCH persisted dead properties: %#v", dead.properties)
	}
}

func TestProppatchProcessesRepeatedDeadPropertiesInDocumentOrder(t *testing.T) {
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work"}
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
		DeadProperties: dead,
	}})
	user := &store.User{ID: 1}
	body := `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:example:custom">
<D:set><D:prop><X:meta>first</X:meta></D:prop></D:set>
<D:remove><D:prop><X:meta/></D:prop></D:remove>
<D:set><D:prop><X:meta>last</X:meta></D:prop></D:set>
</D:propertyupdate>`
	request := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(body))
	request = request.WithContext(auth.WithUser(request.Context(), user))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("PROPPATCH status = %d: %s", response.Code, response.Body.String())
	}
	property := dead.properties["/dav/calendars/1"]["urn:example:custom\x00meta"]
	if property.InnerXML != "last" {
		t.Fatalf("final dead property value = %q, want last", property.InnerXML)
	}
}

func TestProppatchRejectsProtectedDAVPropertiesAtomically(t *testing.T) {
	for _, localName := range []string{"creationdate", "getcontentlanguage", "getcontentlength", "getlastmodified"} {
		t.Run(localName, func(t *testing.T) {
			calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work"}
			dead := &fakeDeadPropertyRepo{}
			h := NewDavServer(Options{Store: &store.Store{
				Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
				DeadProperties: dead,
			}})
			body := `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:test"><D:set><D:prop><X:note>value</X:note><D:` + localName + `>forged</D:` + localName + `></D:prop></D:set></D:propertyupdate>`
			request := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(body))
			request = request.WithContext(auth.WithUser(request.Context(), &store.User{ID: 1}))
			response := httptest.NewRecorder()

			h.ServeHTTP(response, request)

			if response.Code != http.StatusMultiStatus || !strings.Contains(response.Body.String(), "403 Forbidden") || !strings.Contains(response.Body.String(), "424 Failed Dependency") {
				t.Fatalf("PROPPATCH status = %d: %s", response.Code, response.Body.String())
			}
			if len(dead.properties) != 0 {
				t.Fatalf("protected property request persisted dead properties: %#v", dead.properties)
			}
		})
	}
}

func TestProppatchRemovesOptionalCollectionProperties(t *testing.T) {
	description := "description"
	timezone := "BEGIN:VTIMEZONE\r\nEND:VTIMEZONE\r\n"
	color := "#112233FF"
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work", Description: &description, Timezone: &timezone, Color: &color}
	book := &store.AddressBook{ID: 5, UserID: 1, Name: "Contacts", Description: &description}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:    &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{5: book}},
	}})
	user := &store.User{ID: 1}

	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "calendar",
			path: "/dav/calendars/1",
			body: `<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:I="http://apple.com/ns/ical/"><D:remove><D:prop><C:calendar-description/><C:calendar-timezone/><I:calendar-color/></D:prop></D:remove></D:propertyupdate>`,
		},
		{
			name: "address book",
			path: "/dav/addressbooks/5",
			body: `<D:propertyupdate xmlns:D="DAV:" xmlns:A="urn:ietf:params:xml:ns:carddav"><D:remove><D:prop><A:addressbook-description/></D:prop></D:remove></D:propertyupdate>`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest("PROPPATCH", tt.path, strings.NewReader(tt.body))
			request = request.WithContext(auth.WithUser(request.Context(), user))
			response := httptest.NewRecorder()

			h.ServeHTTP(response, request)

			if response.Code != http.StatusMultiStatus || !strings.Contains(response.Body.String(), httpStatusOK) {
				t.Fatalf("PROPPATCH = %d: %s", response.Code, response.Body.String())
			}
		})
	}
	if calendar.Description != nil || calendar.Timezone != nil || calendar.Color != nil {
		t.Fatalf("calendar optional properties were not removed: %#v", calendar)
	}
	if book.Description != nil {
		t.Fatalf("address-book description was not removed: %#v", book)
	}
}

func TestProppatchRejectsInvalidRootAndEmptyPropertyList(t *testing.T) {
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work"}
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
		DeadProperties: dead,
	}})
	user := &store.User{ID: 1}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "wrong root",
			body: `<D:not-propertyupdate xmlns:D="DAV:"><D:set><D:prop><X:meta xmlns:X="urn:test">value</X:meta></D:prop></D:set></D:not-propertyupdate>`,
		},
		{
			name: "empty prop",
			body: `<D:propertyupdate xmlns:D="DAV:"><D:set><D:prop/></D:set></D:propertyupdate>`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(tt.body))
			request = request.WithContext(auth.WithUser(request.Context(), user))
			response := httptest.NewRecorder()

			h.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("PROPPATCH status = %d, want 400: %s", response.Code, response.Body.String())
			}
			if len(dead.properties) != 0 {
				t.Fatalf("invalid PROPPATCH persisted dead properties: %#v", dead.properties)
			}
		})
	}
}

func TestProppatchRejectsInvalidCalendarTimezoneAtomically(t *testing.T) {
	description := "before"
	calendar := &store.Calendar{ID: 1, UserID: 1, Name: "Work", Description: &description}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: calendar}},
	}})
	body := `<D:propertyupdate xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
<D:set><D:prop><C:calendar-description>after</C:calendar-description><C:calendar-timezone>not a VTIMEZONE</C:calendar-timezone></D:prop></D:set>
</D:propertyupdate>`
	request := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(body))
	request = request.WithContext(auth.WithUser(request.Context(), &store.User{ID: 1}))
	response := httptest.NewRecorder()

	h.ServeHTTP(response, request)

	if response.Code != http.StatusMultiStatus || !strings.Contains(response.Body.String(), "409 Conflict") || !strings.Contains(response.Body.String(), "424 Failed Dependency") {
		t.Fatalf("PROPPATCH status = %d: %s", response.Code, response.Body.String())
	}
	if calendar.Description == nil || *calendar.Description != "before" || calendar.Timezone != nil {
		t.Fatalf("invalid timezone patch changed calendar: %#v", calendar)
	}
}

func TestValidCalendarTimezoneAcceptsBareAndCalendarWrappedValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{
			name: "bare VTIMEZONE",
			value: "BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n" +
				"END:VTIMEZONE\r\n",
		},
		{
			name: "VCALENDAR wrapper",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nEND:VTIMEZONE\r\nEND:VCALENDAR\r\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !validCalendarTimezone(test.value) {
				t.Fatalf("validCalendarTimezone() = false for %s", test.value)
			}
		})
	}
}

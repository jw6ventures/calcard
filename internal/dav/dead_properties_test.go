package dav

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

type fakeDeadPropertyRepo struct {
	properties map[string]map[string]store.DeadProperty
	listCalls  int
	applyErr   error
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
	if len(locks.locks) != 2 || locks.locks["destination"].ResourcePath != "/dav/calendars/2/copied" || locks.locks["legacy"].ResourcePath != "/dav/calendars/2/copied.ics" {
		t.Fatalf("COPY changed destination locks: %#v", locks.locks)
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
	if len(locks.locks) != 1 || locks.locks["old-destination"] == nil || locks.locks["old-destination"].ResourcePath != "/dav/calendars/3/moved.ics" {
		t.Fatalf("MOVE lock state = %#v", locks.locks)
	}
	for _, entry := range acls.entries {
		if entry.ResourcePath == "/dav/calendars/2/copied" || entry.ResourcePath == "/dav/calendars/2/copied.ics" || entry.PrincipalHref == "/dav/principals/3/" {
			t.Fatalf("MOVE ACL state = %#v", acls.entries)
		}
	}

	moved, err := events.GetByUID(context.Background(), 3, "event")
	if err != nil || moved == nil {
		t.Fatalf("GetByUID() = %#v, %v", moved, err)
	}
	if err := st.DeleteEventAndState(context.Background(), 3, store.EventDAVResourceState(moved), "/dav/calendars/3/moved", nil); err != nil {
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

	responses, err := h.calendarResourceReportResponses(ctx, user, "/dav/calendars/1/", events, propertySelector{Prop: requested}, calendarDataProjection{})
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
	if f.applyErr != nil {
		return f.applyErr
	}
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
	// Asserted as a resolved tree: a child carrying xmlns twice contains every
	// expected substring yet is rejected by libxml2- and expat-based clients.
	wantMeta := davElement{
		Name:     qn("urn:example:custom", "meta"),
		Children: []davElement{{Name: qn("urn:example:custom", "child"), Text: "value"}},
	}
	find := decodeMultistatus(t, findResponse).responseForHref(t, "/dav/calendars/1/")
	assertDeadPropertyTree(t, find.assertPropStatus(t, wantMeta.Name, http.StatusOK), wantMeta)

	allpropRequest := httptest.NewRequest("PROPFIND", "/dav/calendars/1", strings.NewReader(`<D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>`))
	allpropRequest.Header.Set("Depth", "0")
	allpropRequest = allpropRequest.WithContext(auth.WithUser(allpropRequest.Context(), user))
	allpropResponse := httptest.NewRecorder()
	h.ServeHTTP(allpropResponse, allpropRequest)
	allprop := decodeMultistatus(t, allpropResponse).responseForHref(t, "/dav/calendars/1/")
	assertDeadPropertyTree(t, allprop.assertPropStatus(t, wantMeta.Name, http.StatusOK), wantMeta)
}

// TestDeadPropertyRoundTripPreservesXMLStructure drives PROPPATCH then
// PROPFIND for property values whose structure is what the round trip has to
// carry: nesting, a child in a namespace other than its property's, a default
// declaration the fragment supplies itself, a child in no namespace at all, and
// attributes both plain and qualified.
//
// Every assertion here is on resolved names, so it is indifferent to which
// prefixes or declarations the server picks, and rejects any spelling no
// conformant parser accepts. The fragment as persisted is checked too: it is
// stored as a standalone fragment and re-parsed on every read, so a declaration
// written twice at rest is a defect even when the lenient decoder reading it
// back happens to recover.
func TestDeadPropertyRoundTripPreservesXMLStructure(t *testing.T) {
	const (
		propertyNS  = "urn:example:custom"
		otherNS     = "urn:example:other"
		defaultNS   = "urn:example:default"
		qualifierNS = "urn:example:qualifier"

		declarations = `xmlns:D="DAV:" xmlns:X="` + propertyNS + `" xmlns:Y="` + otherNS + `" xmlns:Z="` + qualifierNS + `"`
	)

	tests := []struct {
		name     string
		property string
		// requested is the element the PROPFIND body names, defaulting to the
		// prefixed X:meta the other cases patch.
		requested string
		want      davElement
	}{
		{
			name:     "child in the property namespace",
			property: `<X:meta><X:child>value</X:child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(propertyNS, "child"), Text: "value"},
			}},
		},
		{
			name:     "multi-level nesting with attributes on nested children",
			property: `<X:meta><X:outer k="1"><X:middle k="2" j="3"><X:inner>deep</X:inner></X:middle></X:outer></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(propertyNS, "outer"), Attr: []xml.Attr{{Name: qn("", "k"), Value: "1"}}, Children: []davElement{
					{
						Name: qn(propertyNS, "middle"),
						Attr: []xml.Attr{{Name: qn("", "j"), Value: "3"}, {Name: qn("", "k"), Value: "2"}},
						Children: []davElement{
							{Name: qn(propertyNS, "inner"), Text: "deep"},
						},
					},
				}},
			}},
		},
		{
			name:     "child in a namespace other than the property's",
			property: `<X:meta><Y:child a="1">other</Y:child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(otherNS, "child"), Attr: []xml.Attr{{Name: qn("", "a"), Value: "1"}}, Text: "other"},
			}},
		},
		{
			name:     "namespaces alternating down the tree",
			property: `<X:meta><Y:outer><X:inner><Y:leaf>back</Y:leaf></X:inner></Y:outer></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(otherNS, "outer"), Children: []davElement{
					{Name: qn(propertyNS, "inner"), Children: []davElement{
						{Name: qn(otherNS, "leaf"), Text: "back"},
					}},
				}},
			}},
		},
		{
			name:     "fragment supplying its own default namespace",
			property: `<X:meta xmlns="urn:example:default"><child><grand k="1">v</grand></child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(defaultNS, "child"), Children: []davElement{
					{Name: qn(defaultNS, "grand"), Attr: []xml.Attr{{Name: qn("", "k"), Value: "1"}}, Text: "v"},
				}},
			}},
		},
		{
			name:     "child in no namespace under a namespaced property",
			property: `<X:meta><child><X:qualified>q</X:qualified></child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn("", "child"), Children: []davElement{
					{Name: qn(propertyNS, "qualified"), Text: "q"},
				}},
			}},
		},
		{
			name:     "child redeclaring the default namespace itself",
			property: `<X:meta><child xmlns="urn:example:other"/></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(otherNS, "child")},
			}},
		},
		{
			name:     "qualified attribute on a nested child",
			property: `<X:meta><X:child Z:k="1" plain="2">v</X:child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{
					Name: qn(propertyNS, "child"),
					Attr: []xml.Attr{{Name: qn("", "plain"), Value: "2"}, {Name: qn(qualifierNS, "k"), Value: "1"}},
					Text: "v",
				},
			}},
		},
		{
			name:     "two qualified attributes sharing a namespace",
			property: `<X:meta><X:child Z:k="1" Z:j="2"/></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{Name: qn(propertyNS, "child"), Attr: []xml.Attr{
					{Name: qn(qualifierNS, "j"), Value: "2"},
					{Name: qn(qualifierNS, "k"), Value: "1"},
				}},
			}},
		},
		{
			name:      "property in no namespace carrying namespaced children",
			property:  `<meta><X:child>value</X:child><plain/></meta>`,
			requested: `<meta/>`,
			want: davElement{Name: qn("", "meta"), Children: []davElement{
				{Name: qn(propertyNS, "child"), Text: "value"},
				{Name: qn("", "plain")},
			}},
		},
		{
			name:     "xml:lang on a nested child",
			property: `<X:meta><X:child xml:lang="de">wert</X:child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{
					Name: qn(propertyNS, "child"),
					Attr: []xml.Attr{{Name: qn(xmlNamespaceURI, "lang"), Value: "de"}},
					Text: "wert",
				},
			}},
		},
		{
			// The reserved namespace is the one namespace no declaration may
			// bind, so the two attributes have to be written different ways:
			// xml:lang by its reserved prefix and Z:k through a declaration the
			// re-emitted element carries itself.
			name:     "xml:lang beside an attribute needing a declared prefix",
			property: `<X:meta><X:child xml:lang="de" Z:k="1">wert</X:child></X:meta>`,
			want: davElement{Name: qn(propertyNS, "meta"), Children: []davElement{
				{
					Name: qn(propertyNS, "child"),
					Attr: []xml.Attr{
						{Name: qn(xmlNamespaceURI, "lang"), Value: "de"},
						{Name: qn(qualifierNS, "k"), Value: "1"},
					},
					Text: "wert",
				},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dead := &fakeDeadPropertyRepo{}
			h := NewDavServer(Options{Store: &store.Store{
				Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 1, Name: "Work"}}},
				Events:         &fakeEventRepo{events: map[string]*store.Event{}},
				DeadProperties: dead,
			}})
			user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}

			patchBody := `<D:propertyupdate ` + declarations + `><D:set><D:prop>` + tt.property + `</D:prop></D:set></D:propertyupdate>`
			patchRequest := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(patchBody))
			patchRequest = patchRequest.WithContext(auth.WithUser(patchRequest.Context(), user))
			patchResponse := httptest.NewRecorder()
			h.ServeHTTP(patchResponse, patchRequest)
			patched := decodeMultistatus(t, patchResponse).responseForHref(t, "/dav/calendars/1")
			patched.assertPropStatus(t, tt.want.Name, http.StatusOK)

			assertStoredDeadPropertyTree(t, dead, "/dav/calendars/1", tt.want)

			requested := tt.requested
			if requested == "" {
				requested = `<X:meta/>`
			}
			findBody := `<D:propfind ` + declarations + `><D:prop>` + requested + `</D:prop></D:propfind>`
			findRequest := httptest.NewRequest("PROPFIND", "/dav/calendars/1", strings.NewReader(findBody))
			findRequest.Header.Set("Depth", "0")
			findRequest = findRequest.WithContext(auth.WithUser(findRequest.Context(), user))
			findResponse := httptest.NewRecorder()
			h.ServeHTTP(findResponse, findRequest)
			found := decodeMultistatus(t, findResponse).responseForHref(t, "/dav/calendars/1/")
			assertDeadPropertyTree(t, found.assertPropStatus(t, tt.want.Name, http.StatusOK), tt.want)
		})
	}
}

// A stored fragment can carry a declaration twice, which the lenient decoder
// resolves; the read path writes it once rather than copying the spelling.
func TestPropfindRepairsDeadPropertyStoredWithDuplicateDeclarations(t *testing.T) {
	dead := &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/calendars/1": {
			"urn:example:custom\x00meta": {
				ResourcePath: "/dav/calendars/1",
				NamespaceURI: "urn:example:custom",
				LocalName:    "meta",
				InnerXML:     `<child xmlns="urn:example:other" xmlns="urn:example:other">value</child>`,
			},
		},
	}}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 1, Name: "Work"}}},
		Events:         &fakeEventRepo{events: map[string]*store.Event{}},
		DeadProperties: dead,
	}})
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}

	findBody := `<D:propfind xmlns:D="DAV:" xmlns:X="urn:example:custom"><D:prop><X:meta/></D:prop></D:propfind>`
	request := httptest.NewRequest("PROPFIND", "/dav/calendars/1", strings.NewReader(findBody))
	request.Header.Set("Depth", "0")
	request = request.WithContext(auth.WithUser(request.Context(), user))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)

	want := davElement{
		Name:     qn("urn:example:custom", "meta"),
		Children: []davElement{{Name: qn("urn:example:other", "child"), Text: "value"}},
	}
	found := decodeMultistatus(t, response).responseForHref(t, "/dav/calendars/1/")
	assertDeadPropertyTree(t, found.assertPropStatus(t, want.Name, http.StatusOK), want)
}

// assertStoredDeadPropertyTree checks the fragment persisted for want's name.
// The fragment is parsed on its own, as the read path parses it, so it must
// carry every declaration its elements need and carry none of them twice.
func assertStoredDeadPropertyTree(t *testing.T, dead *fakeDeadPropertyRepo, resourcePath string, want davElement) {
	t.Helper()
	stored, ok := dead.properties[resourcePath][want.Name.Space+"\x00"+want.Name.Local]
	if !ok {
		t.Fatalf("PROPPATCH stored no %s for %s: %#v", qnString(want.Name), resourcePath, dead.properties)
	}
	// The property element is not part of the stored fragment, so the children
	// are re-parsed under a wrapper in no namespace, which declares nothing and
	// leaves each of them to say what namespace it is in.
	root, err := parseDAVDocument([]byte("<stored-fragment>" + stored.InnerXML + "</stored-fragment>"))
	if err != nil {
		t.Fatalf("stored fragment for %s is not well-formed: %v; fragment: %s", qnString(want.Name), err, stored.InnerXML)
	}
	got := davElement{Name: want.Name, Text: root.Text, Children: root.Children}
	assertDeadPropertyTree(t, got, want)
}

// assertDeadPropertyTree compares a property value's resolved element tree
// against want, ignoring whitespace-only text and attribute order.
func assertDeadPropertyTree(t *testing.T, got, want davElement) {
	t.Helper()
	if gotText, wantText := renderDeadPropertyTree(got), renderDeadPropertyTree(want); gotText != wantText {
		t.Errorf("property tree =\n  %s\nwant\n  %s", gotText, wantText)
	}
}

// renderDeadPropertyTree renders a resolved subtree in a canonical form:
// attributes sorted by resolved name and whitespace-only text dropped, so the
// comparison turns on structure and namespaces rather than serialization.
func renderDeadPropertyTree(el davElement) string {
	var out strings.Builder
	out.WriteString(qnString(el.Name))
	attrs := append([]xml.Attr(nil), el.Attr...)
	sort.Slice(attrs, func(i, j int) bool {
		return qnString(attrs[i].Name) < qnString(attrs[j].Name)
	})
	for _, a := range attrs {
		out.WriteString(" " + qnString(a.Name) + "=" + strconv.Quote(a.Value))
	}
	if text := strings.TrimSpace(el.Text); text != "" {
		out.WriteString(" " + strconv.Quote(text))
	}
	if len(el.Children) > 0 {
		out.WriteString("(")
		for i, child := range el.Children {
			if i > 0 {
				out.WriteString(", ")
			}
			out.WriteString(renderDeadPropertyTree(child))
		}
		out.WriteString(")")
	}
	return out.String()
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
			body: `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:D="DAV:" xmlns:X="urn:test"><D:prop><X:note/></D:prop><C:filter><C:comp-filter name="VCALENDAR"/></C:filter></C:calendar-query>`,
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
			// RFC 4791 §7.8 processes a calendar-query with no Depth header as
			// Depth: 0, which reaches the collection alone; the members these
			// cases assert on need Depth: 1.
			request.Header.Set("Depth", "1")
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
		{
			name: "attribute prefix never declared",
			body: `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:test"><D:set><D:prop><X:meta><X:child q:attr="1"/></X:meta></D:prop></D:set></D:propertyupdate>`,
		},
		{
			name: "element prefix never declared",
			body: `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:test"><D:set><D:prop><X:meta><q:child/></X:meta></D:prop></D:set></D:propertyupdate>`,
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

// RFC 4791 §5.2.2 makes a CALDAV:calendar-timezone value an iCalendar object
// containing exactly one VTIMEZONE, so the VCALENDAR envelope is required and a
// bare component is not a valid value.
// standardObservance is the minimal RFC 5545 §3.6.5 STANDARD sub-component: the
// three properties an observance must declare and nothing else.
const standardObservance = "BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"

// wrapTimezoneValue builds a calendar-timezone value: a complete VCALENDAR
// holding one VTIMEZONE whose sub-components are the given body.
func wrapTimezoneValue(observances string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + observances +
		"END:VTIMEZONE\r\nEND:VCALENDAR\r\n"
}

func TestValidCalendarTimezoneRequiresWrappedSingleVTimezone(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{
			name:  "VCALENDAR wrapper",
			value: wrapTimezoneValue(standardObservance),
			want:  true,
		},
		{
			name:  "a second observance is allowed",
			value: wrapTimezoneValue(standardObservance + "BEGIN:DAYLIGHT\r\nDTSTART:19700308T020000\r\nTZOFFSETFROM:-0600\r\nTZOFFSETTO:-0500\r\nEND:DAYLIGHT\r\n"),
			want:  true,
		},
		{
			name: "valid optional timezone properties",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nLAST-MODIFIED:20260730T120000Z\r\nTZURL:https://example.test/timezones/chicago.ics\r\n" +
				"BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\n" +
				"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU\r\nRDATE:19711107T020000\r\nTZNAME:CST\r\nCOMMENT:Standard time\r\n" +
				"END:STANDARD\r\nEND:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: true,
		},
		{
			name: "bare VTIMEZONE is not an iCalendar object",
			value: "BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance +
				"END:VTIMEZONE\r\n",
			want: false,
		},
		{
			name: "two VTIMEZONE components",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:Europe/London\r\n" + standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name: "VCALENDAR property after its component",
			value: "BEGIN:VCALENDAR\r\nBEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" +
				standardObservance + "END:VTIMEZONE\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name:  "no VTIMEZONE at all",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\nEND:VCALENDAR\r\n",
			want:  false,
		},
		{
			name: "VTIMEZONE without a TZID",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\n" + standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		// RFC 5545 §3.6: VERSION and PRODID are required of every iCalendar
		// object and occur once each. A value missing them is not one, however
		// well-formed the VTIMEZONE inside it is.
		{
			name: "VCALENDAR without VERSION",
			value: "BEGIN:VCALENDAR\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name: "VCALENDAR without PRODID",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name: "VCALENDAR naming another iCalendar version",
			value: "BEGIN:VCALENDAR\r\nVERSION:1.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name: "VCALENDAR repeating VERSION",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		// RFC 5545 §3.6.5: a VTIMEZONE carries at least one observance, and each
		// observance says when it starts and which offsets it moves between.
		{
			name:  "VTIMEZONE with no observance",
			value: wrapTimezoneValue(""),
			want:  false,
		},
		{
			name: "VTIMEZONE repeating TZURL",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\nTZURL:https://example.test/one\r\nTZURL:https://example.test/two\r\n" +
				standardObservance + "END:VTIMEZONE\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name:  "VTIMEZONE carrying a disallowed standard property",
			value: wrapTimezoneValue("SUMMARY:not allowed here\r\n" + standardObservance),
			want:  false,
		},
		{
			name:  "observance without DTSTART",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance without TZOFFSETTO",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with an empty TZOFFSETFROM",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with a malformed DTSTART",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:not-a-date\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance DTSTART must be local time",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000Z\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance DTSTART cannot carry TZID",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART;TZID=America/Chicago:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with a malformed TZOFFSETFROM",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:garbage\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with an out of range TZOFFSETTO",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:+2460\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with negative zero UTC offset",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0000\r\nTZOFFSETTO:+0000\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance repeating RRULE",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nRRULE:FREQ=YEARLY\r\nRRULE:FREQ=MONTHLY\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with malformed RRULE",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nRRULE:not-a-rule\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance RRULE UNTIL must be UTC",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nRRULE:FREQ=YEARLY;UNTIL=20061029T020000\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance RRULE finite end must use UNTIL",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nRRULE:FREQ=YEARLY;COUNT=10\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with malformed RDATE",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nRDATE:not-a-date\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with malformed parameter syntax",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART;VALUE:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "observance with an unescaped text delimiter",
			value: wrapTimezoneValue("BEGIN:STANDARD\r\nDTSTART:19701101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0600\r\nCOMMENT:one;two\r\nEND:STANDARD\r\n"),
			want:  false,
		},
		{
			name:  "an unknown sub-component",
			value: wrapTimezoneValue(standardObservance + "BEGIN:VALARM\r\nACTION:DISPLAY\r\nEND:VALARM\r\n"),
			want:  false,
		},
		{
			name: "a VEVENT beside the VTIMEZONE",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\n" +
				"BEGIN:VEVENT\r\nUID:intruder\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
			want: false,
		},
		{
			name: "an unterminated component",
			value: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//CalCard//EN\r\n" +
				"BEGIN:VTIMEZONE\r\nTZID:America/Chicago\r\n" + standardObservance + "END:VTIMEZONE\r\n",
			want: false,
		},
		{
			name:  "a line that is not a content line",
			value: wrapTimezoneValue(standardObservance + "this is not a property\r\n"),
			want:  false,
		},
		{
			name:  "not iCalendar at all",
			value: "this is not a calendar",
			want:  false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := validCalendarTimezone(test.value); got != test.want {
				t.Fatalf("validCalendarTimezone() = %v, want %v for %s", got, test.want, test.value)
			}
		})
	}
}

// RFC 4918 Section 4.3: a server SHOULD preserve prefixes, because vocabularies
// such as XPath and XML Schema name things with QNames in content. The prefix
// an element was written with, and the declaration its QName content relies
// on, come back as they were sent -- including one declared on an ancestor
// outside the property value.
func TestDeadPropertyRoundTripPreservesPrefixes(t *testing.T) {
	dead := &fakeDeadPropertyRepo{}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars:      &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 1, Name: "Work"}}},
		Events:         &fakeEventRepo{events: map[string]*store.Event{}},
		DeadProperties: dead,
	}})
	user := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}

	patchBody := `<D:propertyupdate xmlns:D="DAV:" xmlns:X="urn:example:custom" xmlns:xs="http://www.w3.org/2001/XMLSchema">` +
		`<D:set><D:prop><X:meta><X:type xmlns:t="urn:example:types" t:kind="k">xs:dateTime</X:type><t2:ref xmlns:t2="urn:example:types">t2:name</t2:ref></X:meta></D:prop></D:set></D:propertyupdate>`
	patchRequest := httptest.NewRequest("PROPPATCH", "/dav/calendars/1", strings.NewReader(patchBody))
	patchRequest = patchRequest.WithContext(auth.WithUser(patchRequest.Context(), user))
	patchResponse := httptest.NewRecorder()
	h.ServeHTTP(patchResponse, patchRequest)
	if patchResponse.Code != http.StatusMultiStatus {
		t.Fatalf("PROPPATCH status = %d: %s", patchResponse.Code, patchResponse.Body.String())
	}

	stored := dead.properties["/dav/calendars/1"]["urn:example:custom\x00meta"].InnerXML
	findBody := `<D:propfind xmlns:D="DAV:" xmlns:X="urn:example:custom"><D:prop><X:meta/></D:prop></D:propfind>`
	findRequest := httptest.NewRequest("PROPFIND", "/dav/calendars/1", strings.NewReader(findBody))
	findRequest.Header.Set("Depth", "0")
	findRequest = findRequest.WithContext(auth.WithUser(findRequest.Context(), user))
	findResponse := httptest.NewRecorder()
	h.ServeHTTP(findResponse, findRequest)

	for name, document := range map[string]string{"stored fragment": "<stored>" + stored + "</stored>", "PROPFIND response": findResponse.Body.String()} {
		elements := rawPrefixedElements(t, document)
		typeElement, ok := elements["X:type"]
		if !ok {
			t.Fatalf("%s lost the X prefix on X:type: %s", name, document)
		}
		if typeElement["xs"] != "http://www.w3.org/2001/XMLSchema" {
			t.Errorf("%s: QName content xs:dateTime has xs bound to %q: %s", name, typeElement["xs"], document)
		}
		if typeElement["t"] != "urn:example:types" {
			t.Errorf("%s: attribute prefix t bound to %q: %s", name, typeElement["t"], document)
		}
		ref, ok := elements["t2:ref"]
		if !ok || ref["t2"] != "urn:example:types" {
			t.Errorf("%s lost the t2 prefix or its binding: %s", name, document)
		}
		if strings.Contains(document, `"q"`) || strings.Contains(document, "xmlns:ns1") {
			t.Errorf("%s invented a prefix: %s", name, document)
		}
	}
}

// rawPrefixedElements maps each element's name, spelled as written, to the
// prefix bindings in scope at it.
func rawPrefixedElements(t *testing.T, document string) map[string]map[string]string {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(document))
	result := map[string]map[string]string{}
	stack := []map[string]string{{}}
	for {
		token, err := dec.RawToken()
		if err == io.EOF {
			return result
		}
		if err != nil {
			t.Fatalf("document is not well-formed: %v\n%s", err, document)
		}
		switch token := token.(type) {
		case xml.StartElement:
			scope := map[string]string{}
			for prefix, uri := range stack[len(stack)-1] {
				scope[prefix] = uri
			}
			for _, attr := range token.Attr {
				if attr.Name.Space == "xmlns" {
					scope[attr.Name.Local] = attr.Value
				}
			}
			stack = append(stack, scope)
			name := token.Name.Local
			if token.Name.Space != "" {
				name = token.Name.Space + ":" + name
			}
			result[name] = scope
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		}
	}
}

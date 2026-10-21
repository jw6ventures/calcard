package dav

// RFC 3253 §3.8: the DAV:expand-property REPORT. RFC 4791 §7.1 requires CalDAV
// servers to support the REPORT method and to advertise their reports in
// DAV:supported-report-set, but expand-property itself is RFC 3253's.

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestRFC3253_ExpandPropertyInlinesReferencedResourceProperties(t *testing.T) {
	now := store.Now()
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com", FullName: "Dana Lee"}

	// RFC 3253 §3.8 grammar: DAV:property elements carrying name/namespace
	// attributes, not nested DAV:prop elements.
	body := `<?xml version="1.0" encoding="utf-8"?>
<d:expand-property xmlns:d="DAV:">
  <d:property name="current-user-principal" namespace="DAV:">
    <d:property name="displayname" namespace="DAV:"/>
  </d:property>
</d:expand-property>`

	req := httptest.NewRequest("REPORT", "/dav/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Report(rr, req)

	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/")
	resp := ms.responseForHref(t, "/dav/")

	// RFC 3253 §3.8 reports the properties the request's DAV:property elements
	// name and no others, so the outer property set is exactly the one asked
	// for.
	resp.assertPropstatNames(t, http.StatusOK, davQN("current-user-principal"))
	prop := resp.assertPropStatus(t, davQN("current-user-principal"), http.StatusOK)

	// RFC 3253 §3.8: each expanded property carries a DAV:response for the
	// referenced resource in place of a bare DAV:href, so the property has that
	// one response as its sole child and the nested document must itself be a
	// well-formed response — right href, 200 propstat, exactly the properties
	// the inner DAV:property elements asked for.
	inner := responseFromElement(t, assertSoleChild(t, prop, davQN("response")))
	inner.assertHref(t, "/dav/principals/1/")
	inner.assertPropstatNames(t, http.StatusOK, davQN("displayname"))
	inner.assertPropValue(t, davQN("displayname"), http.StatusOK, user.FullName)
}

// expandPropertyServer holds one calendar, one object, and one principal so the
// report can exercise collection, object, and principal expansion paths.
func expandPropertyServer() (*DavServer, *store.User) {
	now := store.Now()
	user := &store.User{ID: 1, PrimaryEmail: "user@example.com", FullName: "Dana Lee"}
	h := &DavServer{store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{
				{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test", UpdatedAt: now}, Editor: true},
			},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{
			"1:event": {CalendarID: 1, UID: "event", ResourceName: "event", ETag: "e1",
				RawICAL: "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:event\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"},
		}},
		Users: &aclReportUserRepo{users: map[int64]store.User{user.ID: *user}},
	}}
	return h, user
}

func runExpandProperty(t *testing.T, h *DavServer, user *store.User, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("REPORT", target, strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	return rr
}

// RFC 3253 §3.8: the report returns the properties the request names, on every
// resource it can target. Nothing in the section makes the reported set depend
// on the kind of resource the Request-URI names.
func TestRFC3253_ExpandPropertyReportsTheRequestedPropertySet(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="displayname" namespace="DAV:"/>
  <D:property name="resourcetype" namespace="DAV:"/>
</D:expand-property>`

	tests := map[string]struct {
		target string
		href   string
		ok     []xml.Name
		absent []xml.Name
	}{
		"the DAV root": {
			target: "/dav/", href: "/dav/",
			ok: []xml.Name{davQN("displayname"), davQN("resourcetype")},
		},
		"a calendar collection": {
			target: "/dav/calendars/1/", href: "/dav/calendars/1/",
			ok: []xml.Name{davQN("displayname"), davQN("resourcetype")},
		},
		"a calendar object resource": {
			// RFC 4918 §15.2 defines displayname on collections; §15.9 defines
			// resourcetype everywhere, empty on a non-collection.
			target: "/dav/calendars/1/event.ics", href: "/dav/calendars/1/event.ics",
			ok:     []xml.Name{davQN("resourcetype")},
			absent: []xml.Name{davQN("displayname")},
		},
		"a principal": {
			target: "/dav/principals/1/", href: "/dav/principals/1/",
			ok: []xml.Name{davQN("displayname"), davQN("resourcetype")},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h, user := expandPropertyServer()
			ms := decodeMultistatus(t, runExpandProperty(t, h, user, tt.target, body))
			ms.assertHrefs(t, tt.href)
			resp := ms.responseForHref(t, tt.href)
			resp.assertPropstatNames(t, http.StatusOK, tt.ok...)
			if len(tt.absent) > 0 {
				resp.assertPropstatNames(t, http.StatusNotFound, tt.absent...)
			}
		})
	}
}

// RFC 3253 §3.8 replaces the DAV:href children of an href-valued property with
// a DAV:response for the resource each href references. The rule is not
// specific to DAV:current-user-principal: every href-valued property the
// resource defines expands the same way.
func TestRFC3253_ExpandPropertyExpandsEveryHrefValuedProperty(t *testing.T) {
	tests := map[string]struct {
		target   string
		href     string
		property xml.Name
		expanded []string
	}{
		"current-user-principal on a calendar collection": {
			target: "/dav/calendars/1/", href: "/dav/calendars/1/",
			property: davQN("current-user-principal"), expanded: []string{"/dav/principals/1/"},
		},
		"owner on a calendar collection": {
			target: "/dav/calendars/1/", href: "/dav/calendars/1/",
			property: davQN("owner"), expanded: []string{"/dav/principals/1/"},
		},
		"owner on a calendar object resource": {
			target: "/dav/calendars/1/event.ics", href: "/dav/calendars/1/event.ics",
			property: davQN("owner"), expanded: []string{"/dav/principals/1/"},
		},
		"principal-collection-set": {
			target: "/dav/calendars/1/", href: "/dav/calendars/1/",
			property: davQN("principal-collection-set"), expanded: []string{"/dav/principals/"},
		},
		"principal-URL on a principal": {
			target: "/dav/principals/1/", href: "/dav/principals/1/",
			property: davQN("principal-URL"), expanded: []string{"/dav/principals/1/"},
		},
		"calendar-home-set on a principal": {
			target: "/dav/principals/1/", href: "/dav/principals/1/",
			property: calQN("calendar-home-set"), expanded: []string{"/dav/calendars/"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="` + tt.property.Local + `" namespace="` + tt.property.Space + `">
    <D:property name="resourcetype" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

			h, user := expandPropertyServer()
			ms := decodeMultistatus(t, runExpandProperty(t, h, user, tt.target, body))
			ms.assertHrefs(t, tt.href)
			prop := ms.responseForHref(t, tt.href).assertPropStatus(t, tt.property, http.StatusOK)

			// The hrefs are gone: each is replaced by the response for the
			// resource it named, carrying the nested selection and nothing else.
			var hrefs []string
			for _, child := range prop.Children {
				if child.Name == davQN("href") {
					hrefs = append(hrefs, strings.TrimSpace(child.Text))
				}
			}
			if len(hrefs) != 0 {
				t.Errorf("%s still carries DAV:href children %v", qnString(tt.property), hrefs)
			}
			var got []string
			for _, child := range prop.Children {
				if child.Name != davQN("response") {
					t.Fatalf("%s carries %s, want only DAV:response", qnString(tt.property), qnString(child.Name))
				}
				inner := responseFromElement(t, child)
				inner.assertPropstatNames(t, http.StatusOK, davQN("resourcetype"))
				got = append(got, canonicalHref(inner.Hrefs[0]))
			}
			want := make([]string, 0, len(tt.expanded))
			for _, href := range tt.expanded {
				want = append(want, canonicalHref(href))
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("%s expanded to %v, want %v", qnString(tt.property), got, want)
			}
		})
	}
}

func TestRFC3253_ExpandPropertyExpandsHrefsInDeadProperties(t *testing.T) {
	h, user := expandPropertyServer()
	h.store.DeadProperties = &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/calendars/1": {
			"urn:example:custom\x00links": {
				ResourcePath: "/dav/calendars/1",
				NamespaceURI: "urn:example:custom",
				LocalName:    "links",
				InnerXML: `<X:label xmlns:X="urn:example:custom">principal</X:label>` +
					`<D:href xmlns:D="DAV:">/dav/principals/1/</D:href>`,
			},
		},
	}}
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="links" namespace="urn:example:custom">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

	ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body))
	property := ms.responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, qn("urn:example:custom", "links"), http.StatusOK)
	if len(property.Children) != 2 || property.Children[0].Name != qn("urn:example:custom", "label") {
		t.Fatalf("expanded dead property did not preserve its non-href content: %#v", property.Children)
	}
	inner := responseFromElement(t, property.Children[1])
	inner.assertHref(t, "/dav/principals/1/")
	inner.assertPropValue(t, davQN("displayname"), http.StatusOK, user.FullName)
}

func TestRFC3253_ExpandPropertyExpandsHrefsInCompoundLiveProperties(t *testing.T) {
	body := func(property string) string {
		return `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="` + property + `" namespace="DAV:">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`
	}

	t.Run("ACL principal", func(t *testing.T) {
		h, user := expandPropertyServer()
		ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body("acl")))
		acl := ms.responseForHref(t, "/dav/calendars/1/").assertPropStatus(t, davQN("acl"), http.StatusOK)
		principal := assertSoleChild(t, acl.child(t, davQN("ace")).child(t, davQN("principal")), davQN("response"))
		expanded := responseFromElement(t, principal)
		expanded.assertHref(t, "/dav/principals/1/")
		expanded.assertPropValue(t, davQN("displayname"), http.StatusOK, user.FullName)
	})

	t.Run("lock token and root", func(t *testing.T) {
		h, user := expandPropertyServer()
		h.store.Locks = &fakeLockRepo{locks: map[string]*store.Lock{
			"opaquelocktoken:test": {
				Token:          "opaquelocktoken:test",
				ResourcePath:   "/dav/calendars/1",
				UserID:         user.ID,
				LockScope:      "exclusive",
				LockType:       "write",
				Depth:          "0",
				TimeoutSeconds: 3600,
				ExpiresAt:      time.Now().Add(time.Hour),
			},
		}}

		ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body("lockdiscovery")))
		discovery := ms.responseForHref(t, "/dav/calendars/1/").
			assertPropStatus(t, davQN("lockdiscovery"), http.StatusOK)
		active := assertSoleChild(t, discovery, davQN("activelock"))

		token := responseFromElement(t, assertSoleChild(t, active.child(t, davQN("locktoken")), davQN("response")))
		token.assertHref(t, "opaquelocktoken:test")
		token.assertResponseStatus(t, http.StatusNotFound)

		root := responseFromElement(t, assertSoleChild(t, active.child(t, davQN("lockroot")), davQN("response")))
		root.assertHref(t, "/dav/calendars/1/")
		root.assertPropValue(t, davQN("displayname"), http.StatusOK, "Test")
	})
}

func TestRFC3253_ExpandPropertyResolvesHrefReferencesAgainstTheirResource(t *testing.T) {
	tests := map[string]struct {
		href       string
		wantHref   string
		wantStatus int
	}{
		"relative reference": {
			href:       "../../principals/1/",
			wantHref:   "/dav/principals/1/",
			wantStatus: http.StatusOK,
		},
		"foreign authority": {
			href:       "https://foreign.example/dav/principals/1/",
			wantHref:   "https://foreign.example/dav/principals/1/",
			wantStatus: http.StatusNotFound,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			h, user := expandPropertyServer()
			h.store.DeadProperties = &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
				"/dav/calendars/1": {
					"urn:example:custom\x00link": {
						ResourcePath: "/dav/calendars/1",
						NamespaceURI: "urn:example:custom",
						LocalName:    "link",
						InnerXML:     `<D:href xmlns:D="DAV:">` + tt.href + `</D:href>`,
					},
				},
			}}
			body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="link" namespace="urn:example:custom">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

			ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body))
			property := ms.responseForHref(t, "/dav/calendars/1/").
				assertPropStatus(t, qn("urn:example:custom", "link"), http.StatusOK)
			expanded := responseFromElement(t, assertSoleChild(t, property, davQN("response")))
			expanded.assertHref(t, tt.wantHref)
			if tt.wantStatus == http.StatusOK {
				expanded.assertPropValue(t, davQN("displayname"), http.StatusOK, user.FullName)
			} else {
				expanded.assertResponseStatus(t, tt.wantStatus)
			}
		})
	}
}

func TestRFC3253_ExpandPropertyFailsInsteadOfTruncatingNestedResponses(t *testing.T) {
	h, user := expandPropertyServer()
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 2
	h.cfg = cfg
	h.store.DeadProperties = &fakeDeadPropertyRepo{properties: map[string]map[string]store.DeadProperty{
		"/dav/calendars/1": {
			"urn:example:custom\x00links": {
				ResourcePath: "/dav/calendars/1",
				NamespaceURI: "urn:example:custom",
				LocalName:    "links",
				InnerXML: `<D:href xmlns:D="DAV:">/dav/principals/1/</D:href>` +
					`<D:href xmlns:D="DAV:">/dav/principals/1/</D:href>`,
			},
		},
	}}
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="links" namespace="urn:example:custom">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

	rr := runExpandProperty(t, h, user, "/dav/calendars/1/", body)
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("response limit returned a partial expansion: %s", rr.Body.String())
	}
}

func TestRFC3253_ExpandPropertyReportsUnreadableReferences(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="owner" namespace="DAV:">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

	h, user := expandPropertyServer()
	calendars := h.store.Calendars.(*fakeCalendarRepo)
	calendar := calendars.accessible[0].Calendar
	calendar.UserID = 2
	calendars.accessible[0].Calendar = calendar
	calendars.calendars = map[int64]*store.Calendar{calendar.ID: &calendar}

	ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body))
	owner := ms.responseForHref(t, "/dav/calendars/1/").assertPropStatus(t, davQN("owner"), http.StatusOK)
	inner := responseFromElement(t, assertSoleChild(t, owner, davQN("response")))
	inner.assertHref(t, "/dav/principals/2/")
	inner.assertResponseStatus(t, http.StatusNotFound)
}

// RFC 3253 §3.8 expands nested DAV:property elements against the resource each
// href references, so a request can walk from a collection to its principal to
// that principal's calendar home in one report.
func TestRFC3253_ExpandPropertyNestsThroughReferencedResources(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:property name="current-user-principal" namespace="DAV:">
    <D:property name="calendar-home-set" namespace="urn:ietf:params:xml:ns:caldav">
      <D:property name="displayname" namespace="DAV:"/>
    </D:property>
  </D:property>
</D:expand-property>`

	h, user := expandPropertyServer()
	ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body))
	ms.assertHrefs(t, "/dav/calendars/1/")

	principalProp := ms.responseForHref(t, "/dav/calendars/1/").
		assertPropStatus(t, davQN("current-user-principal"), http.StatusOK)
	principal := responseFromElement(t, assertSoleChild(t, principalProp, davQN("response")))
	principal.assertHref(t, "/dav/principals/1/")

	homeProp := principal.assertPropStatus(t, calQN("calendar-home-set"), http.StatusOK)
	home := responseFromElement(t, assertSoleChild(t, homeProp, davQN("response")))
	home.assertHref(t, "/dav/calendars/")
	home.assertPropValue(t, davQN("displayname"), http.StatusOK, "Calendars")
}

// RFC 3253 §3.8 nests DAV:property children to select what an expanded response
// carries. A property named without children is reported as its own value, so
// the hrefs stay in place rather than being read as a request to expand.
func TestRFC3253_ExpandPropertyWithoutNestingReportsHrefs(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:">
  <D:property name="current-user-principal" namespace="DAV:"/>
</D:expand-property>`

	h, user := expandPropertyServer()
	ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body))
	ms.responseForHref(t, "/dav/calendars/1/").
		assertPropHrefs(t, davQN("current-user-principal"), "/dav/principals/1/")
}

// A property the request names that the resource does not define is reported
// with a 404 status, exactly as PROPFIND reports it (RFC 4918 §9.1).
func TestRFC3253_ExpandPropertyReportsUndefinedPropertiesWith404(t *testing.T) {
	body := `<?xml version="1.0" encoding="utf-8"?>
<D:expand-property xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:property name="resourcetype" namespace="DAV:"/>
  <D:property name="calendar-home-set" namespace="urn:ietf:params:xml:ns:caldav">
    <D:property name="displayname" namespace="DAV:"/>
  </D:property>
</D:expand-property>`

	h, user := expandPropertyServer()
	ms := decodeMultistatus(t, runExpandProperty(t, h, user, "/dav/calendars/1/", body))
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertPropstatNames(t, http.StatusOK, davQN("resourcetype"))
	// CALDAV:calendar-home-set is a principal property (RFC 4791 §6.2.1), so a
	// calendar collection lacks it and the expansion has nothing to walk.
	resp.assertPropstatNames(t, http.StatusNotFound, calQN("calendar-home-set"))
}

func TestRFC3253_ExpandPropertyRejectsMalformedBody(t *testing.T) {
	tests := map[string]string{
		"an undefined root attribute": `<D:expand-property xmlns:D="DAV:" limit="1">` +
			`<D:property name="displayname"/></D:expand-property>`,
		"a property without a name": `<D:expand-property xmlns:D="DAV:">` +
			`<D:property namespace="DAV:"/></D:expand-property>`,
		"a property with an invalid name token": `<D:expand-property xmlns:D="DAV:">` +
			`<D:property name="display name"/></D:expand-property>`,
		"a property with an undefined attribute": `<D:expand-property xmlns:D="DAV:">` +
			`<D:property name="displayname" test="yes"/></D:expand-property>`,
		"character data": `<D:expand-property xmlns:D="DAV:">text` +
			`<D:property name="displayname"/></D:expand-property>`,
		"an unexpected DAV child": `<D:expand-property xmlns:D="DAV:">` +
			`<D:href>/dav/</D:href></D:expand-property>`,
		"the legacy DAV prop shape": `<D:expand-property xmlns:D="DAV:">` +
			`<D:prop><D:displayname/></D:prop></D:expand-property>`,
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			h, user := expandPropertyServer()
			rr := runExpandProperty(t, h, user, "/dav/calendars/1/", body)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
			}
		})
	}
}

func TestRFC3253_ExpandPropertyRejectsNestingBeyondItsExplicitLimit(t *testing.T) {
	buildBody := func(propertyLevels int) string {
		var body strings.Builder
		body.WriteString(`<D:expand-property xmlns:D="DAV:">`)
		for range propertyLevels {
			body.WriteString(`<D:property name="owner" namespace="DAV:">`)
		}
		for range propertyLevels {
			body.WriteString(`</D:property>`)
		}
		body.WriteString(`</D:expand-property>`)
		return body.String()
	}

	for _, tt := range []struct {
		name           string
		propertyLevels int
		wantStatus     int
	}{
		{name: "the maximum fully processed nesting", propertyLevels: expandPropertyMaxDepth + 1, wantStatus: http.StatusMultiStatus},
		{name: "one level beyond the limit", propertyLevels: expandPropertyMaxDepth + 2, wantStatus: http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h, user := expandPropertyServer()
			rr := runExpandProperty(t, h, user, "/dav/calendars/1/", buildBody(tt.propertyLevels))
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rr.Code, tt.wantStatus, rr.Body.String())
			}
		})
	}
}

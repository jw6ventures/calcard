package dav

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

// The four RFC 3744 principal reports: acl-principal-prop-set,
// principal-match, principal-property-search, and
// principal-search-property-set.

type aclReportUserRepo struct {
	users map[int64]store.User
}

func (r *aclReportUserRepo) UpsertOAuthUser(context.Context, string, string, string, string) (*store.User, error) {
	return nil, nil
}

func (r *aclReportUserRepo) GetByID(_ context.Context, id int64) (*store.User, error) {
	user, ok := r.users[id]
	if !ok {
		return nil, nil
	}
	return &user, nil
}

func (r *aclReportUserRepo) GetByEmail(_ context.Context, email string) (*store.User, error) {
	for _, user := range r.users {
		if user.PrimaryEmail == email {
			copy := user
			return &copy, nil
		}
	}
	return nil, nil
}

func (r *aclReportUserRepo) ListActive(context.Context) ([]store.User, error) {
	result := make([]store.User, 0, len(r.users))
	for _, user := range r.users {
		result = append(result, user)
	}
	return result, nil
}

func (r *aclReportUserRepo) MarkOnboardingComplete(context.Context, int64) error { return nil }

func TestACLPrincipalPropSetReturnsEveryURLPrincipalOnce(t *testing.T) {
	owner := store.User{ID: 1, PrimaryEmail: "owner@example.com", FullName: "Owner"}
	delegate := store.User{ID: 2, PrimaryEmail: "delegate@example.com", FullName: "Delegate"}
	calendar := store.Calendar{ID: 5, UserID: owner.ID, Name: "Work"}
	h := NewDavServer(Options{Store: &store.Store{
		Users: &aclReportUserRepo{users: map[int64]store.User{1: owner, 2: delegate}},
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: calendar, Editor: true}},
			calendars:  map[int64]*store.Calendar{5: &calendar},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{}},
		ACLEntries: &fakeACLRepo{entries: []store.ACLEntry{
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read", Position: 0},
			{ResourcePath: "/dav/calendars/5", PrincipalHref: "/dav/principals/2/", IsGrant: true, Privilege: "read-acl", Position: 0},
			{ResourcePath: "/dav/principals/2", PrincipalHref: "/dav/principals/1/", IsGrant: true, Privilege: "read", Position: 0},
		}},
	}})
	body := `<d:acl-principal-prop-set xmlns:d="DAV:"><d:prop><d:displayname/></d:prop></d:acl-principal-prop-set>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/5/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &owner))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("REPORT status = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, "/dav/principals/1/", "/dav/principals/2/")
	if count := strings.Count(rr.Body.String(), "/dav/principals/2/"); count != 1 {
		t.Fatalf("delegate principal returned %d times: %s", count, rr.Body.String())
	}
	ms.responseForHref(t, "/dav/principals/2/").assertPropValue(t, davQN("displayname"), http.StatusOK, "Delegate")
}

func TestPrincipalMatchTraversesMembersAndMatchesOwner(t *testing.T) {
	current := &store.User{ID: 1, PrimaryEmail: "owner@example.com"}
	owned := store.Calendar{ID: 5, UserID: current.ID, Name: "Owned"}
	shared := store.Calendar{ID: 6, UserID: 2, Name: "Shared"}
	h := NewDavServer(Options{Store: &store.Store{
		Calendars: &fakeCalendarRepo{
			accessible: []store.CalendarAccess{{Calendar: owned, Editor: true}, {Calendar: shared, Editor: false}},
			calendars:  map[int64]*store.Calendar{5: &owned, 6: &shared},
		},
		Events: &fakeEventRepo{events: map[string]*store.Event{}},
	}})
	body := `<d:principal-match xmlns:d="DAV:"><d:principal-property><d:owner/></d:principal-property><d:prop><d:displayname/></d:prop></d:principal-match>`
	req := httptest.NewRequest("REPORT", "/dav/calendars/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), current))
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("REPORT status = %d, want 207: %s", rr.Code, rr.Body.String())
	}
	ms := decodeMultistatus(t, rr)
	ms.assertHrefs(t, birthdayCalendarHref(), "/dav/calendars/5/")
	ms.responseForHref(t, "/dav/calendars/5/").assertPropValue(t, davQN("displayname"), http.StatusOK, "Owned")
}

func TestPrincipalPropertySearchAndSearchPropertySet(t *testing.T) {
	current := store.User{ID: 1, PrimaryEmail: "owner@example.com", FullName: "Owner"}
	users := &aclReportUserRepo{users: map[int64]store.User{
		1: current,
		2: {ID: 2, PrimaryEmail: "alice@example.com", FullName: "Alice Adams"},
		3: {ID: 3, PrimaryEmail: "malik@example.com", FullName: "Malik Ali"},
		4: {ID: 4, PrimaryEmail: "bob@example.com", FullName: "Bob Brown"},
	}}
	h := NewDavServer(Options{Store: &store.Store{Users: users}})
	propfindReq := httptest.NewRequest("PROPFIND", "/dav/principals/", strings.NewReader(`<d:propfind xmlns:d="DAV:"><d:prop><d:supported-report-set/></d:prop></d:propfind>`))
	propfindReq.Header.Set("Depth", "0")
	propfindReq = propfindReq.WithContext(auth.WithUser(propfindReq.Context(), &current))
	propfindRR := httptest.NewRecorder()
	h.ServeHTTP(propfindRR, propfindReq)
	decodeMultistatus(t, propfindRR).responseForHref(t, "/dav/principals/").assertSupportedReports(t,
		davQN("acl-principal-prop-set"),
		davQN("principal-match"),
		davQN("principal-property-search"),
		davQN("principal-search-property-set"),
	)

	searchBody := `<d:principal-property-search xmlns:d="DAV:"><d:property-search><d:prop><d:displayname/></d:prop><d:match>ali</d:match></d:property-search><d:prop><d:displayname/></d:prop></d:principal-property-search>`
	searchReq := httptest.NewRequest("REPORT", "/dav/principals/", strings.NewReader(searchBody))
	searchReq = searchReq.WithContext(auth.WithUser(searchReq.Context(), &current))
	searchRR := httptest.NewRecorder()
	h.ServeHTTP(searchRR, searchReq)
	if searchRR.Code != http.StatusMultiStatus {
		t.Fatalf("principal-property-search status = %d: %s", searchRR.Code, searchRR.Body.String())
	}
	decodeMultistatus(t, searchRR).assertHrefs(t, "/dav/principals/2/", "/dav/principals/3/")

	setReq := httptest.NewRequest("REPORT", "/dav/principals/", strings.NewReader(`<d:principal-search-property-set xmlns:d="DAV:"/>`))
	setReq = setReq.WithContext(auth.WithUser(setReq.Context(), &current))
	setRR := httptest.NewRecorder()
	h.ServeHTTP(setRR, setReq)
	if setRR.Code != http.StatusOK {
		t.Fatalf("principal-search-property-set = %d: %s", setRR.Code, setRR.Body.String())
	}
	// RFC 3744 §9.5: each searchable property is reported with a DAV:prop
	// naming it and a human-readable DAV:description carrying xml:lang.
	setRoot, err := parseRootElement(setRR.Body.Bytes(), davQN("principal-search-property-set"))
	if err != nil {
		t.Fatalf("decode principal-search-property-set: %v; body: %s", err, setRR.Body.String())
	}
	searchProperty := assertSoleChild(t, setRoot, davQN("principal-search-property"))
	if got := qnList(searchProperty.childNames()); got != qnList([]xml.Name{davQN("prop"), davQN("description")}) {
		t.Fatalf("principal-search-property children = %s, want DAV:prop then DAV:description", got)
	}
	if got := qnList(searchProperty.child(t, davQN("prop")).childNames()); got != qnList([]xml.Name{davQN("displayname")}) {
		t.Errorf("searchable properties = %s, want DAV:displayname", got)
	}
	if got := xmlLangOf(searchProperty.child(t, davQN("description"))); got != "en" {
		t.Errorf("description xml:lang = %q, want %q", got, "en")
	}

	depthReq := httptest.NewRequest("REPORT", "/dav/principals/", strings.NewReader(`<d:principal-search-property-set xmlns:d="DAV:"/>`))
	depthReq.Header.Set("Depth", "1")
	depthReq = depthReq.WithContext(auth.WithUser(depthReq.Context(), &current))
	depthRR := httptest.NewRecorder()
	h.ServeHTTP(depthRR, depthReq)
	if depthRR.Code != http.StatusBadRequest {
		t.Fatalf("Depth: 1 status = %d, want 400: %s", depthRR.Code, depthRR.Body.String())
	}

}

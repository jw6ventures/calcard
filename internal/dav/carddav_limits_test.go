package dav

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestCardDAVQueryMetadataLimits(t *testing.T) {
	filter := `<card:filter><card:prop-filter name="FN"/></card:filter>`
	selector := `<card:prop name="FN"/>`
	for _, tc := range []struct {
		name                                        string
		selectors, byteLimit, propertyLimit, status int
	}{
		{"at byte budget", 1, len(filter) + len(`<card:address-data></card:address-data>`) + len(selector), 10, 207},
		{"one byte over", 1, len(filter) + len(`<card:address-data></card:address-data>`) + len(selector) - 1, 10, 400},
		{"at selector budget", 3, 65536, 3, 207},
		{"one selector over", 4, 65536, 3, 400},
		{"limits disabled", 101, math.MaxInt, math.MaxInt, 207},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxCardDAVQueryBytes = tc.byteLimit
			cfg.DAV.MaxAddressDataProperties = tc.propertyLimit
			h := cardLimitsTestServer(t, cfg, 1)
			projection := `<card:address-data>` + strings.Repeat(selector, tc.selectors) + `</card:address-data>`
			body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"><D:prop>` + projection + `</D:prop>` + filter + `</card:addressbook-query>`
			for _, rr := range []*httptest.ResponseRecorder{
				cardLimitsReportRequest(t, h, body),
				cardLimitsObjectReportRequest(t, h, "contact-00001", body),
			} {
				if rr.Code != tc.status {
					t.Fatalf("status = %d, want %d; %s", rr.Code, tc.status, rr.Body.String())
				}
			}
		})
	}
}

func TestCardDAVMultigetBoundsAddressDataSelectors(t *testing.T) {
	for _, count := range []int{2, 3} {
		cfg := &config.Config{}
		cfg.DAV.MaxAddressDataProperties = 2
		h := cardLimitsTestServer(t, cfg, 1)
		body := strings.Replace(cardMultigetBody(1), `<D:getetag/>`, `<card:address-data>`+strings.Repeat(`<card:prop name="FN"/>`, count)+`</card:address-data>`, 1)
		rr := cardLimitsReportRequest(t, h, body)
		if count == 2 {
			decodeMultistatus(t, rr)
		} else if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rr.Code)
		}
	}
}

func TestCardDAVMetadataBudgetCountsAllSubtrees(t *testing.T) {
	for _, tc := range []struct {
		body      string
		byteLimit int
	}{
		{`<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:filter/><card:filter/></card:addressbook-query>`, len(`<card:filter/>`)},
		{`<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav"><card:address-data><card:prop name="FN"/></card:address-data><card:address-data><card:prop name="EMAIL"/></card:address-data></card:addressbook-query>`, 65536},
	} {
		cfg := &config.Config{}
		cfg.DAV.MaxCardDAVQueryBytes = tc.byteLimit
		cfg.DAV.MaxAddressDataProperties = 1
		h := cardLimitsTestServer(t, cfg, 1)
		if fault := h.checkReportBodyLimits([]byte(tc.body)); fault == nil {
			t.Fatal("separate metadata subtrees escaped the shared budget")
		}
	}
}

func TestPreparedCardDAVTextMatchesKeepCollationAndParameterSemantics(t *testing.T) {
	for _, tc := range []struct {
		collation, negate string
		matches           int
	}{
		{"i;unicode-casemap", "no", 1},
		{"i;ascii-casemap", "no", 0},
		{"i;unicode-casemap", "yes", 0},
	} {
		t.Run(tc.collation+"/"+tc.negate, func(t *testing.T) {
			h := cardLimitsTestServer(t, &config.Config{}, 1)
			for _, contact := range h.store.Contacts.(*fakeContactRepo).contacts {
				contact.RawVCard = buildVCard("3.0", "UID:"+contact.UID, "FN:Straße", "EMAIL;TYPE=HOME:test@example.com")
			}
			body := fmt.Sprintf(`<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"><D:prop><D:getetag/></D:prop><card:filter test="allof"><card:prop-filter name="FN"><card:text-match collation="%s" match-type="equals" negate-condition="%s"> STRASSE </card:text-match></card:prop-filter><card:prop-filter name="EMAIL"><card:param-filter name="TYPE"><card:text-match collation="i;ascii-casemap">home</card:text-match></card:param-filter></card:prop-filter></card:filter></card:addressbook-query>`, tc.collation, tc.negate)
			ms := decodeMultistatus(t, cardLimitsReportRequest(t, h, body))
			if len(ms.Responses) != tc.matches {
				t.Fatalf("matches = %d, want %d", len(ms.Responses), tc.matches)
			}
		})
	}
}

func BenchmarkPreparedCardDAVTextMatch(b *testing.B) {
	for _, size := range []int{64, 65536} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			filter := &cardFilter{PropFilter: []cardPropFilter{{Name: "NOTE", TextMatch: &textMatch{Text: strings.Repeat("z", size)}}}}
			prepareCardFilter(filter)
			contact := store.Contact{RawVCard: buildVCard("3.0", "UID:bench", "FN:Person", "NOTE:a note")}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				contactMatchesCardFilter(contact, filter)
			}
		})
	}
}

// cardLimitsTestServer builds an address book of size contacts behind the given
// limits, so a test can drive one REPORT through the whole handler path rather
// than against a query helper in isolation. It is the CardDAV counterpart of
// limitsTestServer.
func cardLimitsTestServer(t *testing.T, cfg *config.Config, contacts int) *DavServer {
	t.Helper()
	bookRepo := &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
		5: {ID: 5, UserID: 1, Name: "Contacts", UpdatedAt: limitsFixtureModified, CTag: 1},
	}}
	contactRepo := &fakeContactRepo{contacts: make(map[string]*store.Contact, contacts)}
	for id := int64(1); id <= int64(contacts); id++ {
		uid := fmt.Sprintf("contact-%05d", id)
		contactRepo.contacts["5:"+uid] = &store.Contact{
			ID:            id,
			AddressBookID: 5,
			UID:           uid,
			ResourceName:  uid,
			ETag:          "e",
			LastModified:  limitsFixtureModified,
			RawVCard:      buildVCard("3.0", "UID:"+uid, "FN:Person "+uid, "NOTE:a note"),
		}
	}
	return NewDavServer(Options{Config: cfg, Store: &store.Store{
		AddressBooks:     bookRepo,
		Contacts:         contactRepo,
		DeletedResources: &fakeDeletedResourceRepo{},
	}})
}

func cardLimitsReportRequest(t *testing.T, h *DavServer, body string) *httptest.ResponseRecorder {
	t.Helper()
	// RFC 6352 §8.7 fixes addressbook-multiget at Depth: 0; §8.6 reaches the
	// collection members at Depth: 1.
	depth := "1"
	if strings.Contains(body, "addressbook-multiget") {
		depth = "0"
	}
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/", strings.NewReader(body))
	req.Header.Set("Depth", depth)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	return rr
}

// cardLimitsObjectReportRequest drives the same report at one address object
// resource. RFC 6352 §8.6 serves addressbook-query there as well, and the Depth
// header is what separates it from a report the resource does not support.
func cardLimitsObjectReportRequest(t *testing.T, h *DavServer, resourceName, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("REPORT", "/dav/addressbooks/5/"+resourceName+".vcf", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	return rr
}

// cardFilterBody wraps propFilters CARDDAV:prop-filter elements in one
// CARDDAV:filter. Each one is evaluated against every candidate contact, and
// each evaluation scans every property of the vCard, so the element count
// multiplies the collection scan exactly as the CalDAV filter count does.
func cardFilterBody(propFilters int) string {
	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	body.WriteString(`<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">`)
	body.WriteString(`<D:prop><D:getetag/></D:prop><card:filter>`)
	for i := 0; i < propFilters; i++ {
		body.WriteString(`<card:prop-filter name="NOTE"><card:text-match>zz</card:text-match></card:prop-filter>`)
	}
	body.WriteString(`</card:filter></card:addressbook-query>`)
	return body.String()
}

const cardLimitsQueryBody = `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter><card:prop-filter name="FN"/></card:filter>
</card:addressbook-query>`

// RFC 6352 §10.5 evaluates every CARDDAV:prop-filter against every candidate
// contact, so an unbounded element count is the CPU exhaustion an address book
// REPORT has to be guarded against just as a calendar REPORT is. RFC 6352
// defines no CARDDAV:valid-filter, and §8.5 and §8.6 scope
// CARDDAV:supported-filter to a filter naming a vCard property or parameter the
// server cannot query, so a filter past the server's budget violates no named
// precondition and is an ordinary malformed request.
func TestAddressBookQueryOverTheFilterElementLimitIsRefused(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxFilterElements = 10
	h := cardLimitsTestServer(t, cfg, 3)

	// One CARDDAV:filter plus ten CARDDAV:prop-filter children, each carrying a
	// CARDDAV:text-match, is 21 elements against a budget of 10.
	rr := cardLimitsReportRequest(t, h, cardFilterBody(10))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("an over-budget filter returned a multistatus: %s", rr.Body.String())
	}
	// RFC 6352 §10 reserves the CardDAV namespace for the elements the CardDAV
	// specifications define, so the refusal may not invent a condition in it.
	if strings.Contains(rr.Body.String(), "valid-filter") {
		t.Fatalf("the refusal named a condition RFC 6352 does not define: %s", rr.Body.String())
	}
}

// The budget is inclusive: a filter carrying exactly as many elements as the
// server evaluates is answered, and only the one past it is refused.
func TestAddressBookQueryAtTheFilterElementLimitIsAnswered(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	// One CARDDAV:filter, three CARDDAV:prop-filter and three
	// CARDDAV:text-match elements is seven.
	cfg.DAV.MaxFilterElements = 7
	h := cardLimitsTestServer(t, cfg, 3)

	rr := cardLimitsReportRequest(t, h, cardFilterBody(3))

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207; body: %s", rr.Code, rr.Body.String())
	}
}

// The CalDAV budget answers CALDAV:valid-filter, which RFC 4791 §7.8.7 defines
// and RFC 6352 has no counterpart for. Pinned so the two grammars keep their own
// answers as the shared bound moves.
func TestCalendarQueryOverTheFilterElementLimitStillFailsValidFilter(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxFilterElements = 2
	h := limitsTestServer(t, cfg, 1)

	rr := limitsReportRequest(t, h, limitsCalendarQueryBody)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-filter"))
}

// RFC 6352 §8.6.2: a server may limit the resources in a response "to limit the
// amount of work expended in processing a query", and a result truncated for
// that reason is a 207 carrying a 507 DAV:response for the Request-URI with the
// DAV:number-of-matches-within-limits precondition. A filter matching nothing
// still reads and parses every candidate row, so the row budget is what bounds a
// query whose match count never grows.
//
// The budget bounds the rows the query examines, so the matches it reports are
// every match among those rows: a page reaching past the budget is trimmed to
// what is left of it rather than discarded. The counts are exact because the
// budget and the page size are deliberately unrelated -- a budget under one
// page, and a budget falling inside one -- and dropping the page instead of
// trimming it answers 0 matches for the first and 256 for the second.
func TestAddressBookQueryOverTheCandidateRowLimitTruncatesWithNumberOfMatches(t *testing.T) {
	for _, tc := range []struct {
		name     string
		budget   int
		contacts int
	}{
		{name: "budget under one page", budget: 100, contacts: 400},
		{name: "budget inside a page", budget: 300, contacts: 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
			cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
			cfg.DAV.MaxReportCandidateRows = tc.budget
			h := cardLimitsTestServer(t, cfg, tc.contacts)

			rr := cardLimitsReportRequest(t, h, cardLimitsQueryBody)

			ms := decodeMultistatus(t, rr)
			assertTruncationMarker(t, ms, "/dav/addressbooks/5/")
			// Every contact matches the filter, so the budget is the match count
			// and the marker is the one response beside them.
			if len(ms.Responses) != tc.budget+1 {
				t.Fatalf("responses = %d, want the %d rows the budget allows plus the marker",
					len(ms.Responses), tc.budget)
			}
		})
	}
}

// RFC 6352 §8.6 serves addressbook-query on an address object resource as well
// as on a collection, and one resource is one read: paging the book to find it
// would spend the candidate-row budget on rows the report cannot answer over,
// and a book past that budget would answer the truncation marker instead of the
// resource the Request-URI names.
func TestAddressBookObjectQueryAnswersTheResourcePastTheRowBudget(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxReportCandidateRows = 10
	h := cardLimitsTestServer(t, cfg, 400)

	rr := cardLimitsObjectReportRequest(t, h, "contact-00399", cardLimitsQueryBody)

	ms := decodeMultistatus(t, rr)
	assertNoTruncationMarker(t, ms)
	if len(ms.Responses) != 1 || len(ms.Responses[0].Hrefs) != 1 ||
		ms.Responses[0].Hrefs[0] != "/dav/addressbooks/5/contact-00399.vcf" {
		t.Fatalf("responses = %#v, want the one resource the Request-URI names", ms.Responses)
	}
}

// A filter the resource does not match answers an empty multistatus rather than
// the resource, and still reads no more than the one row.
func TestAddressBookObjectQueryAppliesTheFilterToTheResource(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := cardLimitsTestServer(t, cfg, 3)

	body := `<?xml version="1.0" encoding="utf-8"?>
<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">
  <D:prop><D:getetag/></D:prop>
  <card:filter><card:prop-filter name="NICKNAME"/></card:filter>
</card:addressbook-query>`
	rr := cardLimitsObjectReportRequest(t, h, "contact-00002", body)

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 0 {
		t.Fatalf("responses = %#v, want none: the resource carries no NICKNAME", ms.Responses)
	}
}

// A Request-URI naming no stored resource answers an empty multistatus, which is
// what the collection path answered for the same name.
func TestAddressBookObjectQueryOnAMissingResourceAnswersNothing(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := cardLimitsTestServer(t, cfg, 3)

	rr := cardLimitsObjectReportRequest(t, h, "contact-09999", cardLimitsQueryBody)

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 0 {
		t.Fatalf("responses = %#v, want none for a resource the book does not hold", ms.Responses)
	}
}

// At the budget the whole collection is read, so the answer is complete and
// carries no truncation marker.
func TestAddressBookQueryAtTheCandidateRowLimitIsComplete(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxReportCandidateRows = 300
	h := cardLimitsTestServer(t, cfg, 300)

	rr := cardLimitsReportRequest(t, h, cardLimitsQueryBody)

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 300 {
		t.Fatalf("responses = %d, want 300", len(ms.Responses))
	}
	assertNoTruncationMarker(t, ms)
}

// The server's own response ceiling truncates through the same §8.6.2 form, and
// the 507 marker occupies one of the response slots it allows.
func TestAddressBookQueryOverTheResponseLimitTruncatesWithNumberOfMatches(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 3
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := cardLimitsTestServer(t, cfg, 10)

	rr := cardLimitsReportRequest(t, h, cardLimitsQueryBody)

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 3 {
		t.Fatalf("responses = %d, want 3 -- two matches and the 507 marker", len(ms.Responses))
	}
	assertTruncationMarker(t, ms, "/dav/addressbooks/5/")
}

// At the response ceiling the result is complete, so no slot is spent on a
// marker.
func TestAddressBookQueryAtTheResponseLimitIsComplete(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = 3
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	h := cardLimitsTestServer(t, cfg, 3)

	rr := cardLimitsReportRequest(t, h, cardLimitsQueryBody)

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 3 {
		t.Fatalf("responses = %d, want 3", len(ms.Responses))
	}
	assertNoTruncationMarker(t, ms)
}

// RFC 6352 §8.7 owes one DAV:response per DAV:href and states no truncation
// rule of its own -- §8.6.2 is scoped to addressbook-query -- so a list past
// what the server will answer is refused with the RFC 4918 capacity status
// rather than answered over its leading hrefs.
func TestAddressBookMultiGetOverTheHrefLimitIsRefused(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxMultigetHrefs = 4
	h := cardLimitsTestServer(t, cfg, 5)

	rr := cardLimitsReportRequest(t, h, cardMultigetBody(5))

	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("status = %d, want 507; body: %s", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "multistatus") {
		t.Fatalf("an over-limit multiget returned a multistatus: %s", rr.Body.String())
	}
}

// At the limit every href is answered, which is the one-response-per-href rule
// the refusal above protects.
func TestAddressBookMultiGetAtTheHrefLimitAnswersEveryHref(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxMultistatusResponses = defaultMaxMultistatusResponses
	cfg.DAV.MaxMultistatusBytes = defaultMaxMultistatusBytes
	cfg.DAV.MaxMultigetHrefs = 5
	h := cardLimitsTestServer(t, cfg, 5)

	rr := cardLimitsReportRequest(t, h, cardMultigetBody(5))

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 5 {
		t.Fatalf("responses = %d, want one per href", len(ms.Responses))
	}
}

func cardMultigetBody(hrefs int) string {
	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	body.WriteString(`<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:">`)
	body.WriteString(`<D:prop><D:getetag/></D:prop>`)
	for id := 1; id <= hrefs; id++ {
		fmt.Fprintf(&body, `<D:href>/dav/addressbooks/5/contact-%05d.vcf</D:href>`, id)
	}
	body.WriteString(`</card:addressbook-multiget>`)
	return body.String()
}

// assertTruncationMarker checks for the DAV:response RFC 6352 §8.6.2 requires on
// a truncated result: the Request-URI at 507 carrying
// DAV:number-of-matches-within-limits.
func assertTruncationMarker(t *testing.T, ms davMultistatus, requestURI string) {
	t.Helper()
	for _, resp := range ms.Responses {
		if len(resp.Hrefs) != 1 || resp.Hrefs[0] != requestURI || !strings.Contains(resp.Status, "507") {
			continue
		}
		if resp.Error == nil {
			t.Fatalf("the 507 response for %s carries no DAV:error", requestURI)
		}
		for _, condition := range resp.Error.Conditions {
			if condition.Name == davQN("number-of-matches-within-limits") {
				return
			}
		}
		t.Fatalf("the 507 response for %s carries no DAV:number-of-matches-within-limits", requestURI)
	}
	t.Fatalf("no 507 DAV:response for %s in %d responses", requestURI, len(ms.Responses))
}

func assertNoTruncationMarker(t *testing.T, ms davMultistatus) {
	t.Helper()
	for _, resp := range ms.Responses {
		if strings.Contains(resp.Status, "507") {
			t.Fatalf("a complete result carries a truncation marker for %v", resp.Hrefs)
		}
	}
}

func TestCardDAVRejectsExpensiveQueryMetadataBeforeReadingContacts(t *testing.T) {
	for _, tc := range []struct{ name, projection, filter string }{
		{"long text match", "<D:getetag/>", `<card:prop-filter name="NOTE"><card:text-match>` + strings.Repeat("z", 8000000) + `</card:text-match></card:prop-filter>`},
		{"many selectors", `<card:address-data>` + strings.Repeat(`<card:prop name="X-NO-MATCH"/>`, 100000) + `</card:address-data>`, `<card:prop-filter name="FN"/>`},
		{"long selector name", `<card:address-data><card:prop name="` + strings.Repeat("X", 8000000) + `"/></card:address-data>`, `<card:prop-filter name="FN"/>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := cardLimitsTestServer(t, &config.Config{}, 1)
			body := `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:"><D:prop>` + tc.projection + `</D:prop><card:filter>` + tc.filter + `</card:filter></card:addressbook-query>`
			rr := cardLimitsReportRequest(t, h, body)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
			if calls := h.store.Contacts.(*fakeContactRepo).pageLookupCount; calls != 0 {
				t.Fatalf("contact page reads = %d, want 0", calls)
			}
		})
	}
}

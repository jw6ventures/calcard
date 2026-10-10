package dav

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

const propSelectorTestNamespace = "urn:x"

// customPropElements names distinct properties in the propSelectorTestNamespace,
// each repeated times over, interleaved so a repeat never sits next to its
// first occurrence.
func customPropElements(distinct, repeats int) string {
	var b strings.Builder
	for r := 0; r < repeats; r++ {
		for i := 0; i < distinct; i++ {
			fmt.Fprintf(&b, `<X:p%d/>`, i)
		}
	}
	return b.String()
}

func propfindSelectorBody(props string) string {
	return `<?xml version="1.0" encoding="utf-8"?><D:propfind xmlns:D="DAV:" xmlns:X="urn:x"><D:prop>` + props + `</D:prop></D:propfind>`
}

func propSelectorPropfind(t testing.TB, h *DavServer, target, depth, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("PROPFIND", target, strings.NewReader(body))
	req.Header.Set("Depth", depth)
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Propfind(rr, req)
	return rr
}

// propSelectorReports is every REPORT whose DAV:prop selector goes through the
// shared property filter, each with the body that carries props.
var propSelectorReports = []struct {
	name     string
	calendar bool
	body     func(props string) string
}{
	{"calendar-query", true, func(props string) string {
		return `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:X="urn:x"><D:prop>` + props +
			`</D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`
	}},
	{"calendar-multiget", true, func(props string) string {
		return `<C:calendar-multiget xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav" xmlns:X="urn:x"><D:prop>` + props +
			`</D:prop><D:href>/dav/calendars/1/event-1.ics</D:href><D:href>/dav/calendars/1/event-2.ics</D:href></C:calendar-multiget>`
	}},
	{"calendar sync-collection", true, func(props string) string {
		return `<D:sync-collection xmlns:D="DAV:" xmlns:X="urn:x"><D:sync-token/><D:prop>` + props + `</D:prop></D:sync-collection>`
	}},
	{"addressbook-query", false, func(props string) string {
		return `<card:addressbook-query xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:" xmlns:X="urn:x"><D:prop>` + props +
			`</D:prop><card:filter><card:prop-filter name="FN"/></card:filter></card:addressbook-query>`
	}},
	{"addressbook-multiget", false, func(props string) string {
		return `<card:addressbook-multiget xmlns:card="urn:ietf:params:xml:ns:carddav" xmlns:D="DAV:" xmlns:X="urn:x"><D:prop>` + props +
			`</D:prop><D:href>/dav/addressbooks/5/contact-00001.vcf</D:href><D:href>/dav/addressbooks/5/contact-00002.vcf</D:href></card:addressbook-multiget>`
	}},
	{"addressbook sync-collection", false, func(props string) string {
		return `<D:sync-collection xmlns:D="DAV:" xmlns:X="urn:x"><D:sync-token/><D:prop>` + props + `</D:prop></D:sync-collection>`
	}},
}

func propSelectorReport(t *testing.T, cfg *config.Config, calendar bool, body string) *httptest.ResponseRecorder {
	t.Helper()
	if calendar {
		return limitsReportRequest(t, limitsTestServer(t, cfg, 2), body)
	}
	return cardLimitsReportRequest(t, cardLimitsTestServer(t, cfg, 2), body)
}

func propSelectorCalendarReport(tb testing.TB, h *DavServer, body string) *httptest.ResponseRecorder {
	tb.Helper()
	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	return rr
}

// propertyOccurrences counts, per property name, the elements a response's
// propstats carry, in the order they appear.
func propertyOccurrences(resp davResponse) ([]xml.Name, map[xml.Name]int) {
	var order []xml.Name
	counts := map[xml.Name]int{}
	for _, ps := range resp.Propstats {
		for _, p := range ps.Prop.Props {
			if counts[p.Name] == 0 {
				order = append(order, p.Name)
			}
			counts[p.Name]++
		}
	}
	return order, counts
}

func assertEachPropertyAnsweredOnce(t *testing.T, ms davMultistatus, wantCustom []string) {
	t.Helper()
	if len(ms.Responses) == 0 {
		t.Fatal("multistatus carries no responses")
	}
	for _, resp := range ms.Responses {
		if len(resp.Propstats) == 0 {
			continue
		}
		order, counts := propertyOccurrences(resp)
		for name, n := range counts {
			if n != 1 {
				t.Errorf("%v: %s answered %d times, want once", resp.Hrefs, qnString(name), n)
			}
		}
		if counts[davQN("getetag")] != 1 {
			t.Errorf("%v: DAV:getetag answered %d times, want once", resp.Hrefs, counts[davQN("getetag")])
		}
		var custom []string
		for _, name := range order {
			if name.Space == propSelectorTestNamespace {
				custom = append(custom, name.Local)
			}
		}
		if strings.Join(custom, ",") != strings.Join(wantCustom, ",") {
			t.Errorf("%v: custom properties answered in order %v, want %v", resp.Hrefs, custom, wantCustom)
		}
	}
}

// RFC 4918 §9.1 answers each requested property in one propstat. A name the
// request repeats is still one property, so it is answered once, in the
// position of its first occurrence.
func TestPropfindAnswersARepeatedPropertyOnce(t *testing.T) {
	h := limitsTestServer(t, &config.Config{}, 2)
	props := `<D:getetag/>` + customPropElements(3, 3) + `<D:getetag/>`

	rr := propSelectorPropfind(t, h, "/dav/calendars/1/", "1", propfindSelectorBody(props))

	ms := decodeMultistatus(t, rr)
	if len(ms.Responses) != 3 {
		t.Fatalf("responses = %d, want the collection and its 2 events", len(ms.Responses))
	}
	assertEachPropertyAnsweredOnce(t, ms, []string{"p0", "p1", "p2"})
}

func TestReportsAnswerARepeatedPropertyOnce(t *testing.T) {
	props := `<D:getetag/>` + customPropElements(3, 3) + `<D:getetag/>`
	for _, report := range propSelectorReports {
		t.Run(report.name, func(t *testing.T) {
			rr := propSelectorReport(t, &config.Config{}, report.calendar, report.body(props))
			assertEachPropertyAnsweredOnce(t, decodeMultistatus(t, rr), []string{"p0", "p1", "p2"})
		})
	}
}

// The requested-property limit counts distinct names, so repeating a name
// within the limit costs nothing against it, and 0 turns the limit off as it
// does every DAV limit.
func TestPropfindRequestedPropertyLimit(t *testing.T) {
	for _, tc := range []struct {
		name               string
		limit              int
		distinct, repeats  int
		wantStatus         int
		wantMultistatusLen int
	}{
		{"at the limit", 4, 3, 1, http.StatusMultiStatus, 3},
		{"repeats within the limit", 4, 3, 50, http.StatusMultiStatus, 3},
		{"one past the limit", 4, 4, 1, http.StatusRequestEntityTooLarge, 0},
		{"limit disabled", math.MaxInt, 2000, 1, http.StatusMultiStatus, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxRequestedProperties = tc.limit
			h := limitsTestServer(t, cfg, 2)
			// DAV:getetag is one of the distinct names counted.
			body := propfindSelectorBody(`<D:getetag/>` + customPropElements(tc.distinct, tc.repeats))

			rr := propSelectorPropfind(t, h, "/dav/calendars/1/", "1", body)

			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body: %s", rr.Code, tc.wantStatus, rr.Body.String())
			}
			if tc.wantStatus == http.StatusMultiStatus {
				if got := len(decodeMultistatus(t, rr).Responses); got != tc.wantMultistatusLen {
					t.Fatalf("responses = %d, want %d", got, tc.wantMultistatusLen)
				}
			}
		})
	}
}

func TestPropfindRequestedPropertyLimitDefaultsWhenUnconfigured(t *testing.T) {
	h := limitsTestServer(t, nil, 1)

	at := propSelectorPropfind(t, h, "/dav/calendars/1/", "0", propfindSelectorBody(customPropElements(defaultMaxRequestedProperties, 1)))
	if at.Code != http.StatusMultiStatus {
		t.Fatalf("status at the default limit = %d, want 207", at.Code)
	}
	over := propSelectorPropfind(t, h, "/dav/calendars/1/", "0", propfindSelectorBody(customPropElements(defaultMaxRequestedProperties+1, 1)))
	if over.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status past the default limit = %d, want 413", over.Code)
	}
}

// A REPORT past a body limit is a malformed request, which is how every
// REPORT body bound with no named precondition already answers.
func TestReportsRequestedPropertyLimit(t *testing.T) {
	for _, report := range propSelectorReports {
		t.Run(report.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.DAV.MaxRequestedProperties = 4

			at := propSelectorReport(t, cfg, report.calendar, report.body(`<D:getetag/>`+customPropElements(3, 20)))
			decodeMultistatus(t, at)

			over := propSelectorReport(t, cfg, report.calendar, report.body(`<D:getetag/>`+customPropElements(4, 1)))
			if over.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", over.Code, over.Body.String())
			}
		})
	}
}

// DAV:expand-property names its properties by DAV:property elements rather
// than DAV:prop children, at every nesting level (RFC 3253 §3.8), and each
// name is answered for every resource the expansion reaches.
func TestExpandPropertyRequestedPropertyLimit(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxRequestedProperties = 3
	h := limitsTestServer(t, cfg, 1)
	report := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(body))
		req.Header.Set("Depth", "0")
		req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
		rr := httptest.NewRecorder()
		h.Report(rr, req)
		return rr
	}

	at := report(`<D:expand-property xmlns:D="DAV:"><D:property name="getetag"/><D:property name="owner"><D:property name="displayname"/><D:property name="getetag"/></D:property></D:expand-property>`)
	decodeMultistatus(t, at)

	over := report(`<D:expand-property xmlns:D="DAV:"><D:property name="getetag"/><D:property name="owner"><D:property name="displayname"/><D:property name="p" namespace="urn:x"/></D:property></D:expand-property>`)
	if over.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", over.Code, over.Body.String())
	}
}

// The limit is enforced from the raw body, ahead of the struct decode that
// materializes every name: a body of one distinct name repeated many times is
// within a limit of one, and two distinct names are refused even when one of
// them only appears after a long run of repeats.
func TestRequestedPropertyLimitCountsDistinctNamesInTheBody(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxRequestedProperties = 1
	h := limitsTestServer(t, cfg, 1)
	repeated := strings.Repeat(`<X:p0/>`, 20000)

	if rr := propSelectorPropfind(t, h, "/dav/calendars/1/", "0", propfindSelectorBody(repeated)); rr.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND of one repeated name: status = %d, want 207", rr.Code)
	}
	if rr := propSelectorPropfind(t, h, "/dav/calendars/1/", "0", propfindSelectorBody(repeated+`<X:p1/>`)); rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("PROPFIND of two names: status = %d, want 413", rr.Code)
	}
	query := propSelectorReports[0].body
	if fault := h.checkReportBodyLimits([]byte(query(repeated))); fault != nil {
		t.Fatalf("REPORT of one repeated name refused: %v", fault)
	}
	if fault := h.checkReportBodyLimits([]byte(query(repeated + `<X:p1/>`))); fault == nil {
		t.Fatal("REPORT of two names escaped the limit")
	}
}

// With the limit turned off nothing can exceed it, so neither method pays to
// count: PROPFIND skips its pre-pass outright, which allocates nothing, and the
// REPORT pass records no names.
func TestUnlimitedRequestedPropertiesAreNotCounted(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxRequestedProperties = math.MaxInt
	h := limitsTestServer(t, cfg, 1)
	body := []byte(propfindSelectorBody(customPropElements(5000, 1)))

	if allocs := testing.AllocsPerRun(10, func() {
		if h.propfindBodyExceedsPropertyLimit(body) {
			t.Fatal("unlimited PROPFIND refused")
		}
	}); allocs != 0 {
		t.Fatalf("unlimited PROPFIND pre-pass allocated %.0f times per request, want it skipped", allocs)
	}
	if rr := propSelectorPropfind(t, h, "/dav/calendars/1/", "0", string(body)); rr.Code != http.StatusMultiStatus {
		t.Fatalf("unlimited PROPFIND status = %d, want 207", rr.Code)
	}

	elements := make([]xml.StartElement, 5000)
	for i := range elements {
		elements[i] = xml.StartElement{Name: xml.Name{Space: propSelectorTestNamespace, Local: fmt.Sprintf("p%d", i)}}
	}
	counter := requestedPropertyCounter{limit: math.MaxInt}
	prop := xml.StartElement{Name: davQName("prop")}
	if allocs := testing.AllocsPerRun(10, func() {
		counter.start(prop, 1)
		for _, element := range elements {
			if counter.start(element, 2) {
				t.Fatal("unlimited counter reported the limit exceeded")
			}
		}
		counter.end(1)
	}); allocs != 0 || counter.names != nil {
		t.Fatalf("unlimited counter allocated %.0f times and kept %d names, want it to record nothing", allocs, len(counter.names))
	}
	if fault := h.checkReportBodyLimits([]byte(propSelectorReports[0].body(customPropElements(5000, 1)))); fault != nil {
		t.Fatalf("unlimited REPORT refused: %v", fault)
	}
}

// Names inside a property value or a nested selector grammar are not
// requested properties: only the direct children of DAV:prop are.
func TestRequestedPropertyLimitCountsOnlyDirectChildrenOfProp(t *testing.T) {
	cfg := &config.Config{}
	cfg.DAV.MaxRequestedProperties = 2
	h := limitsTestServer(t, cfg, 1)
	body := `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/>` +
		`<C:calendar-data><C:comp name="VCALENDAR"><C:prop name="VERSION"/><C:comp name="VEVENT"><C:prop name="SUMMARY"/><C:prop name="UID"/></C:comp></C:comp></C:calendar-data>` +
		`</D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`

	decodeMultistatus(t, limitsReportRequest(t, h, body))
}

// propstatNamesInOrder renders a response and lists the property names of its
// propstat with the given status, in document order.
func propstatNamesInOrder(t *testing.T, resp response, status string) []string {
	t.Helper()
	var buf bytes.Buffer
	if err := xml.NewEncoder(&buf).Encode(newMultistatus([]response{resp}, "")); err != nil {
		t.Fatalf("encode: %v", err)
	}
	ms, err := parseMultistatus(buf.Bytes())
	if err != nil {
		t.Fatalf("parse: %v; body: %s", err, buf.String())
	}
	var names []string
	for _, ps := range ms.Responses[0].Propstats {
		if !strings.Contains(ps.Status, status) {
			continue
		}
		for _, p := range ps.Prop.Props {
			names = append(names, qnString(p.Name))
		}
	}
	return names
}

// The 404 propstat lists the properties the resource kind lacks in table
// order, then the requested names the resource does not carry in request
// order. A name the resource does carry moves to the 200 propstat and leaves
// no gap or reordering behind it; where the resource carries a name twice, its
// first property answers.
func TestPropertyFilterKeepsRequestOrderAroundAnsweredCustomProperties(t *testing.T) {
	x := func(local string) xml.Name { return xml.Name{Space: propSelectorTestNamespace, Local: local} }
	resp := calendarObjectPropfindResponse()
	resp.Propstat[0].Prop.CustomXML = []XMLProperty{
		{Name: x("d"), Value: rawXMLValue("two")},
		{Name: x("b"), Value: rawXMLValue("one")},
		{Name: x("b"), Value: rawXMLValue("three")},
	}
	req := &propfindRequest{suppressData: true, Prop: &propfindPropQuery{propertySelection: propertySelection{
		DisplayName: &struct{}{},
		GetETag:     &struct{}{},
		CustomXML:   []xml.Name{x("a"), x("b"), x("c"), x("a"), x("d"), x("e"), x("b")},
	}}}

	filtered := filterNonPrincipalPropfindResponse(resp, req)

	if got, want := strings.Join(propstatNamesInOrder(t, filtered, "200"), " "), "{DAV:}getetag {urn:x}b {urn:x}d"; got != want {
		t.Errorf("200 propstat = %s, want %s", got, want)
	}
	if got, want := strings.Join(propstatNamesInOrder(t, filtered, "404"), " "), "{DAV:}displayname {urn:x}a {urn:x}c {urn:x}e"; got != want {
		t.Errorf("404 propstat = %s, want %s", got, want)
	}
	if answered, _ := filtered.Propstat[0].Prop.customXMLProperty(x("b")); answered.Value != rawXMLValue("one") {
		t.Errorf("{urn:x}b answered with %v, want the resource's first value", answered.Value)
	}
}

// filterRequestHeap is the heap the property filter allocates answering one
// request that names customNames properties over the given number of
// responses, each of which carries one of the requested names as a dead
// property.
func filterRequestHeap(customNames, responses int) float64 {
	names := make([]xml.Name, customNames)
	for i := range names {
		names[i] = xml.Name{Space: propSelectorTestNamespace, Local: fmt.Sprintf("p%d", i)}
	}
	base := make([]response, responses)
	for i := range base {
		base[i] = calendarObjectPropfindResponse()
		base[i].Propstat[0].Prop.CustomXML = []XMLProperty{{Name: names[len(names)/2], Value: rawXMLValue("v")}}
	}
	work := make([]response, responses)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const rounds = 4
	for range rounds {
		req := &propfindRequest{suppressData: true, Prop: &propfindPropQuery{propertySelection: propertySelection{
			GetETag:   &struct{}{},
			CustomXML: names,
		}}}
		copy(work, base)
		for i := range work {
			work[i] = filterNonPrincipalPropfindResponse(work[i], req)
		}
	}
	runtime.ReadMemStats(&after)
	return float64(after.TotalAlloc-before.TotalAlloc) / rounds
}

// filterHeapPerResponse is the heap one more response adds to a request
// naming customNames properties, with the once-per-request cost taken out. It
// keeps the smallest of a few measurements, since TotalAlloc also counts
// whatever another goroutine allocates meanwhile.
func filterHeapPerResponse(customNames int) float64 {
	const few, many = 32, 512
	smallest := math.Inf(1)
	for range 3 {
		smallest = min(smallest, (filterRequestHeap(customNames, many)-filterRequestHeap(customNames, few))/(many-few))
	}
	return smallest
}

// The requested names are request-wide, so the filter's per-response heap must
// not grow with how many the request names: a cost of responses times names
// would exhaust memory before the multistatus byte limit could refuse the
// request.
func TestPropertyFilterHeapPerResponseIsIndependentOfTheRequestedNames(t *testing.T) {
	small := filterHeapPerResponse(8)
	large := filterHeapPerResponse(1000)
	t.Logf("per-response heap: %.0f bytes at 8 names, %.0f at 1000", small, large)
	if growth := large - small; growth > 256 {
		t.Fatalf("per-response heap grew by %.0f bytes going from 8 to 1000 requested names (%.0f -> %.0f); want it independent of the name count", growth, small, large)
	}
}

// peakHeapSampler records the largest live heap seen while a measured call
// runs, which TotalAlloc alone cannot show.
type peakHeapSampler struct {
	peak atomic.Uint64
	stop chan struct{}
	done sync.WaitGroup
}

func startPeakHeapSampler() *peakHeapSampler {
	s := &peakHeapSampler{stop: make(chan struct{})}
	s.done.Add(1)
	go func() {
		defer s.done.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		var stats runtime.MemStats
		for {
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > s.peak.Load() {
				s.peak.Store(stats.HeapAlloc)
			}
			select {
			case <-s.stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return s
}

func (s *peakHeapSampler) finish() uint64 {
	close(s.stop)
	s.done.Wait()
	return s.peak.Load()
}

// propfindBodyOfSize repeats distinct property elements until the PROPFIND
// body reaches roughly size bytes, so a fixed set of names can fill a large
// body.
func propfindBodyOfSize(distinct, size int) string {
	unit := customPropElements(distinct, 1)
	repeats := max(1, (size-len(propfindSelectorBody("")))/len(unit))
	return propfindSelectorBody(strings.Repeat(unit, repeats))
}

func benchmarkPropfindBody(b *testing.B, events int, body string) {
	h := limitsTestServer(b, &config.Config{}, events)
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	var peak uint64
	var status int
	for b.Loop() {
		runtime.GC()
		sampler := startPeakHeapSampler()
		rr := propSelectorPropfind(b, h, "/dav/calendars/1/", "1", body)
		peak = max(peak, sampler.finish())
		status = rr.Code
	}
	b.ReportMetric(float64(peak)/(1<<20), "peak-heap-MB")
	b.ReportMetric(float64(status), "status")
}

// The reviewer's shape: a ~9 MB PROPFIND whose property list repeats a set of
// distinct names, answered over a calendar of 100 events.
func BenchmarkPropfindDepth1LargePropertyList(b *testing.B) {
	for _, distinct := range []int{1000, 50000} {
		b.Run(fmt.Sprintf("9MB/distinct=%d", distinct), func(b *testing.B) {
			benchmarkPropfindBody(b, 100, propfindBodyOfSize(distinct, 9<<20))
		})
	}
}

func BenchmarkPropfindDepth1NormalRequest(b *testing.B) {
	body := propfindSelectorBody(`<D:getetag/><D:getcontenttype/><D:resourcetype/><D:displayname/><D:sync-token/><X:a/><X:b/>`)
	benchmarkPropfindBody(b, 500, body)
}

func BenchmarkCalendarQueryLargePropertyList(b *testing.B) {
	unit := customPropElements(1000, 1)
	props := strings.Repeat(unit, (9<<20)/len(unit))
	body := propSelectorReports[0].body(props)
	h := limitsTestServer(b, &config.Config{}, 100)
	b.ReportAllocs()
	var peak uint64
	var status int
	for b.Loop() {
		runtime.GC()
		sampler := startPeakHeapSampler()
		rr := propSelectorCalendarReport(b, h, body)
		peak = max(peak, sampler.finish())
		status = rr.Code
	}
	b.ReportMetric(float64(peak)/(1<<20), "peak-heap-MB")
	b.ReportMetric(float64(status), "status")
}

func BenchmarkCalendarQueryNormalRequest(b *testing.B) {
	body := propSelectorReports[0].body(`<D:getetag/><C:calendar-data/>`)
	h := limitsTestServer(b, &config.Config{}, 500)
	b.ReportAllocs()
	for b.Loop() {
		propSelectorCalendarReport(b, h, body)
	}
}

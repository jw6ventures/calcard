package dav

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestIsValidConditionName(t *testing.T) {
	tests := []struct {
		condition string
		valid     bool
	}{
		// Valid conditions
		{"max-resource-size", true},
		{"valid-calendar-data", true},
		{"no-uid-conflict", true},
		{"supported-calendar-component", true},
		{"min-date-time", true},
		{"a", true},
		{"abc123", true},
		{"test-condition-123", true},

		// Invalid conditions
		{"", false},
		{"Max-Resource-Size", false},   // Uppercase
		{"123-start", false},           // Starts with digit
		{"-start", false},              // Starts with hyphen
		{"test_condition", false},      // Underscore not allowed
		{"test condition", false},      // Space not allowed
		{"test<script>", false},        // XML characters
		{"test&amp;", false},           // XML entity
		{"../../../etc/passwd", false}, // Path traversal attempt
		{"test;DROP TABLE", false},     // SQL injection attempt
		{"VALID-CONDITION", false},     // All uppercase
	}

	for _, tt := range tests {
		t.Run(tt.condition, func(t *testing.T) {
			result := isValidConditionName(tt.condition)
			if result != tt.valid {
				t.Errorf("isValidConditionName(%q) = %v, want %v", tt.condition, result, tt.valid)
			}
		})
	}
}

func TestWriteCalDAVError_ValidCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVError(w, 403, "max-resource-size")

	body := w.Body.String()
	if !strings.Contains(body, "<C:max-resource-size/>") {
		t.Errorf("expected valid condition in response, got: %s", body)
	}
	if !strings.Contains(body, "<?xml version") {
		t.Error("expected XML declaration in response")
	}
}

func TestDAVErrorWritersEmitExactWireXML(t *testing.T) {
	tests := []struct {
		name  string
		write func(*httptest.ResponseRecorder)
		want  string
	}{
		{
			name: "CalDAV condition",
			write: func(w *httptest.ResponseRecorder) {
				writeCalDAVError(w, 403, "max-resource-size")
			},
			want: `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><C:max-resource-size/></D:error>`,
		},
		{
			name: "DAV condition",
			write: func(w *httptest.ResponseRecorder) {
				writeDAVError(w, 409, "resource-must-be-null")
			},
			want: `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:"><D:resource-must-be-null/></D:error>`,
		},
		{
			name: "CardDAV condition",
			write: func(w *httptest.ResponseRecorder) {
				writeCardDAVPrecondition(w, 415, "supported-address-data")
			},
			want: `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"><C:supported-address-data/></D:error>`,
		},
		{
			// RFC 6352 §10 reserves the CardDAV namespace as RFC 4791 §1.2
			// reserves the CalDAV one, so this writer drops a name that is no
			// condition name rather than putting it on the wire.
			name: "CardDAV condition that is no name",
			write: func(w *httptest.ResponseRecorder) {
				writeCardDAVPrecondition(w, 415, "not a <name>")
			},
			want: `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:carddav"></D:error>`,
		},
		{
			name: "UID conflict href",
			write: func(w *httptest.ResponseRecorder) {
				writeCalDAVUIDConflict(w, "/dav/calendars/1/a&b.ics")
			},
			want: `<?xml version="1.0" encoding="utf-8"?><D:error xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><C:no-uid-conflict><D:href>/dav/calendars/1/a&amp;b.ics</D:href></C:no-uid-conflict></D:error>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tt.write(w)
			if got := w.Body.String(); got != tt.want {
				t.Fatalf("response body = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCalendarObjectHrefEscapesResourceNameAsOnePathSegment(t *testing.T) {
	tests := map[string]string{
		"a#b":   "/dav/calendars/7/a%23b.ics",
		"a?b":   "/dav/calendars/7/a%3Fb.ics",
		"a%b":   "/dav/calendars/7/a%25b.ics",
		"a b":   "/dav/calendars/7/a%20b.ics",
		"a/b":   "/dav/calendars/7/a%2Fb.ics",
		"plain": "/dav/calendars/7/plain.ics",
	}
	for resourceName, want := range tests {
		t.Run(resourceName, func(t *testing.T) {
			got := calendarObjectConflictHref(7, &store.Event{ResourceName: resourceName})
			if got != want {
				t.Fatalf("calendarObjectConflictHref() = %q, want %q", got, want)
			}
		})
	}
}

// RFC 4791 §1.2 reserves the CalDAV namespace for elements the CalDAV
// specifications define, so a condition name the caller got wrong cannot be
// answered with an invented one: substituting a placeholder would put an
// element in that namespace no specification names. The status carries the
// failure and the DAV:error is returned without a condition child.
func TestWriteCalDAVError_InvalidCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVError(w, 403, "test<script>alert(1)</script>")

	body := w.Body.String()
	if strings.Contains(body, "<script>") {
		t.Error("XML injection vulnerability: script tag present in output")
	}
	if strings.Contains(body, "invalid-condition") {
		t.Errorf("an unspecified CalDAV element was invented for a bad condition name: %s", body)
	}
	if !strings.Contains(body, "<D:error") {
		t.Errorf("expected a DAV:error body, got: %s", body)
	}
	if w.Code != 403 {
		t.Errorf("status = %d, want 403", w.Code)
	}
}

func TestWriteDAVError_InvalidCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeDAVError(w, 403, "not a name")

	body := w.Body.String()
	if strings.Contains(body, "invalid-condition") {
		t.Errorf("an unspecified DAV element was invented for a bad condition name: %s", body)
	}
	if !strings.Contains(body, "<D:error") {
		t.Errorf("expected a DAV:error body, got: %s", body)
	}
}

func TestWriteCalDAVErrorMulti_ValidConditions(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 400, "valid-calendar-data", "valid-calendar-object-resource")

	body := w.Body.String()
	if !strings.Contains(body, "<C:valid-calendar-data/>") {
		t.Error("expected first condition in response")
	}
	if !strings.Contains(body, "<C:valid-calendar-object-resource/>") {
		t.Error("expected second condition in response")
	}
}

func TestWriteCalDAVErrorMulti_SkipsInvalidConditions(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 400, "valid-condition", "<injection>", "another-valid")

	body := w.Body.String()
	if strings.Contains(body, "<injection>") {
		t.Error("XML injection vulnerability: invalid condition not filtered")
	}
	if !strings.Contains(body, "<C:valid-condition/>") {
		t.Error("expected valid condition to be included")
	}
	if !strings.Contains(body, "<C:another-valid/>") {
		t.Error("expected second valid condition to be included")
	}
}

func TestWriteCalDAVErrorMulti_EmptyConditions(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 400)

	if w.Code != 400 {
		t.Errorf("expected status 400, got: %d", w.Code)
	}
	body := w.Body.String()
	if body != "" {
		t.Errorf("expected empty body for no conditions, got: %s", body)
	}
}

func TestWriteCalDAVErrorMulti_SingleCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 403, "max-resource-size")

	body := w.Body.String()
	// Should call writeCalDAVError for single condition
	if !strings.Contains(body, "<C:max-resource-size/>") {
		t.Errorf("expected condition in response, got: %s", body)
	}
}

func TestBuildConditionErrorXMLSingle(t *testing.T) {
	xml := buildConditionErrorXML(namespaceCalDAV, []string{"max-resource-size"})
	if !strings.Contains(xml, "<?xml version") {
		t.Error("expected XML declaration")
	}
	if !strings.Contains(xml, "xmlns:D=\"DAV:\"") || !strings.Contains(xml, "xmlns:C=\"urn:ietf:params:xml:ns:caldav\"") {
		t.Error("expected DAV and CalDAV namespaces")
	}
	if !strings.Contains(xml, "<C:max-resource-size/>") {
		t.Errorf("expected condition in XML, got: %s", xml)
	}
}

func TestBuildConditionErrorXMLMulti(t *testing.T) {
	xml := buildConditionErrorXML(namespaceCalDAV, []string{"valid-calendar-data", "<bad>", "valid-calendar-object-resource"})
	if strings.Contains(xml, "<bad>") {
		t.Error("expected invalid condition to be skipped")
	}
	if !strings.Contains(xml, "<C:valid-calendar-data/>") {
		t.Error("expected first condition in XML")
	}
	if !strings.Contains(xml, "<C:valid-calendar-object-resource/>") {
		t.Error("expected second condition in XML")
	}
}

// RFC 4918 §9.2.1: a PROPPATCH refused for naming a protected property "SHOULD
// use the precondition code 'cannot-modify-protected-property' inside the
// response body", and §16 puts that element inside the propstat carrying the
// properties it applies to, since a 207 has no top-level DAV:error.
func TestProppatchProtectedPropertyReportsCannotModifyProtectedProperty(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set><d:prop><cal:max-resource-size>1024</cal:max-resource-size></d:prop></d:set>
</d:propertyupdate>`
	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1")
	resp.assertPropstatNames(t, http.StatusForbidden, calQN("max-resource-size"))
	// The condition follows the status inside the same propstat, which is the
	// §14.22 (prop, status, error?) order and the §16 placement.
	if !strings.Contains(rr.Body.String(), "403 Forbidden</d:status><d:error><d:cannot-modify-protected-property>") {
		t.Fatalf("the 403 propstat did not carry the precondition: %s", rr.Body.String())
	}
}

// A 403 that names no condition does not borrow one: only the protected-property
// refusal has a precondition, so the two share a status but not a propstat.
func TestProppatchUnnamedForbiddenCarriesNoPrecondition(t *testing.T) {
	h, _, user := calendarPropertyServer(store.Calendar{})

	body := `<?xml version="1.0" encoding="utf-8"?>
<d:propertyupdate xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">
  <d:set><d:prop>
    <cal:max-resource-size>1024</cal:max-resource-size>
    <cal:calendar-timezone><x/></cal:calendar-timezone>
  </d:prop></d:set>
</d:propertyupdate>`
	req := httptest.NewRequest("PROPPATCH", "/dav/calendars/1/", strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()

	h.Proppatch(rr, req)

	rendered := rr.Body.String()
	if count := strings.Count(rendered, "<d:cannot-modify-protected-property"); count != 1 {
		t.Fatalf("precondition appeared %d times, want once: %s", count, rendered)
	}
	if strings.Contains(rendered, "409 Conflict</d:status><d:error") {
		t.Fatalf("a 409 borrowed the protected-property precondition: %s", rendered)
	}
	resp := decodeMultistatus(t, rr).responseForHref(t, "/dav/calendars/1")
	resp.assertPropstatNames(t, http.StatusForbidden, calQN("max-resource-size"))
	resp.assertPropstatNames(t, http.StatusConflict, calQN("calendar-timezone"))
}

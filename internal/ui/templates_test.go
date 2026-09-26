package ui

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

func TestTemplatesEmbedded(t *testing.T) {
	names := []string{
		"base.html",
		"dashboard.html",
	}
	for _, name := range names {
		if _, err := templateFS.Open("templates/" + name); err != nil {
			t.Fatalf("expected embedded template %s, got error: %v", name, err)
		}
	}
}

func TestFormatMonthAbbrevAndDayOfMonth(t *testing.T) {
	when := time.Date(2026, time.September, 7, 13, 30, 0, 0, time.UTC)
	tests := []struct {
		name      string
		value     any
		wantMonth string
		wantDay   string
	}{
		{name: "value", value: when, wantMonth: "SEP", wantDay: "7"},
		{name: "pointer", value: &when, wantMonth: "SEP", wantDay: "7"},
		{name: "nil", value: nil},
		{name: "nil pointer", value: (*time.Time)(nil)},
		{name: "zero time", value: time.Time{}},
		{name: "wrong type", value: "2026-09-07"},
	}

	month := funcMap["formatMonthAbbrev"].(func(any) string)
	day := funcMap["formatDayOfMonth"].(func(any) string)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := month(tt.value); got != tt.wantMonth {
				t.Errorf("formatMonthAbbrev(%v) = %q, want %q", tt.value, got, tt.wantMonth)
			}
			if got := day(tt.value); got != tt.wantDay {
				t.Errorf("formatDayOfMonth(%v) = %q, want %q", tt.value, got, tt.wantDay)
			}
		})
	}
}

// The recent-events badge shows the event's own date: a fixed 📅 glyph renders
// as a July 17 page on most platforms regardless of when the event is.
func TestDashboardEventBadgeReadsTheEventDate(t *testing.T) {
	source, err := templateFS.ReadFile("templates/dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard.html: %v", err)
	}
	for _, want := range []string{
		"formatMonthAbbrev .DTStart",
		"formatDayOfMonth .DTStart",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("dashboard.html is missing %q: the event badge is not built from the event date", want)
		}
	}
	// U+1F4C5 renders as a July 17 calendar page on most platforms, so it can
	// never stand next to a real date badge.
	if strings.Contains(string(source), "&#128197;") {
		t.Error("dashboard.html still renders the fixed calendar glyph for a recent event")
	}
}

// formatTime renders RFC3339, which is what put "2026-02-05T03:16:24Z" in the
// CREATED columns while the rest of the UI showed "Feb 5, 2026".
func TestTemplatesDoNotRenderRawTimestamps(t *testing.T) {
	files, err := fs.Glob(templateFS, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		source, err := templateFS.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(source), "formatTime ") {
			t.Errorf("%s renders a timestamp with formatTime; use formatDate or formatDateTime", file)
		}
	}
}

// A CN that only repeats the address renders as "x@y.z <x@y.z>" without this.
func TestCalendarViewsCollapseRedundantAttendeeName(t *testing.T) {
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "name.toLowerCase() === email.toLowerCase()") {
			t.Errorf("%s does not collapse a CN that repeats the address", name)
		}
	}
}

// A backwards range is the one rejection the browser can catch before the
// redirect drops the form, so both editors have to check it themselves.
func TestCalendarViewsRejectABackwardsRangeBeforeSubmitting(t *testing.T) {
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "function createEventDateError(startValue, endValue, allDay)") {
			t.Errorf("%s does not check the entered range before submitting", name)
		}
	}
}

// The create modal is wiped by the redirect a server-side rejection performs,
// so it has to both catch the bad range itself and put the draft back when the
// server rejects for some other reason.
func TestCalendarViewPreservesCreateEventDraft(t *testing.T) {
	source, err := templateFS.ReadFile("templates/calendar_view.html")
	if err != nil {
		t.Fatalf("read calendar_view.html: %v", err)
	}
	for _, want := range []string{
		"function createEventDateError(startValue, endValue, allDay)",
		"function saveCreateEventDraft(",
		"function restoreCreateEventDraft(",
		"id=\"create-event-form\"",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("calendar_view.html is missing %q: a rejected create loses the form", want)
		}
	}
	// The view bootstrap replaces the URL with one carrying only the view and
	// date before the create wiring runs, so reading the rejection from the
	// query string would always find nothing.
	if !strings.Contains(string(source), "var CREATE_EVENT_REJECTION = {{if .FlashError}}") {
		t.Error("calendar_view.html does not take the rejection from the rendered flash")
	}
	if strings.Contains(string(source), "URLSearchParams(window.location.search).get('error')") {
		t.Error("calendar_view.html reads the rejection from a URL the bootstrap has already rewritten")
	}
}

func TestEventFormHelpers(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "event_form_helpers.mjs"),
				filepath.Join("templates", name))
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("template helpers: %v\n%s", err, output)
			}
		})
	}
}

// The aggregate editor's all-day end conversion lives in template JavaScript,
// which no Go test can call. Two things stand in for that: this assertion that
// the conversion is still applied where the end input is filled, and the node
// script below, which runs the template's own helpers.
func TestAggregateEditorConvertsAllDayEndForDisplay(t *testing.T) {
	source, err := templateFS.ReadFile("templates/all_calendars_view.html")
	if err != nil {
		t.Fatalf("read all_calendars_view.html: %v", err)
	}
	for _, want := range []string{
		"function inclusiveAllDayEnd(start, exclusiveEnd)",
		"function exclusiveAllDayEnd(start, lastDay)",
		"allDay ? formatDateInput(inclusiveAllDayEnd(startOfDay(start), end)) : formatDateTimeInput(end, timezone)",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("all_calendars_view.html is missing %q: an all-day end reaches the form unconverted", want)
		}
	}
}

// Both calendar views read the same server payload, so an all-day date has to be
// moved onto the local day in both. Leaving either on the instant the server
// sends puts the event a day early everywhere west of UTC.
func TestCalendarViewsNormalizeAllDayServerDates(t *testing.T) {
	for _, name := range []string{"all_calendars_view.html", "calendar_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, want := range []string{
			"function parseServerDate(value, allDay)",
			"parsed.dtstart = parseServerDate(raw.dtstart, allDay);",
			"parsed.dtend = parseServerDate(raw.dtend, allDay);",
			"parsed.recurrenceId = parseServerDate(raw.recurrenceId, raw.recurrenceIdAllDay || allDay);",
		} {
			if !strings.Contains(string(source), want) {
				t.Errorf("%s is missing %q: an all-day date keeps the instant the server sent", name, want)
			}
		}
		// map would hand parseServerDate the array index as its all-day flag,
		// which is false for the first EXDATE and true for every one after it.
		if strings.Contains(string(source), ".map(parseServerDate)") {
			t.Errorf("%s passes parseServerDate to map, so the index becomes the all-day flag", name)
		}
	}
}

// Runs the template's own date helpers over the save round-trip, which must not
// extend an all-day event by a day each time, and over the server payload,
// which must not move it back a day. Skipped where node is absent outside CI,
// which the assertions above partly cover for.
//
// The zones are the point of the table: an all-day date the server sends as UTC
// midnight only lands on the wrong day where the browser's offset is negative,
// so a run in one zone proves nothing about the others.
func TestAggregateEditorAllDayEndRoundTrip(t *testing.T) {
	node := requireNode(t)
	zones := []string{
		"UTC",
		"America/New_York",   // -04:00/-05:00, where the drift appeared
		"America/Anchorage",  // -08:00/-09:00
		"Pacific/Kiritimati", // +14:00, the far side
		"Asia/Kolkata",       // +05:30, not a whole hour
	}
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "all_day_end_roundtrip.mjs"),
				filepath.Join("templates", "all_calendars_view.html"))
			cmd.Env = append(os.Environ(), "TZ="+zone)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("all-day round-trip failed in %s: %v\n%s", zone, err, output)
			}
		})
	}
}

// calendar_view.html converts the same exclusive DTEND by hand in two places
// (the edit modal and the create-modal prefill) instead of through a shared
// helper. Both must clamp against start the same way the aggregate editor's
// inclusiveAllDayEnd does, or a stored DTEND equal to DTSTART shows an end
// date before the start.
func TestCalendarViewConvertsAllDayEndForDisplay(t *testing.T) {
	source, err := templateFS.ReadFile("templates/calendar_view.html")
	if err != nil {
		t.Fatalf("read calendar_view.html: %v", err)
	}
	for _, want := range []string{
		"function inclusiveAllDayEnd(start, exclusiveEnd)",
		"function exclusiveAllDayEnd(start, lastDay)",
		"formatDateForInput(window.inclusiveAllDayEnd(allDayStart, event.dtend))",
		"formatDateForInput(window.inclusiveAllDayEnd(window.startOfDay(prefill.start), prefill.end))",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("calendar_view.html is missing %q: an all-day end reaches the form unconverted or unclamped", want)
		}
	}
}

// Runs calendar_view.html's own showEditEventModal, showCreateEventModal and
// toggleAllDay over the same all-day end scenarios as
// TestAggregateEditorAllDayEndRoundTrip, including a stored DTEND equal to
// DTSTART -- degenerate, but storable, and the shape that showed an end
// before the start. calendar_view.html has no equivalent to the aggregate
// editor's single setEditorDateValues, so this drives a sibling script
// (calendar_view_all_day_end_roundtrip.mjs) rather than the shared one; see
// that file for why.
func TestCalendarViewAllDayEndRoundTrip(t *testing.T) {
	node := requireNode(t)
	zones := []string{
		"UTC",
		"America/New_York",   // -04:00/-05:00, where the drift appeared
		"America/Anchorage",  // -08:00/-09:00
		"Pacific/Kiritimati", // +14:00, the far side
		"Asia/Kolkata",       // +05:30, not a whole hour
	}
	for _, zone := range zones {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "calendar_view_all_day_end_roundtrip.mjs"),
				filepath.Join("templates", "calendar_view.html"))
			cmd.Env = append(os.Environ(), "TZ="+zone)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("all-day round-trip failed in %s: %v\n%s", zone, err, output)
			}
		})
	}
}

func TestTimeGridDST(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(node, "testdata/time_grid_dst.mjs", filepath.Join("templates", name))
			cmd.Env = append(os.Environ(), "TZ=America/New_York")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

func TestTimedEditorTimezoneRoundTrip(t *testing.T) {
	node := requireNode(t)
	for _, template := range []string{"all_calendars_view.html", "calendar_view.html"} {
		for _, browserZone := range []string{"America/Los_Angeles", "UTC", "Pacific/Kiritimati"} {
			t.Run(template+"/"+browserZone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "timed_editor_roundtrip.mjs"), filepath.Join("templates", template))
				cmd.Env = append(os.Environ(), "TZ="+browserZone)
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("template helpers: %v: %s", err, output)
				}
				var cases []struct {
					Start, End, Zone, RecurrenceZone string
					Values                           []string
				}
				if err := json.Unmarshal(output, &cases); err != nil {
					t.Fatal(err)
				}
				for _, c := range cases {
					for i, prop := range []string{"DTSTART", "DTEND", "RECURRENCE-ID"} {
						zone := c.Zone
						if i == 2 {
							zone = c.RecurrenceZone
						}
						line, err := utils.FormatICalDateTime(c.Values[i], false, false, prop, zone)
						if err != nil {
							t.Fatal(err)
						}
						key, value, _ := strings.Cut(line, ":")
						got, ok := ical.ParsePropertyDateTimeLocal(key, value)
						original := c.Start
						if i == 1 {
							original = c.End
						}
						want, err := time.Parse(time.RFC3339, original)
						if err != nil {
							t.Fatal(err)
						}
						if !ok || !got.Equal(want) {
							t.Errorf("%s: %s submitted as %s, want instant %s", prop, original, line, want)
						}
					}
				}
			})
		}
	}
}

// The event modal builds a markup string and assigns it to innerHTML, so every
// value interpolated into it has to be escaped at the point of interpolation.
// formatRRule reads its parts straight out of the stored RRULE, which arrives
// over CalDAV PUT or an ICS import and is never sanitized on the way in.
func TestEventModalEscapesRecurrenceSummary(t *testing.T) {
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "escapeHtml(formatRRule(event.rrule))") {
			t.Errorf("%s interpolates formatRRule output into the modal unescaped", name)
		}
	}
}

// An on* attribute is a program the HTML parser hands to the JavaScript engine
// after decoding the character references in it, so a value escaped for markup
// arrives there unescaped. No template may build one out of data.
func TestTemplatesDoNotBuildEventHandlersByConcatenation(t *testing.T) {
	node := requireNode(t)
	files, err := fs.Glob(templateFS, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		source, err := templateFS.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(source), "<script") {
			continue
		}
		t.Run(filepath.Base(file), func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "no_concatenated_handlers.mjs"), file)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// The contact modal's edit button is attached with addEventListener: an
// onclick attribute assembled as markup carries the contact UID as a
// JavaScript string literal the attribute parser hands over with its escaping
// already undone.
func TestContactModalAttachesTheEditHandler(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "contact_modal_wiring.mjs"),
		filepath.Join("templates", "addressbook_view.html"), safeHTMLPartial)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("contact modal wiring: %v\n%s", err, output)
	}
}

// sanitizeHtml is handed the X-ALT-DESC of an event, which arrives over CalDAV
// PUT or an ICS import as attacker-authored HTML and is never sanitized on the
// way in. Each assertion stands for a way the sanitizer can be defeated without
// ever tripping its allow list, checked where it matters: in the sanitizer's
// own body, and in how the pages hand it the description.
func TestCalendarViewsSanitizeHTMLDescriptionsInertly(t *testing.T) {
	partial, err := templateFS.ReadFile("templates/partials/safe_html_js.tmpl")
	if err != nil {
		t.Fatalf("read the shared helpers: %v", err)
	}
	body, ok := javaScriptFunction(string(partial), "sanitizeHtml")
	if !ok {
		t.Fatal("the shared helpers have no sanitizeHtml")
	}
	// A node of the live document starts loading an <img onerror> as soon as
	// innerHTML is assigned -- before the allow list has looked at anything. A
	// document with no browsing context loads nothing.
	if !strings.Contains(body, "document.implementation.createHTMLDocument(") {
		t.Error("sanitizeHtml does not parse the description into an inert document")
	}
	for _, liveParse := range []string{"document.createElement('div')", "document.createElement(\"div\")", "document.body.innerHTML"} {
		if strings.Contains(body, liveParse) {
			t.Errorf("sanitizeHtml parses in the live document (%s)", liveParse)
		}
	}
	if strings.Contains(body, ".innerHTML;") || strings.Contains(body, "return inert") {
		t.Error("sanitizeHtml returns markup or the parsed tree rather than a node built afresh")
	}
	// trim removes no interior whitespace, but the URL parser removes tab and
	// newline from a scheme, so java&#9;script: passes a startsWith test and
	// still navigates as javascript:.
	if strings.Contains(body, "startsWith('javascript:')") {
		t.Error("sanitizeHtml decides an href scheme from the unparsed text")
	}
	if !strings.Contains(body, "safeUrl(node.getAttribute('href'))") {
		t.Error("sanitizeHtml does not put an href through the parsing scheme allow list")
	}

	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		// Re-serializing the sanitized tree and parsing it a second time
		// reopens the hole wherever the parser and the serializer disagree.
		if strings.Contains(string(source), "+ sanitizeHtml(") {
			t.Errorf("%s concatenates the sanitized description back into markup that is parsed again", name)
		}
		if !strings.Contains(string(source), ".appendChild(sanitizeHtml(event.htmlDescription))") {
			t.Errorf("%s does not append the sanitized description as a node", name)
		}
	}
}

// Runs the shared sanitizeHtml over the descriptions it has to defuse. The
// attribute filter has to be an allow list: a filter that refuses attributes
// by name keeps every attribute nobody named, and on the tags this sanitizer
// allows that includes id, which is enough to take over the modal without
// executing anything.
func TestEventDescriptionSanitizerKeepsOnlyAllowedAttributes(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "description_sanitizer.mjs"), safeHTMLPartial)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("description sanitizer: %v\n%s", err, output)
	}
}

// The helpers that stand between stored calendar and contact data and the page
// live in one partial. A page carrying its own copy of any of them is a copy a
// hardening applied to the partial does not reach.
func TestPagesShareOneCopyOfTheSafeHTMLHelpers(t *testing.T) {
	helpers := []string{"escapeHtml", "unescapeText", "safeUrl", "safeHref", "sanitizeHtml"}
	partial, err := templateFS.ReadFile("templates/partials/safe_html_js.tmpl")
	if err != nil {
		t.Fatalf("read the shared helpers: %v", err)
	}
	for _, helper := range helpers {
		if _, ok := javaScriptFunction(string(partial), helper); !ok {
			t.Errorf("the shared helpers have no %s", helper)
		}
	}
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html", "addressbook_view.html", "birthdays.html"} {
		source, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(source), "<script>\n{{template \"safe_html_js\"}}\n</script>") {
			t.Errorf("%s does not include the shared helpers in a script of their own", name)
		}
		if templates[name].Lookup("safe_html_js") == nil {
			t.Errorf("%s is parsed without the shared helpers", name)
		}
		for _, helper := range append(helpers, "unescapeICAL", "unescapeVCard") {
			if strings.Contains(string(source), "function "+helper+"(") {
				t.Errorf("%s defines its own %s", name, helper)
			}
		}
	}
}

// The text of a named JavaScript function declaration, from the keyword through
// the brace that closes its body.
func javaScriptFunction(source, name string) (string, bool) {
	start := strings.Index(source, "function "+name+"(")
	if start < 0 {
		return "", false
	}
	depth := 0
	for i := start; i < len(source); i++ {
		switch source[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return source[start : i+1], true
			}
		}
	}
	return "", false
}

// escapeHtml, safeUrl and safeHref are the guards between stored data and the
// modals' innerHTML.
func TestEventModalEscapingHelpers(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "modal_escaping.mjs"), safeHTMLPartial)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("escaping helpers: %v\n%s", err, output)
	}
}

// A 29 February birthday built as a Date in a common year is 1 March, so the
// birthdays page places, labels and counts down to it through its own helpers.
func TestBirthdaysPageKeepsLeapDayBirthdays(t *testing.T) {
	node := requireNode(t)
	for _, zone := range []string{"UTC", "America/New_York"} {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "birthdays_leap_day.mjs"),
				filepath.Join("templates", "birthdays.html"), safeHTMLPartial)
			cmd.Env = append(os.Environ(), "TZ="+zone)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// A DATE-TIME ending in Z is UTC. Read as local time it moves by the browser's
// offset, which only shows where that offset is not zero, so the zones are the
// point of the table.
func TestCalendarViewsReadUTCDateTimesAsUTC(t *testing.T) {
	node := requireNode(t)
	for _, name := range []string{"calendar_view.html", "all_calendars_view.html"} {
		for _, zone := range []string{"UTC", "America/New_York", "Asia/Kolkata", "Pacific/Kiritimati"} {
			t.Run(name+"/"+zone, func(t *testing.T) {
				cmd := exec.Command(node, filepath.Join("testdata", "ical_date_parsing.mjs"),
					filepath.Join("templates", name), safeHTMLPartial)
				cmd.Env = append(os.Environ(), "TZ="+zone)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("%v\n%s", err, output)
				}
			})
		}
	}
}

// The birthday controls spell a date out of three fields, and only the page
// decides how long the day list is. A date no month has is refused by the
// server, and an empty field removes the stored birthday, so the form has to
// decline to spell the first and refuse to submit the second.
func TestBirthdayDayOptionsFollowTheMonth(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, "testdata/birthday_day_options.mjs", filepath.Join("templates", "addressbook_view.html"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
}

// A stored date goes into the edit form and back out as the value the server
// writes, so the page's reading of it has to be exact in every zone: a
// date-time read through Date lands on the previous day west of UTC.
func TestContactVCardParsing(t *testing.T) {
	node := requireNode(t)
	for _, zone := range []string{"UTC", "America/Los_Angeles", "Pacific/Kiritimati"} {
		t.Run(zone, func(t *testing.T) {
			cmd := exec.Command(node, filepath.Join("testdata", "contact_vcard_parsing.mjs"),
				filepath.Join("templates", "addressbook_view.html"), safeHTMLPartial)
			cmd.Env = append(os.Environ(), "TZ="+zone)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%v\n%s", err, output)
			}
		})
	}
}

// The handler-attribute check is only as good as the shapes it recognizes, so
// it is run over a catalogue of them -- each concatenation shape it must flag
// and each inert one it must pass.
func TestNoConcatenatedHandlersDetector(t *testing.T) {
	node := requireNode(t)
	cmd := exec.Command(node, filepath.Join("testdata", "no_concatenated_handlers.mjs"), "--self-test")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, output)
	}
}

// safeHTMLPartial is the shared helper partial, relative to the package.
var safeHTMLPartial = filepath.Join("templates", "partials", "safe_html_js.tmpl")

// requireNode returns the node binary the template JavaScript tests run on. A
// developer machine without node skips them; CI must not, or the helpers these
// tests guard would go unchecked without anyone noticing.
func requireNode(t *testing.T) string {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("node is not installed, and CI must run the template JavaScript tests")
		}
		t.Skip("node is not installed; skipping the template JavaScript tests")
	}
	return node
}

// The partial reaches the browser through html/template, which parses the
// script it sits in. Running the shared helpers' tests over each rendered page
// proves every page serves them, once, and unaltered.
func TestRenderedPagesServeTheSharedSafeHTMLHelpers(t *testing.T) {
	node := requireNode(t)
	withUser := func(req *http.Request, params map[string]string) *http.Request {
		rctx := chi.NewRouteContext()
		for key, value := range params {
			rctx.URLParams.Add(key, value)
		}
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
		return req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "owner@example.com"}))
	}
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 100, Name: "Contacts"},
		}},
		Contacts: &fakeContactRepoWithBirthdays{fakeContactRepo: fakeContactRepo{contacts: map[string]*store.Contact{}}},
	}, nil)
	pages := []struct {
		name  string
		serve http.HandlerFunc
		req   *http.Request
	}{
		{"calendar_view.html", handler.ViewCalendar, withUser(httptest.NewRequest(http.MethodGet, "/calendars/1", nil), map[string]string{"id": "1"})},
		{"all_calendars_view.html", handler.ViewAllCalendars, withUser(httptest.NewRequest(http.MethodGet, "/calendars/all", nil), nil)},
		{"addressbook_view.html", handler.ViewAddressBook, withUser(httptest.NewRequest(http.MethodGet, "/addressbooks/1", nil), map[string]string{"id": "1"})},
		{"birthdays.html", handler.ViewBirthdays, withUser(httptest.NewRequest(http.MethodGet, "/birthdays", nil), nil)},
	}
	for _, page := range pages {
		t.Run(page.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			page.serve(w, page.req)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
			}
			body := w.Body.String()
			for _, helper := range []string{"escapeHtml", "unescapeText", "safeUrl", "safeHref", "sanitizeHtml"} {
				if got := strings.Count(body, "function "+helper+"("); got != 1 {
					t.Errorf("the page defines %s %d times, want once", helper, got)
				}
			}
			rendered := filepath.Join(t.TempDir(), page.name)
			if err := os.WriteFile(rendered, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, script := range []string{"modal_escaping.mjs", "description_sanitizer.mjs"} {
				cmd := exec.Command(node, filepath.Join("testdata", script), rendered)
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Errorf("%s over the rendered page: %v\n%s", script, err, output)
				}
			}
		})
	}
}

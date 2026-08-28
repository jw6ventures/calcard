package dav

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// calDAVSpecifiedElements is every element name the CalDAV specifications place
// in urn:ietf:params:xml:ns:caldav: the RFC 4791 properties, REPORT bodies,
// filter and calendar-data grammars, preconditions and postconditions, the §6.1
// privilege, and the RFC 6638 scheduling additions CalCard advertises. RFC 4791
// §1.2 reserves the namespace for exactly these, so anything CalCard invents
// belongs in a namespace of its own.
var calDAVSpecifiedElements = map[string]string{
	// §4.2, §5.2 properties.
	"calendar":                         "RFC 4791 §4.2",
	"calendar-description":             "RFC 4791 §5.2.1",
	"calendar-timezone":                "RFC 4791 §5.2.2",
	"supported-calendar-component-set": "RFC 4791 §5.2.3",
	"supported-calendar-data":          "RFC 4791 §5.2.4",
	"max-resource-size":                "RFC 4791 §5.2.5",
	"min-date-time":                    "RFC 4791 §5.2.6",
	"max-date-time":                    "RFC 4791 §5.2.7",
	"max-instances":                    "RFC 4791 §5.2.8",
	"max-attendees-per-instance":       "RFC 4791 §5.2.9",
	"calendar-home-set":                "RFC 4791 §6.2.1",
	"supported-collation-set":          "RFC 4791 §7.5.1",
	"supported-collation":              "RFC 4791 §9.4",
	// §5.3.1 MKCALENDAR.
	"mkcalendar":          "RFC 4791 §9.2",
	"mkcalendar-response": "RFC 4791 §9.3",
	// §7 reports and §9 grammars.
	"calendar-query":       "RFC 4791 §9.5",
	"calendar-multiget":    "RFC 4791 §9.10",
	"free-busy-query":      "RFC 4791 §9.11",
	"calendar-data":        "RFC 4791 §9.6",
	"comp":                 "RFC 4791 §9.6.1",
	"allcomp":              "RFC 4791 §9.6.2",
	"allprop":              "RFC 4791 §9.6.3",
	"prop":                 "RFC 4791 §9.6.4",
	"expand":               "RFC 4791 §9.6.5",
	"limit-recurrence-set": "RFC 4791 §9.6.6",
	"limit-freebusy-set":   "RFC 4791 §9.6.7",
	"filter":               "RFC 4791 §9.7",
	"comp-filter":          "RFC 4791 §9.7.1",
	"prop-filter":          "RFC 4791 §9.7.2",
	"param-filter":         "RFC 4791 §9.7.3",
	"is-not-defined":       "RFC 4791 §9.7.4",
	"text-match":           "RFC 4791 §9.7.5",
	"timezone":             "RFC 4791 §9.8",
	"time-range":           "RFC 4791 §9.9",
	// §6.1 privilege.
	"read-free-busy": "RFC 4791 §6.1",
	// Preconditions and postconditions, §5.3.1.1, §5.3.2.1 and §7.8.
	"calendar-collection-location-ok": "RFC 4791 §5.3.1.1",
	"valid-calendar-data":             "RFC 4791 §5.3.1.1",
	"initialize-calendar-collection":  "RFC 4791 §5.3.1.1",
	"valid-calendar-object-resource":  "RFC 4791 §5.3.2.1",
	"supported-calendar-component":    "RFC 4791 §5.3.2.1",
	"no-uid-conflict":                 "RFC 4791 §5.3.2.1",
	"supported-filter":                "RFC 4791 §7.8.8",
	"valid-filter":                    "RFC 4791 §7.8.7",
	// RFC 6638 scheduling.
	"schedule-calendar-transp":      "RFC 6638 §9.1",
	"opaque":                        "RFC 6638 §9.1",
	"transparent":                   "RFC 6638 §9.1",
	"schedule-default-calendar-URL": "RFC 6638 §9.2",
	"calendar-user-address-set":     "RFC 6638 §2.4.1",
	"calendar-user-type":            "RFC 6638 §2.4.2",
	"schedule-inbox-URL":            "RFC 6638 §2.2",
	"schedule-outbox-URL":           "RFC 6638 §2.1",
	"schedule-inbox":                "RFC 6638 §2.2",
	"schedule-outbox":               "RFC 6638 §2.1",
	"schedule-tag":                  "RFC 6638 §3.2.10",
}

// calDAVNamespaceUse finds every element name this package places in the CalDAV
// namespace, in both spellings it writes them: the struct tags that name the
// namespace in full, and the prefixed literals the hand-written error and
// property bodies use.
var calDAVNamespaceUse = []*regexp.Regexp{
	regexp.MustCompile(`urn:ietf:params:xml:ns:caldav ([a-zA-Z][a-zA-Z0-9-]*)`),
	regexp.MustCompile(`\b(?:cal|C):([a-zA-Z][a-zA-Z0-9-]*)`),
	// The condition writers build their element from a name passed as a plain
	// string, so the prefixed spellings above never see it. This is where an
	// invented name would be least visible and most likely.
	regexp.MustCompile(`writeCalDAVError(?:Multi)?\(\s*w\s*,[^)]*?"([a-z][a-z0-9-]*)"`),
	regexp.MustCompile(`condition\s*=\s*"([a-z][a-z0-9-]*)"`),
}

// calDAVNamespaceSentinels are names the scan must find. Without them a glob or
// pattern that quietly stopped matching would leave the audit passing over an
// empty set.
var calDAVNamespaceSentinels = []string{
	"calendar-data", "comp-filter", "no-uid-conflict", "supported-calendar-component-set",
}

// RFC 4791 §1.2: "This document defines the XML elements in the
// namespace urn:ietf:params:xml:ns:caldav ... Implementations MUST NOT use this
// namespace for elements not defined by the CalDAV specifications." An element
// CalCard invents therefore has to carry a namespace of its own, and the ones
// it does invent do: the calendar colour and sync properties live in the Apple
// and CalendarServer namespaces they came from.
func TestCalDAVNamespaceCarriesOnlySpecifiedElements(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}

	unspecified := map[string][]string{}
	seen := map[string]struct{}{}
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		content, err := os.ReadFile(source)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", source, err)
		}
		for _, pattern := range calDAVNamespaceUse {
			for _, match := range pattern.FindAllStringSubmatch(string(content), -1) {
				name := match[1]
				seen[name] = struct{}{}
				if _, ok := calDAVSpecifiedElements[name]; ok {
					continue
				}
				unspecified[name] = append(unspecified[name], source)
			}
		}
	}

	for _, sentinel := range calDAVNamespaceSentinels {
		if _, ok := seen[sentinel]; !ok {
			t.Fatalf("the scan found no use of %q, so it is not reading the package's CalDAV elements", sentinel)
		}
	}

	if len(unspecified) == 0 {
		return
	}
	names := make([]string, 0, len(unspecified))
	for name := range unspecified {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Errorf("RFC 4791 §1.2: %q is placed in the CalDAV namespace but no CalDAV specification defines it (%s)",
			name, strings.Join(unspecified[name], ", "))
	}
}

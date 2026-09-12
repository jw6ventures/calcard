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

// cardDAVSpecifiedElements is every element name RFC 6352 places in
// urn:ietf:params:xml:ns:carddav: the §6.2 and §7.1 properties, the §8.3.1
// collation property, the §10 REPORT bodies and address-data and filter
// grammars, and the preconditions §5.1.1.1, §6.3.2.1 and §8.6 define. RFC 6352
// §10 reserves the namespace for exactly these, on the same terms RFC 4791 §1.2
// reserves its own.
var cardDAVSpecifiedElements = map[string]string{
	// §6.2 address book collection properties.
	"addressbook-description": "RFC 6352 §6.2.1",
	"supported-address-data":  "RFC 6352 §6.2.2",
	"address-data-type":       "RFC 6352 §6.2.2",
	"max-resource-size":       "RFC 6352 §6.2.3",
	// §7.1 principal properties.
	"addressbook-home-set": "RFC 6352 §7.1.1",
	"principal-address":    "RFC 6352 §7.1.2",
	// §8.3.1 collation property.
	"supported-collation-set": "RFC 6352 §8.3.1",
	// §10 elements.
	"addressbook":          "RFC 6352 §10.1",
	"supported-collation":  "RFC 6352 §10.2",
	"addressbook-query":    "RFC 6352 §10.3",
	"address-data":         "RFC 6352 §10.4",
	"allprop":              "RFC 6352 §10.4.1",
	"prop":                 "RFC 6352 §10.4.2",
	"filter":               "RFC 6352 §10.5",
	"prop-filter":          "RFC 6352 §10.5.1",
	"param-filter":         "RFC 6352 §10.5.2",
	"is-not-defined":       "RFC 6352 §10.5.3",
	"text-match":           "RFC 6352 §10.5.4",
	"limit":                "RFC 6352 §10.6",
	"nresults":             "RFC 6352 §10.6.1",
	"addressbook-multiget": "RFC 6352 §10.7",
	// Preconditions.
	"supported-address-data-conversion":  "RFC 6352 §5.1.1.1",
	"valid-address-data":                 "RFC 6352 §6.3.2.1",
	"no-uid-conflict":                    "RFC 6352 §6.3.2.1",
	"addressbook-collection-location-ok": "RFC 6352 §6.3.2.1",
	"supported-filter":                   "RFC 6352 §8.6",
}

// namespaceAudit is one reserved namespace and the machinery that finds every
// element name this package places in it.
type namespaceAudit struct {
	// label names the namespace in failure messages.
	label string
	// rule cites the section reserving the namespace.
	rule string
	// specified is the element set that section admits.
	specified map[string]string
	// use finds names in the spellings this package writes them: the struct
	// tags that name the namespace in full, the prefix bound to it, and the
	// condition writers, which build their element from a name passed as a
	// plain string and so never appear in a prefixed spelling. The last is
	// where an invented name would be least visible and most likely.
	use []*regexp.Regexp
	// sentinels are names the scan must find. Without them a pattern that
	// quietly stopped matching would leave the audit passing over an empty set.
	sentinels []string
}

var namespaceAudits = []namespaceAudit{
	{
		label:     "CalDAV",
		rule:      "RFC 4791 §1.2",
		specified: calDAVSpecifiedElements,
		use: []*regexp.Regexp{
			regexp.MustCompile(`urn:ietf:params:xml:ns:caldav ([a-zA-Z][a-zA-Z0-9-]*)`),
			regexp.MustCompile(`\bcal:([a-zA-Z][a-zA-Z0-9-]*)`),
			regexp.MustCompile(`writeCalDAVError(?:Multi)?\(\s*w\s*,[^)]*?"([a-z][a-z0-9-]*)"`),
			regexp.MustCompile(`condition\s*=\s*"([a-z][a-z0-9-]*)"`),
		},
		sentinels: []string{
			"calendar-data", "comp-filter", "no-uid-conflict", "supported-calendar-component-set",
		},
	},
	{
		label:     "CardDAV",
		rule:      "RFC 6352 §10",
		specified: cardDAVSpecifiedElements,
		use: []*regexp.Regexp{
			regexp.MustCompile(`urn:ietf:params:xml:ns:carddav ([a-zA-Z][a-zA-Z0-9-]*)`),
			regexp.MustCompile(`\bcard:([a-zA-Z][a-zA-Z0-9-]*)`),
			regexp.MustCompile(`writeCardDAVPrecondition\(\s*w\s*,[^)]*?"([a-z][a-z0-9-]*)"`),
		},
		sentinels: []string{
			"address-data", "prop-filter", "no-uid-conflict", "supported-address-data",
		},
	},
}

// cPrefixUse finds the hand-written error bodies that bind the prefix "C" and
// then spell an element with it. Both namespaces use that prefix, so the
// binding decides which audit the name belongs to; attributing it by prefix
// alone would let a CardDAV element hide inside a CalDAV body and the reverse.
var (
	cPrefixBinding = regexp.MustCompile(`xmlns:C="(urn:ietf:params:xml:ns:(?:cal|card)dav)"`)
	cPrefixElement = regexp.MustCompile(`\bC:([a-zA-Z][a-zA-Z0-9-]*)`)
)

// namespaceUsage is every element name one source file places in one namespace,
// keyed by name.
type namespaceUsage map[string][]string

// scanCPrefixElements attributes each C:-prefixed element to the namespace the
// nearest preceding xmlns:C declaration in the same file binds. A C: element
// with no binding ahead of it is reported rather than skipped: the audit cannot
// say which namespace it lands in, which is itself the defect.
func scanCPrefixElements(t *testing.T, source, content string, byNamespace map[string]namespaceUsage) {
	t.Helper()
	bindings := cPrefixBinding.FindAllStringSubmatchIndex(content, -1)
	for _, match := range cPrefixElement.FindAllStringSubmatchIndex(content, -1) {
		namespace := ""
		for _, binding := range bindings {
			if binding[0] < match[0] {
				namespace = content[binding[2]:binding[3]]
				continue
			}
			break
		}
		name := content[match[2]:match[3]]
		if namespace == "" {
			t.Errorf("%s writes C:%s with no xmlns:C binding ahead of it, so the namespace it lands in is unknown", source, name)
			continue
		}
		byNamespace[namespace][name] = append(byNamespace[namespace][name], source)
	}
}

// RFC 4791 §1.2 and RFC 6352 §10 each reserve a namespace for the elements
// their specifications define: "Implementations MUST NOT use this namespace for
// elements not defined by [the] specifications." An element CalCard invents
// therefore has to carry a namespace of its own, and the ones it does invent do:
// the calendar colour and sync properties live in the Apple and CalendarServer
// namespaces they came from.
func TestReservedNamespacesCarryOnlySpecifiedElements(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}

	byNamespace := map[string]namespaceUsage{
		namespaceCalDAV:  {},
		namespaceCardDAV: {},
	}
	byAudit := make([]namespaceUsage, len(namespaceAudits))
	for i := range byAudit {
		byAudit[i] = namespaceUsage{}
	}
	for _, source := range sources {
		if strings.HasSuffix(source, "_test.go") {
			continue
		}
		content, err := os.ReadFile(source)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", source, err)
		}
		for i, audit := range namespaceAudits {
			for _, pattern := range audit.use {
				for _, match := range pattern.FindAllStringSubmatch(string(content), -1) {
					byAudit[i][match[1]] = append(byAudit[i][match[1]], source)
				}
			}
		}
		scanCPrefixElements(t, source, string(content), byNamespace)
	}

	for i, audit := range namespaceAudits {
		namespace := namespaceCalDAV
		if audit.label == "CardDAV" {
			namespace = namespaceCardDAV
		}
		seen := byAudit[i]
		for name, sources := range byNamespace[namespace] {
			seen[name] = append(seen[name], sources...)
		}
		auditNamespace(t, audit, seen)
	}
}

func auditNamespace(t *testing.T, audit namespaceAudit, seen namespaceUsage) {
	t.Helper()
	for _, sentinel := range audit.sentinels {
		if _, ok := seen[sentinel]; !ok {
			t.Fatalf("the %s scan found no use of %q, so it is not reading the package's %s elements",
				audit.label, sentinel, audit.label)
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		if _, ok := audit.specified[name]; ok {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Errorf("%s: %q is placed in the %s namespace but no %s specification defines it (%s)",
			audit.rule, name, audit.label, audit.label, strings.Join(dedupeSources(seen[name]), ", "))
	}
}

func dedupeSources(sources []string) []string {
	seen := make(map[string]struct{}, len(sources))
	unique := make([]string, 0, len(sources))
	for _, source := range sources {
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		unique = append(unique, source)
	}
	sort.Strings(unique)
	return unique
}

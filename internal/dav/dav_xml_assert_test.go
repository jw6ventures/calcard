package dav

// Namespace-aware assertion helpers for the DAV compliance suites.
//
// The response models in xml_models.go carry marshal-only tags such as
// `xml:"d:multistatus"`, where the prefix is part of the local name, so they
// cannot be unmarshalled namespace-aware. Compliance tests decode the wire
// bytes into a generic namespace-resolved element tree instead, then retype
// that tree into the models below after checking it against the RFC content
// models. Assertions therefore compare resolved QNames and are indifferent to
// the prefix the server chooses.
//
// Struct-tag unmarshalling is deliberately not used for any of this. Given
// `xml:"DAV: propstat"` fields, encoding/xml silently discards a child no
// field matches and lets a repeated child overwrite the earlier one, so a
// document carrying two DAV:prop elements under one DAV:propstat, or a bogus
// element beside them, would decode exactly like a conforming one and pass
// assertions advertised as exact. checkChildModel is what rejects those.
//
// The content models are sequences, so checkChildModel checks sibling order
// too, and rejects character data inside the element-only containers. Both are
// normative: RFC 6578 §6.4 restates DAV:multistatus as
// (response*, responsedescription?, sync-token?), fixing where the sync token
// sits, and a DAV:propstat whose status precedes its prop is malformed however
// readable it is.
//
// Namespaces are resolved here rather than by encoding/xml, over RawToken, so
// that a prefix nothing binds is reported instead of being carried through as
// though it were a namespace name. See parseElement and namespaceScope.

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

const (
	nsDAV            = "DAV:"
	nsCalDAV         = "urn:ietf:params:xml:ns:caldav"
	nsCardDAV        = "urn:ietf:params:xml:ns:carddav"
	nsCalendarServer = "http://calendarserver.org/ns/"
	nsAppleICal      = "http://apple.com/ns/ical/"
)

// qn builds a namespace-qualified element name. An empty space matches an
// element serialized without a namespace, which is how a dead property stored
// with no namespace is written.
func qn(space, local string) xml.Name { return xml.Name{Space: space, Local: local} }

func davQN(local string) xml.Name { return qn(nsDAV, local) }

func calQN(local string) xml.Name { return qn(nsCalDAV, local) }

// davElement is a namespace-resolved XML subtree. DAV property values are
// arbitrary XML, so assertions need the whole tree rather than a typed struct.
type davElement struct {
	Name     xml.Name
	Attr     []xml.Attr
	Text     string
	Children []davElement
}

// parseElement reads one element and its subtree from d, which must be
// positioned just after start, resolving names against the namespace scope its
// ancestors establish.
//
// Tokens are read with RawToken rather than Token. Token resolves prefixes
// itself and, for a prefix nothing binds, leaves the bare prefix in Name.Space,
// which is then indistinguishable from a namespace whose URI is that same
// string: under Token a document declaring xmlns:x="d" makes an undeclared
// d:displayname look resolved. RawToken keeps the prefix, so resolving it here
// against a prefix-to-URI map reports the unbound prefix as the serialization
// defect it is. RawToken does not check that an end tag matches its start tag
// either, so this does.
func parseElement(d *xml.Decoder, start xml.StartElement, inherited namespaceScope) (davElement, error) {
	scope, err := inherited.extend(start.Attr)
	if err != nil {
		return davElement{}, err
	}
	name, err := scope.resolveElement(start.Name)
	if err != nil {
		return davElement{}, err
	}

	el := davElement{Name: name}
	for _, a := range start.Attr {
		// A namespace declaration is markup rather than an attribute of the
		// element, and the scope now carries what it said.
		if _, isDeclaration := namespaceDeclaration(a); isDeclaration {
			continue
		}
		resolved, err := scope.resolveAttr(a.Name)
		if err != nil {
			return davElement{}, err
		}
		el.Attr = append(el.Attr, xml.Attr{Name: resolved, Value: a.Value})
	}

	var text strings.Builder
	for {
		tok, err := d.RawToken()
		if errors.Is(err, io.EOF) {
			return davElement{}, fmt.Errorf("<%s> is never closed", rawQName(start.Name))
		}
		if err != nil {
			return davElement{}, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			child, err := parseElement(d, t, scope)
			if err != nil {
				return davElement{}, err
			}
			el.Children = append(el.Children, child)
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			if t.Name != start.Name {
				return davElement{}, fmt.Errorf("</%s> closes <%s>", rawQName(t.Name), rawQName(start.Name))
			}
			el.Text = text.String()
			return el, nil
		}
	}
}

// rawQName renders a name as it was written, for reporting a prefix that could
// not be resolved into a QName.
func rawQName(name xml.Name) string {
	if name.Space == "" {
		return name.Local
	}
	return name.Space + ":" + name.Local
}

// Value is the trimmed character data, which is what property-value assertions
// compare against.
func (e davElement) Value() string { return strings.TrimSpace(e.Text) }

// scalarText returns the direct character data of an element whose content
// model is text-only. Reading Text without checking Children would make
// `<DAV:status>HTTP/1.1 200 OK<DAV:x/></DAV:status>` indistinguishable from a
// conforming status line to every assertion built on it.
func scalarText(e davElement) (string, error) {
	if len(e.Children) != 0 {
		return "", fmt.Errorf("%s carries element children %s, want character data only",
			qnString(e.Name), qnList(e.childNames()))
	}
	return e.Text, nil
}

func (e davElement) childNames() []xml.Name {
	names := make([]xml.Name, 0, len(e.Children))
	for _, child := range e.Children {
		names = append(names, child.Name)
	}
	return names
}

func (e davElement) child(t *testing.T, name xml.Name) davElement {
	t.Helper()
	for _, child := range e.Children {
		if child.Name == name {
			return child
		}
	}
	t.Fatalf("%s has no child %s; children: %s", qnString(e.Name), qnString(name), qnList(e.childNames()))
	return davElement{}
}

// attr returns an unqualified attribute by name. Every attribute these
// assertions read — CALDAV:comp@name, DAV:time-range@start — is declared
// unqualified, so a prefixed x:name names a different attribute and must not
// answer the lookup.
func (e davElement) attr(local string) string {
	for _, a := range e.Attr {
		if a.Name.Space == "" && a.Name.Local == local {
			return a.Value
		}
	}
	return ""
}

type davMultistatus struct {
	SyncToken string
	Responses []davResponse
}

// davResponse holds Hrefs as a slice so tests can assert cardinality: RFC 4918
// §14.24 permits multiple hrefs only in a response carrying a bare status.
type davResponse struct {
	Hrefs     []string
	Propstats []davPropstat
	Status    string
	Error     *davError
}

type davPropstat struct {
	Prop   davPropBag
	Status string
	Error  *davError
}

// davPropBag captures every child of DAV:prop whatever its namespace.
type davPropBag struct {
	Props []davElement
}

type davError struct {
	XMLName    xml.Name
	Conditions []davElement
}

func qnString(name xml.Name) string {
	if name.Space == "" {
		return name.Local
	}
	return "{" + name.Space + "}" + name.Local
}

// qnList renders a set of QNames. It sorts, so the caller's order carries no
// meaning and two sets compare equal however they were built.
func qnList(names []xml.Name) string {
	if len(names) == 0 {
		return "(none)"
	}
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = qnString(name)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}

// qnSequence renders QNames in the given order, for reporting a content model
// whose order is the thing being violated.
func qnSequence(names []xml.Name) string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = qnString(name)
	}
	return strings.Join(out, ", ")
}

// xmlNamespaceURI is the namespace the "xml" prefix is bound to by the XML
// Namespaces specification itself, so a document never declares it.
const xmlNamespaceURI = "http://www.w3.org/XML/1998/namespace"

// namespaceScope is the prefix-to-URI mapping in force at one point in the
// document. Namespace names and prefixes are kept apart because they are
// different things that happen to be strings: a document is free to declare
// xmlns:x="d", and pooling the two would then let that declaration answer for
// an unbound d: prefix somewhere else.
//
// The default declaration is held separately from the prefixes because XML
// Namespaces §6.2 applies it to element names only. An unprefixed attribute is
// in no namespace at all, whatever default is in force.
type namespaceScope struct {
	prefixes  map[string]string
	byDefault string
}

// rootNamespaceScope is the scope outside the document element. XML Namespaces
// §3 binds the "xml" prefix by specification, so no document declares it.
func rootNamespaceScope() namespaceScope {
	return namespaceScope{prefixes: map[string]string{"xml": xmlNamespaceURI}}
}

// extend returns the scope in force inside an element carrying attrs. Scope is
// lexical (XML Namespaces §6.1): a declaration binds the element carrying it
// and that element's descendants, and nothing else, so the inherited map is
// copied rather than written through.
func (s namespaceScope) extend(attrs []xml.Attr) (namespaceScope, error) {
	extended, cloned := s, false
	for _, a := range attrs {
		prefix, isDeclaration := namespaceDeclaration(a)
		if !isDeclaration {
			continue
		}
		switch {
		case prefix == "xmlns":
			return s, fmt.Errorf(`the "xmlns" prefix is reserved and cannot be declared (XML Namespaces §3)`)
		case prefix == "xml" && a.Value != xmlNamespaceURI:
			return s, fmt.Errorf(`the "xml" prefix is bound to %s and cannot be rebound to %q (XML Namespaces §3)`,
				xmlNamespaceURI, a.Value)
		case prefix != "" && a.Value == "":
			return s, fmt.Errorf("xmlns:%s is empty; only the default namespace may be undeclared (XML Namespaces §2)", prefix)
		case prefix == "":
			extended.byDefault = a.Value
			continue
		}
		if !cloned {
			extended.prefixes, cloned = maps.Clone(s.prefixes), true
		}
		extended.prefixes[prefix] = a.Value
	}
	return extended, nil
}

// namespaceDeclaration reports whether a declares a namespace and, if so, which
// prefix it binds — the empty string for the default declaration. RawToken
// splits a qualified name on its colon without translating it, so xmlns:d
// arrives as {Space: "xmlns", Local: "d"} and a bare xmlns as {Local: "xmlns"}.
func namespaceDeclaration(a xml.Attr) (string, bool) {
	switch {
	case a.Name.Space == "xmlns":
		return a.Name.Local, true
	case a.Name.Space == "" && a.Name.Local == "xmlns":
		return "", true
	}
	return "", false
}

// resolveElement resolves an element's QName, giving an unprefixed name the
// default namespace in force.
func (s namespaceScope) resolveElement(name xml.Name) (xml.Name, error) {
	if name.Space == "" {
		return xml.Name{Space: s.byDefault, Local: name.Local}, nil
	}
	return s.resolvePrefixed(name)
}

// resolveAttr resolves an attribute's QName. XML Namespaces §6.2: the default
// declaration never applies to an attribute, so an unprefixed attribute name
// stays in no namespace — which is what the CALDAV:comp@name and
// CALDAV:time-range@start lookups depend on.
func (s namespaceScope) resolveAttr(name xml.Name) (xml.Name, error) {
	if name.Space == "" {
		return xml.Name{Local: name.Local}, nil
	}
	return s.resolvePrefixed(name)
}

func (s namespaceScope) resolvePrefixed(name xml.Name) (xml.Name, error) {
	uri, bound := s.prefixes[name.Space]
	if !bound {
		return xml.Name{}, fmt.Errorf("%s has an unresolved namespace prefix %q: no xmlns declaration in scope binds it",
			rawQName(name), name.Space)
	}
	return xml.Name{Space: uri, Local: name.Local}, nil
}

// unbounded is the upper bound of a childBound that admits any number.
const unbounded = -1

// childBound is one entry of an element's content model: a QName it admits and
// the number of times that name may appear.
type childBound struct {
	name xml.Name
	min  int
	max  int
}

func bound(name xml.Name, min, max int) childBound { return childBound{name: name, min: min, max: max} }

func boundNames(bounds []childBound) []xml.Name {
	names := make([]xml.Name, len(bounds))
	for i, b := range bounds {
		names[i] = b.name
	}
	return names
}

func (b childBound) rangeString() string {
	switch {
	case b.max == unbounded:
		return fmt.Sprintf("at least %d", b.min)
	case b.min == b.max:
		return fmt.Sprintf("exactly %d", b.min)
	default:
		return fmt.Sprintf("between %d and %d", b.min, b.max)
	}
}

// checkChildModel reports an error unless el matches the content model its
// bounds spell: every child is named by some bound, every bound's occurrence
// count falls within its range, and the children appear in the order the
// bounds are listed. Callers pass their bounds in the order the RFC declares
// them, because these models are sequences rather than sets.
//
// Every model expressed this way is element-only, so character data between
// the children is insignificant whitespace at best and a serialization defect
// at worst.
func checkChildModel(el davElement, bounds ...childBound) error {
	if text := strings.TrimSpace(el.Text); text != "" {
		return fmt.Errorf("%s carries character data %q; its content model admits only the elements %s",
			qnString(el.Name), text, qnList(boundNames(bounds)))
	}
	order := make(map[xml.Name]int, len(bounds))
	for i, b := range bounds {
		order[b.name] = i
	}
	counts := make(map[xml.Name]int, len(bounds))
	previous := -1
	var previousName xml.Name
	for _, child := range el.Children {
		position, admitted := order[child.Name]
		if !admitted {
			return fmt.Errorf("%s child %s is outside its content model, which admits %s",
				qnString(el.Name), qnString(child.Name), qnList(boundNames(bounds)))
		}
		if position < previous {
			return fmt.Errorf("%s child %s follows %s, but its content model sequences them the other way round: %s",
				qnString(el.Name), qnString(child.Name), qnString(previousName), qnSequence(boundNames(bounds)))
		}
		previous, previousName = position, child.Name
		counts[child.Name]++
	}
	for _, b := range bounds {
		if n := counts[b.name]; n < b.min || (b.max != unbounded && n > b.max) {
			return fmt.Errorf("%s carries %d %s children, want %s",
				qnString(el.Name), n, qnString(b.name), b.rangeString())
		}
	}
	return nil
}

// parseDAVDocument decodes a complete XML document into one namespace-resolved
// element tree. encoding/xml's Decode stops at the document element's end tag,
// so a second root element or trailing character data is accepted silently;
// XML well-formedness admits neither, and a body carrying either is a
// serialization defect rather than something to read past.
//
// Decoder.Entity is left empty on purpose, so only the five entities XML
// predefines resolve. Installing xml.HTMLEntity would make an undeclared
// &nbsp; — which no DTD-less DAV body may carry — decode as though the server
// had emitted something well-formed.
func parseDAVDocument(data []byte) (davElement, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	var (
		root  davElement
		found bool
	)
	for {
		tok, err := decoder.RawToken()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return davElement{}, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if found {
				return davElement{}, fmt.Errorf("document carries a second root element %s", rawQName(t.Name))
			}
			el, err := parseElement(decoder, t, rootNamespaceScope())
			if err != nil {
				return davElement{}, fmt.Errorf("decode %s: %w", rawQName(t.Name), err)
			}
			root, found = el, true
		case xml.CharData:
			if text := strings.TrimSpace(string(t)); text != "" {
				return davElement{}, fmt.Errorf("document carries character data %q outside its root element", text)
			}
		case xml.EndElement:
			return davElement{}, fmt.Errorf("</%s> closes nothing", rawQName(t.Name))
		}
	}
	if !found {
		return davElement{}, errors.New("document carries no root element")
	}
	return root, nil
}

// parseRootElement decodes a document and requires its root to be want.
func parseRootElement(data []byte, want xml.Name) (davElement, error) {
	root, err := parseDAVDocument(data)
	if err != nil {
		return davElement{}, err
	}
	if root.Name != want {
		return davElement{}, fmt.Errorf("root element = %s, want %s", qnString(root.Name), qnString(want))
	}
	return root, nil
}

// parseMultistatus decodes a DAV:multistatus body and validates the RFC 4918
// §14.16 content model, plus RFC 6578 §6.4's DAV:sync-token addition.
func parseMultistatus(data []byte) (davMultistatus, error) {
	root, err := parseRootElement(data, davQN("multistatus"))
	if err != nil {
		return davMultistatus{}, err
	}
	if err := checkChildModel(root,
		bound(davQN("response"), 0, unbounded),
		bound(davQN("responsedescription"), 0, 1),
		bound(davQN("sync-token"), 0, 1),
	); err != nil {
		return davMultistatus{}, err
	}
	var ms davMultistatus
	for _, child := range root.Children {
		switch child.Name {
		case davQN("response"):
			resp, err := parseResponse(child)
			if err != nil {
				return davMultistatus{}, err
			}
			ms.Responses = append(ms.Responses, resp)
		case davQN("sync-token"):
			text, err := scalarText(child)
			if err != nil {
				return davMultistatus{}, err
			}
			ms.SyncToken = strings.TrimSpace(text)
		}
	}
	return ms, nil
}

// parseResponse validates the RFC 4918 §14.24 content model of a DAV:response
// and retypes it:
//
//	<!ELEMENT response (href, ((href*, status)|(propstat+)), error?,
//	                    responsedescription?, location?) >
//
// The alternation is the substantive half: a response answers with per-property
// statuses or with one bare status for the whole resource, never both, and only
// the bare-status form may name several hrefs. Whether a DAV:status is present
// is tracked apart from what it says, because an empty <DAV:status/> is present
// for the purposes of that alternation however little it carries.
func parseResponse(el davElement) (davResponse, error) {
	if err := checkChildModel(el,
		bound(davQN("href"), 1, unbounded),
		bound(davQN("propstat"), 0, unbounded),
		bound(davQN("status"), 0, 1),
		bound(davQN("error"), 0, 1),
		bound(davQN("responsedescription"), 0, 1),
		bound(davQN("location"), 0, 1),
	); err != nil {
		return davResponse{}, err
	}

	var (
		resp      davResponse
		hasStatus bool
	)
	for _, child := range el.Children {
		switch child.Name {
		case davQN("href"):
			href, err := hrefText(child)
			if err != nil {
				return davResponse{}, err
			}
			resp.Hrefs = append(resp.Hrefs, href)
		case davQN("propstat"):
			ps, err := parsePropstat(child)
			if err != nil {
				return davResponse{}, err
			}
			resp.Propstats = append(resp.Propstats, ps)
		case davQN("status"):
			text, err := scalarText(child)
			if err != nil {
				return davResponse{}, err
			}
			resp.Status, hasStatus = text, true
		case davQN("error"):
			resp.Error = &davError{XMLName: child.Name, Conditions: child.Children}
		}
	}

	switch {
	case len(resp.Propstats) > 0 && hasStatus:
		return davResponse{}, fmt.Errorf("DAV:response carries both %d DAV:propstat elements and a bare DAV:status %q, which RFC 4918 §14.24 does not admit",
			len(resp.Propstats), resp.Status)
	case len(resp.Propstats) == 0 && !hasStatus:
		return davResponse{}, errors.New("DAV:response carries neither a DAV:propstat nor a DAV:status")
	case len(resp.Hrefs) > 1 && len(resp.Propstats) > 0:
		return davResponse{}, fmt.Errorf("DAV:response carries %d DAV:href elements alongside DAV:propstat; RFC 4918 §14.24 admits several hrefs only with a bare DAV:status", len(resp.Hrefs))
	}
	if hasStatus {
		if _, err := parseStatusLine(resp.Status); err != nil {
			return davResponse{}, err
		}
	}
	return resp, nil
}

// parsePropstat validates the RFC 4918 §14.22 content model of a DAV:propstat:
//
//	<!ELEMENT propstat (prop, status, error?, responsedescription?) >
func parsePropstat(el davElement) (davPropstat, error) {
	if err := checkChildModel(el,
		bound(davQN("prop"), 1, 1),
		bound(davQN("status"), 1, 1),
		bound(davQN("error"), 0, 1),
		bound(davQN("responsedescription"), 0, 1),
	); err != nil {
		return davPropstat{}, err
	}

	var ps davPropstat
	for _, child := range el.Children {
		switch child.Name {
		case davQN("prop"):
			ps.Prop.Props = child.Children
		case davQN("status"):
			text, err := scalarText(child)
			if err != nil {
				return davPropstat{}, err
			}
			ps.Status = text
		case davQN("error"):
			ps.Error = &davError{XMLName: child.Name, Conditions: child.Children}
		}
	}
	if _, err := parseStatusLine(ps.Status); err != nil {
		return davPropstat{}, err
	}
	return ps, nil
}

// decodeMultistatus decodes a 207 body, failing the test unless the status is
// 207 Multi-Status and the body is a DAV:multistatus conforming to the RFC 4918
// content models.
func decodeMultistatus(t *testing.T, rr *httptest.ResponseRecorder) davMultistatus {
	t.Helper()
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want 207 Multi-Status; body: %s", rr.Code, rr.Body.String())
	}
	ms, err := parseMultistatus(rr.Body.Bytes())
	if err != nil {
		t.Fatalf("decode multistatus: %v; body: %s", err, rr.Body.String())
	}
	return ms
}

// hrefText returns the URI a DAV:href carries. RFC 4918 §14.7 makes the
// element's content the URI itself, so surrounding whitespace is not
// insignificant markup to trim away: it is a malformed href, and trimming it
// would let one satisfy an assertion advertised as exact.
func hrefText(el davElement) (string, error) {
	text, err := scalarText(el)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(text) != text {
		return "", fmt.Errorf("%s content %q is padded with whitespace; RFC 4918 §14.7 makes the element content the URI itself",
			qnString(el.Name), text)
	}
	return text, nil
}

// canonicalHref normalizes a DAV href for comparison, applying RFC 3986 §6.2.2
// syntax-based normalization and nothing beyond it:
//
//   - scheme and authority are case-normalized (§6.2.2.1) but kept whole, so an
//     href naming a foreign host never matches a path-absolute expectation and
//     one carrying userinfo never matches the same URI without it;
//   - percent-escapes are decoded only for unreserved characters (§6.2.2.2), so
//     "%2F" stays distinct from "/";
//   - the path is otherwise compared verbatim. Dot segments and empty segments
//     are deliberately *not* removed: CalCard emits neither, so an href
//     carrying one is a serialization regression rather than something to
//     normalize into agreement.
//
// Any query or fragment is kept for the same reason, as is the opaque part of a
// non-hierarchical URI such as the mailto: form RFC 6638 §2.4.1 uses for a
// calendar user address. The trailing slash is preserved because it is what
// distinguishes a collection href from a calendar object resource href.
//
// Whitespace is not trimmed. hrefText rejects a padded href outright, so
// folding one into agreement here would only hide it from the href assertions.
func canonicalHref(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	var out strings.Builder
	if u.Scheme != "" {
		out.WriteString(strings.ToLower(u.Scheme))
		out.WriteString(":")
	}
	if u.Opaque != "" {
		out.WriteString(normalizePercentEncoding(u.Opaque))
	}
	if u.Host != "" || u.User != nil {
		out.WriteString("//")
		if u.User != nil {
			out.WriteString(u.User.String())
			out.WriteString("@")
		}
		out.WriteString(strings.ToLower(u.Host))
	}
	out.WriteString(normalizePercentEncoding(u.EscapedPath()))
	if u.ForceQuery || u.RawQuery != "" {
		out.WriteString("?")
		out.WriteString(u.RawQuery)
	}
	if u.Fragment != "" {
		out.WriteString("#")
		out.WriteString(u.EscapedFragment())
	}
	return out.String()
}

// normalizePercentEncoding implements RFC 3986 §6.2.2.2: an escape naming an
// unreserved character is decoded, and every other escape keeps its percent
// form with upper-cased hex digits. Reserved characters therefore stay escaped,
// which is what keeps "/a%2Fb.ics" from comparing equal to "/a/b.ics".
func normalizePercentEncoding(escaped string) string {
	var out strings.Builder
	for i := 0; i < len(escaped); i++ {
		c := escaped[i]
		if c != '%' || i+2 >= len(escaped) {
			out.WriteByte(c)
			continue
		}
		hi, hiOK := unhexDigit(escaped[i+1])
		lo, loOK := unhexDigit(escaped[i+2])
		if !hiOK || !loOK {
			out.WriteByte(c)
			continue
		}
		if decoded := hi<<4 | lo; isUnreservedURIChar(decoded) {
			out.WriteByte(decoded)
		} else {
			out.WriteString("%")
			out.WriteString(strings.ToUpper(escaped[i+1 : i+3]))
		}
		i += 2
	}
	return out.String()
}

func unhexDigit(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// isUnreservedURIChar reports the RFC 3986 §2.3 unreserved set.
func isUnreservedURIChar(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '-', c == '.', c == '_', c == '~':
		return true
	}
	return false
}

func (ms davMultistatus) hrefs() []string {
	out := make([]string, 0, len(ms.Responses))
	for _, resp := range ms.Responses {
		for _, href := range resp.Hrefs {
			out = append(out, canonicalHref(href))
		}
	}
	return out
}

// responseForHref returns the single DAV:response for want. RFC 4918 §13 allows
// exactly one response per URL, so zero or several matches both fail.
func (ms davMultistatus) responseForHref(t *testing.T, want string) davResponse {
	t.Helper()
	target := canonicalHref(want)
	var found []davResponse
	for _, resp := range ms.Responses {
		for _, href := range resp.Hrefs {
			if canonicalHref(href) == target {
				found = append(found, resp)
				break
			}
		}
	}
	switch len(found) {
	case 1:
		return found[0]
	case 0:
		t.Fatalf("no DAV:response for href %s; got %v", target, ms.hrefs())
	default:
		t.Fatalf("%d DAV:response elements for href %s, want exactly 1", len(found), target)
	}
	return davResponse{}
}

// assertHrefs fails unless the multistatus carries exactly these hrefs, in any
// order. This replaces counting "<d:response>" occurrences in the raw body.
func (ms davMultistatus) assertHrefs(t *testing.T, want ...string) {
	t.Helper()
	got := ms.hrefs()
	wantCanonical := make([]string, len(want))
	for i, href := range want {
		wantCanonical[i] = canonicalHref(href)
	}
	sortedGot := append([]string(nil), got...)
	sort.Strings(sortedGot)
	sort.Strings(wantCanonical)
	if strings.Join(sortedGot, "\n") != strings.Join(wantCanonical, "\n") {
		t.Errorf("multistatus hrefs = %v, want %v", sortedGot, wantCanonical)
	}
}

// assertHref asserts the response carries exactly one DAV:href and that it is
// want. RFC 4918 §14.24 permits several hrefs only in a response whose status
// is a bare DAV:status.
func (r davResponse) assertHref(t *testing.T, want string) {
	t.Helper()
	if len(r.Hrefs) != 1 {
		t.Fatalf("DAV:response carries %d DAV:href elements, want exactly 1: %v", len(r.Hrefs), r.Hrefs)
	}
	if got := canonicalHref(r.Hrefs[0]); got != canonicalHref(want) {
		t.Errorf("DAV:response href = %q, want %q", got, canonicalHref(want))
	}
}

// responseFromElement retypes a DAV:response that appears somewhere other than
// as a direct child of DAV:multistatus — RFC 3253 §3.8 inlines one inside an
// expanded property — so the nested document is asserted with the same typed
// helpers, and against the same content models, as a top-level response.
func responseFromElement(t *testing.T, el davElement) davResponse {
	t.Helper()
	if el.Name != davQN("response") {
		t.Fatalf("element = %s, want %s", qnString(el.Name), qnString(davQN("response")))
	}
	resp, err := parseResponse(el)
	if err != nil {
		t.Fatalf("inlined DAV:response: %v", err)
	}
	return resp
}

// statusCodeFromLine parses an RFC 4918 §14.28 status line such as
// "HTTP/1.1 200 OK". The element carries an RFC 2616 Status-Line, so every part
// of it is checked: a bare code, an unparseable HTTP-Version, a status code
// that is not exactly three digits, and a missing Reason-Phrase all fail.
// Anything looser lets a mutation of the DAV status serialization satisfy the
// exact-status assertions built on this.
func statusCodeFromLine(t *testing.T, line string) int {
	t.Helper()
	code, err := parseStatusLine(line)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return code
}

// parseStatusLine is the whole of that check, split out so the malformed
// spellings can be pinned by a table rather than by a test that must survive
// its own t.Fatalf.
//
// RFC 2616 §6.1 gives the production as
//
//	Status-Line = HTTP-Version SP Status-Code SP Reason-Phrase CRLF
//
// so the two separators are one space each and are parsed as such: a tab, a
// repeated space or a padded field is a malformed line rather than whitespace
// to fold away. The Reason-Phrase is *<TEXT, excluding CR, LF>, which admits
// the empty phrase, so "HTTP/1.1 200 " is well formed and only the second SP
// is required. Reading the raw character data rather than the trimmed value is
// what keeps that case distinguishable from "HTTP/1.1 200", which is not.
func parseStatusLine(line string) (int, error) {
	const want = "want an RFC 2616 §6.1 Status-Line `HTTP/1.1 <code> <reason>`"
	version, rest, ok := strings.Cut(line, " ")
	if !ok {
		return 0, fmt.Errorf("malformed DAV:status %q, %s", line, want)
	}
	if _, _, ok := http.ParseHTTPVersion(version); !ok {
		return 0, fmt.Errorf("malformed HTTP-Version %q in DAV:status %q, %s", version, line, want)
	}
	code, reason, ok := strings.Cut(rest, " ")
	if !ok {
		return 0, fmt.Errorf("DAV:status %q carries no Reason-Phrase separator, %s", line, want)
	}
	if !isThreeDigitStatusCode(code) {
		return 0, fmt.Errorf("status code %q in DAV:status %q is not exactly three digits, %s", code, line, want)
	}
	if strings.ContainsAny(reason, "\r\n") {
		return 0, fmt.Errorf("Reason-Phrase %q in DAV:status %q carries CR or LF, which RFC 2616 §6.1 excludes", reason, line)
	}
	parsed, err := strconv.Atoi(code)
	if err != nil {
		return 0, fmt.Errorf("malformed status code in DAV:status %q: %v", line, err)
	}
	return parsed, nil
}

// isThreeDigitStatusCode reports the RFC 2616 §6.1.1 Status-Code production.
// The digit check is what rejects a zero-padded "0200" or a signed "+99",
// both of which strconv.Atoi would answer with a plausible-looking code.
func isThreeDigitStatusCode(field string) bool {
	if len(field) != 3 {
		return false
	}
	for i := 0; i < len(field); i++ {
		if field[i] < '0' || field[i] > '9' {
			return false
		}
	}
	return true
}

// findProp locates name across r's propstats. It fails when the property
// appears in more than one propstat, which RFC 4918 §9.1 forbids and which is
// the defect class commit 89d7538 had to fix by hand.
func (r davResponse) findProp(t *testing.T, name xml.Name) (davElement, int, bool) {
	t.Helper()
	var (
		found  davElement
		status int
		hits   int
	)
	for _, ps := range r.Propstats {
		for _, p := range ps.Prop.Props {
			if p.Name != name {
				continue
			}
			hits++
			found = p
			status = statusCodeFromLine(t, ps.Status)
		}
	}
	if hits > 1 {
		t.Errorf("property %s appears in %d propstats, want at most 1", qnString(name), hits)
	}
	return found, status, hits > 0
}

func (r davResponse) propNames() []xml.Name {
	var names []xml.Name
	for _, ps := range r.Propstats {
		for _, p := range ps.Prop.Props {
			names = append(names, p.Name)
		}
	}
	return names
}

// assertPropStatus asserts name appears exactly once across r's propstats and
// that its propstat carries wantStatus. It returns the element so callers can
// go on to assert its value or children.
func (r davResponse) assertPropStatus(t *testing.T, name xml.Name, wantStatus int) davElement {
	t.Helper()
	el, status, ok := r.findProp(t, name)
	if !ok {
		t.Fatalf("property %s is in no propstat; response carries: %s", qnString(name), qnList(r.propNames()))
	}
	if status != wantStatus {
		t.Errorf("property %s is in the %d propstat, want %d", qnString(name), status, wantStatus)
	}
	return el
}

// assertPropValue asserts placement in a wantStatus propstat and an exact
// trimmed text value.
func (r davResponse) assertPropValue(t *testing.T, name xml.Name, wantStatus int, want string) {
	t.Helper()
	el := r.assertPropStatus(t, name, wantStatus)
	text, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(text); got != want {
		t.Errorf("property %s = %q, want %q", qnString(name), got, want)
	}
}

// assertPropDateTime asserts name is served at 200 and carries an RFC 5545 UTC
// DATE-TIME, returning the parsed instant. RFC 4791 §5.2.6/§5.2.7 fix that form
// for min-date-time and max-date-time, so a local-time or offset spelling is a
// failure rather than something to normalize away.
func (r davResponse) assertPropDateTime(t *testing.T, name xml.Name) time.Time {
	t.Helper()
	el := r.assertPropStatus(t, name, http.StatusOK)
	text, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}
	value := strings.TrimSpace(text)
	parsed, err := time.Parse("20060102T150405Z", value)
	if err != nil {
		t.Fatalf("property %s = %q, want an RFC 5545 UTC DATE-TIME (YYYYMMDDTHHMMSSZ): %v", qnString(name), value, err)
	}
	return parsed
}

// assertPropInt asserts name is served at 200 and carries a positive integer,
// which is the value form RFC 4791 §5.2.5/§5.2.8/§5.2.9 give the numeric limit
// properties.
func (r davResponse) assertPropInt(t *testing.T, name xml.Name) int64 {
	t.Helper()
	el := r.assertPropStatus(t, name, http.StatusOK)
	text, err := scalarText(el)
	if err != nil {
		t.Fatal(err)
	}
	value := strings.TrimSpace(text)
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("property %s = %q, want a base-10 integer: %v", qnString(name), value, err)
	}
	if parsed <= 0 {
		t.Errorf("property %s = %d, want a positive limit", qnString(name), parsed)
	}
	return parsed
}

// assertPropAbsent asserts name appears in no propstat at all. This is the
// correct assertion for a property excluded from DAV:allprop: absent, not
// present with a 404.
func (r davResponse) assertPropAbsent(t *testing.T, name xml.Name) {
	t.Helper()
	if _, status, ok := r.findProp(t, name); ok {
		t.Errorf("property %s is present in the %d propstat, want absent", qnString(name), status)
	}
}

// assertPropHrefs asserts name is served at 200 and carries exactly these
// DAV:href children, compared canonically and order-insensitively. The home
// sets, principal-URL, current-user-principal and principal-collection-set all
// take this shape, and a child that is not a DAV:href is a failure.
func (r davResponse) assertPropHrefs(t *testing.T, name xml.Name, want ...string) {
	t.Helper()
	el := r.assertPropStatus(t, name, http.StatusOK)
	if text := strings.TrimSpace(el.Text); text != "" {
		t.Errorf("property %s carries character data %q, want DAV:href children only", qnString(name), text)
	}
	got := make([]string, 0, len(el.Children))
	for _, child := range el.Children {
		if child.Name != davQN("href") {
			t.Errorf("property %s child = %s, want %s", qnString(name), qnString(child.Name), qnString(davQN("href")))
			continue
		}
		href, err := hrefText(child)
		if err != nil {
			t.Errorf("property %s: %v", qnString(name), err)
			continue
		}
		got = append(got, canonicalHref(href))
	}
	wantCanonical := make([]string, len(want))
	for i, href := range want {
		wantCanonical[i] = canonicalHref(href)
	}
	sort.Strings(got)
	sort.Strings(wantCanonical)
	if strings.Join(got, "\n") != strings.Join(wantCanonical, "\n") {
		t.Errorf("property %s hrefs = %v, want %v", qnString(name), got, wantCanonical)
	}
}

// assertPropChildNames asserts the exact set of direct child QNames under name,
// order-insensitive. Used for resourcetype, supported-report-set,
// current-user-privilege-set and supported-calendar-component-set.
func (r davResponse) assertPropChildNames(t *testing.T, name xml.Name, want ...xml.Name) {
	t.Helper()
	el := r.assertPropStatus(t, name, http.StatusOK)
	if got := qnList(el.childNames()); got != qnList(want) {
		t.Errorf("property %s children = %s, want %s", qnString(name), got, qnList(want))
	}
}

// assertSoleChild asserts parent carries exactly one child and that it is
// named want, returning it. RFC 3253 §3.1.5 and RFC 3744 §5.4/§5.5 all define
// single-child wrappers, so an extra sibling is a malformed response rather
// than something to search past.
func assertSoleChild(t *testing.T, parent davElement, want xml.Name) davElement {
	t.Helper()
	if len(parent.Children) != 1 || parent.Children[0].Name != want {
		t.Fatalf("%s children = %s, want exactly %s", qnString(parent.Name), qnList(parent.childNames()), qnString(want))
	}
	return parent.Children[0]
}

// assertSoleHref asserts parent wraps exactly one DAV:href and that it names
// want. It is the single-href counterpart of assertPropHrefs, for the wrappers
// reached through an element rather than by property name: a DAV:principal
// inside an ACE, or a home set already located by assertPropStatus. Reading
// the URI through hrefText is what keeps a padded href from being trimmed into
// agreement here.
func assertSoleHref(t *testing.T, parent davElement, want string) {
	t.Helper()
	href, err := hrefText(assertSoleChild(t, parent, davQN("href")))
	if err != nil {
		t.Fatalf("%s: %v", qnString(parent.Name), err)
	}
	if got := canonicalHref(href); got != canonicalHref(want) {
		t.Errorf("%s href = %q, want %q", qnString(parent.Name), got, canonicalHref(want))
	}
}

// supportedReports returns the QName of every report advertised in
// DAV:supported-report-set, unwrapping the RFC 3253 §3.1.5
// supported-report-set/supported-report/report nesting. Each wrapper must carry
// exactly one child, which is what a flattened, double-wrapped or
// extra-sibling encoding would violate.
func (r davResponse) supportedReports(t *testing.T) []xml.Name {
	t.Helper()
	set := r.assertPropStatus(t, davQN("supported-report-set"), http.StatusOK)
	var reports []xml.Name
	for _, supported := range set.Children {
		if supported.Name != davQN("supported-report") {
			t.Errorf("supported-report-set child = %s, want %s", qnString(supported.Name), qnString(davQN("supported-report")))
			continue
		}
		report := assertSoleChild(t, supported, davQN("report"))
		if len(report.Children) != 1 {
			t.Errorf("DAV:report wraps %d elements, want exactly 1: %s", len(report.Children), qnList(report.childNames()))
			continue
		}
		reports = append(reports, report.Children[0].Name)
	}
	return reports
}

// assertSupportedReports asserts the exact set of advertised reports.
func (r davResponse) assertSupportedReports(t *testing.T, want ...xml.Name) {
	t.Helper()
	if got := qnList(r.supportedReports(t)); got != qnList(want) {
		t.Errorf("supported-report-set advertises %s, want %s", got, qnList(want))
	}
}

// privileges returns the privilege QNames carried by DAV:current-user-privilege-set
// (RFC 3744 §5.4), each of which is wrapped in its own DAV:privilege element.
func (r davResponse) privileges(t *testing.T) []xml.Name {
	t.Helper()
	set := r.assertPropStatus(t, davQN("current-user-privilege-set"), http.StatusOK)
	var granted []xml.Name
	for _, priv := range set.Children {
		if priv.Name != davQN("privilege") {
			t.Errorf("current-user-privilege-set child = %s, want %s", qnString(priv.Name), qnString(davQN("privilege")))
			continue
		}
		if len(priv.Children) != 1 {
			t.Errorf("DAV:privilege wraps %d elements, want exactly 1: %s", len(priv.Children), qnList(priv.childNames()))
			continue
		}
		granted = append(granted, priv.Children[0].Name)
	}
	return granted
}

// supportedPrivileges walks DAV:supported-privilege-set (RFC 3744 §5.3) and
// returns one slash-joined path per privilege, naming each privilege beneath
// the privileges that aggregate it.
//
// Membership cannot express aggregation, which is what RFC 4791 §6.1.1 requires
// of CALDAV:read-free-busy: a server advertising read and read-free-busy as two
// unrelated privileges satisfies every containment check and still does not
// aggregate one in the other. Paths do express it.
func (r davResponse) supportedPrivileges(t *testing.T) []string {
	t.Helper()
	set := r.assertPropStatus(t, davQN("supported-privilege-set"), http.StatusOK)

	var paths []string
	var walk func(prefix string, el davElement)
	walk = func(prefix string, el davElement) {
		// <!ELEMENT supported-privilege
		//           (privilege, abstract?, description, supported-privilege*) >
		if err := checkChildModel(el,
			bound(davQN("privilege"), 1, 1),
			bound(davQN("abstract"), 0, 1),
			bound(davQN("description"), 1, 1),
			bound(davQN("supported-privilege"), 0, unbounded),
		); err != nil {
			t.Errorf("RFC 3744 §5.3: %v", err)
			return
		}
		privilege := el.child(t, davQN("privilege"))
		if len(privilege.Children) != 1 {
			t.Errorf("DAV:privilege wraps %d elements, want exactly 1: %s",
				len(privilege.Children), qnList(privilege.childNames()))
			return
		}
		path := qnString(privilege.Children[0].Name)
		if prefix != "" {
			path = prefix + "/" + path
		}
		paths = append(paths, path)
		for _, child := range el.Children {
			if child.Name == davQN("supported-privilege") {
				walk(path, child)
			}
		}
	}

	for _, child := range set.Children {
		if child.Name != davQN("supported-privilege") {
			t.Errorf("supported-privilege-set child = %s, want %s",
				qnString(child.Name), qnString(davQN("supported-privilege")))
			continue
		}
		walk("", child)
	}
	sort.Strings(paths)
	return paths
}

// assertSupportedPrivileges asserts the exact aggregation hierarchy, each entry
// naming a privilege by the path of aggregates that contain it.
func (r davResponse) assertSupportedPrivileges(t *testing.T, want ...string) {
	t.Helper()
	got := r.supportedPrivileges(t)
	sortedWant := append([]string(nil), want...)
	sort.Strings(sortedWant)
	if strings.Join(got, "\n") != strings.Join(sortedWant, "\n") {
		t.Errorf("supported-privilege-set = %v, want %v", got, sortedWant)
	}
}

// checkExactlyOne reports an error unless el carries exactly one child drawn
// from names. The RFC 3744 ACE models spell alternations — (principal | invert)
// and (grant | deny) — which checkChildModel cannot express: it bounds each name
// on its own, and "one of these, and not the others" is a constraint across
// names.
func checkExactlyOne(el davElement, names ...xml.Name) error {
	var found []xml.Name
	for _, child := range el.Children {
		for _, name := range names {
			if child.Name == name {
				found = append(found, child.Name)
			}
		}
	}
	if len(found) != 1 {
		return fmt.Errorf("%s carries %s, want exactly one of %s",
			qnString(el.Name), qnList(found), qnList(names))
	}
	return nil
}

// checkACEModel validates a DAV:ace against RFC 3744 §5.5:
//
//	<!ELEMENT ace ((principal | invert), (grant|deny), protected?, inherited?)>
//
// Both alternations carry weight for a reader of the ACE. An ACE naming two
// principals says which of them the privileges apply to only by the order it
// happens to serialize them in, and one carrying both a DAV:grant and a
// DAV:deny says the opposite of itself.
func checkACEModel(ace davElement) error {
	if err := checkChildModel(ace,
		bound(davQN("principal"), 0, 1),
		bound(davQN("invert"), 0, 1),
		bound(davQN("grant"), 0, 1),
		bound(davQN("deny"), 0, 1),
		bound(davQN("protected"), 0, 1),
		bound(davQN("inherited"), 0, 1),
	); err != nil {
		return err
	}
	if err := checkExactlyOne(ace, davQN("principal"), davQN("invert")); err != nil {
		return err
	}
	return checkExactlyOne(ace, davQN("grant"), davQN("deny"))
}

// checkACEPrincipalModel validates a DAV:principal against RFC 3744 §5.5.1:
//
//	<!ELEMENT principal (href | all | authenticated | unauthenticated
//	                     | property | self)>
func checkACEPrincipalModel(principal davElement) error {
	forms := []xml.Name{
		davQN("href"), davQN("all"), davQN("authenticated"),
		davQN("unauthenticated"), davQN("property"), davQN("self"),
	}
	bounds := make([]childBound, len(forms))
	for i, form := range forms {
		bounds[i] = bound(form, 0, 1)
	}
	if err := checkChildModel(principal, bounds...); err != nil {
		return err
	}
	return checkExactlyOne(principal, forms...)
}

// checkPrivilegeContainerModel validates a DAV:grant or DAV:deny against
// RFC 3744 §5.5.2:
//
//	<!ELEMENT grant (privilege+)>
//	<!ELEMENT deny (privilege+)>
//
// DAV:privilege is declared ANY, but every privilege RFC 3744 §3 and RFC 4791
// §6.1 define is one element naming it, so a DAV:privilege wrapping none or
// several is a serialization no client can read as a privilege.
func checkPrivilegeContainerModel(el davElement) error {
	if err := checkChildModel(el, bound(davQN("privilege"), 1, unbounded)); err != nil {
		return err
	}
	for _, privilege := range el.Children {
		if len(privilege.Children) != 1 {
			return fmt.Errorf("%s carries a DAV:privilege wrapping %d elements, want exactly 1: %s",
				qnString(el.Name), len(privilege.Children), qnList(privilege.childNames()))
		}
	}
	return nil
}

// aces returns the DAV:ace children of DAV:acl (RFC 3744 §5.5), asserting the
// property is served at 200, that it carries nothing but ACEs, and that each
// ACE matches its content model. The model check belongs here rather than in
// the extractors below, so an ACE reaches them only once it is known to hold a
// single principal and a single grant-or-deny.
func (r davResponse) aces(t *testing.T) []davElement {
	t.Helper()
	acl := r.assertPropStatus(t, davQN("acl"), http.StatusOK)
	for _, ace := range acl.Children {
		if ace.Name != davQN("ace") {
			t.Errorf("DAV:acl child = %s, want %s", qnString(ace.Name), qnString(davQN("ace")))
			continue
		}
		if err := checkACEModel(ace); err != nil {
			t.Errorf("RFC 3744 §5.5: %v", err)
		}
	}
	return acl.Children
}

// acePrincipals returns the QNames identifying an ACE's DAV:principal, after
// checking it against RFC 3744 §5.5.1. The result is a slice because §5.5.1
// admits a DAV:href as well as the special DAV:all, DAV:authenticated,
// DAV:unauthenticated, DAV:property and DAV:self elements, and the caller
// compares against whichever form it expects; the model check is what stops a
// principal naming two of them from reading as the first.
//
// An ACE that wraps its principal in DAV:invert is reported here as an ACE with
// no DAV:principal. CalCard emits none, and a test asserting about one would be
// asserting about the inverted set rather than the named principal.
func acePrincipals(t *testing.T, ace davElement) []xml.Name {
	t.Helper()
	principal := ace.child(t, davQN("principal"))
	if err := checkACEPrincipalModel(principal); err != nil {
		t.Errorf("RFC 3744 §5.5.1: %v", err)
	}
	return principal.childNames()
}

// acePrivileges returns the privilege QNames under an ACE's container, which is
// DAV:grant or DAV:deny, after checking it against RFC 3744 §5.5.2. Each
// privilege is wrapped in its own DAV:privilege.
func acePrivileges(t *testing.T, ace davElement, container xml.Name) []xml.Name {
	t.Helper()
	el := ace.child(t, container)
	if err := checkPrivilegeContainerModel(el); err != nil {
		t.Errorf("RFC 3744 §5.5.2: %v", err)
		return nil
	}
	granted := make([]xml.Name, 0, len(el.Children))
	for _, privilege := range el.Children {
		granted = append(granted, privilege.Children[0].Name)
	}
	return granted
}

// assertPrivileges asserts the exact set of privileges the response grants.
// RFC 4791 §6.1.1 lets CALDAV:read-free-busy be aggregated inside DAV:read, and
// an aggregated child is not part of this set: it is DAV:read that is granted.
func (r davResponse) assertPrivileges(t *testing.T, want ...xml.Name) {
	t.Helper()
	if got := qnList(r.privileges(t)); got != qnList(want) {
		t.Errorf("current-user-privilege-set grants %s, want %s", got, qnList(want))
	}
}

// assertHasPrivilege asserts want is among the granted privileges. It is a
// containment check, not an exact-set check, because the aggregate privileges a
// server chooses to enumerate alongside it are its own business.
func (r davResponse) assertHasPrivilege(t *testing.T, want xml.Name) {
	t.Helper()
	granted := r.privileges(t)
	for _, p := range granted {
		if p == want {
			return
		}
	}
	t.Errorf("current-user-privilege-set grants %s, want it to include %s", qnList(granted), qnString(want))
}

// assertSupportedComponents asserts the exact set of component names advertised
// by CALDAV:supported-calendar-component-set (RFC 4791 §5.2.3), which is carried
// in the "name" attribute of each CALDAV:comp child.
func (r davResponse) assertSupportedComponents(t *testing.T, want ...string) {
	t.Helper()
	set := r.assertPropStatus(t, calQN("supported-calendar-component-set"), http.StatusOK)
	if text := strings.TrimSpace(set.Text); text != "" {
		t.Errorf("supported-calendar-component-set carries character data %q, want CALDAV:comp children only", text)
	}
	var got []string
	for _, comp := range set.Children {
		if comp.Name != calQN("comp") {
			t.Errorf("supported-calendar-component-set child = %s, want %s", qnString(comp.Name), qnString(calQN("comp")))
			continue
		}
		text, err := scalarText(comp)
		if err != nil {
			t.Error(err)
			continue
		}
		if strings.TrimSpace(text) != "" {
			t.Errorf("%s carries character data %q, want an empty element", qnString(comp.Name), text)
		}
		name := comp.attr("name")
		if name == "" {
			t.Errorf("%s has no non-empty name attribute", qnString(comp.Name))
		}
		got = append(got, name)
	}
	sortedGot := append([]string(nil), got...)
	sortedWant := append([]string(nil), want...)
	sort.Strings(sortedGot)
	sort.Strings(sortedWant)
	if strings.Join(sortedGot, ",") != strings.Join(sortedWant, ",") {
		t.Errorf("supported-calendar-component-set advertises %v, want %v", sortedGot, sortedWant)
	}
}

// propstatsWithStatus returns every propstat of r whose status is wantStatus.
func (r davResponse) propstatsWithStatus(t *testing.T, wantStatus int) []davPropstat {
	t.Helper()
	var found []davPropstat
	for _, ps := range r.Propstats {
		if statusCodeFromLine(t, ps.Status) == wantStatus {
			found = append(found, ps)
		}
	}
	return found
}

// assertPropstatNames asserts the response carries exactly one propstat with
// wantStatus and that it carries exactly these property QNames, which is what
// catches a property leaking into the wrong propstat.
//
// The cardinality half matters as much as the name set. A response grouping
// one status across several propstats, or carrying an empty one beside the
// populated one, says the same thing to a client and is still a defect no
// concatenated name set can see. Use assertNoPropstatWithStatus to assert a
// status is absent; passing no names here would otherwise be satisfied by an
// empty propstat.
func (r davResponse) assertPropstatNames(t *testing.T, wantStatus int, want ...xml.Name) {
	t.Helper()
	matching := r.propstatsWithStatus(t, wantStatus)
	if len(matching) != 1 {
		t.Errorf("response carries %d propstats with status %d, want exactly 1 (carrying %s)",
			len(matching), wantStatus, qnList(want))
		return
	}
	var got []xml.Name
	for _, p := range matching[0].Prop.Props {
		got = append(got, p.Name)
	}
	if qnList(got) != qnList(want) {
		t.Errorf("%d propstat carries %s, want %s", wantStatus, qnList(got), qnList(want))
	}
}

// assertNoPropstatWithStatus asserts the response carries no propstat with
// wantStatus at all — the correct assertion for "nothing was reported at this
// status", which an empty propstat would not satisfy.
func (r davResponse) assertNoPropstatWithStatus(t *testing.T, wantStatus int) {
	t.Helper()
	for _, ps := range r.propstatsWithStatus(t, wantStatus) {
		t.Errorf("response carries a propstat with status %d, want none; it carries %s",
			wantStatus, qnList(propBagNames(ps.Prop)))
	}
}

func propBagNames(bag davPropBag) []xml.Name {
	names := make([]xml.Name, 0, len(bag.Props))
	for _, p := range bag.Props {
		names = append(names, p.Name)
	}
	return names
}

// assertErrorConditions decodes a standalone DAV:error body and asserts its
// condition elements are exactly want, compared as QNames.
func assertErrorConditions(t *testing.T, rr *httptest.ResponseRecorder, wantStatus int, want ...xml.Name) {
	t.Helper()
	if rr.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body: %s", rr.Code, wantStatus, rr.Body.String())
	}
	root, err := parseRootElement(rr.Body.Bytes(), davQN("error"))
	if err != nil {
		t.Fatalf("decode DAV:error: %v; body: %s", err, rr.Body.String())
	}
	var got []xml.Name
	for _, cond := range root.Children {
		got = append(got, cond.Name)
	}
	if qnList(got) != qnList(want) {
		t.Errorf("DAV:error conditions = %s, want %s", qnList(got), qnList(want))
	}
}

// assertMKCalendarCreated asserts the normative half of RFC 4791 §5.3.1 — a
// success body, when present, is a CALDAV:mkcalendar-response element — and
// pins the exact success status CalCard returns.
//
// The status half is CalCard policy, not an RFC 4791 requirement. §5.3.1.1 is
// "examples of response codes... by no means exhaustive", so it cannot make 201
// mandatory; a server answering 200 with a mkcalendar-response body would be
// conformant. CalCard answers 201, and pinning that here is what keeps the
// callers' postcondition assertions anchored to a known response.
func assertMKCalendarCreated(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	if rr.Code != http.StatusCreated {
		t.Fatalf("MKCALENDAR status = %d, want the 201 Created CalCard returns on success; body: %s", rr.Code, rr.Body.String())
	}
	if strings.TrimSpace(rr.Body.String()) == "" {
		return
	}
	// §9.3 declares the element ANY, so only its identity is asserted here.
	if _, err := parseRootElement(rr.Body.Bytes(), calQN("mkcalendar-response")); err != nil {
		t.Fatalf("RFC 4791 §5.3.1: a MKCALENDAR success body must be CALDAV:mkcalendar-response: %v; body: %s", err, rr.Body.String())
	}
}

// --- helper self-tests -----------------------------------------------------

func TestDavElementDecodeIsPrefixIndependent(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "server prefixes",
			body: `<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">` +
				`<d:response><d:href>/dav/calendars/1/</d:href><d:propstat><d:prop>` +
				`<d:displayname>Work</d:displayname><cal:calendar-description>Desc</cal:calendar-description>` +
				`</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>`,
		},
		{
			name: "uppercase prefixes",
			body: `<D:multistatus xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">` +
				`<D:response><D:href>/dav/calendars/1/</D:href><D:propstat><D:prop>` +
				`<D:displayname>Work</D:displayname><C:calendar-description>Desc</C:calendar-description>` +
				`</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response></D:multistatus>`,
		},
		{
			name: "default namespace",
			body: `<multistatus xmlns="DAV:" xmlns:x="urn:ietf:params:xml:ns:caldav">` +
				`<response><href>/dav/calendars/1/</href><propstat><prop>` +
				`<displayname>Work</displayname><x:calendar-description>Desc</x:calendar-description>` +
				`</prop><status>HTTP/1.1 200 OK</status></propstat></response></multistatus>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			rr.Code = http.StatusMultiStatus
			rr.Body.WriteString(tt.body)

			ms := decodeMultistatus(t, rr)
			ms.assertHrefs(t, "/dav/calendars/1/")
			resp := ms.responseForHref(t, "/dav/calendars/1/")
			resp.assertPropValue(t, davQN("displayname"), http.StatusOK, "Work")
			resp.assertPropValue(t, calQN("calendar-description"), http.StatusOK, "Desc")
			resp.assertPropAbsent(t, davQN("getetag"))
		})
	}
}

func TestDecodeMultistatusAcceptsProductionEncoding(t *testing.T) {
	payload := newMultistatus([]response{{
		Href: "/dav/calendars/1/",
		Propstat: []propstat{{
			Prop:   prop{DisplayName: stringPtr("Work")},
			Status: httpStatusOK,
		}, {
			PropNames: []xml.Name{davQN("getetag")},
			Status:    httpStatusNotFound,
		}},
	}}, "")

	rr := httptest.NewRecorder()
	writeMultiStatus(rr, payload)

	ms := decodeMultistatus(t, rr)
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertPropValue(t, davQN("displayname"), http.StatusOK, "Work")
	resp.assertPropStatus(t, davQN("getetag"), http.StatusNotFound)
	resp.assertPropstatNames(t, http.StatusOK, davQN("displayname"))
	resp.assertPropstatNames(t, http.StatusNotFound, davQN("getetag"))
}

func TestDecodeMultistatusAgainstHandlerResponse(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Work", UpdatedAt: store.Now()}, Editor: true},
		},
	}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: &fakeEventRepo{}}}

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/1/", strings.NewReader(
		`<d:propfind xmlns:d="DAV:"><d:prop><d:resourcetype/><d:displayname/></d:prop></d:propfind>`))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()

	h.Propfind(rr, req)

	ms := decodeMultistatus(t, rr)
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertPropChildNames(t, davQN("resourcetype"), davQN("collection"), calQN("calendar"))
	resp.assertPropValue(t, davQN("displayname"), http.StatusOK, "Work")
}

func TestCanonicalHref(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"/dav/calendars/1/", "/dav/calendars/1/"},
		{"/dav/calendars/1/event.ics", "/dav/calendars/1/event.ics"},
		// RFC 3986 §6.2.2.1: scheme and host are case-insensitive, the rest is not.
		{"HTTP://Example.COM/dav/Calendars/1/", "http://example.com/dav/Calendars/1/"},
		// §3.2.1 userinfo is part of the authority and is kept whole.
		{"https://Alice@Example.COM/dav/", "https://Alice@example.com/dav/"},
		// §3 opaque part of a non-hierarchical URI, the RFC 6638 §2.4.1 form.
		{"MAILTO:alice@Example.com", "mailto:alice@Example.com"},
		// §6.2.2.2: unreserved escapes decode, reserved escapes keep an
		// upper-cased percent form.
		{"/dav/calendars/1/%61%2Db.ics", "/dav/calendars/1/a-b.ics"},
		{"/dav/calendars/1/a%20b.ics", "/dav/calendars/1/a%20b.ics"},
		{"/dav/calendars/1/a%2fb.ics", "/dav/calendars/1/a%2Fb.ics"},
	}
	for _, tt := range tests {
		if got := canonicalHref(tt.raw); got != tt.want {
			t.Errorf("canonicalHref(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

// TestCanonicalHrefDistinguishesDistinctURIs pins the negative half: each pair
// below denotes a different resource, so the helper backing roughly 150 href
// assertions must not fold them together. Most compared equal under the
// path.Clean-based normalization this replaced; "collection versus resource"
// guards the one distinction that one did preserve; and the last four pin
// authority and whitespace handling, each of which collapsed a pair when
// canonicalHref serialized the host without its userinfo, dropped a
// non-hierarchical URI's opaque part, and trimmed its input.
func TestCanonicalHrefDistinguishesDistinctURIs(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{"encoded slash", "/dav/calendars/1/a%2Fb.ics", "/dav/calendars/1/a/b.ics"},
		{"foreign authority", "http://evil.example.net/dav/calendars/1/", "/dav/calendars/1/"},
		{"different authority", "http://a.example.com/dav/calendars/1/", "http://b.example.com/dav/calendars/1/"},
		{"query string", "/dav/calendars/1/?export", "/dav/calendars/1/"},
		{"fragment", "/dav/calendars/1/event.ics#vevent", "/dav/calendars/1/event.ics"},
		{"dot-dot segment", "/dav/calendars/1/../2/", "/dav/calendars/2/"},
		{"dot segment", "/dav/calendars/1/./event.ics", "/dav/calendars/1/event.ics"},
		{"empty segment", "/dav/calendars/1//event.ics", "/dav/calendars/1/event.ics"},
		{"collection versus resource", "/dav/calendars/1/", "/dav/calendars/1"},
		{"userinfo", "https://alice@cal.example/dav/calendars/1/", "https://cal.example/dav/calendars/1/"},
		{"different userinfo", "https://alice@cal.example/x", "https://bob@cal.example/x"},
		{"opaque mailbox", "mailto:alice@example.com", "mailto:bob@example.com"},
		{"whitespace padding", "  /dav/calendars/1/  ", "/dav/calendars/1/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, other := canonicalHref(tt.a), canonicalHref(tt.b); got == other {
				t.Errorf("canonicalHref(%q) == canonicalHref(%q) == %q, want distinct URIs", tt.a, tt.b, got)
			}
		})
	}
}

// TestParseMultistatusRejectsMalformedDocuments pins the negative half of the
// decode path. Every case below decodes without complaint under struct-tag
// unmarshalling — the unknown and duplicate elements are discarded or merged,
// the trailing content is never read — so each one would have reached an
// assertion advertised as exact and passed it.
func TestParseMultistatusRejectsMalformedDocuments(t *testing.T) {
	const (
		msOpen  = `<d:multistatus xmlns:d="DAV:" xmlns:cal="urn:ietf:params:xml:ns:caldav">`
		msClose = `</d:multistatus>`
		okPS    = `<d:propstat><d:prop><d:displayname>Work</d:displayname></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>`
	)
	href := `<d:href>/dav/calendars/1/</d:href>`

	tests := []struct {
		name string
		body string
	}{
		{
			name: "unknown child of multistatus",
			body: msOpen + `<d:response>` + href + okPS + `</d:response><d:propstat/>` + msClose,
		},
		{
			name: "unknown child of response",
			body: msOpen + `<d:response>` + href + okPS + `<d:prop/></d:response>` + msClose,
		},
		{
			name: "unknown child of propstat",
			body: msOpen + `<d:response>` + href + `<d:propstat><d:prop/><d:status>HTTP/1.1 200 OK</d:status><d:href>/x/</d:href></d:propstat></d:response>` + msClose,
		},
		{
			name: "duplicate prop in one propstat",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:prop><d:displayname>Work</d:displayname></d:prop><d:prop><d:displayname>Home</d:displayname></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>` +
				`</d:response>` + msClose,
		},
		{
			name: "duplicate status in one propstat",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:prop/><d:status>HTTP/1.1 200 OK</d:status><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>` +
				`</d:response>` + msClose,
		},
		{
			name: "propstat without a status",
			body: msOpen + `<d:response>` + href + `<d:propstat><d:prop/></d:propstat></d:response>` + msClose,
		},
		{
			name: "duplicate sync-token",
			body: msOpen + `<d:sync-token>1</d:sync-token><d:sync-token>2</d:sync-token>` + msClose,
		},
		{
			name: "response without an href",
			body: msOpen + `<d:response>` + okPS + `</d:response>` + msClose,
		},
		{
			name: "response with neither propstat nor status",
			body: msOpen + `<d:response>` + href + `</d:response>` + msClose,
		},
		{
			name: "response with both propstat and a bare status",
			body: msOpen + `<d:response>` + href + okPS + `<d:status>HTTP/1.1 404 Not Found</d:status></d:response>` + msClose,
		},
		{
			// The §14.24 alternation is about which children are present, not
			// about what they carry, so an empty DAV:status is as much a second
			// branch of it as a populated one.
			name: "response with propstat and an empty bare status",
			body: msOpen + `<d:response>` + href + okPS + `<d:status/></d:response>` + msClose,
		},
		{
			name: "response with an empty bare status alone",
			body: msOpen + `<d:response>` + href + `<d:status/></d:response>` + msClose,
		},
		{
			name: "propstat with an empty status",
			body: msOpen + `<d:response>` + href + `<d:propstat><d:prop/><d:status/></d:propstat></d:response>` + msClose,
		},
		{
			name: "propstat with a malformed status line",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:prop/><d:status>HTTP/1.1 0200 OK</d:status></d:propstat></d:response>` + msClose,
		},
		{
			name: "undeclared namespace prefix",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:prop><dx:displayname>Work</dx:displayname></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>` +
				`</d:response>` + msClose,
		},
		{
			name: "several hrefs alongside propstat",
			body: msOpen + `<d:response>` + href + `<d:href>/dav/calendars/2/</d:href>` + okPS + `</d:response>` + msClose,
		},
		{
			name: "propstat with its status before its prop",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:status>HTTP/1.1 200 OK</d:status><d:prop><d:displayname>Work</d:displayname></d:prop></d:propstat>` +
				`</d:response>` + msClose,
		},
		{
			name: "response with its propstat before its href",
			body: msOpen + `<d:response>` + okPS + href + `</d:response>` + msClose,
		},
		{
			name: "sync-token before the responses",
			body: msOpen + `<d:sync-token>1</d:sync-token><d:response>` + href + okPS + `</d:response>` + msClose,
		},
		{
			name: "character data inside multistatus",
			body: msOpen + `garbage<d:response>` + href + okPS + `</d:response>` + msClose,
		},
		{
			name: "character data inside response",
			body: msOpen + `<d:response>` + href + `garbage` + okPS + `</d:response>` + msClose,
		},
		{
			name: "character data inside propstat",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:prop/>garbage<d:status>HTTP/1.1 200 OK</d:status></d:propstat>` +
				`</d:response>` + msClose,
		},
		{
			name: "undeclared HTML entity",
			body: msOpen + `<d:response><d:href>/dav/calendars/1/a&nbsp;b/</d:href>` + okPS + `</d:response>` + msClose,
		},
		{
			name: "whitespace-padded href",
			body: msOpen + `<d:response><d:href>  /dav/calendars/1/  </d:href>` + okPS + `</d:response>` + msClose,
		},
		{
			name: "href with an element child",
			body: msOpen + `<d:response><d:href>/dav/<d:displayname>hidden</d:displayname></d:href>` + okPS + `</d:response>` + msClose,
		},
		{
			name: "propstat status with an element child",
			body: msOpen + `<d:response>` + href +
				`<d:propstat><d:prop/><d:status>HTTP/1.1 200 OK<d:displayname>hidden</d:displayname></d:status></d:propstat>` +
				`</d:response>` + msClose,
		},
		{
			name: "bare response status with an element child",
			body: msOpen + `<d:response>` + href +
				`<d:status>HTTP/1.1 404 Not Found<d:displayname>hidden</d:displayname></d:status></d:response>` + msClose,
		},
		{
			name: "sync-token with an element child",
			body: msOpen + `<d:sync-token>1<d:displayname>hidden</d:displayname></d:sync-token>` + msClose,
		},
		{
			name: "wrong root element",
			body: `<d:propfind xmlns:d="DAV:"/>`,
		},
		{
			name: "second root element",
			body: msOpen + msClose + msOpen + msClose,
		},
		{
			name: "trailing character data",
			body: msOpen + msClose + `garbage`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseMultistatus([]byte(tt.body)); err == nil {
				t.Errorf("parseMultistatus accepted a malformed document: %s", tt.body)
			}
		})
	}
}

// parseACE decodes a hand-written DAV:ace for the ACE model self-tests.
func parseACE(t *testing.T, body string) davElement {
	t.Helper()
	ace, err := parseRootElement([]byte(body), davQN("ace"))
	if err != nil {
		t.Fatalf("decode DAV:ace: %v; body: %s", err, body)
	}
	return ace
}

// TestACEModelChecksRejectMalformedACEs pins the negative half of the ACL
// helpers. Every ACE below yields a clean principal and privilege set under a
// "take the first matching child" reading, which is what acePrincipals and
// acePrivileges did before they checked the content model, and every one of
// them says something RFC 3744 §5.5 gives an ACE no way to say.
func TestACEModelChecksRejectMalformedACEs(t *testing.T) {
	const (
		open      = `<d:ace xmlns:d="DAV:">`
		close     = `</d:ace>`
		principal = `<d:principal><d:href>/dav/principals/2/</d:href></d:principal>`
		grantRead = `<d:grant><d:privilege><d:read/></d:privilege></d:grant>`
	)

	t.Run("ace", func(t *testing.T) {
		tests := []struct {
			name string
			body string
		}{
			{"two principals", open + principal + `<d:principal><d:all/></d:principal>` + grantRead + close},
			{"principal and invert", open + principal + `<d:invert>` + principal + `</d:invert>` + grantRead + close},
			{"no principal", open + grantRead + close},
			{"grant and deny", open + principal + grantRead + `<d:deny><d:privilege><d:write/></d:privilege></d:deny>` + close},
			{"neither grant nor deny", open + principal + close},
			{"two grants", open + principal + grantRead + grantRead + close},
			{"grant before principal", open + grantRead + principal + close},
			{"unknown child", open + principal + grantRead + `<d:href>/dav/principals/3/</d:href>` + close},
			{"character data", open + principal + `garbage` + grantRead + close},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if err := checkACEModel(parseACE(t, tt.body)); err == nil {
					t.Errorf("checkACEModel accepted an ACE with %s: %s", tt.name, tt.body)
				}
			})
		}
	})

	t.Run("principal", func(t *testing.T) {
		tests := []struct {
			name string
			body string
		}{
			{"two principal forms", `<d:href>/dav/principals/2/</d:href><d:all/>`},
			{"two hrefs", `<d:href>/dav/principals/2/</d:href><d:href>/dav/principals/3/</d:href>`},
			{"no principal form", ``},
			{"unknown child", `<d:href>/dav/principals/2/</d:href><d:privilege><d:read/></d:privilege>`},
			{"character data", `garbage<d:all/>`},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				el := parseACE(t, open+`<d:principal>`+tt.body+`</d:principal>`+grantRead+close).
					child(t, davQN("principal"))
				if err := checkACEPrincipalModel(el); err == nil {
					t.Errorf("checkACEPrincipalModel accepted a principal with %s: %s", tt.name, tt.body)
				}
			})
		}
	})

	t.Run("grant", func(t *testing.T) {
		tests := []struct {
			name string
			body string
		}{
			{"no privilege", ``},
			{"empty privilege", `<d:privilege/>`},
			{"privilege wrapping two elements", `<d:privilege><d:read/><d:write/></d:privilege>`},
			{"unknown child", `<d:privilege><d:read/></d:privilege><d:read/>`},
			{"character data", `garbage<d:privilege><d:read/></d:privilege>`},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				el := parseACE(t, open+principal+`<d:grant>`+tt.body+`</d:grant>`+close).
					child(t, davQN("grant"))
				if err := checkPrivilegeContainerModel(el); err == nil {
					t.Errorf("checkPrivilegeContainerModel accepted a grant with %s: %s", tt.name, tt.body)
				}
			})
		}
	})
}

// TestACEHelpersReadAConformingACE is the positive half: the optional DAV:invert
// alternative aside, an ACE spelled exactly as RFC 3744 §5.5 admits passes every
// check and reads back the principal and the privileges it names, in order.
func TestACEHelpersReadAConformingACE(t *testing.T) {
	ace := parseACE(t, `<d:ace xmlns:d="DAV:">`+
		`<d:principal><d:authenticated/></d:principal>`+
		`<d:deny><d:privilege><d:write/></d:privilege><d:privilege><d:read/></d:privilege></d:deny>`+
		`<d:protected/>`+
		`</d:ace>`)

	if err := checkACEModel(ace); err != nil {
		t.Fatalf("checkACEModel rejected a conforming ACE: %v", err)
	}
	if got, want := qnList(acePrincipals(t, ace)), qnString(davQN("authenticated")); got != want {
		t.Errorf("ACE principal = %s, want %s", got, want)
	}
	want := qnList([]xml.Name{davQN("write"), davQN("read")})
	if got := qnList(acePrivileges(t, ace, davQN("deny"))); got != want {
		t.Errorf("ACE denies %s, want %s", got, want)
	}
}

// TestParseDAVDocumentAcceptsAnyDeclaredNamespaceURI pins that namespace
// resolution is decided by the document's declarations rather than by the shape
// of the namespace name. A namespace name is any URI, so a dead property stored
// under an RFC 4151 tag: URI or a bare-scheme URI resolves exactly as the
// http:// and urn: forms the DAV specifications use, and a syntactic
// "contains ://" heuristic reported both as unresolved prefixes.
func TestParseDAVDocumentAcceptsAnyDeclaredNamespaceURI(t *testing.T) {
	body := `<d:multistatus xmlns:d="DAV:" xmlns:t="tag:example.com,2026:props" xmlns:m="mid:calcard">` +
		`<d:response><d:href>/dav/calendars/1/</d:href><d:propstat><d:prop>` +
		`<t:tagged>a</t:tagged><m:opaque>b</m:opaque>` +
		`</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>`

	ms, err := parseMultistatus([]byte(body))
	if err != nil {
		t.Fatalf("parseMultistatus rejected declared non-HTTP namespaces: %v", err)
	}
	resp := ms.responseForHref(t, "/dav/calendars/1/")
	resp.assertPropValue(t, qn("tag:example.com,2026:props", "tagged"), http.StatusOK, "a")
	resp.assertPropValue(t, qn("mid:calcard", "opaque"), http.StatusOK, "b")
}

// TestNamespaceBindingsAreScopedByPrefix pins that names resolve through the
// prefix-to-URI bindings in scope, per XML Namespaces §6.1.
//
// The two negative cases are what a set of in-scope namespace *URIs* cannot
// express, whichever way it is scoped. A namespace name and a prefix are
// different things that happen to both be strings, so a document declaring
// xmlns:x="t" puts the string "t" in the URI set, and an unbound t: prefix then
// matches it. A dead property holds whatever XML a client stored, so it is the
// client that would choose the collision.
func TestNamespaceBindingsAreScopedByPrefix(t *testing.T) {
	const (
		status = `<d:status>HTTP/1.1 200 OK</d:status>`
		tagNS  = "tag:example.com,2026:props"
	)
	response := func(href, prop string) string {
		return `<d:response><d:href>` + href + `</d:href><d:propstat><d:prop>` +
			prop + `</d:prop>` + status + `</d:propstat></d:response>`
	}

	rejected := []struct {
		name string
		body string
	}{
		{
			// The declaration is in scope on the document element, and still
			// binds the prefix "x" rather than the prefix "t".
			name: "prefix matching a namespace name declared in scope",
			body: `<d:multistatus xmlns:d="DAV:" xmlns:x="t">` +
				response("/dav/calendars/1/", `<t:tagged>a</t:tagged>`) +
				`</d:multistatus>`,
		},
		{
			name: "prefix bound only by a later sibling",
			body: `<d:multistatus xmlns:d="DAV:">` +
				response("/dav/calendars/1/", `<t:tagged>a</t:tagged>`) +
				response("/dav/calendars/2/", `<x:dead xmlns:x="t">b</x:dead>`) +
				`</d:multistatus>`,
		},
		{
			name: "prefix bound only by a descendant",
			body: `<d:multistatus xmlns:d="DAV:">` +
				response("/dav/calendars/1/", `<t:outer><t:inner xmlns:t="`+tagNS+`"/></t:outer>`) +
				`</d:multistatus>`,
		},
		{
			// XML Namespaces §6.1 scopes attribute prefixes the same way.
			name: "attribute carrying an unbound prefix",
			body: `<d:multistatus xmlns:d="DAV:" xmlns:cal="` + nsCalDAV + `">` +
				response("/dav/calendars/1/", `<cal:comp t:name="VEVENT"/>`) +
				`</d:multistatus>`,
		},
		{
			name: "rebinding the reserved xml prefix",
			body: `<d:multistatus xmlns:d="DAV:" xmlns:xml="urn:not-the-xml-namespace">` +
				response("/dav/calendars/1/", `<d:displayname>Work</d:displayname>`) +
				`</d:multistatus>`,
		},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseMultistatus([]byte(tt.body)); err == nil {
				t.Errorf("parseMultistatus accepted %s", tt.name)
			}
		})
	}

	// The positive half: a declaration on the element that uses the prefix
	// binds that element and its descendants, and an inner rebinding applies
	// only inside.
	t.Run("declaration binds the element and its descendants", func(t *testing.T) {
		body := `<d:multistatus xmlns:d="DAV:">` +
			response("/dav/calendars/1/",
				`<t:outer xmlns:t="`+tagNS+`"><t:inner>a</t:inner><t:inner xmlns:t="mid:calcard">b</t:inner></t:outer>`) +
			`</d:multistatus>`
		ms, err := parseMultistatus([]byte(body))
		if err != nil {
			t.Fatalf("parseMultistatus rejected an in-scope declaration: %v", err)
		}
		outer := ms.responseForHref(t, "/dav/calendars/1/").
			assertPropStatus(t, qn(tagNS, "outer"), http.StatusOK)
		if got := qnList(outer.childNames()); got != qnList([]xml.Name{qn(tagNS, "inner"), qn("mid:calcard", "inner")}) {
			t.Errorf("children = %s, want one inner in each namespace", got)
		}
	})
}

// TestAttributeNamespacesFollowXMLNames pins XML Namespaces §6.2: the default
// declaration applies to element names and never to attribute names, and the
// "xml" prefix is bound by the specification rather than by the document.
// RFC 4791 §5.2.1 puts xml:lang on calendar-description and §9.6.1 gives
// CALDAV:comp an unqualified "name", so both spellings reach these assertions.
func TestAttributeNamespacesFollowXMLNames(t *testing.T) {
	body := `<multistatus xmlns="DAV:">` +
		`<response><href>/dav/calendars/1/</href><propstat><prop>` +
		`<c:comp xmlns:c="` + nsCalDAV + `" name="VEVENT"/>` +
		`<c:calendar-description xmlns:c="` + nsCalDAV + `" xml:lang="en-GB">Work</c:calendar-description>` +
		`</prop><status>HTTP/1.1 200 OK</status></propstat></response></multistatus>`

	ms, err := parseMultistatus([]byte(body))
	if err != nil {
		t.Fatalf("parseMultistatus: %v", err)
	}
	resp := ms.responseForHref(t, "/dav/calendars/1/")

	comp := resp.assertPropStatus(t, calQN("comp"), http.StatusOK)
	if got := comp.attr("name"); got != "VEVENT" {
		t.Errorf(`attr("name") = %q under a default namespace, want %q: a default declaration does not reach attributes`, got, "VEVENT")
	}

	description := resp.assertPropStatus(t, calQN("calendar-description"), http.StatusOK)
	for _, a := range description.Attr {
		if a.Name.Local != "lang" {
			continue
		}
		if a.Name.Space != xmlNamespaceURI {
			t.Errorf("xml:lang resolved to namespace %q, want %q", a.Name.Space, xmlNamespaceURI)
		}
		if a.Value != "en-GB" {
			t.Errorf("xml:lang = %q, want %q", a.Value, "en-GB")
		}
		return
	}
	t.Error("xml:lang is absent from the decoded attributes")
}

// TestAttrRequiresUnqualifiedName pins that a namespaced attribute does not
// answer a lookup for the unqualified one RFC 4791 §9.6.1 defines.
func TestAttrRequiresUnqualifiedName(t *testing.T) {
	el := davElement{
		Name: calQN("comp"),
		Attr: []xml.Attr{
			{Name: xml.Name{Space: nsCalDAV, Local: "name"}, Value: "VEVENT"},
			{Name: xml.Name{Space: "xmlns", Local: "cal"}, Value: nsCalDAV},
		},
	}
	if got := el.attr("name"); got != "" {
		t.Errorf("attr(\"name\") = %q for a namespaced cal:name attribute, want \"\"", got)
	}

	el.Attr = append(el.Attr, xml.Attr{Name: xml.Name{Local: "name"}, Value: "VTODO"})
	if got := el.attr("name"); got != "VTODO" {
		t.Errorf("attr(\"name\") = %q, want %q", got, "VTODO")
	}
}

func TestStatusCodeFromLineRequiresStatusLine(t *testing.T) {
	if got := statusCodeFromLine(t, "HTTP/1.1 404 Not Found"); got != http.StatusNotFound {
		t.Errorf("statusCodeFromLine = %d, want 404", got)
	}
	if got := statusCodeFromLine(t, "HTTP/1.1 424 Failed Dependency"); got != http.StatusFailedDependency {
		t.Errorf("statusCodeFromLine = %d, want 424", got)
	}
	// RFC 2616 §6.1: Reason-Phrase is *<TEXT>, so the empty phrase after the
	// second SP is well formed. Trimming the element's character data before
	// parsing would turn this into the "HTTP/1.1 200" case, which is not.
	if got := statusCodeFromLine(t, "HTTP/1.1 200 "); got != http.StatusOK {
		t.Errorf("statusCodeFromLine of an empty Reason-Phrase = %d, want 200", got)
	}
}

// TestParseStatusLineRejectsMalformedLines pins the negative half. Each line
// below carries the code an assertion would ask for, so a serialization that
// emitted one would satisfy every exact-status assertion in the suite unless
// the whole Status-Line is checked.
func TestParseStatusLineRejectsMalformedLines(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{"unparseable HTTP-Version", "HTTP/BOGUS 200 OK"},
		{"version without a slash", "HTTP1.1 200 OK"},
		{"no version at all", "200 OK"},
		{"zero-padded status code", "HTTP/1.1 0200 OK"},
		{"signed status code", "HTTP/1.1 +20 OK"},
		{"two-digit status code", "HTTP/1.1 20 OK"},
		{"non-numeric status code", "HTTP/1.1 2xx OK"},
		{"no Reason-Phrase separator", "HTTP/1.1 200"},
		{"empty", ""},
		// The separators are one SP each, so none of these is a Status-Line
		// however readable it looks. strings.Fields accepted all four.
		{"tab separators", "HTTP/1.1\t200\tOK"},
		{"repeated space before the code", "HTTP/1.1  200 OK"},
		{"leading whitespace", " HTTP/1.1 200 OK"},
		{"CR in the Reason-Phrase", "HTTP/1.1 200 OK\r\nX-Injected: 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if code, err := parseStatusLine(tt.line); err == nil {
				t.Errorf("parseStatusLine(%q) = %d, want an error", tt.line, code)
			}
		})
	}
}

// TestPropstatsWithStatusSeesEmptyPropstats pins what assertPropstatNames and
// assertNoPropstatWithStatus are built on. An empty DAV:propstat carries no
// property name, so a helper that only concatenated names across matching
// propstats could not tell one from no propstat at all, and "assert nothing is
// reported at 200" silently accepted a response reporting an empty 200.
func TestPropstatsWithStatusSeesEmptyPropstats(t *testing.T) {
	resp := davResponse{
		Hrefs: []string{"/dav/calendars/1/"},
		Propstats: []davPropstat{
			{Status: "HTTP/1.1 200 OK"},
			{Status: "HTTP/1.1 404 Not Found", Prop: davPropBag{Props: []davElement{{Name: davQN("getetag")}}}},
		},
	}
	if got := len(resp.propstatsWithStatus(t, http.StatusOK)); got != 1 {
		t.Errorf("propstatsWithStatus(200) found %d propstats, want the empty one", got)
	}
	if got := len(resp.propstatsWithStatus(t, http.StatusNotFound)); got != 1 {
		t.Errorf("propstatsWithStatus(404) found %d propstats, want 1", got)
	}
	if got := len(resp.propstatsWithStatus(t, http.StatusForbidden)); got != 0 {
		t.Errorf("propstatsWithStatus(403) found %d propstats, want 0", got)
	}
}

func TestAssertErrorConditionsDecodesCalDAVError(t *testing.T) {
	rr := httptest.NewRecorder()
	writeCalDAVError(rr, http.StatusConflict, "no-uid-conflict")
	assertErrorConditions(t, rr, http.StatusConflict, calQN("no-uid-conflict"))
}

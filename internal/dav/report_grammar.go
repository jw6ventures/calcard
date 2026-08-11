package dav

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"slices"
	"strings"
)

// REPORT bodies whose content model matters are read token by token here rather
// than through struct tags, because encoding/xml cannot express cardinality or
// document order and silently drops what it has no field for.
//
// RFC 2518 Appendix 3 asks a server to tolerate elements it does not
// understand, which the RFC 4791 §9 content models otherwise contradict. The
// two are reconciled by scope: inside an element whose model a specification
// defines, a child or unprefixed attribute the model does not admit is
// malformed, while an element in any other namespace is skipped as an
// extension.

// reportGrammarFault is a rejected REPORT body. condition names the precondition
// the body violates, answered at 403 because no resubmission of the same request
// can succeed (RFC 4791 §1.3); an empty condition is a bare content model
// violation, which names no precondition and answers 400.
type reportGrammarFault struct {
	condition string
	offending []filterElementRef
	reason    string
}

// filterElementRef names one offending filter element for the
// CALDAV:supported-filter content model of RFC 4791 §7.8.8, which reports the
// comp-filter, prop-filter or param-filter the server cannot honour.
type filterElementRef struct {
	Element string
	Name    string
}

func (f *reportGrammarFault) Error() string { return f.reason }

// faultFunc builds the fault a content-model violation raises. Which one a
// walk uses is what separates a bare malformed body from a named precondition:
// the §9.7 filter elements answer CALDAV:valid-filter, everything else 400.
type faultFunc func(format string, args ...any) *reportGrammarFault

func malformedReport(format string, args ...any) *reportGrammarFault {
	return &reportGrammarFault{reason: fmt.Sprintf(format, args...)}
}

// openReportBody positions a decoder just inside the expected root element and
// refuses a body whose root is anything else.
func openReportBody(body []byte, want xml.Name) (*xml.Decoder, xml.StartElement, *reportGrammarFault) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.Entity = xml.HTMLEntity
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil, xml.StartElement{}, malformedReport("%s: %v", xmlNameString(want), err)
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if start.Name != want {
			return nil, xml.StartElement{}, malformedReport("unexpected REPORT root %s, want %s", xmlNameString(start.Name), xmlNameString(want))
		}
		return decoder, start, nil
	}
}

// walkElementChildren drives one element's content, handing each child start
// element to visit. Character data other than whitespace is refused through
// newFault, because every content model here is element-only, and a child in a
// namespace neither RFC 4791 nor RFC 4918 owns is skipped as the extension
// RFC 2518 Appendix 3 asks a server to tolerate. A decoder error stays
// malformed whatever newFault is: a body that is not well-formed XML violates
// no precondition.
func walkElementChildren(dec *xml.Decoder, start xml.StartElement, label string, newFault faultFunc, visit func(*xml.Decoder, xml.StartElement) *reportGrammarFault) *reportGrammarFault {
	for {
		token, err := dec.Token()
		if err != nil {
			return malformedReport("%s: %v", label, err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			if !modelledNamespace(token.Name.Space) {
				if fault := skipElement(dec, token); fault != nil {
					return fault
				}
				continue
			}
			if fault := visit(dec, token); fault != nil {
				return fault
			}
		case xml.CharData:
			if strings.TrimSpace(string(token)) != "" {
				return newFault("%s carries character data", label)
			}
		case xml.EndElement:
			if token.Name == start.Name {
				return nil
			}
		}
	}
}

func modelledNamespace(space string) bool {
	return space == namespaceDAV || space == namespaceCalDAV
}

func skipElement(dec *xml.Decoder, start xml.StartElement) *reportGrammarFault {
	if err := dec.Skip(); err != nil {
		return malformedReport("%s: %v", xmlNameString(start.Name), err)
	}
	return nil
}

// expectEmptyElement reads an element whose content model is EMPTY, admitting
// only the attributes its ATTLIST declares.
func expectEmptyElement(dec *xml.Decoder, start xml.StartElement, label string, newFault faultFunc, allowed ...string) *reportGrammarFault {
	if fault := checkAttributes(start, label, allowed...); fault != nil {
		return newFault("%s", fault.reason)
	}
	return walkElementChildren(dec, start, label, newFault, func(_ *xml.Decoder, child xml.StartElement) *reportGrammarFault {
		return newFault("%s is EMPTY but carries %s", label, xmlNameString(child.Name))
	})
}

// decodeTextElement reads an element whose content model is #PCDATA. The
// attributes are the caller's to validate, since a caller that already checked
// them would otherwise check them twice.
func decodeTextElement(dec *xml.Decoder, start xml.StartElement, label string) (string, *reportGrammarFault) {
	var text strings.Builder
	for {
		token, err := dec.Token()
		if err != nil {
			return "", malformedReport("%s: %v", label, err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			if !modelledNamespace(token.Name.Space) {
				if fault := skipElement(dec, token); fault != nil {
					return "", fault
				}
				continue
			}
			return "", malformedReport("%s carries child element %s", label, xmlNameString(token.Name))
		case xml.CharData:
			text.Write(token)
		case xml.EndElement:
			if token.Name == start.Name {
				return text.String(), nil
			}
		}
	}
}

// checkAttributes refuses an unprefixed attribute the element's ATTLIST does
// not declare. Namespace declarations and prefixed attributes are extensions
// and are left alone.
func checkAttributes(start xml.StartElement, label string, allowed ...string) *reportGrammarFault {
	for _, attr := range start.Attr {
		if attr.Name.Space != "" || attr.Name.Local == "xmlns" {
			continue
		}
		// XML attribute names are case-sensitive, so an ATTLIST name matches
		// only its exact spelling.
		if !slices.Contains(allowed, attr.Name.Local) {
			return malformedReport("%s carries undefined attribute %q", label, attr.Name.Local)
		}
	}
	return nil
}

func attributeValue(start xml.StartElement, name string) (string, bool) {
	for _, attr := range start.Attr {
		if attr.Name.Space == "" && attr.Name.Local == name {
			return attr.Value, true
		}
	}
	return "", false
}

func davQName(local string) xml.Name {
	return xml.Name{Space: namespaceDAV, Local: local}
}

func calDAVQName(local string) xml.Name {
	return xml.Name{Space: namespaceCalDAV, Local: local}
}

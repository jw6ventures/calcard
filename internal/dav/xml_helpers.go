package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func safeUnmarshalXML(data []byte, v interface{}) error {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.Entity = xml.HTMLEntity
	if err := decoder.Decode(v); err != nil {
		return err
	}
	return expectXMLDocumentEnd(decoder)
}

// expectXMLDocumentEnd reports whether the decoder is positioned at the end of
// the document. An XML document holds exactly one root element (XML 1.0 §2.1),
// so a second one after it is a second document smuggled into the body -- which
// a decoder that stops at the first root would silently accept. Comments,
// processing instructions and whitespace are the misc content the production
// allows there.
func expectXMLDocumentEnd(decoder *xml.Decoder) error {
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch token := token.(type) {
		case xml.StartElement:
			return fmt.Errorf("trailing element %q after the document root", xmlNameString(token.Name))
		case xml.EndElement:
			return fmt.Errorf("trailing end element %q after the document root", xmlNameString(token.Name))
		case xml.Directive:
			return errors.New("trailing directive after the document root")
		case xml.CharData:
			if len(bytes.TrimSpace(token)) != 0 {
				return errors.New("trailing character data after the document root")
			}
		}
	}
}

// validXMLNameToken reports whether value is an XML 1.0 §2.3 Name. A request
// that carries a property name as an attribute value rather than as an element
// -- DAV:expand-property does -- has to check that itself, because the decoder
// only enforces the production on names it parsed as markup.
func validXMLNameToken(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if xmlNameStartCharacter(r) || r == '-' || r == '.' || r >= '0' && r <= '9' ||
			r == '·' || r >= '̀' && r <= 'ͯ' || r >= '‿' && r <= '⁀' {
			continue
		}
		return false
	}
	return true
}

func xmlNameStartCharacter(r rune) bool {
	return r == ':' || r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' ||
		r >= 'À' && r <= 'Ö' || r >= 'Ø' && r <= 'ö' ||
		r >= 'ø' && r <= '˿' || r >= 'Ͱ' && r <= 'ͽ' ||
		r >= 'Ϳ' && r <= '῿' || r >= '‌' && r <= '‍' ||
		r >= '⁰' && r <= '↏' || r >= 'Ⰰ' && r <= '⿯' ||
		r >= '、' && r <= '퟿' || r >= '豈' && r <= '﷏' ||
		r >= 'ﷰ' && r <= '�' || r >= '\U00010000' && r <= '\U000effff'
}

var errRequestTooLarge = errors.New("request too large")

func readDAVBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	if r.ContentLength > maxBytes {
		return nil, errRequestTooLarge
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(limitedBody)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, errRequestTooLarge
		}
		return nil, err
	}
	return body, nil
}

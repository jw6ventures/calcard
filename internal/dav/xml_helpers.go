package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
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

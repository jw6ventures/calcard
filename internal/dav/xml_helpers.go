package dav

import (
	"bytes"
	"encoding/xml"
)

// safeUnmarshalXML safely unmarshals XML data with protection against XXE attacks.
// It creates a decoder with Entity set to xml.HTMLEntity to prevent external entity injection.
func safeUnmarshalXML(data []byte, v interface{}) error {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.Entity = xml.HTMLEntity
	return decoder.Decode(v)
}

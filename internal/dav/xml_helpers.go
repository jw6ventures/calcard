package dav

import (
	"bytes"
	"encoding/xml"
)

func safeUnmarshalXML(data []byte, v interface{}) error {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.Entity = xml.HTMLEntity
	return decoder.Decode(v)
}

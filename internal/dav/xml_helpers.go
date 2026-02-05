package dav

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
)

func safeUnmarshalXML(data []byte, v interface{}) error {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.Entity = xml.HTMLEntity
	return decoder.Decode(v)
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

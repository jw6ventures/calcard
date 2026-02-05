package dav

import (
	"encoding/xml"
	"net/http"
)

func writeMultiStatus(w http.ResponseWriter, payload multistatus) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_ = xml.NewEncoder(w).Encode(payload)
}

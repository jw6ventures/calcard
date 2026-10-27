package ui

import (
	"net/http"

	"github.com/jw6ventures/calcard/internal/store"
)

// storeRetryAfterSeconds is the Retry-After sent with a 503 while the store is
// unavailable.
const storeRetryAfterSeconds = "5"

// writeServerFailure answers a request that failed with err for a reason of
// the server's own: 503 with the status text alone while the store is
// unavailable, so the page is not mistaken for a broken one, and 500 with
// message otherwise.
func writeServerFailure(w http.ResponseWriter, err error, message string) {
	if writeStoreUnavailable(w, err) {
		return
	}
	http.Error(w, message, http.StatusInternalServerError)
}

// writeStoreUnavailable answers 503 and reports true when err shows the store
// is unavailable. Handlers that turn a failure into a flash message or a
// client status check it first: neither is true of an outage, and a flash
// redirect leads only to another page the store cannot serve.
func writeStoreUnavailable(w http.ResponseWriter, err error) bool {
	if !store.IsUnavailable(err) {
		return false
	}
	writeServiceUnavailable(w)
	return true
}

// writeServiceUnavailable answers 503 with Retry-After and the status text
// alone, which carries no storage or connection detail.
func writeServiceUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", storeRetryAfterSeconds)
	http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
}

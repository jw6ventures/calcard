package api

import (
	"net/http"

	"github.com/jw6ventures/calcard/internal/store"
)

// storeRetryAfterSeconds is the Retry-After sent with a 503 while the store is
// unavailable.
const storeRetryAfterSeconds = "5"

// writeServerFailure answers a request that failed with err for a reason of
// the server's own: 503 with the status text alone while the store is
// unavailable, so a client retries rather than giving up, and 500 with
// message otherwise.
func writeServerFailure(w http.ResponseWriter, err error, message string) {
	if writeStoreUnavailable(w, err) {
		return
	}
	http.Error(w, message, http.StatusInternalServerError)
}

// writeStoreUnavailable answers 503 and reports true when err shows the store
// is unavailable.
func writeStoreUnavailable(w http.ResponseWriter, err error) bool {
	if !store.IsUnavailable(err) {
		return false
	}
	w.Header().Set("Retry-After", storeRetryAfterSeconds)
	http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
	return true
}

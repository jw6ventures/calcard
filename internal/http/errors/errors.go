package errors

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
)

func InternalError(w http.ResponseWriter, r *http.Request, err error, message string) {
	requestID := middleware.GetReqID(r.Context())

	// Log the actual error with request ID for debugging
	if requestID != "" {
		log.Printf("[ERROR] RequestID=%s: %s: %v", requestID, message, err)
	} else {
		log.Printf("[ERROR] %s: %v", message, err)
	}

	// Return generic error to client
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func BadRequestError(w http.ResponseWriter, r *http.Request, err error, clientMessage string) {
	requestID := middleware.GetReqID(r.Context())

	if requestID != "" {
		log.Printf("[WARN] RequestID=%s: bad request: %v", requestID, err)
	} else {
		log.Printf("[WARN] bad request: %v", err)
	}

	http.Error(w, clientMessage, http.StatusBadRequest)
}

func LogError(r *http.Request, message string, err error) {
	requestID := middleware.GetReqID(r.Context())

	if requestID != "" {
		log.Printf("[ERROR] RequestID=%s: %s: %v", requestID, message, err)
	} else {
		log.Printf("[ERROR] %s: %v", message, err)
	}
}

func LogInfo(r *http.Request, message string) {
	requestID := middleware.GetReqID(r.Context())

	if requestID != "" {
		log.Printf("[INFO] RequestID=%s: %s", requestID, message)
	} else {
		log.Printf("[INFO] %s", message)
	}
}

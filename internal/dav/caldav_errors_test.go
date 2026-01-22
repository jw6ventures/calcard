package dav

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsValidCalDAVCondition(t *testing.T) {
	tests := []struct {
		condition string
		valid     bool
	}{
		// Valid conditions
		{"max-resource-size", true},
		{"valid-calendar-data", true},
		{"no-uid-conflict", true},
		{"supported-calendar-component", true},
		{"min-date-time", true},
		{"a", true},
		{"abc123", true},
		{"test-condition-123", true},

		// Invalid conditions
		{"", false},
		{"Max-Resource-Size", false},   // Uppercase
		{"123-start", false},           // Starts with digit
		{"-start", false},              // Starts with hyphen
		{"test_condition", false},      // Underscore not allowed
		{"test condition", false},      // Space not allowed
		{"test<script>", false},        // XML characters
		{"test&amp;", false},           // XML entity
		{"../../../etc/passwd", false}, // Path traversal attempt
		{"test;DROP TABLE", false},     // SQL injection attempt
		{"VALID-CONDITION", false},     // All uppercase
	}

	for _, tt := range tests {
		t.Run(tt.condition, func(t *testing.T) {
			result := isValidCalDAVCondition(tt.condition)
			if result != tt.valid {
				t.Errorf("isValidCalDAVCondition(%q) = %v, want %v", tt.condition, result, tt.valid)
			}
		})
	}
}

func TestWriteCalDAVError_ValidCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVError(w, 403, "max-resource-size")

	body := w.Body.String()
	if !strings.Contains(body, "<C:max-resource-size/>") {
		t.Errorf("expected valid condition in response, got: %s", body)
	}
	if !strings.Contains(body, "<?xml version") {
		t.Error("expected XML declaration in response")
	}
}

func TestWriteCalDAVError_InvalidCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVError(w, 403, "test<script>alert(1)</script>")

	body := w.Body.String()
	if strings.Contains(body, "<script>") {
		t.Error("XML injection vulnerability: script tag present in output")
	}
	if !strings.Contains(body, "<C:invalid-condition/>") {
		t.Errorf("expected fallback to invalid-condition, got: %s", body)
	}
}

func TestWriteCalDAVErrorMulti_ValidConditions(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 400, "valid-calendar-data", "valid-calendar-object-resource")

	body := w.Body.String()
	if !strings.Contains(body, "<C:valid-calendar-data/>") {
		t.Error("expected first condition in response")
	}
	if !strings.Contains(body, "<C:valid-calendar-object-resource/>") {
		t.Error("expected second condition in response")
	}
}

func TestWriteCalDAVErrorMulti_SkipsInvalidConditions(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 400, "valid-condition", "<injection>", "another-valid")

	body := w.Body.String()
	if strings.Contains(body, "<injection>") {
		t.Error("XML injection vulnerability: invalid condition not filtered")
	}
	if !strings.Contains(body, "<C:valid-condition/>") {
		t.Error("expected valid condition to be included")
	}
	if !strings.Contains(body, "<C:another-valid/>") {
		t.Error("expected second valid condition to be included")
	}
}

func TestWriteCalDAVErrorMulti_EmptyConditions(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 400)

	if w.Code != 400 {
		t.Errorf("expected status 400, got: %d", w.Code)
	}
	body := w.Body.String()
	if body != "" {
		t.Errorf("expected empty body for no conditions, got: %s", body)
	}
}

func TestWriteCalDAVErrorMulti_SingleCondition(t *testing.T) {
	w := httptest.NewRecorder()
	writeCalDAVErrorMulti(w, 403, "max-resource-size")

	body := w.Body.String()
	// Should call writeCalDAVError for single condition
	if !strings.Contains(body, "<C:max-resource-size/>") {
		t.Errorf("expected condition in response, got: %s", body)
	}
}

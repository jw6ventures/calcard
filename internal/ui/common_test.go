package ui

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/store"
)

func TestParsePagination(t *testing.T) {
	h := &Handler{}

	tests := []struct {
		name      string
		query     url.Values
		wantPage  int
		wantLimit int
	}{
		{
			name:      "no parameters",
			query:     url.Values{},
			wantPage:  1,
			wantLimit: defaultPageSize,
		},
		{
			name: "valid page and limit",
			query: url.Values{
				"page":  []string{"3"},
				"limit": []string{"25"},
			},
			wantPage:  3,
			wantLimit: 25,
		},
		{
			name: "invalid page defaults to 1",
			query: url.Values{
				"page": []string{"invalid"},
			},
			wantPage:  1,
			wantLimit: defaultPageSize,
		},
		{
			name: "zero page defaults to 1",
			query: url.Values{
				"page": []string{"0"},
			},
			wantPage:  1,
			wantLimit: defaultPageSize,
		},
		{
			name: "negative page defaults to 1",
			query: url.Values{
				"page": []string{"-5"},
			},
			wantPage:  1,
			wantLimit: defaultPageSize,
		},
		{
			name: "limit exceeding max caps at 100",
			query: url.Values{
				"limit": []string{"200"},
			},
			wantPage:  1,
			wantLimit: defaultPageSize, // Will default since >100 is ignored
		},
		{
			name: "zero limit defaults to defaultPageSize",
			query: url.Values{
				"limit": []string{"0"},
			},
			wantPage:  1,
			wantLimit: defaultPageSize,
		},
		{
			name: "valid limit within range",
			query: url.Values{
				"limit": []string{"10"},
			},
			wantPage:  1,
			wantLimit: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				URL: &url.URL{
					RawQuery: tt.query.Encode(),
				},
			}

			page, limit := h.parsePagination(req)

			if page != tt.wantPage {
				t.Errorf("parsePagination() page = %d, want %d", page, tt.wantPage)
			}
			if limit != tt.wantLimit {
				t.Errorf("parsePagination() limit = %d, want %d", limit, tt.wantLimit)
			}
		})
	}
}

func TestWithFlash(t *testing.T) {
	h := &Handler{}

	tests := []struct {
		name      string
		query     url.Values
		inputData map[string]any
		wantKeys  []string
	}{
		{
			name:      "no flash parameters",
			query:     url.Values{},
			inputData: map[string]any{"Title": "Test"},
			wantKeys:  []string{"Title"},
		},
		{
			name: "status message",
			query: url.Values{
				"status": []string{"created"},
			},
			inputData: map[string]any{},
			wantKeys:  []string{"FlashMessage"},
		},
		{
			name: "error message",
			query: url.Values{
				"error": []string{"failed"},
			},
			inputData: map[string]any{},
			wantKeys:  []string{"FlashError"},
		},
		{
			name: "plain token",
			query: url.Values{
				"token": []string{"abc123"},
			},
			inputData: map[string]any{},
			wantKeys:  []string{"PlainToken"},
		},
		{
			name: "all flash parameters",
			query: url.Values{
				"status": []string{"success"},
				"error":  []string{"warning"},
				"token":  []string{"xyz789"},
			},
			inputData: map[string]any{"ExistingKey": "value"},
			wantKeys:  []string{"FlashMessage", "FlashError", "PlainToken", "ExistingKey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				URL: &url.URL{
					RawQuery: tt.query.Encode(),
				},
			}

			result := h.withFlash(req, tt.inputData)

			for _, key := range tt.wantKeys {
				if _, exists := result[key]; !exists {
					t.Errorf("withFlash() missing expected key: %s", key)
				}
			}

			// Original data should be preserved
			for k, v := range tt.inputData {
				if result[k] != v {
					t.Errorf("withFlash() modified original data: %s", k)
				}
			}
		})
	}
}

func TestRedirect(t *testing.T) {
	h := &Handler{}

	tests := []struct {
		name           string
		path           string
		params         map[string]string
		wantLocation   string
		wantStatusCode int
	}{
		{
			name:           "redirect without params",
			path:           "/dashboard",
			params:         nil,
			wantLocation:   "/dashboard",
			wantStatusCode: http.StatusFound,
		},
		{
			name: "redirect with single param",
			path: "/calendars",
			params: map[string]string{
				"status": "created",
			},
			wantLocation:   "/calendars?status=created",
			wantStatusCode: http.StatusFound,
		},
		{
			name: "redirect with multiple params",
			path: "/calendars",
			params: map[string]string{
				"status": "updated",
				"id":     "123",
			},
			wantStatusCode: http.StatusFound,
		},
		{
			name: "redirect with empty param values",
			path: "/test",
			params: map[string]string{
				"key1": "",
				"key2": "value",
			},
			wantLocation:   "/test?key2=value",
			wantStatusCode: http.StatusFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/", nil)

			h.redirect(w, r, tt.path, tt.params)

			if w.Code != tt.wantStatusCode {
				t.Errorf("redirect() status = %d, want %d", w.Code, tt.wantStatusCode)
			}

			location := w.Header().Get("Location")
			if tt.wantLocation != "" && location != tt.wantLocation {
				t.Errorf("redirect() location = %q, want %q", location, tt.wantLocation)
			}

			// Verify all non-empty params are in the URL
			for k, v := range tt.params {
				if v != "" && !containsParam(location, k, v) {
					t.Errorf("redirect() location missing param %s=%s in %s", k, v, location)
				}
			}
		})
	}
}

func TestRender(t *testing.T) {
	// Create a simple test template
	testTemplate := template.Must(template.New("test.html").Parse("Hello {{.Name}}"))

	h := &Handler{
		templates: map[string]*template.Template{
			"test.html": testTemplate,
		},
	}

	tests := []struct {
		name         string
		templateName string
		data         any
		wantStatus   int
		wantBody     string
	}{
		{
			name:         "valid template",
			templateName: "test.html",
			data:         map[string]any{"Name": "World"},
			wantStatus:   http.StatusOK,
			wantBody:     "Hello World",
		},
		{
			name:         "template not found",
			templateName: "nonexistent.html",
			data:         nil,
			wantStatus:   http.StatusInternalServerError,
			wantBody:     "template \"nonexistent.html\" not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			h.render(w, tt.templateName, tt.data)

			if w.Code != tt.wantStatus {
				t.Errorf("render() status = %d, want %d", w.Code, tt.wantStatus)
			}

			body := w.Body.String()
			if tt.wantBody != "" && !strings.Contains(body, tt.wantBody) {
				t.Errorf("render() body = %q, want to contain %q", body, tt.wantBody)
			}
		})
	}
}

// Helper function to check if a URL contains a specific query parameter
func containsParam(urlStr, key, value string) bool {
	u, err := url.Parse(urlStr)
	if err != nil {
		return false
	}
	return u.Query().Get(key) == value
}

func TestDefaultPageSize(t *testing.T) {
	if defaultPageSize <= 0 {
		t.Errorf("defaultPageSize should be positive, got %d", defaultPageSize)
	}
	if defaultPageSize != 50 {
		t.Errorf("defaultPageSize = %d, expected 50", defaultPageSize)
	}
}

// Test handler creation
func TestNewHandler(t *testing.T) {
	cfg := &config.Config{}
	store := &store.Store{}
	authService := &auth.Service{}

	handler := NewHandler(cfg, store, authService)

	if handler == nil {
		t.Fatal("NewHandler() returned nil")
	}
	if handler.cfg != cfg {
		t.Error("NewHandler() did not set config correctly")
	}
	if handler.store != store {
		t.Error("NewHandler() did not set store correctly")
	}
	if handler.authService != authService {
		t.Error("NewHandler() did not set authService correctly")
	}
}

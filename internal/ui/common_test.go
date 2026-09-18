package ui

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
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

// Every status key a handler redirects with has to resolve to a sentence. A key
// missing from the table reaches the page as itself, which is how "contact_updated"
// ended up on screen.
func TestFlashMessageResolvesEveryHandlerStatusKey(t *testing.T) {
	sources, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	statusKey := regexp.MustCompile(`"status": *"([a-z_]+)"`)
	seen := map[string]bool{}
	for _, name := range sources {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range statusKey.FindAllStringSubmatch(string(body), -1) {
			seen[match[1]] = true
		}
	}
	if len(seen) == 0 {
		t.Fatal("found no status keys to check; the scan is broken, not the code")
	}
	for key := range seen {
		if _, ok := flashMessages[key]; !ok {
			t.Errorf("status key %q has no entry in flashMessages, so the raw key is shown to the user", key)
		}
	}
}

func TestFlashMessage(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   string
	}{
		{"known key becomes a sentence", "contact_updated", "Contact updated."},
		{"key is not shown verbatim", "event_created", "Event created."},
		{"generic keys are scoped to their resource", "calendar_created", "Calendar created."},
		{"dynamic message passes through sentence-cased", "imported 3 contact(s)", "Imported 3 contact(s)."},
		{"already a sentence is left alone", "Imported 3 event(s).", "Imported 3 event(s)."},
		{"terminal punctuation is preserved", "Really?", "Really?"},
		{"empty stays empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := flashMessage(tt.status); got != tt.want {
				t.Errorf("flashMessage(%q) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestWithFlashHumanizesStatusAndError(t *testing.T) {
	h := &Handler{}
	r := &http.Request{URL: &url.URL{RawQuery: url.Values{
		"status": []string{"contact_updated"},
		"error":  []string{"end date must be after start date"},
	}.Encode()}}

	data := h.withFlash(r, map[string]any{})

	if got := data["FlashMessage"]; got != "Contact updated." {
		t.Errorf("FlashMessage = %q, want %q", got, "Contact updated.")
	}
	if got := data["FlashError"]; got != "End date must be after start date." {
		t.Errorf("FlashError = %q, want %q", got, "End date must be after start date.")
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
			wantBody:     "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/", nil)

			h.render(w, r, tt.templateName, tt.data)

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

func TestSessionDeviceKind(t *testing.T) {
	tests := []struct {
		name      string
		userAgent string
		want      string
	}{
		{
			name:      "desktop firefox on linux",
			userAgent: "Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0",
			want:      "desktop",
		},
		{
			name:      "desktop chrome on windows",
			userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
			want:      "desktop",
		},
		{
			name:      "desktop safari on macos",
			userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
			want:      "desktop",
		},
		{
			name:      "iphone safari",
			userAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
			want:      "phone",
		},
		{
			name:      "android phone chrome",
			userAgent: "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36",
			want:      "phone",
		},
		{
			name:      "android phone firefox",
			userAgent: "Mozilla/5.0 (Android 14; Mobile; rv:153.0) Gecko/153.0 Firefox/153.0",
			want:      "phone",
		},
		{
			name:      "ipad safari",
			userAgent: "Mozilla/5.0 (iPad; CPU OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
			want:      "tablet",
		},
		{
			// Android tablets differ from Android phones only by the absent
			// "Mobile" token, so the tablet check has to run before the phone one.
			name:      "android tablet chrome",
			userAgent: "Mozilla/5.0 (Linux; Android 14; SM-X200) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
			want:      "tablet",
		},
		{
			name:      "empty user agent",
			userAgent: "",
			want:      "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sessionDeviceKind(tt.userAgent); got != tt.want {
				t.Errorf("sessionDeviceKind(%q) = %q, want %q", tt.userAgent, got, tt.want)
			}
		})
	}
}

func TestValidateContactEmail(t *testing.T) {
	tests := []struct {
		name    string
		email   string
		wantErr bool
	}{
		{name: "empty is allowed", email: ""},
		{name: "plain address", email: "james@jameswilliams.business"},
		{name: "plus tag", email: "james+calcard@example.com"},
		{name: "subdomain", email: "james@mail.example.co.uk"},
		{
			// A label that reads like a typo is still a routable domain, and
			// vCard EMAIL carries no domain policy, so this is accepted.
			name:  "unusual but syntactically valid tld",
			email: "apple-load-001@example.not-an-email",
		},
		{name: "missing at sign", email: "jamesexample.com", wantErr: true},
		{name: "missing domain", email: "james@", wantErr: true},
		{name: "missing local part", email: "@example.com", wantErr: true},
		{name: "embedded space", email: "james williams@example.com", wantErr: true},
		{name: "two at signs", email: "james@@example.com", wantErr: true},
		{
			// The field holds a bare address; a display name belongs in FN.
			name:    "display name form",
			email:   "James <james@example.com>",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateContactEmail(tt.email)
			if tt.wantErr && err == nil {
				t.Errorf("validateContactEmail(%q) = nil, want an error", tt.email)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validateContactEmail(%q) = %v, want nil", tt.email, err)
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

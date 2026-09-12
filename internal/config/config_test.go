package config

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadUsesExplicitDSNAndParsesFlags(t *testing.T) {
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_DISCOVERY_URL", "https://issuer.example/.well-known/openid-configuration")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
	t.Setenv("APP_PROMETHEUS_ENDPOINT_ENABLED", "yes")
	t.Setenv("APP_TRUSTED_PROXIES", "10.0.0.0/8, 127.0.0.1/32 ,2001:db8::1/128")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.DB.DSN != "postgres://dsn" {
		t.Fatalf("DB.DSN = %q", cfg.DB.DSN)
	}
	if !cfg.PrometheusEnabled {
		t.Fatal("expected PrometheusEnabled")
	}
	want := []string{"10.0.0.0/8", "127.0.0.1/32", "2001:db8::1/128"}
	if !reflect.DeepEqual(cfg.TrustedProxies, want) {
		t.Fatalf("TrustedProxies = %#v, want %#v", cfg.TrustedProxies, want)
	}
	if cfg.DB.MaxOpenConns != 25 {
		t.Fatalf("DB.MaxOpenConns = %d, want 25", cfg.DB.MaxOpenConns)
	}
	if cfg.DB.MaxIdleConns != 10 {
		t.Fatalf("DB.MaxIdleConns = %d, want 10", cfg.DB.MaxIdleConns)
	}
	if cfg.DB.ConnMaxLifetime != 30*time.Minute {
		t.Fatalf("DB.ConnMaxLifetime = %s, want 30m", cfg.DB.ConnMaxLifetime)
	}
	if cfg.HTTP.ReadTimeout != 15*time.Second {
		t.Fatalf("HTTP.ReadTimeout = %s, want 15s", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.WriteTimeout != 15*time.Second {
		t.Fatalf("HTTP.WriteTimeout = %s, want 15s", cfg.HTTP.WriteTimeout)
	}
	if cfg.HTTP.IdleTimeout != 60*time.Second {
		t.Fatalf("HTTP.IdleTimeout = %s, want 60s", cfg.HTTP.IdleTimeout)
	}
	if !cfg.DAV.PropfindInfinityEnabled {
		t.Fatal("expected DAV.PropfindInfinityEnabled to default to true")
	}
	if cfg.DAV.MaxMultistatusResponses != 10000 {
		t.Fatalf("DAV.MaxMultistatusResponses = %d, want 10000", cfg.DAV.MaxMultistatusResponses)
	}
	if cfg.DAV.MaxMultistatusBytes != 67108864 {
		t.Fatalf("DAV.MaxMultistatusBytes = %d, want 67108864", cfg.DAV.MaxMultistatusBytes)
	}
	if cfg.DAV.MaxFilterElements != 100 {
		t.Fatalf("DAV.MaxFilterElements = %d, want 100", cfg.DAV.MaxFilterElements)
	}
	if cfg.DAV.MaxCardDAVQueryBytes != 65536 || cfg.DAV.MaxAddressDataProperties != 100 {
		t.Fatalf("CardDAV query limits = %d bytes, %d selectors", cfg.DAV.MaxCardDAVQueryBytes, cfg.DAV.MaxAddressDataProperties)
	}
	if cfg.DAV.MaxReportElementDepth != 20 {
		t.Fatalf("DAV.MaxReportElementDepth = %d, want 20", cfg.DAV.MaxReportElementDepth)
	}
	if cfg.DAV.MaxMultigetHrefs != 5000 {
		t.Fatalf("DAV.MaxMultigetHrefs = %d, want 5000", cfg.DAV.MaxMultigetHrefs)
	}
	if cfg.DAV.MaxReportCandidateRows != 50000 {
		t.Fatalf("DAV.MaxReportCandidateRows = %d, want 50000", cfg.DAV.MaxReportCandidateRows)
	}
	if cfg.DAV.SyncHistoryRetention != 90*24*time.Hour {
		t.Fatalf("DAV.SyncHistoryRetention = %s, want 90 days", cfg.DAV.SyncHistoryRetention)
	}
	if cfg.TrafficCaptureFile != "" {
		t.Fatalf("TrafficCaptureFile = %q, want disabled by default", cfg.TrafficCaptureFile)
	}
}

func TestLoadEnablesTrafficCaptureWhenFileIsConfigured(t *testing.T) {
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_ISSUER_URL", "https://issuer.example")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
	t.Setenv("APP_TRAFFIC_CAPTURE_FILE", "/tmp/calcard-traffic.jsonl")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TrafficCaptureFile != "/tmp/calcard-traffic.jsonl" {
		t.Fatalf("TrafficCaptureFile = %q", cfg.TrafficCaptureFile)
	}
}

func TestLoadParsesDAVMultistatusLimits(t *testing.T) {
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_ISSUER_URL", "https://issuer.example")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
	t.Setenv("APP_DAV_MAX_MULTISTATUS_RESPONSES", "321")
	t.Setenv("APP_DAV_MAX_MULTISTATUS_BYTES", "654321")
	t.Setenv("APP_DAV_MAX_FILTER_ELEMENTS", "17")
	t.Setenv("APP_DAV_MAX_CARDDAV_QUERY_BYTES", "4096")
	t.Setenv("APP_DAV_MAX_ADDRESS_DATA_PROPERTIES", "12")
	t.Setenv("APP_DAV_MAX_REPORT_ELEMENT_DEPTH", "7")
	t.Setenv("APP_DAV_MAX_MULTIGET_HREFS", "42")
	t.Setenv("APP_DAV_MAX_REPORT_CANDIDATE_ROWS", "9876")
	t.Setenv("APP_DAV_SYNC_HISTORY_RETENTION", "36h")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DAV.MaxMultistatusResponses != 321 {
		t.Fatalf("DAV.MaxMultistatusResponses = %d, want 321", cfg.DAV.MaxMultistatusResponses)
	}
	if cfg.DAV.MaxMultistatusBytes != 654321 {
		t.Fatalf("DAV.MaxMultistatusBytes = %d, want 654321", cfg.DAV.MaxMultistatusBytes)
	}
	if cfg.DAV.MaxFilterElements != 17 {
		t.Fatalf("DAV.MaxFilterElements = %d, want 17", cfg.DAV.MaxFilterElements)
	}
	if cfg.DAV.MaxCardDAVQueryBytes != 4096 || cfg.DAV.MaxAddressDataProperties != 12 {
		t.Fatalf("CardDAV query limits = %d bytes, %d selectors", cfg.DAV.MaxCardDAVQueryBytes, cfg.DAV.MaxAddressDataProperties)
	}
	if cfg.DAV.MaxReportElementDepth != 7 {
		t.Fatalf("DAV.MaxReportElementDepth = %d, want 7", cfg.DAV.MaxReportElementDepth)
	}
	if cfg.DAV.MaxMultigetHrefs != 42 {
		t.Fatalf("DAV.MaxMultigetHrefs = %d, want 42", cfg.DAV.MaxMultigetHrefs)
	}
	if cfg.DAV.MaxReportCandidateRows != 9876 {
		t.Fatalf("DAV.MaxReportCandidateRows = %d, want 9876", cfg.DAV.MaxReportCandidateRows)
	}
	if cfg.DAV.SyncHistoryRetention != 36*time.Hour {
		t.Fatalf("DAV.SyncHistoryRetention = %s, want 36h", cfg.DAV.SyncHistoryRetention)
	}
}

// davLimitKeys is every DAV resource limit, which share one loader and one
// meaning for 0.
var davLimitKeys = []string{
	"APP_DAV_MAX_MULTISTATUS_RESPONSES",
	"APP_DAV_MAX_MULTISTATUS_BYTES",
	"APP_DAV_MAX_FILTER_ELEMENTS",
	"APP_DAV_MAX_CARDDAV_QUERY_BYTES",
	"APP_DAV_MAX_ADDRESS_DATA_PROPERTIES",
	"APP_DAV_MAX_REPORT_ELEMENT_DEPTH",
	"APP_DAV_MAX_MULTIGET_HREFS",
	"APP_DAV_MAX_REPORT_CANDIDATE_ROWS",
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_ISSUER_URL", "https://issuer.example")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
}

func TestLoadRejectsNegativeAndNonNumericDAVLimits(t *testing.T) {
	for _, key := range davLimitKeys {
		for _, value := range []string{"-1", "invalid"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				setRequiredEnv(t)
				t.Setenv(key, value)

				_, err := Load()
				if err == nil || !strings.Contains(err.Error(), key+" must be a non-negative integer") {
					t.Fatalf("Load() error = %v, want non-negative validation for %s=%q", err, key, value)
				}
			})
		}
	}
}

// A limit set to 0 turns that limit off. It is loaded as math.MaxInt so the
// comparisons downstream need no separate "is it set" test, and so an operator
// who disables one knob does not silently get the default back.
func TestLoadTreatsZeroDAVLimitAsUnlimited(t *testing.T) {
	read := map[string]func(*Config) int{
		"APP_DAV_MAX_MULTISTATUS_RESPONSES":   func(c *Config) int { return c.DAV.MaxMultistatusResponses },
		"APP_DAV_MAX_MULTISTATUS_BYTES":       func(c *Config) int { return c.DAV.MaxMultistatusBytes },
		"APP_DAV_MAX_FILTER_ELEMENTS":         func(c *Config) int { return c.DAV.MaxFilterElements },
		"APP_DAV_MAX_CARDDAV_QUERY_BYTES":     func(c *Config) int { return c.DAV.MaxCardDAVQueryBytes },
		"APP_DAV_MAX_ADDRESS_DATA_PROPERTIES": func(c *Config) int { return c.DAV.MaxAddressDataProperties },
		"APP_DAV_MAX_REPORT_ELEMENT_DEPTH":    func(c *Config) int { return c.DAV.MaxReportElementDepth },
		"APP_DAV_MAX_MULTIGET_HREFS":          func(c *Config) int { return c.DAV.MaxMultigetHrefs },
		"APP_DAV_MAX_REPORT_CANDIDATE_ROWS":   func(c *Config) int { return c.DAV.MaxReportCandidateRows },
	}
	for _, key := range davLimitKeys {
		t.Run(key, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(key, "0")

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v, want 0 accepted as unlimited", err)
			}
			if got := read[key](cfg); got != math.MaxInt {
				t.Fatalf("%s = %d, want math.MaxInt", key, got)
			}
		})
	}
}

// The sync-history retention window turns off at 0, on the same terms every
// DAV resource limit does: an operator who does not want tombstones pruned must
// not silently get the default window, because pruning is what makes a sync
// token past the window unanswerable.
func TestLoadTreatsZeroSyncHistoryRetentionAsOff(t *testing.T) {
	for _, value := range []string{"0", "0s"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("APP_DAV_SYNC_HISTORY_RETENTION", value)

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v, want 0 accepted as retention off", err)
			}
			if cfg.DAV.SyncHistoryRetention != 0 {
				t.Fatalf("DAV.SyncHistoryRetention = %s, want 0", cfg.DAV.SyncHistoryRetention)
			}
		})
	}
}

// A negative window would prune tombstones the server still owes a client, and a
// value the duration parser cannot read must not fall back to the default.
func TestLoadRejectsNegativeAndNonDurationSyncHistoryRetention(t *testing.T) {
	for _, value := range []string{"-1h", "invalid", "90"} {
		t.Run(value, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("APP_DAV_SYNC_HISTORY_RETENTION", value)

			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "APP_DAV_SYNC_HISTORY_RETENTION must be a non-negative duration") {
				t.Fatalf("Load() error = %v, want non-negative validation for %q", err, value)
			}
		})
	}
}

func TestLoadDisablesPropfindInfinity(t *testing.T) {
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_DISCOVERY_URL", "https://issuer.example/.well-known/openid-configuration")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
	t.Setenv("APP_DAV_PROPFIND_INFINITY_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.DAV.PropfindInfinityEnabled {
		t.Fatal("expected APP_DAV_PROPFIND_INFINITY_ENABLED=false to disable Depth: infinity PROPFIND")
	}
}

func TestLoadParsesPoolAndHTTPTimeoutConfig(t *testing.T) {
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_ISSUER_URL", "https://issuer.example")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
	t.Setenv("APP_DB_MAX_OPEN_CONNS", "50")
	t.Setenv("APP_DB_MAX_IDLE_CONNS", "20")
	t.Setenv("APP_DB_CONN_MAX_LIFETIME", "45m")
	t.Setenv("APP_HTTP_READ_TIMEOUT", "30s")
	t.Setenv("APP_HTTP_WRITE_TIMEOUT", "60s")
	t.Setenv("APP_HTTP_IDLE_TIMEOUT", "2m")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.DB.MaxOpenConns != 50 {
		t.Fatalf("DB.MaxOpenConns = %d, want 50", cfg.DB.MaxOpenConns)
	}
	if cfg.DB.MaxIdleConns != 20 {
		t.Fatalf("DB.MaxIdleConns = %d, want 20", cfg.DB.MaxIdleConns)
	}
	if cfg.DB.ConnMaxLifetime != 45*time.Minute {
		t.Fatalf("DB.ConnMaxLifetime = %s, want 45m", cfg.DB.ConnMaxLifetime)
	}
	if cfg.HTTP.ReadTimeout != 30*time.Second {
		t.Fatalf("HTTP.ReadTimeout = %s, want 30s", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.WriteTimeout != 60*time.Second {
		t.Fatalf("HTTP.WriteTimeout = %s, want 60s", cfg.HTTP.WriteTimeout)
	}
	if cfg.HTTP.IdleTimeout != 2*time.Minute {
		t.Fatalf("HTTP.IdleTimeout = %s, want 2m", cfg.HTTP.IdleTimeout)
	}
}

func TestLoadAcceptsSingleTrustedProxyIP(t *testing.T) {
	t.Setenv("APP_DB_DSN", "postgres://dsn")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_ISSUER_URL", "https://issuer.example")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))
	t.Setenv("APP_TRUSTED_PROXIES", "127.0.0.1,2001:db8::1")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := []string{"127.0.0.1", "2001:db8::1"}
	if !reflect.DeepEqual(cfg.TrustedProxies, want) {
		t.Fatalf("TrustedProxies = %#v, want %#v", cfg.TrustedProxies, want)
	}
}

func TestLoadBuildsDSNFromComponents(t *testing.T) {
	t.Setenv("APP_DB_HOST", "db")
	t.Setenv("APP_DB_NAME", "calcard")
	t.Setenv("APP_DB_USER", "user")
	t.Setenv("APP_DB_PASSWORD", "pass")
	t.Setenv("APP_DB_PORT", "5433")
	t.Setenv("APP_DB_SSLMODE", "require")
	t.Setenv("APP_OAUTH_CLIENT_ID", "client")
	t.Setenv("APP_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("APP_OAUTH_ISSUER_URL", "https://issuer.example")
	t.Setenv("APP_SESSION_SECRET", strings.Repeat("s", 32))

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := "postgres://user:pass@db:5433/calcard?sslmode=require"
	if cfg.DB.DSN != want {
		t.Fatalf("DB.DSN = %q, want %q", cfg.DB.DSN, want)
	}
}

func TestLoadReturnsUsefulValidationErrors(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			name: "missing database config",
			env: map[string]string{
				"APP_OAUTH_CLIENT_ID":     "client",
				"APP_OAUTH_CLIENT_SECRET": "secret",
				"APP_OAUTH_ISSUER_URL":    "https://issuer.example",
				"APP_SESSION_SECRET":      strings.Repeat("s", 32),
			},
			wantErr: "APP_DB_DSN is required",
		},
		{
			name: "missing oauth secret",
			env: map[string]string{
				"APP_DB_DSN":          "postgres://dsn",
				"APP_OAUTH_CLIENT_ID": "client",
				"APP_SESSION_SECRET":  strings.Repeat("s", 32),
			},
			wantErr: "oauth configuration is required",
		},
		{
			name: "missing discovery and issuer",
			env: map[string]string{
				"APP_DB_DSN":              "postgres://dsn",
				"APP_OAUTH_CLIENT_ID":     "client",
				"APP_OAUTH_CLIENT_SECRET": "secret",
				"APP_SESSION_SECRET":      strings.Repeat("s", 32),
			},
			wantErr: "APP_OAUTH_DISCOVERY_URL or APP_OAUTH_ISSUER_URL is required",
		},
		{
			name: "secret too short",
			env: map[string]string{
				"APP_DB_DSN":              "postgres://dsn",
				"APP_OAUTH_CLIENT_ID":     "client",
				"APP_OAUTH_CLIENT_SECRET": "secret",
				"APP_OAUTH_ISSUER_URL":    "https://issuer.example",
				"APP_SESSION_SECRET":      "short",
			},
			wantErr: "must be at least 32 characters",
		},
		{
			name: "invalid trusted proxy value",
			env: map[string]string{
				"APP_DB_DSN":              "postgres://dsn",
				"APP_OAUTH_CLIENT_ID":     "client",
				"APP_OAUTH_CLIENT_SECRET": "secret",
				"APP_OAUTH_ISSUER_URL":    "https://issuer.example",
				"APP_SESSION_SECRET":      strings.Repeat("s", 32),
				"APP_TRUSTED_PROXIES":     "not-an-ip",
			},
			wantErr: "APP_TRUSTED_PROXIES contains invalid IP or CIDR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"APP_LISTEN_ADDR", "APP_BASE_URL", "APP_DB_DSN", "APP_DB_HOST", "APP_DB_NAME",
				"APP_DB_USER", "APP_DB_PASSWORD", "APP_DB_PORT", "APP_DB_SSLMODE",
				"APP_OAUTH_CLIENT_ID", "APP_OAUTH_CLIENT_SECRET", "APP_OAUTH_ISSUER_URL",
				"APP_OAUTH_DISCOVERY_URL", "APP_OAUTH_REDIRECT_PATH", "APP_SESSION_SECRET",
				"APP_PROMETHEUS_ENDPOINT_ENABLED", "APP_TRUSTED_PROXIES",
				"APP_DB_MAX_OPEN_CONNS", "APP_DB_MAX_IDLE_CONNS", "APP_DB_CONN_MAX_LIFETIME",
				"APP_HTTP_READ_TIMEOUT", "APP_HTTP_WRITE_TIMEOUT", "APP_HTTP_IDLE_TIMEOUT",
			} {
				t.Setenv(key, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Load() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestGetenvHelpers(t *testing.T) {
	t.Setenv("BOOL_TRUE", "on")
	t.Setenv("BOOL_FALSE", "off")
	t.Setenv("LIST", " alpha, ,beta,gamma ")

	if got := getenvDefault("MISSING_DEFAULT", "fallback"); got != "fallback" {
		t.Fatalf("getenvDefault() = %q", got)
	}
	if !getenvBool("BOOL_TRUE", false) {
		t.Fatal("expected getenvBool true")
	}
	if getenvBool("BOOL_FALSE", true) {
		t.Fatal("expected getenvBool false")
	}
	wantList := []string{"alpha", "beta", "gamma"}
	if got := getenvList("LIST"); !reflect.DeepEqual(got, wantList) {
		t.Fatalf("getenvList() = %#v, want %#v", got, wantList)
	}
	if got := getenvList("MISSING_LIST"); got != nil {
		t.Fatalf("getenvList() = %#v, want nil", got)
	}
}

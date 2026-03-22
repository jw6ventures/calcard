package config

import (
	"reflect"
	"strings"
	"testing"
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

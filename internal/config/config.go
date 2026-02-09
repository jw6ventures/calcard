package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	ListenAddr string
	BaseURL    string

	DB struct {
		DSN string
	}

	OAuth struct {
		ClientID     string
		ClientSecret string
		IssuerURL    string
		DiscoveryURL string
		RedirectPath string
	}

	Session struct {
		Secret string
	}

	PrometheusEnabled bool
	TrustedProxies    []string
}

func Load() (*Config, error) {
	cfg := &Config{}

	cfg.ListenAddr = getenvDefault("APP_LISTEN_ADDR", ":8080")
	cfg.BaseURL = getenvDefault("APP_BASE_URL", "http://localhost:8080")
	cfg.DB.DSN = os.Getenv("APP_DB_DSN")

	if cfg.DB.DSN == "" {
		host := os.Getenv("APP_DB_HOST")
		name := os.Getenv("APP_DB_NAME")
		user := os.Getenv("APP_DB_USER")
		password := os.Getenv("APP_DB_PASSWORD")
		port := getenvDefault("APP_DB_PORT", "5432")
		sslmode := getenvDefault("APP_DB_SSLMODE", "disable")

		var missing []string
		if host == "" {
			missing = append(missing, "APP_DB_HOST")
		}
		if name == "" {
			missing = append(missing, "APP_DB_NAME")
		}
		if user == "" {
			missing = append(missing, "APP_DB_USER")
		}
		if password == "" {
			missing = append(missing, "APP_DB_PASSWORD")
		}

		if len(missing) == 0 {
			cfg.DB.DSN = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", user, password, host, port, name, sslmode)
		}
	}

	cfg.OAuth.ClientID = os.Getenv("APP_OAUTH_CLIENT_ID")
	cfg.OAuth.ClientSecret = os.Getenv("APP_OAUTH_CLIENT_SECRET")
	cfg.OAuth.IssuerURL = os.Getenv("APP_OAUTH_ISSUER_URL")
	cfg.OAuth.DiscoveryURL = os.Getenv("APP_OAUTH_DISCOVERY_URL")
	cfg.OAuth.RedirectPath = getenvDefault("APP_OAUTH_REDIRECT_PATH", "/auth/callback")
	cfg.Session.Secret = os.Getenv("APP_SESSION_SECRET")
	cfg.PrometheusEnabled = getenvBool("APP_PROMETHEUS_ENDPOINT_ENABLED", false)
	cfg.TrustedProxies = getenvList("APP_TRUSTED_PROXIES")

	if cfg.DB.DSN == "" {
		return nil, errors.New("APP_DB_DSN is required (or set APP_DB_HOST, APP_DB_NAME, APP_DB_USER, and APP_DB_PASSWORD)")
	}
	if cfg.OAuth.ClientID == "" || cfg.OAuth.ClientSecret == "" {
		return nil, fmt.Errorf("oauth configuration is required: client id and secret")
	}
	if cfg.OAuth.DiscoveryURL == "" && cfg.OAuth.IssuerURL == "" {
		return nil, errors.New("APP_OAUTH_DISCOVERY_URL or APP_OAUTH_ISSUER_URL is required")
	}
	if cfg.Session.Secret == "" {
		return nil, errors.New("APP_SESSION_SECRET is required")
	}
	if len(cfg.Session.Secret) < 32 {
		return nil, fmt.Errorf("APP_SESSION_SECRET must be at least 32 characters long (got %d)", len(cfg.Session.Secret))
	}

	if len(cfg.TrustedProxies) == 0 {
		fmt.Println("WARNING: No APP_TRUSTED_PROXIES configured. CalCard will trust all proxies - Not recommended for public environments.")
	}

	return cfg, nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}

func getenvList(key string) []string {
	if v := os.Getenv(key); v != "" {
		var result []string
		for _, item := range strings.Split(v, ",") {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				result = append(result, trimmed)
			}
		}
		return result
	}
	return nil
}

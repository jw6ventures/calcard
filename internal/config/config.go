package config

import (
	"errors"
	"fmt"
	"os"
)

// Config holds application configuration derived from environment variables.
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
}

// Load reads configuration from environment variables with basic validation.
func Load() (*Config, error) {
	cfg := &Config{}

	cfg.ListenAddr = getenvDefault("APP_LISTEN_ADDR", ":8080")
	cfg.BaseURL = getenvDefault("APP_BASE_URL", "http://localhost:8080")
	cfg.DB.DSN = os.Getenv("APP_DB_DSN")

	cfg.OAuth.ClientID = os.Getenv("APP_OAUTH_CLIENT_ID")
	cfg.OAuth.ClientSecret = os.Getenv("APP_OAUTH_CLIENT_SECRET")
	cfg.OAuth.IssuerURL = os.Getenv("APP_OAUTH_ISSUER_URL")
	cfg.OAuth.DiscoveryURL = os.Getenv("APP_OAUTH_DISCOVERY_URL")
	cfg.OAuth.RedirectPath = getenvDefault("APP_OAUTH_REDIRECT_PATH", "/auth/callback")
	cfg.Session.Secret = os.Getenv("APP_SESSION_SECRET")

	if cfg.DB.DSN == "" {
		return nil, errors.New("APP_DB_DSN is required")
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

	return cfg, nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

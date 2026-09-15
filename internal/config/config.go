package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr   string
	BaseURL      string
	CommunityURL string

	DB struct {
		DSN             string
		MaxOpenConns    int
		MaxIdleConns    int
		ConnMaxLifetime time.Duration
	}

	HTTP struct {
		ReadTimeout  time.Duration
		WriteTimeout time.Duration
		IdleTimeout  time.Duration
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

	PrometheusEnabled  bool
	TrustedProxies     []string
	TrafficCaptureFile string

	DAV struct {
		// PropfindInfinityEnabled allows Depth: infinity PROPFIND requests
		// (RFC 4918 §9.1). Enabled by default; when disabled the server
		// refuses them with 403 DAV:propfind-finite-depth, which §9.1.1
		// permits for deployments where deep listings are a DoS concern.
		PropfindInfinityEnabled bool
		// DigestEnabled advertises and accepts HTTP Digest on the DAV
		// endpoints (RFC 7616). It is off by default because Digest verifies
		// against a stored HA1, and an app password issued before Digest
		// existed has none: advertising the scheme to a client that then
		// selects it would answer a valid credential with 401. An app password
		// gains its HA1 when it is issued, or the first time it authenticates
		// over Basic, so a deployment turns this on once its credentials have
		// caught up.
		DigestEnabled           bool
		MaxMultistatusResponses int
		MaxMultistatusBytes     int
		// MaxFilterElements bounds how many filter elements one CALDAV:filter or
		// CARDDAV:filter may carry. Every one of them is evaluated against every
		// candidate resource, so an unbounded count is the CPU exhaustion
		// RFC 4791 §11 asks a server to guard against; the two grammars cost the
		// same per element and share the one budget, though which failure they
		// raise past it differs, since RFC 6352 defines no CALDAV:valid-filter
		// counterpart.
		MaxFilterElements int
		// MaxCardDAVQueryBytes bounds the combined serialized filter and
		// address-data metadata evaluated for each contact in a REPORT.
		MaxCardDAVQueryBytes     int
		MaxAddressDataProperties int
		// MaxReportElementDepth bounds the nesting of the recursive REPORT
		// grammar elements, CALDAV:comp-filter and the CALDAV:comp of
		// CALDAV:calendar-data. RFC 4791's own component tree reaches
		// VCALENDAR -> VEVENT -> VALARM, so the default leaves ample room.
		MaxReportElementDepth int
		// MaxMultigetHrefs bounds the DAV:href count of one calendar-multiget.
		// RFC 4791 §7.9 owes one DAV:response per href, so the report is
		// refused rather than answered over a truncated href list.
		MaxMultigetHrefs int
		// MaxReportCandidateRows bounds how many stored resources one report
		// reads and parses before its matches are declared outside the
		// server's predefined limits.
		MaxReportCandidateRows int
		// SyncHistoryRetention is how far back the server keeps deletion
		// history for incremental sync. It sets two things at once, because
		// they are the same fact: tombstones older than this are pruned, and a
		// DAV:sync-token naming an instant before it is refused with
		// DAV:valid-sync-token so the client falls back to a full
		// synchronization, as RFC 6578 §3.2 provides for. Pruning without that
		// refusal would lose a deletion silently. Zero turns pruning off and
		// leaves every token answerable, at the cost of a table that only
		// grows.
		//
		// The window may be narrowed at will but widened only when no client
		// holds a token older than the previous one: raising it re-admits tokens
		// whose tombstones the old window already pruned, and the deletions
		// between the two windows are then never reported.
		SyncHistoryRetention time.Duration
	}

	// PprofEnabled exposes net/http/pprof on a dedicated debug listener
	// (PprofAddr). It is off by default and the listener should stay bound to
	// loopback: the profiling handlers leak runtime internals and the
	// profile/trace endpoints are easy denial-of-service vectors.
	PprofEnabled bool
	PprofAddr    string
}

func Load() (*Config, error) {
	cfg := &Config{}

	cfg.ListenAddr = getenvDefault("APP_LISTEN_ADDR", ":8080")
	cfg.BaseURL = getenvDefault("APP_BASE_URL", "http://localhost:8080")
	cfg.CommunityURL = getenvDefault("APP_COMMUNITY_URL", "https://github.com/jw6ventures/calcard/issues")
	cfg.DB.DSN = os.Getenv("APP_DB_DSN")
	var err error
	cfg.DB.MaxOpenConns, err = getenvIntDefault("APP_DB_MAX_OPEN_CONNS", 25)
	if err != nil {
		return nil, err
	}
	cfg.DB.MaxIdleConns, err = getenvIntDefault("APP_DB_MAX_IDLE_CONNS", 10)
	if err != nil {
		return nil, err
	}
	cfg.DB.ConnMaxLifetime, err = getenvDurationDefault("APP_DB_CONN_MAX_LIFETIME", 30*time.Minute)
	if err != nil {
		return nil, err
	}
	cfg.HTTP.ReadTimeout, err = getenvDurationDefault("APP_HTTP_READ_TIMEOUT", 15*time.Second)
	if err != nil {
		return nil, err
	}
	cfg.HTTP.WriteTimeout, err = getenvDurationDefault("APP_HTTP_WRITE_TIMEOUT", 15*time.Second)
	if err != nil {
		return nil, err
	}
	cfg.HTTP.IdleTimeout, err = getenvDurationDefault("APP_HTTP_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return nil, err
	}

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
	cfg.DAV.PropfindInfinityEnabled = getenvBool("APP_DAV_PROPFIND_INFINITY_ENABLED", true)
	cfg.DAV.DigestEnabled = getenvBool("APP_DAV_DIGEST_ENABLED", false)
	cfg.DAV.MaxMultistatusResponses, err = getenvLimitDefault("APP_DAV_MAX_MULTISTATUS_RESPONSES", 10000)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxMultistatusBytes, err = getenvLimitDefault("APP_DAV_MAX_MULTISTATUS_BYTES", 67108864)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxFilterElements, err = getenvLimitDefault("APP_DAV_MAX_FILTER_ELEMENTS", 100)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxCardDAVQueryBytes, err = getenvLimitDefault("APP_DAV_MAX_CARDDAV_QUERY_BYTES", 65536)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxAddressDataProperties, err = getenvLimitDefault("APP_DAV_MAX_ADDRESS_DATA_PROPERTIES", 100)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxReportElementDepth, err = getenvLimitDefault("APP_DAV_MAX_REPORT_ELEMENT_DEPTH", 20)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxMultigetHrefs, err = getenvLimitDefault("APP_DAV_MAX_MULTIGET_HREFS", 5000)
	if err != nil {
		return nil, err
	}
	cfg.DAV.MaxReportCandidateRows, err = getenvLimitDefault("APP_DAV_MAX_REPORT_CANDIDATE_ROWS", 50000)
	if err != nil {
		return nil, err
	}
	// 90 days is four times the "3 weeks worth of changes" RFC 6578 §3.2 offers
	// as its own example of a retention a server may hold, so a client syncing
	// anything like regularly is never sent back for a full synchronization.
	cfg.DAV.SyncHistoryRetention, err = getenvWindowDefault("APP_DAV_SYNC_HISTORY_RETENTION", 90*24*time.Hour)
	if err != nil {
		return nil, err
	}
	cfg.PprofEnabled = getenvBool("APP_PPROF_ENABLED", false)
	cfg.PprofAddr = getenvDefault("APP_PPROF_ADDR", "127.0.0.1:6060")
	cfg.TrustedProxies = getenvList("APP_TRUSTED_PROXIES")
	cfg.TrafficCaptureFile = strings.TrimSpace(os.Getenv("APP_TRAFFIC_CAPTURE_FILE"))

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
	if err := validateTrustedProxies(cfg.TrustedProxies); err != nil {
		return nil, err
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

func getenvIntDefault(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return n, nil
}

func getenvPositiveIntDefault(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

// getenvLimitDefault reads a DAV resource limit, where 0 turns the limit off
// rather than being rejected. Unlimited is carried as math.MaxInt so every
// comparison downstream keeps working without a second "is it set" test.
func getenvLimitDefault(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer, where 0 means unlimited", key)
	}
	if n == 0 {
		return math.MaxInt, nil
	}
	return n, nil
}

func getenvDurationDefault(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}

// getenvWindowDefault reads a duration where 0 turns the behavior off rather
// than being rejected, which is what getenvLimitDefault does for the integer
// limits. A window carries no unlimited sentinel: zero is the off state every
// reader of it already tests for, since a window that is not enforced is not the
// same thing as one of unbounded length.
func getenvWindowDefault(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s must be a non-negative duration, where 0 turns it off", key)
	}
	return d, nil
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

func validateTrustedProxies(values []string) error {
	for _, value := range values {
		if _, _, err := net.ParseCIDR(value); err == nil {
			continue
		}
		if net.ParseIP(value) == nil {
			return fmt.Errorf("APP_TRUSTED_PROXIES contains invalid IP or CIDR %q", value)
		}
	}
	return nil
}

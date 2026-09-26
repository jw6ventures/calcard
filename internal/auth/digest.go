package auth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

const (
	davDigestRealm    = "CalCard DAV"
	davDigestNonceTTL = 5 * time.Minute

	// digestHA1Version prefixes every stored HA1 ciphertext. Rotating the
	// encryption scheme means writing a new prefix and keeping the old one
	// readable, so the prefix is part of the stored value rather than implied.
	digestHA1Version = "v1"

	// digestNonceKeyInfo and digestHA1KeyInfo separate the two keys derived
	// from the one configured session secret. Distinct HKDF info strings keep
	// a nonce signature from ever being usable as an HA1 key or the reverse.
	digestNonceKeyInfo = "calcard/dav-digest-nonce/v1"
	digestHA1KeyInfo   = "calcard/dav-digest-ha1/v1"
)

// errDigestHA1KeyUnavailable reports that no session secret is configured, so
// stored HA1s can be neither written nor read.
var errDigestHA1KeyUnavailable = errors.New("digest credential key unavailable")

func digestHash(algorithm, value string) string {
	switch strings.ToUpper(algorithm) {
	case "MD5":
		sum := md5.Sum([]byte(value))
		return hex.EncodeToString(sum[:])
	case "SHA-256":
		sum := sha256.Sum256([]byte(value))
		return hex.EncodeToString(sum[:])
	default:
		return ""
	}
}

// digestHA1 derives the HA1 that RFC 7616 section 3.4.2 hashes the credentials
// against. It is password-equivalent for this realm, so it is only ever
// persisted through sealDigestCredentials.
func digestHA1(algorithm, username, password string) string {
	return digestHash(algorithm, username+":"+davDigestRealm+":"+password)
}

// encryptDigestHA1 seals an HA1 for storage. The algorithm is authenticated
// alongside the ciphertext so a value written for one algorithm cannot be moved
// into the other's column and still verify.
func encryptDigestHA1(cfg *config.Config, algorithm, ha1 string) (string, error) {
	aead, err := digestHA1AEAD(cfg)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := aead.Seal(nonce, nonce, []byte(ha1), []byte(algorithm))
	return digestHA1Version + ":" + base64.RawURLEncoding.EncodeToString(sealed), nil
}

// decryptDigestHA1 opens a stored HA1. A value that does not carry a known
// version prefix is rejected rather than treated as plaintext: accepting an
// unsealed HA1 would silently restore the exposure sealing exists to remove.
func decryptDigestHA1(cfg *config.Config, algorithm, stored string) (string, error) {
	version, encoded, ok := strings.Cut(stored, ":")
	if !ok || version != digestHA1Version {
		return "", errors.New("unrecognized digest credential encoding")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	aead, err := digestHA1AEAD(cfg)
	if err != nil {
		return "", err
	}
	if len(sealed) < aead.NonceSize() {
		return "", errors.New("truncated digest credential")
	}
	nonce, ciphertext := sealed[:aead.NonceSize()], sealed[aead.NonceSize():]
	plaintext, err := aead.Open(nil, nonce, ciphertext, []byte(algorithm))
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// sealDigestCredentials returns the sealed MD5 and SHA-256 HA1s to persist for
// an app password, or two nils when there is nothing to persist, which leaves
// the app password Basic-only.
//
// Nothing is produced unless the deployment has opted into Digest. An HA1
// authenticates its holder without the password, and although it is sealed, the
// key is derived from APP_SESSION_SECRET -- so the row and the secret together
// recover it, and those two are routinely captured together in an environment
// block, a Kubernetes Secret, or a backup. A bcrypt hash alone gives an attacker
// holding the database nothing, and an operator who left Digest off never agreed
// to trade that away. Gating here rather than at each caller keeps the rule on
// the one function whose output exists to be written to a row.
func (s *Service) sealDigestCredentials(username, password string) (*string, *string, error) {
	if !s.digestEnabled() {
		return nil, nil, nil
	}
	sealed := make([]*string, 0, 2)
	for _, algorithm := range []string{"MD5", "SHA-256"} {
		value, err := encryptDigestHA1(s.cfg, algorithm, digestHA1(algorithm, username, password))
		if errors.Is(err, errDigestHA1KeyUnavailable) {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
		sealed = append(sealed, &value)
	}
	return sealed[0], sealed[1], nil
}

// cachedDigestHA1AEAD is the HA1 cipher for the most recently used session
// secret. A process runs with one secret, so one entry saves the HKDF expansion
// and key schedule on every seal, every Digest verification and every row the
// readiness badge checks. GCM keeps no per-call state, so the one instance
// serves concurrent callers.
type cachedDigestHA1AEAD struct {
	secret string
	aead   cipher.AEAD
}

var digestHA1AEADCache atomic.Pointer[cachedDigestHA1AEAD]

func digestHA1AEAD(cfg *config.Config) (cipher.AEAD, error) {
	if cached := digestHA1AEADCache.Load(); cached != nil && cfg != nil && cached.secret == cfg.Session.Secret {
		return cached.aead, nil
	}
	key, err := deriveDigestKey(cfg, digestHA1KeyInfo)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	digestHA1AEADCache.Store(&cachedDigestHA1AEAD{secret: cfg.Session.Secret, aead: aead})
	return aead, nil
}

// deriveDigestKey expands the configured session secret into one purpose-bound
// key. Deriving rather than generating is what lets nonces survive a restart and
// lets every replica accept the others' nonces and read the others' HA1s.
func deriveDigestKey(cfg *config.Config, info string) ([]byte, error) {
	if cfg == nil || cfg.Session.Secret == "" {
		return nil, errDigestHA1KeyUnavailable
	}
	return hkdf.Key(sha256.New, []byte(cfg.Session.Secret), nil, info, 32)
}

// DigestEnabled reports whether this deployment has opted into the DAV Digest
// scheme. It gates offering and accepting the scheme and writing any HA1; see
// sealDigestCredentials.
func DigestEnabled(cfg *config.Config) bool {
	return cfg != nil && cfg.DAV.DigestEnabled
}

func (s *Service) digestEnabled() bool {
	return s != nil && DigestEnabled(s.cfg)
}

// DigestReady reports whether an app password can answer a DAV Digest challenge
// right now: the scheme is enabled, both HA1s are present, and this server can
// open them. A value sealed under a since-rotated session secret verifies
// nothing, so non-null columns alone are not enough.
func DigestReady(cfg *config.Config, token store.AppPassword) bool {
	if !DigestEnabled(cfg) {
		return false
	}
	return digestCredentialOpens(cfg, "MD5", token.DigestMD5HA1) && digestCredentialOpens(cfg, "SHA-256", token.DigestSHA256HA1)
}

func digestCredentialOpens(cfg *config.Config, algorithm string, stored *string) bool {
	if stored == nil || *stored == "" {
		return false
	}
	_, err := decryptDigestHA1(cfg, algorithm, *stored)
	return err == nil
}

// reportUnreadableDigestCredential names the condition behind a stored HA1
// that will not open. Without a line for it, Digest failing across the
// deployment is indistinguishable from users mistyping passwords.
//
// It reports once per process. This runs before the response is checked, so any
// client that can obtain a challenge reaches it, and a per-request line would
// let an unauthenticated one flood the log. The condition is global, not
// per-credential, so one line is enough.
func (s *Service) reportUnreadableDigestCredential(algorithm string, tokenID int64, err error) {
	s.digestUnreadableOnce.Do(func() {
		if errors.Is(err, errDigestHA1KeyUnavailable) {
			s.logger().Error("dav_digest", "stored %s credential for app password %d cannot be opened because APP_SESSION_SECRET is not configured; Digest cannot verify any app password until it is", algorithm, tokenID)
			return
		}
		s.logger().Error("dav_digest", "stored %s credential for app password %d could not be opened, so Digest cannot verify it; APP_SESSION_SECRET was most likely rotated. An affected app password is re-sealed the next time it authenticates over Basic, which is only accepted over HTTPS; where DAV is served without HTTPS, affected app passwords must be revoked and reissued: %v", algorithm, tokenID, err)
	})
}

func (s *Service) writeDAVAuthChallenge(w http.ResponseWriter, r *http.Request, stale bool) error {
	w.Header().Del("WWW-Authenticate")
	if s.digestEnabled() {
		nonce, opaque, err := s.newDigestNonce()
		if err != nil {
			return err
		}
		staleParameter := ""
		if stale {
			staleParameter = ", stale=true"
		}
		w.Header().Add("WWW-Authenticate", fmt.Sprintf(`Digest realm=%q, nonce=%q, opaque=%q, algorithm=SHA-256, qop="auth"%s`, davDigestRealm, nonce, opaque, staleParameter))
		w.Header().Add("WWW-Authenticate", fmt.Sprintf(`Digest realm=%q, nonce=%q, opaque=%q, algorithm=MD5, qop="auth"%s`, davDigestRealm, nonce, opaque, staleParameter))
	}
	// RFC 4791 Section 11 forbids Basic without TLS, so a cleartext transport
	// with Digest turned off leaves no scheme this server will honour and the
	// 401 carries no challenge. RFC 7235 Section 3.1 asks for one, but naming a
	// scheme that would then be refused is the worse answer, and a deployment
	// serving DAV over cleartext at all is already misconfigured.
	if s.secureRequest(r) {
		w.Header().Add("WWW-Authenticate", `Basic realm="CalCard DAV"`)
	}
	return nil
}

func (s *Service) newDigestNonce() (string, string, error) {
	s.digestMu.Lock()
	defer s.digestMu.Unlock()
	if err := s.ensureDigestKeyLocked(); err != nil {
		return "", "", err
	}
	payload := make([]byte, 24)
	binary.BigEndian.PutUint64(payload[:8], uint64(s.digestTime().Unix()))
	if _, err := rand.Read(payload[8:]); err != nil {
		return "", "", err
	}
	mac := hmac.New(sha256.New, s.digestKey)
	_, _ = mac.Write(payload)
	signed := append(payload, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(signed), s.digestOpaqueLocked(), nil
}

// ensureDigestKeyLocked resolves the key that signs and verifies nonces. It is
// derived from the configured session secret so a restart does not invalidate
// every outstanding nonce — which would force each client to re-prompt for
// credentials, since a signature that no longer verifies cannot be reported as
// stale — and so replicas accept one another's nonces. Without a configured
// secret the key is process-local, which is correct but cannot outlive the
// process; that only arises in tooling and tests, because the configuration
// loader requires the secret.
func (s *Service) ensureDigestKeyLocked() error {
	if len(s.digestKey) != 0 {
		return nil
	}
	if derived, err := deriveDigestKey(s.cfg, digestNonceKeyInfo); err == nil {
		s.digestKey = derived
		return nil
	} else if !errors.Is(err, errDigestHA1KeyUnavailable) {
		return err
	}
	s.digestKey = make([]byte, 32)
	if _, err := rand.Read(s.digestKey); err != nil {
		s.digestKey = nil
		return err
	}
	return nil
}

func (s *Service) digestOpaqueLocked() string {
	mac := hmac.New(sha256.New, s.digestKey)
	_, _ = mac.Write([]byte(davDigestRealm))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Service) digestTime() time.Time {
	if s.digestNow != nil {
		return s.digestNow()
	}
	return time.Now()
}

func (s *Service) validateDigestNonce(nonce, opaque string) (valid, stale bool) {
	signed, err := base64.RawURLEncoding.DecodeString(nonce)
	if err != nil || len(signed) != 56 {
		return false, false
	}
	s.digestMu.Lock()
	defer s.digestMu.Unlock()
	if err := s.ensureDigestKeyLocked(); err != nil {
		return false, false
	}
	payload, signature := signed[:24], signed[24:]
	mac := hmac.New(sha256.New, s.digestKey)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) || subtle.ConstantTimeCompare([]byte(opaque), []byte(s.digestOpaqueLocked())) != 1 {
		return false, false
	}
	issuedAt := time.Unix(int64(binary.BigEndian.Uint64(payload[:8])), 0)
	now := s.digestTime()
	if issuedAt.After(now.Add(time.Minute)) {
		return false, false
	}
	if now.Sub(issuedAt) > davDigestNonceTTL {
		return false, true
	}
	return true, false
}

func (s *Service) validateDAVDigest(r *http.Request) (*store.User, bool, error) {
	_, encoded, ok := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
	if !ok {
		return nil, false, errors.New("malformed digest authorization")
	}
	params, err := parseDigestParameters(encoded)
	if err != nil {
		return nil, false, err
	}
	for _, required := range []string{"username", "realm", "nonce", "uri", "response", "qop", "nc", "cnonce", "opaque"} {
		if params[required] == "" {
			return nil, false, fmt.Errorf("missing digest parameter %q", required)
		}
	}
	algorithm := strings.ToUpper(params["algorithm"])
	if algorithm == "" {
		algorithm = "MD5"
	}
	if algorithm != "MD5" && algorithm != "SHA-256" {
		return nil, false, errors.New("unsupported digest algorithm")
	}
	if params["realm"] != davDigestRealm || !strings.EqualFold(params["qop"], "auth") || strings.EqualFold(params["userhash"], "true") {
		return nil, false, errors.New("invalid digest parameters")
	}
	requestTarget := r.RequestURI
	if requestTarget == "" {
		requestTarget = r.URL.RequestURI()
	}
	if params["uri"] != requestTarget {
		return nil, false, errors.New("digest URI does not match request target")
	}
	if len(params["nc"]) != 8 {
		return nil, false, errors.New("invalid digest nonce count")
	}
	nonceCount64, err := strconv.ParseUint(params["nc"], 16, 32)
	if err != nil || nonceCount64 == 0 {
		return nil, false, errors.New("invalid digest nonce count")
	}
	validNonce, stale := s.validateDigestNonce(params["nonce"], params["opaque"])
	if !validNonce {
		return nil, stale, errors.New("invalid digest nonce")
	}
	if _, err := hex.DecodeString(params["response"]); err != nil {
		return nil, false, errors.New("invalid digest response")
	}

	user, err := s.store.Users.GetByEmail(r.Context(), params["username"])
	if err != nil || user == nil {
		return nil, false, errors.New("invalid digest user")
	}
	tokens, err := s.store.AppPasswords.FindValidByUser(r.Context(), user.ID)
	if err != nil {
		return nil, false, err
	}
	now := s.digestTime()
	for _, token := range tokens {
		if token.RevokedAt != nil || token.ExpiresAt != nil && !token.ExpiresAt.After(now) {
			continue
		}
		var stored *string
		if algorithm == "MD5" {
			stored = token.DigestMD5HA1
		} else {
			stored = token.DigestSHA256HA1
		}
		if stored == nil || *stored == "" {
			continue
		}
		ha1, err := decryptDigestHA1(s.cfg, algorithm, *stored)
		if err != nil {
			s.reportUnreadableDigestCredential(algorithm, token.ID, err)
			continue
		}
		ha2 := digestHash(algorithm, r.Method+":"+requestTarget)
		expected := digestHash(algorithm, ha1+":"+params["nonce"]+":"+params["nc"]+":"+params["cnonce"]+":auth:"+ha2)
		if len(expected) != len(params["response"]) || subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(params["response"]))) != 1 {
			continue
		}
		claimed, err := s.acceptDigestNonceCount(r.Context(), token.ID, params["nonce"], uint32(nonceCount64))
		if err != nil {
			// The credential itself checked out; only the replay-guard write
			// failed. stale=true lets a well-behaved client retry with a fresh
			// nonce instead of re-prompting the user for credentials that were
			// never the problem.
			s.logger().Error("dav_digest", "replay guard unavailable for app password %d: %v", token.ID, err)
			return nil, true, errors.New("digest replay guard unavailable")
		}
		if !claimed {
			return nil, false, errors.New("replayed digest credentials")
		}
		s.touchLastUsedThrottled(token)
		return user, false, nil
	}
	return nil, false, errors.New("invalid digest credentials")
}

// acceptDigestNonceCount claims one (nonce, nonce count) pair, which RFC 7616
// section 3.4 allows a client to present once.
//
// The claim is recorded in the database rather than in this process. A nonce is
// signed with a key derived from the configured session secret, so it verifies
// at every instance holding that secret and after a restart; a per-process
// record of what had been spent would leave captured credentials replayable
// anywhere else for the nonce lifetime.
//
// The bool and the error are both needed: a store that cannot answer fails the
// claim exactly like a spent nonce count does (admitting the request would be
// admitting one this server cannot tell apart from a replay), but the caller
// still needs to tell the two apart to report and challenge them differently.
func (s *Service) acceptDigestNonceCount(ctx context.Context, tokenID int64, nonce string, nonceCount uint32) (bool, error) {
	if s.store == nil || s.store.DigestNonces == nil {
		return false, errors.New("digest nonce store not configured")
	}
	claimed, err := s.store.DigestNonces.Consume(ctx, tokenID, nonce, nonceCount, s.digestTime().Add(davDigestNonceTTL))
	if err != nil {
		return false, fmt.Errorf("consume digest nonce count: %w", err)
	}
	return claimed, nil
}

func parseDigestParameters(input string) (map[string]string, error) {
	params := make(map[string]string)
	for offset := 0; ; {
		for offset < len(input) && (input[offset] == ' ' || input[offset] == '\t') {
			offset++
		}
		if offset == len(input) {
			return params, nil
		}
		start := offset
		for offset < len(input) && input[offset] != '=' && input[offset] != ',' && input[offset] != ' ' && input[offset] != '\t' {
			offset++
		}
		name := strings.ToLower(input[start:offset])
		for offset < len(input) && (input[offset] == ' ' || input[offset] == '\t') {
			offset++
		}
		if name == "" || offset >= len(input) || input[offset] != '=' {
			return nil, errors.New("malformed digest parameter")
		}
		offset++
		for offset < len(input) && (input[offset] == ' ' || input[offset] == '\t') {
			offset++
		}
		var value strings.Builder
		if offset < len(input) && input[offset] == '"' {
			offset++
			closed := false
			for offset < len(input) {
				character := input[offset]
				offset++
				if character == '"' {
					closed = true
					break
				}
				if character == '\\' {
					if offset >= len(input) {
						return nil, errors.New("malformed digest quoted string")
					}
					character = input[offset]
					offset++
				}
				if character < 0x20 || character == 0x7f {
					return nil, errors.New("invalid digest quoted string")
				}
				value.WriteByte(character)
			}
			if !closed {
				return nil, errors.New("unterminated digest quoted string")
			}
		} else {
			start = offset
			for offset < len(input) && input[offset] != ',' {
				offset++
			}
			value.WriteString(strings.TrimSpace(input[start:offset]))
		}
		if value.Len() == 0 {
			return nil, errors.New("empty digest parameter")
		}
		if _, duplicate := params[name]; duplicate {
			return nil, errors.New("duplicate digest parameter")
		}
		params[name] = value.String()
		for offset < len(input) && (input[offset] == ' ' || input[offset] == '\t') {
			offset++
		}
		if offset == len(input) {
			return params, nil
		}
		if input[offset] != ',' {
			return nil, errors.New("malformed digest parameter separator")
		}
		offset++
		if offset == len(input) {
			return nil, errors.New("trailing digest separator")
		}
	}
}

package auth

import (
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
	"time"

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
// stored HA1s can be neither written nor read. Digest then has no credentials
// to verify against and app passwords remain usable over Basic alone.
var errDigestHA1KeyUnavailable = errors.New("digest credential key unavailable")

type digestReplayEntry struct {
	nonceCounts map[uint32]struct{}
	expiresAt   time.Time
}

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
// against. It is password-equivalent for this realm: anything holding it can
// authenticate without knowing the password, which is why it is only ever
// persisted through encryptDigestHA1.
func digestHA1(algorithm, username, password string) string {
	return digestHash(algorithm, username+":"+davDigestRealm+":"+password)
}

// encryptDigestHA1 seals an HA1 for storage. The algorithm is authenticated
// alongside the ciphertext so a value written for one algorithm cannot be moved
// into the other's column and still verify.
func (s *Service) encryptDigestHA1(algorithm, ha1 string) (string, error) {
	aead, err := s.digestHA1AEAD()
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
func (s *Service) decryptDigestHA1(algorithm, stored string) (string, error) {
	version, encoded, ok := strings.Cut(stored, ":")
	if !ok || version != digestHA1Version {
		return "", errors.New("unrecognized digest credential encoding")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	aead, err := s.digestHA1AEAD()
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
// a newly issued app password, or two nils when no key is configured. A missing
// key is not an error: it leaves the app password Basic-only, the same state
// every app password issued before Digest existed is already in.
func (s *Service) sealDigestCredentials(username, password string) (*string, *string, error) {
	sealed := make([]*string, 0, 2)
	for _, algorithm := range []string{"MD5", "SHA-256"} {
		value, err := s.encryptDigestHA1(algorithm, digestHA1(algorithm, username, password))
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

func (s *Service) digestHA1AEAD() (cipher.AEAD, error) {
	key, err := s.deriveDigestKey(digestHA1KeyInfo)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// deriveDigestKey expands the configured session secret into one purpose-bound
// key. Deriving rather than generating is what lets nonces survive a restart and
// lets every replica accept the others' nonces and read the others' HA1s.
func (s *Service) deriveDigestKey(info string) ([]byte, error) {
	if s == nil || s.cfg == nil || s.cfg.Session.Secret == "" {
		return nil, errDigestHA1KeyUnavailable
	}
	return hkdf.Key(sha256.New, []byte(s.cfg.Session.Secret), nil, info, 32)
}

func (s *Service) writeDAVAuthChallenge(w http.ResponseWriter, r *http.Request, stale bool) error {
	nonce, opaque, err := s.newDigestNonce()
	if err != nil {
		return err
	}
	staleParameter := ""
	if stale {
		staleParameter = ", stale=true"
	}
	w.Header().Del("WWW-Authenticate")
	w.Header().Add("WWW-Authenticate", fmt.Sprintf(`Digest realm=%q, nonce=%q, opaque=%q, algorithm=SHA-256, qop="auth"%s`, davDigestRealm, nonce, opaque, staleParameter))
	w.Header().Add("WWW-Authenticate", fmt.Sprintf(`Digest realm=%q, nonce=%q, opaque=%q, algorithm=MD5, qop="auth"%s`, davDigestRealm, nonce, opaque, staleParameter))
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
	if derived, err := s.deriveDigestKey(digestNonceKeyInfo); err == nil {
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
		ha1, err := s.decryptDigestHA1(algorithm, *stored)
		if err != nil {
			continue
		}
		ha2 := digestHash(algorithm, r.Method+":"+requestTarget)
		expected := digestHash(algorithm, ha1+":"+params["nonce"]+":"+params["nc"]+":"+params["cnonce"]+":auth:"+ha2)
		if len(expected) != len(params["response"]) || subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(params["response"]))) != 1 {
			continue
		}
		if !s.acceptDigestNonceCount(token.ID, params["nonce"], uint32(nonceCount64)) {
			return nil, false, errors.New("replayed digest credentials")
		}
		s.touchLastUsedThrottled(token)
		return user, false, nil
	}
	return nil, false, errors.New("invalid digest credentials")
}

func (s *Service) acceptDigestNonceCount(tokenID int64, nonce string, nonceCount uint32) bool {
	now := s.digestTime()
	key := strconv.FormatInt(tokenID, 10) + "\x00" + nonce
	s.digestMu.Lock()
	defer s.digestMu.Unlock()
	if s.digestReplay == nil {
		s.digestReplay = make(map[string]digestReplayEntry)
	}
	for replayKey, entry := range s.digestReplay {
		if !entry.expiresAt.After(now) {
			delete(s.digestReplay, replayKey)
		}
	}
	entry, exists := s.digestReplay[key]
	if !exists {
		entry = digestReplayEntry{
			nonceCounts: make(map[uint32]struct{}),
			expiresAt:   now.Add(davDigestNonceTTL),
		}
	}
	if _, duplicate := entry.nonceCounts[nonceCount]; duplicate {
		return false
	}
	entry.nonceCounts[nonceCount] = struct{}{}
	s.digestReplay[key] = entry
	return true
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

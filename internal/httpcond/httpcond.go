// Package httpcond evaluates the If-Match and If-None-Match request headers
// against a stored entity tag as RFC 9110 section 13.1 defines them, so every
// write path decides them alike.
package httpcond

import (
	"slices"
	"strings"
)

// Check reports whether a request's If-Match and If-None-Match headers hold
// for a resource that exists or not, with entity tag etag. Both are evaluated.
// If-Match uses the strong comparison and fails where the resource is absent;
// If-None-Match uses the weak comparison and constrains only a resource that
// exists. "*" matches any resource that exists, and a list matches when any of
// its tags does. An empty header is absent.
func Check(ifMatch, ifNoneMatch, etag string, exists bool) bool {
	if ifMatch = strings.TrimSpace(ifMatch); ifMatch != "" {
		if !exists || !ListMatches(ifMatch, etag, false) {
			return false
		}
	}
	if ifNoneMatch = strings.TrimSpace(ifNoneMatch); ifNoneMatch != "" {
		if exists && ListMatches(ifNoneMatch, etag, true) {
			return false
		}
	}
	return true
}

// ListMatches reports whether any entity tag in a comma-separated If-Match or
// If-None-Match value matches etag. "*" matches any existing resource. Stored
// ETags are strong, so a weak candidate (W/"...") can only weak-compare equal
// (RFC 9110 section 8.8.3.2); it never matches under the strong comparison
// If-Match requires.
func ListMatches(headerValue, etag string, allowWeak bool) bool {
	return headerValue == "*" || slices.Contains(EntityTags(headerValue, allowWeak), etag)
}

// EntityTags parses a comma-separated If-Match or If-None-Match value into the
// bare entity tags it names, dropping weak ones where the comparison is
// strong.
func EntityTags(headerValue string, allowWeak bool) []string {
	var tags []string
	for _, candidate := range strings.Split(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		if strings.HasPrefix(candidate, "W/") {
			if !allowWeak {
				continue
			}
			candidate = strings.TrimPrefix(candidate, "W/")
		}
		if candidate = strings.Trim(candidate, "\""); candidate != "" {
			tags = append(tags, candidate)
		}
	}
	return tags
}

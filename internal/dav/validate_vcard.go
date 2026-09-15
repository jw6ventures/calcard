package dav

import (
	"fmt"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
)

// extractUIDFromVCard extracts the UID property from vCard data
func extractUIDFromVCard(vcardData string) (string, error) {
	// Unfold lines per RFC 6350 (same as RFC 5545)
	lines := ical.UnfoldLines(vcardData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "UID") {
			// Check for proper delimiter (: or ;)
			if len(upperLine) == len("UID") || (len(upperLine) > len("UID") && (upperLine[len("UID")] == ':' || upperLine[len("UID")] == ';')) {
				colonIdx := strings.Index(line, ":")
				if colonIdx == -1 {
					continue
				}
				uid := strings.TrimSpace(line[colonIdx+1:])
				if uid == "" {
					return "", fmt.Errorf("empty UID property")
				}
				return uid, nil
			}
		}
	}
	return "", fmt.Errorf("no UID property found in vCard data")
}

// vcardStructure is the property presence both the PUT gate and the version
// conversion judge a card by. They read the same card but apply different
// policies to it -- what the server accepts from a client is deliberately more
// forgiving than what it emits -- so the reading is shared and the rules are not.
type vcardStructure struct {
	versions  []string
	uidCount  int
	emptyUID  bool
	badValues []string
	hasFN     bool
	hasN      bool
}

func readVCardStructure(data string) vcardStructure {
	var structure vcardStructure
	for _, line := range ical.UnfoldLines(data) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx == -1 {
			continue
		}
		head := strings.ToUpper(strings.TrimSpace(line[:colonIdx]))
		if semiIdx := strings.IndexByte(head, ';'); semiIdx >= 0 {
			head = head[:semiIdx]
		}
		value := strings.TrimSpace(line[colonIdx+1:])
		switch vcardPropertyBaseName(head) {
		case "VERSION":
			structure.versions = append(structure.versions, value)
		case "FN":
			structure.hasFN = structure.hasFN || value != ""
		case "N":
			structure.hasN = structure.hasN || value != ""
		case "UID":
			structure.uidCount++
			structure.emptyUID = structure.emptyUID || value == ""
		}
	}
	return structure
}

// validateVCardEnvelope checks that the octets are one whole VCARD, which every
// reading of the content lines below depends on.
func validateVCardEnvelope(data string) error {
	trimmed := strings.TrimSpace(data)

	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCARD") {
		return fmt.Errorf("missing BEGIN:VCARD")
	}

	if !strings.HasSuffix(strings.ToUpper(trimmed), "END:VCARD") {
		return fmt.Errorf("missing END:VCARD")
	}

	upper := strings.ToUpper(trimmed)
	beginCount := strings.Count(upper, "BEGIN:VCARD")
	endCount := strings.Count(upper, "END:VCARD")
	if beginCount != endCount {
		return fmt.Errorf("unbalanced VCARD tags")
	}
	if beginCount != 1 {
		return fmt.Errorf("address object resources must contain exactly one VCARD")
	}
	return nil
}

func (h *DavServer) validateVCard(data string) error {
	if err := validateVCardEnvelope(data); err != nil {
		return err
	}

	structure := readVCardStructure(data)
	for _, version := range structure.versions {
		if version != "3.0" && version != "4.0" {
			return fmt.Errorf("unsupported VCARD version")
		}
	}
	if structure.emptyUID {
		return fmt.Errorf("VCARD UID must not be empty")
	}
	if len(structure.versions) != 1 {
		return fmt.Errorf("VCARD must contain exactly one VERSION")
	}
	if structure.uidCount != 1 {
		return fmt.Errorf("VCARD must contain exactly one UID")
	}
	if !structure.hasFN {
		return fmt.Errorf("VCARD must contain FN")
	}

	return nil
}

// validateVCardForVersion reports whether data is a valid card of exactly the
// named version. It is what the version conversion checks its own output
// against, and it is deliberately stricter than validateVCard: a card arriving
// from a client is stored as it was written, but a card this server produces
// has no author to blame for being invalid.
//
// The versions differ in what they require. RFC 2426 Section 3.1.2 makes N
// mandatory in vCard 3.0, and RFC 6350 Section 6.2.2 made it optional in 4.0
// while Section 6.2.1 kept FN mandatory.
func validateVCardForVersion(data, version string) error {
	if err := validateVCardEnvelope(data); err != nil {
		return err
	}

	structure := readVCardStructure(data)
	if len(structure.versions) != 1 {
		return fmt.Errorf("VCARD must contain exactly one VERSION")
	}
	if structure.versions[0] != version {
		return fmt.Errorf("VCARD declares version %q, want %q", structure.versions[0], version)
	}
	if !structure.hasFN {
		return fmt.Errorf("vCard %s requires FN", version)
	}
	if version == "3.0" && !structure.hasN {
		return fmt.Errorf("vCard 3.0 requires N")
	}
	return nil
}

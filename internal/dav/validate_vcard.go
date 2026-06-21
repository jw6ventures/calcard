package dav

import (
	"fmt"
	"strings"
)

// extractUIDFromVCard extracts the UID property from vCard data
func extractUIDFromVCard(vcardData string) (string, error) {
	// Unfold lines per RFC 6350 (same as RFC 5545)
	lines := unfoldICalLines(vcardData)
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

func (h *DavServer) validateVCard(data string) error {
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

	lines := unfoldICalLines(trimmed)
	versionCount := 0
	hasFN := false
	uidCount := 0
	for _, line := range lines {
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
		name := vcardPropertyBaseName(head)
		value := strings.TrimSpace(line[colonIdx+1:])
		switch name {
		case "VERSION":
			versionCount++
			if value != "3.0" && value != "4.0" {
				return fmt.Errorf("unsupported VCARD version")
			}
		case "FN":
			if value != "" {
				hasFN = true
			}
		case "UID":
			uidCount++
			if value == "" {
				return fmt.Errorf("VCARD UID must not be empty")
			}
		}
	}
	if versionCount != 1 {
		return fmt.Errorf("VCARD must contain exactly one VERSION")
	}
	if uidCount != 1 {
		return fmt.Errorf("VCARD must contain exactly one UID")
	}
	if !hasFN {
		return fmt.Errorf("VCARD must contain FN")
	}

	return nil
}

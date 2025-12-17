package utils

import (
	"fmt"
	"strings"
	"time"
)

// BuildVCard constructs a valid vCard 3.0.
func BuildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes, company string) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCARD\r\n")
	sb.WriteString("VERSION:3.0\r\n")
	sb.WriteString(fmt.Sprintf("UID:%s\r\n", uid))
	sb.WriteString(fmt.Sprintf("FN:%s\r\n", EscapeVCardValue(displayName)))

	// N: Last;First;Middle;Prefix;Suffix
	sb.WriteString(fmt.Sprintf("N:%s;%s;;;\r\n", EscapeVCardValue(lastName), EscapeVCardValue(firstName)))

	if company != "" {
		sb.WriteString(fmt.Sprintf("ORG:%s\r\n", EscapeVCardValue(company)))
	}

	if email != "" {
		sb.WriteString(fmt.Sprintf("EMAIL;TYPE=INTERNET:%s\r\n", email))
	}

	if phone != "" {
		sb.WriteString(fmt.Sprintf("TEL;TYPE=CELL:%s\r\n", phone))
	}

	if birthday != "" {
		// Handle both YYYY-MM-DD and --MM-DD formats
		if strings.HasPrefix(birthday, "--") {
			// No year specified, use --MM-DD vCard format
			sb.WriteString(fmt.Sprintf("BDAY:%s\r\n", birthday))
		} else if t, err := time.Parse("2006-01-02", birthday); err == nil {
			sb.WriteString(fmt.Sprintf("BDAY:%s\r\n", t.Format("2006-01-02")))
		}
	}

	if notes != "" {
		sb.WriteString(fmt.Sprintf("NOTE:%s\r\n", EscapeVCardValue(notes)))
	}

	sb.WriteString(fmt.Sprintf("REV:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))
	sb.WriteString("END:VCARD\r\n")

	return sb.String()
}

// EscapeVCardValue escapes special characters for vCard format.
func EscapeVCardValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ";", "\\;")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// ParseVCFFile parses a VCF file and returns individual vCard components.
// A VCF file can contain multiple vCards (BEGIN:VCARD...END:VCARD blocks).
func ParseVCFFile(content string) ([]string, error) {
	// Unfold lines (vCards can have folded lines with spaces/tabs)
	unfolded := strings.ReplaceAll(content, "\r\n ", "")
	unfolded = strings.ReplaceAll(unfolded, "\r\n\t", "")
	unfolded = strings.ReplaceAll(unfolded, "\n ", "")
	unfolded = strings.ReplaceAll(unfolded, "\n\t", "")

	// Split into lines
	lines := strings.Split(strings.ReplaceAll(unfolded, "\r\n", "\n"), "\n")

	var vcards []string
	var current strings.Builder
	inVCard := false

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "BEGIN:VCARD") {
			inVCard = true
			current.Reset()
			current.WriteString(line)
			current.WriteString("\r\n")
		} else if strings.HasPrefix(line, "END:VCARD") {
			current.WriteString(line)
			current.WriteString("\r\n")
			if inVCard {
				vcards = append(vcards, current.String())
			}
			inVCard = false
		} else if inVCard {
			current.WriteString(line)
			current.WriteString("\r\n")
		}
	}

	return vcards, nil
}

// ExtractVCardUID extracts the UID from a vCard string.
func ExtractVCardUID(vcard string) string {
	lines := strings.Split(vcard, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(line), "UID:") {
			return strings.TrimSpace(line[4:])
		}
	}
	return ""
}

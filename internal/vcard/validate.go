package vcard

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxUIDOctets bounds a UID: it is the key of a unique index, whose entries
// PostgreSQL limits in size. It matches store.MaxIdentifierOctets.
const MaxUIDOctets = 1024

var (
	// ErrMalformed is matched by every refusal of a card's structure: its
	// envelope, its VERSION, or content that cannot be read.
	ErrMalformed = errors.New("malformed vCard")
	// ErrMissingFN reports a card with no name to show.
	ErrMissingFN = errors.New("VCARD must contain FN")
)

// kindError keeps a refusal's own text while matching a sentinel.
type kindError struct {
	message string
	kind    error
}

func (e *kindError) Error() string { return e.message }
func (e *kindError) Unwrap() error { return e.kind }

func malformed(message string) error { return &kindError{message: message, kind: ErrMalformed} }

// ValidateEnvelope checks that the data is exactly one whole VCARD: the first
// content line opens it, the last closes it, and no delimiter line sits between.
// Delimiters are matched as whole content lines, so a value that merely spells
// one out is not mistaken for one.
func ValidateEnvelope(data string) error {
	return validateEnvelope(ContentLines(data))
}

func validateEnvelope(lines []string) error {
	if len(lines) == 0 || !isDelimiter(lines[0], "BEGIN") {
		return malformed("missing BEGIN:VCARD")
	}
	if !isDelimiter(lines[len(lines)-1], "END") {
		return malformed("missing END:VCARD")
	}
	var begins, ends int
	for _, line := range lines {
		switch {
		case isDelimiter(line, "BEGIN"):
			begins++
		case isDelimiter(line, "END"):
			ends++
		}
	}
	if begins != ends {
		return malformed("unbalanced VCARD tags")
	}
	if begins != 1 {
		return malformed("address object resources must contain exactly one VCARD")
	}
	return nil
}

func isDelimiter(line, keyword string) bool {
	return strings.EqualFold(strings.TrimSpace(line), keyword+":VCARD")
}

// Structure is the property presence callers judge a card by. Readers apply
// different policies to it, so the reading is shared and the rules are not.
type Structure struct {
	Versions []string
	// UID is the value of the first UID property.
	UID      string
	UIDCount int
	// LongestUID is the length in octets of the longest UID value.
	LongestUID int
	EmptyUID   bool
	HasFN      bool
	HasN       bool
}

// ReadStructure reads the Structure of a card whose envelope is valid.
func ReadStructure(data string) Structure {
	return readStructure(ContentLines(data))
}

func readStructure(lines []string) Structure {
	var s Structure
	for _, raw := range lines {
		line, ok := ParseLine(strings.TrimSpace(raw))
		if !ok {
			continue
		}
		value := strings.TrimSpace(line.Value)
		switch strings.ToUpper(line.Name) {
		case "VERSION":
			s.Versions = append(s.Versions, value)
		case "FN":
			s.HasFN = s.HasFN || value != ""
		case "N":
			s.HasN = s.HasN || value != ""
		case "UID":
			if s.UIDCount == 0 {
				s.UID = value
			}
			s.UIDCount++
			s.LongestUID = max(s.LongestUID, len(value))
			s.EmptyUID = s.EmptyUID || value == ""
		}
	}
	return s
}

// Validate applies the rules every stored card is held to: one whole VCARD
// declaring exactly one supported VERSION, at most one non-empty UID (exactly
// one when requireUID), and an FN.
func Validate(data string, requireUID bool) error {
	_, err := Inspect(data, requireUID)
	return err
}

// Inspect validates a card as Validate does, unfolding it once, and returns
// its Structure.
func Inspect(data string, requireUID bool) (Structure, error) {
	lines := ContentLines(data)
	if err := validateEnvelope(lines); err != nil {
		return Structure{}, err
	}
	s := readStructure(lines)
	return s, s.check(requireUID)
}

func (s Structure) check(requireUID bool) error {
	for _, version := range s.Versions {
		if version != "3.0" && version != "4.0" {
			return malformed("unsupported VCARD version")
		}
	}
	if s.EmptyUID {
		return errors.New("VCARD UID must not be empty")
	}
	if len(s.Versions) != 1 {
		return malformed("VCARD must contain exactly one VERSION")
	}
	if s.UIDCount > 1 || (requireUID && s.UIDCount != 1) {
		return errors.New("VCARD must contain exactly one UID")
	}
	if s.LongestUID > MaxUIDOctets {
		return fmt.Errorf("VCARD UID must be at most %d octets", MaxUIDOctets)
	}
	if !s.HasFN {
		return ErrMissingFN
	}
	return nil
}

// CheckOctets refuses data a card cannot be stored as: octets that are not
// UTF-8, and control characters other than the line breaks between content
// lines and tab.
func CheckOctets(data string) error {
	if !utf8.ValidString(data) {
		return malformed("vCard data is not valid UTF-8")
	}
	for i := 0; i < len(data); i++ {
		if c := data[i]; c != '\r' && c != '\n' && IsControlOctet(c) {
			return malformed("vCard data contains control characters")
		}
	}
	return nil
}

// UID returns the value of the card's first UID property, parameters and group
// allowed, or "" when it has none.
func UID(data string) string {
	for _, raw := range ContentLines(data) {
		line, ok := ParseLine(strings.TrimSpace(raw))
		if ok && line.Is("UID") {
			return strings.TrimSpace(line.Value)
		}
	}
	return ""
}

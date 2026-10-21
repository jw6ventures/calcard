package utils

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/vcard"
)

var (
	// ErrInvalidBirthday reports a birthday that is not a YYYY-MM-DD or
	// --MM-DD date.
	ErrInvalidBirthday = errors.New("vCard BDAY must be a YYYY-MM-DD or --MM-DD date")
	// ErrInvalidUID reports a UID that cannot be written into a card.
	ErrInvalidUID = errors.New("invalid vCard UID")
	// ErrControlCharacter reports a value carrying a control character.
	ErrControlCharacter = vcard.ErrControlCharacter
)

// uidError keeps a UID refusal's text while matching ErrInvalidUID, and any
// further cause, under errors.Is.
type uidError struct {
	message string
	causes  []error
}

func (e *uidError) Error() string   { return e.message }
func (e *uidError) Unwrap() []error { return append([]error{ErrInvalidUID}, e.causes...) }

// BuildVCard constructs a vCard 3.0 carrying every value it is given, or
// refuses. TEXT values are escaped; TEL, EMAIL, UID and BDAY are written raw and
// refused if they carry a control character, because a raw line break would end
// the content line and let the rest of the value be read as new properties.
func BuildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes, company string) (string, error) {
	if err := validateVCardUID(uid); err != nil {
		return "", err
	}
	bday, err := vCardBirthday(birthday)
	if err != nil {
		return "", err
	}
	if err := vcard.CheckRawValue(email); err != nil {
		return "", fmt.Errorf("vCard EMAIL: %w", err)
	}
	if err := vcard.CheckRawValue(phone); err != nil {
		return "", fmt.Errorf("vCard TEL: %w", err)
	}
	fn, err := EscapeVCardValue(displayName)
	if err != nil {
		return "", fmt.Errorf("vCard FN: %w", err)
	}
	family, err := EscapeVCardValue(lastName)
	if err != nil {
		return "", fmt.Errorf("vCard N: %w", err)
	}
	given, err := EscapeVCardValue(firstName)
	if err != nil {
		return "", fmt.Errorf("vCard N: %w", err)
	}
	org, err := EscapeVCardValue(company)
	if err != nil {
		return "", fmt.Errorf("vCard ORG: %w", err)
	}
	note, err := EscapeVCardValue(notes)
	if err != nil {
		return "", fmt.Errorf("vCard NOTE: %w", err)
	}

	var sb strings.Builder
	vcard.WriteLine(&sb, "BEGIN:VCARD")
	vcard.WriteLine(&sb, "VERSION:3.0")
	vcard.WriteLine(&sb, "UID:"+uid)
	vcard.WriteLine(&sb, "FN:"+fn)
	vcard.WriteLine(&sb, "N:"+family+";"+given+";;;")
	if org != "" {
		vcard.WriteLine(&sb, "ORG:"+org)
	}
	if email != "" {
		vcard.WriteLine(&sb, "EMAIL;TYPE=INTERNET:"+email)
	}
	if phone != "" {
		vcard.WriteLine(&sb, "TEL;TYPE=CELL:"+phone)
	}
	if bday != "" {
		vcard.WriteLine(&sb, "BDAY:"+bday)
	}
	if note != "" {
		vcard.WriteLine(&sb, "NOTE:"+note)
	}
	vcard.WriteLine(&sb, "REV:"+time.Now().UTC().Format("20060102T150405Z"))
	vcard.WriteLine(&sb, "END:VCARD")

	return sb.String(), nil
}

// validateVCardUID rejects a UID that cannot be written raw. The UID is stored
// beside the card and must come back from ExtractVCardUID unchanged, which does
// not unescape, so it is not TEXT-escaped either.
func validateVCardUID(uid string) error {
	if uid == "" {
		return &uidError{message: "vCard UID must not be empty"}
	}
	if strings.TrimSpace(uid) != uid {
		return &uidError{message: "vCard UID must not be padded with whitespace"}
	}
	if err := vcard.CheckRawValue(uid); err != nil {
		return &uidError{message: "vCard UID must not contain control characters", causes: []error{err}}
	}
	return nil
}

// vCardBirthday renders the RFC 2426 Section 3.1.5 date value, from either a
// full ISO date or the --MM-DD form that omits the year. A value that is not a
// date is refused rather than dropped; an empty one means no BDAY line.
func vCardBirthday(birthday string) (string, error) {
	if birthday == "" {
		return "", nil
	}
	monthDay, noYear := strings.CutPrefix(birthday, "--")
	layout, value := "2006-01-02", birthday
	if noYear {
		layout, value = "01-02", monthDay
	}
	date, err := time.Parse(layout, value)
	if err != nil {
		// Not quoted back: the message reaches the web UI through a redirect
		// query string.
		return "", ErrInvalidBirthday
	}
	if noYear {
		return "--" + date.Format(layout), nil
	}
	return date.Format(layout), nil
}

// EscapeVCardValue escapes a string as an RFC 2426 Section 5 TEXT value. A line
// break becomes \n; any other control character is refused.
func EscapeVCardValue(s string) (string, error) {
	return vcard.EscapeText(s)
}

// ExtractVCardUID returns the card's UID, parameters and group allowed, or ""
// when it has none.
func ExtractVCardUID(card string) string {
	return vcard.UID(card)
}

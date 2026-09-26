package contacts

import (
	"fmt"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ui/utils"
	"github.com/jw6ventures/calcard/internal/vcard"
)

// contactForm is a structured contact after trimming: the fields the web form
// and the structured API edit.
type contactForm struct {
	displayName string
	firstName   string
	lastName    string
	email       string
	phone       string
	birthday    string
	notes       string
	company     string
}

// ErrCardNotEditable refuses a structured edit of a stored card the merge
// cannot read. Rebuilding it from the form would drop every field the form
// does not show.
var ErrCardNotEditable = fmt.Errorf("%w: this contact is stored in a form the editor cannot update without losing data; edit it in a CardDAV client", ErrBadRequest)

type mergeLine struct {
	raw     string
	parsed  vcard.Line
	ok      bool
	changed bool
	dropped bool
}

// cardMerge edits a stored card in place: lines the form does not own, and
// owned lines whose value the form leaves as it was, are written back as read.
type cardMerge struct {
	lines         []mergeLine
	version       string
	droppedGroups map[string]struct{}
}

// mergeStructuredContact applies a structured edit to the stored card. It owns
// FN, N (family and given names), ORG (organization name), NOTE, BDAY and the
// first EMAIL and TEL, bumps REV, and keeps every other line and the card's
// VERSION. A form field left empty removes only its own property.
func mergeStructuredContact(stored, uid string, form contactForm, now time.Time) (string, error) {
	m, err := newCardMerge(stored, uid)
	if err != nil {
		return "", err
	}
	steps := []func() error{
		func() error { return m.setText("FN", form.displayName) },
		func() error { return m.setName(form.lastName, form.firstName) },
		func() error { return m.setOrganization(form.company) },
		func() error { return m.setRaw("EMAIL", form.email, m.emailPrefix()) },
		func() error { return m.setRaw("TEL", form.phone, m.phonePrefix()) },
		func() error { return m.setBirthday(form.birthday) },
		func() error { return m.setText("NOTE", form.notes) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return "", fmt.Errorf("%w: %w", ErrBadRequest, err)
		}
	}
	m.setRevision(now)
	m.dropOrphanedLabels()

	body := m.String()
	if err := vcard.Validate(body, true); err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	return body, nil
}

func newCardMerge(stored, uid string) (*cardMerge, error) {
	if vcard.ValidateEnvelope(stored) != nil {
		return nil, ErrCardNotEditable
	}
	if vcard.Version(stored) == "2.1" {
		converted, err := vcard.Convert21To30(stored)
		if err != nil {
			return nil, ErrCardNotEditable
		}
		stored = converted
	}
	structure := vcard.ReadStructure(stored)
	version := "3.0"
	switch len(structure.Versions) {
	case 0:
	case 1:
		version = structure.Versions[0]
	default:
		return nil, ErrCardNotEditable
	}
	if version != "3.0" && version != "4.0" {
		return nil, ErrCardNotEditable
	}

	m := &cardMerge{version: version, droppedGroups: map[string]struct{}{}}
	for _, raw := range vcard.ContentLines(stored) {
		parsed, ok := vcard.ParseLine(raw)
		m.lines = append(m.lines, mergeLine{raw: raw, parsed: parsed, ok: ok})
	}
	if structure.UIDCount == 0 {
		m.insertAfterBegin("UID:" + uid)
	}
	if len(structure.Versions) == 0 {
		m.insertAfterBegin("VERSION:" + version)
	}
	return m, nil
}

func (m *cardMerge) find(name string) int {
	for i := range m.lines {
		if l := &m.lines[i]; l.ok && !l.dropped && l.parsed.Is(name) {
			return i
		}
	}
	return -1
}

func (m *cardMerge) replace(i int, line vcard.Line) {
	m.lines[i].parsed = line
	m.lines[i].changed = true
}

func (m *cardMerge) remove(i int) {
	m.lines[i].dropped = true
	if group := m.lines[i].parsed.Group; group != "" {
		m.droppedGroups[strings.ToUpper(group)] = struct{}{}
	}
}

func (m *cardMerge) insertAfterBegin(raw string) {
	line, _ := vcard.ParseLine(raw)
	m.lines = append(m.lines[:1], append([]mergeLine{{raw: raw, parsed: line, ok: true}}, m.lines[1:]...)...)
}

func (m *cardMerge) insertBeforeEnd(raw string) {
	line, _ := vcard.ParseLine(raw)
	last := len(m.lines) - 1
	m.lines = append(m.lines[:last], mergeLine{raw: raw, parsed: line, ok: true}, m.lines[last])
}

// setText sets a single TEXT property, compared by its unescaped value so a
// value the form read back unchanged leaves the line untouched.
func (m *cardMerge) setText(name, value string) error {
	value = normalizeLineBreaks(value)
	i := m.find(name)
	if i >= 0 && vcard.UnescapeText(strings.TrimSpace(m.lines[i].parsed.Value)) == value {
		return nil
	}
	if value == "" {
		if i >= 0 {
			m.remove(i)
		}
		return nil
	}
	escaped, err := vcard.EscapeText(value)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if i < 0 {
		m.insertBeforeEnd(name + ":" + escaped)
		return nil
	}
	line := m.lines[i].parsed
	line.Value = escaped
	m.replace(i, line)
	return nil
}

// setName edits the family and given components of N, keeping the rest.
func (m *cardMerge) setName(family, given string) error {
	i := m.find("N")
	if i < 0 {
		// vCard 3.0 requires N; 4.0 makes it optional.
		if m.version == "4.0" && family == "" && given == "" {
			return nil
		}
		return m.insertComponents("N", []string{family, given, "", "", ""})
	}
	components := vcard.SplitComponents(m.lines[i].parsed.Value)
	for len(components) < 5 {
		components = append(components, "")
	}
	if vcard.UnescapeText(components[0]) == family && vcard.UnescapeText(components[1]) == given {
		return nil
	}
	var err error
	if components[0], err = vcard.EscapeText(family); err != nil {
		return fmt.Errorf("N: %w", err)
	}
	if components[1], err = vcard.EscapeText(given); err != nil {
		return fmt.Errorf("N: %w", err)
	}
	if m.version == "4.0" && strings.Trim(strings.Join(components, ""), " ") == "" {
		m.remove(i)
		return nil
	}
	line := m.lines[i].parsed
	line.Value = strings.Join(components, ";")
	m.replace(i, line)
	return nil
}

// setOrganization edits the organization name, the first ORG component, and
// keeps the organizational units after it.
func (m *cardMerge) setOrganization(company string) error {
	i := m.find("ORG")
	if i < 0 {
		if company == "" {
			return nil
		}
		return m.insertComponents("ORG", []string{company})
	}
	components := vcard.SplitComponents(m.lines[i].parsed.Value)
	if vcard.UnescapeText(components[0]) == company {
		return nil
	}
	if company == "" {
		m.remove(i)
		return nil
	}
	escaped, err := vcard.EscapeText(company)
	if err != nil {
		return fmt.Errorf("ORG: %w", err)
	}
	components[0] = escaped
	line := m.lines[i].parsed
	line.Value = strings.Join(components, ";")
	m.replace(i, line)
	return nil
}

func (m *cardMerge) insertComponents(name string, components []string) error {
	escaped := make([]string, len(components))
	for i, component := range components {
		var err error
		if escaped[i], err = vcard.EscapeText(component); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	m.insertBeforeEnd(name + ":" + strings.Join(escaped, ";"))
	return nil
}

// setRaw edits the first EMAIL or TEL. The web form reads these values raw
// rather than as TEXT, so they are written raw and compared raw; escaping them
// would add backslashes on every save.
func (m *cardMerge) setRaw(name, value, prefix string) error {
	if err := vcard.CheckRawValue(value); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	i := m.find(name)
	if i < 0 {
		if value != "" {
			m.insertBeforeEnd(prefix + value)
		}
		return nil
	}
	if strings.TrimSpace(m.lines[i].parsed.Value) == value {
		return nil
	}
	if value == "" {
		m.remove(i)
		return nil
	}
	line := m.lines[i].parsed
	if !strings.Contains(value, ":") {
		// A value with no scheme is no longer the URI VALUE=uri declared.
		line = line.WithoutParam("VALUE")
	}
	line.Value = value
	m.replace(i, line)
	return nil
}

func (m *cardMerge) emailPrefix() string {
	if m.version == "4.0" {
		return "EMAIL:"
	}
	return "EMAIL;TYPE=INTERNET:"
}

func (m *cardMerge) phonePrefix() string {
	if m.version == "4.0" {
		return "TEL;TYPE=cell:"
	}
	return "TEL;TYPE=CELL:"
}

// setBirthday edits BDAY, written in the card's version. A stored BDAY the form
// cannot show, such as free text, reaches the form empty, so an empty field
// leaves it alone rather than deleting what the user never saw.
func (m *cardMerge) setBirthday(value string) error {
	i := m.find("BDAY")
	var current vcard.Date
	representable := false
	if i >= 0 {
		current, representable = vcard.ParseDateProperty(m.lines[i].parsed)
	}
	if value == "" {
		if i >= 0 && representable {
			m.remove(i)
		}
		return nil
	}
	want, ok := vcard.ParseDate(value)
	if !ok {
		return utils.ErrInvalidBirthday
	}
	if representable && current == want {
		return nil
	}
	if i < 0 {
		m.insertBeforeEnd("BDAY:" + want.Format(m.version))
		return nil
	}
	line := m.lines[i].parsed.WithoutParam("VALUE")
	if omitted, apple := vcard.OmittedYear(line); apple && !want.HasYear {
		// Keep the Apple spelling the card's author reads back.
		want.Year, want.HasYear = omitted, true
	} else {
		line = line.WithoutParam("X-APPLE-OMIT-YEAR")
	}
	line.Value = want.Format(m.version)
	m.replace(i, line)
	return nil
}

func (m *cardMerge) setRevision(now time.Time) {
	stamp := now.UTC().Format("20060102T150405Z")
	i := m.find("REV")
	if i < 0 {
		m.insertBeforeEnd("REV:" + stamp)
		return
	}
	line := m.lines[i].parsed.WithoutParam("VALUE")
	line.Value = stamp
	m.replace(i, line)
}

// dropOrphanedLabels removes the X-ABLabel lines of a group whose property the
// edit removed, which would otherwise label nothing.
func (m *cardMerge) dropOrphanedLabels() {
	for group := range m.droppedGroups {
		labelled := false
		for _, l := range m.lines {
			if !l.dropped && l.ok && strings.EqualFold(l.parsed.Group, group) && !l.parsed.Is("X-ABLABEL") {
				labelled = true
				break
			}
		}
		if labelled {
			continue
		}
		for i := range m.lines {
			if l := &m.lines[i]; l.ok && strings.EqualFold(l.parsed.Group, group) {
				l.dropped = true
			}
		}
	}
}

func (m *cardMerge) String() string {
	var sb strings.Builder
	for _, l := range m.lines {
		switch {
		case l.dropped:
		case l.changed:
			vcard.WriteLine(&sb, l.parsed.String())
		default:
			vcard.WriteLine(&sb, l.raw)
		}
	}
	return sb.String()
}

func normalizeLineBreaks(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\r", "\n")
}

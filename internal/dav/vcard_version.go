package dav

import (
	"regexp"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
)

// Address data is stored exactly as the writing client sent it, but CardDAV
// clients pick a vCard version from CARDDAV:supported-address-data and then ask
// for that version on every read (RFC 6352 Section 10.4). Because CalCard
// advertises both 3.0 and 4.0, it has to be able to hand either one back: a
// refusal makes the resource invisible to the client, and the sync token still
// moves, so the client never asks again.

// Properties RFC 6350 removed from vCard 3.0. Nothing in vCard 4.0 names them,
// so an upgrade carries them across under vcardPreservedPropertyPrefix instead
// of emitting a property a 4.0 parser must reject.
var vcard3OnlyProperties = map[string]struct{}{
	"AGENT":       {},
	"CLASS":       {},
	"LABEL":       {},
	"MAILER":      {},
	"NAME":        {},
	"PROFILE":     {},
	"SORT-STRING": {},
}

// Properties RFC 6350 introduced that RFC 2426 has no equivalent for, not even
// a conventional X- name, so a downgrade carries them the same way.
var vcard4OnlyProperties = map[string]struct{}{
	"CLIENTPIDMAP": {},
	"LANG":         {},
	"RELATED":      {},
	"XML":          {},
}

// vcardPreservedPropertyPrefix carries a property the target version has no
// name for. Clients read a card in their preferred version, edit one field and
// PUT the whole card back, which replaces the stored copy -- so a property
// dropped on the way out is deleted from the server on the next ordinary edit.
// Riding across under an x-name, legal in both RFC 2426 Section 4 and RFC 6350
// Section 6.10, keeps the value intact for the reverse conversion to restore.
const vcardPreservedPropertyPrefix = "X-CALCARD-"

// restoreVCardProperty undoes the carry when a card is converted back to the
// version that named the property. The prefix is sliced off the verbatim name
// rather than the upper-cased one so the client's original spelling survives.
func restoreVCardProperty(line *vcardLine, upperName string, named map[string]struct{}) bool {
	if !strings.HasPrefix(upperName, vcardPreservedPropertyPrefix) {
		return false
	}
	if _, ok := named[upperName[len(vcardPreservedPropertyPrefix):]]; !ok {
		return false
	}
	line.name = line.name[len(vcardPreservedPropertyPrefix):]
	return true
}

// Group membership predates vCard 4.0 KIND/MEMBER; 3.0 clients (and DAVx5's
// vCard 3 group strategy) use the Apple address book server names instead.
// X-GENDER has no upgrade counterpart on purpose: its values are free text,
// while RFC 6350 GENDER takes a single sex code.
var vcardPropertyDowngrades = map[string]string{
	"KIND":        "X-ADDRESSBOOKSERVER-KIND",
	"MEMBER":      "X-ADDRESSBOOKSERVER-MEMBER",
	"ANNIVERSARY": "X-ANNIVERSARY",
	"GENDER":      "X-GENDER",
}

var vcardPropertyUpgrades = map[string]string{
	"X-ADDRESSBOOKSERVER-KIND":   "KIND",
	"X-ADDRESSBOOKSERVER-MEMBER": "MEMBER",
	"X-ANNIVERSARY":              "ANNIVERSARY",
}

// vCard 3.0 names a binary property's format with a TYPE parameter; vCard 4.0
// carries the same payload in a data: URI, so the two have to be mapped onto
// each other in both directions.
var vcardBinaryMediaTypes = map[string]string{
	"JPEG":  "image/jpeg",
	"JPG":   "image/jpeg",
	"PNG":   "image/png",
	"GIF":   "image/gif",
	"BMP":   "image/bmp",
	"TIFF":  "image/tiff",
	"WEBP":  "image/webp",
	"SVG":   "image/svg+xml",
	"MP3":   "audio/mpeg",
	"MPEG":  "audio/mpeg",
	"WAVE":  "audio/wav",
	"WAV":   "audio/wav",
	"AIFF":  "audio/aiff",
	"OGG":   "audio/ogg",
	"PGP":   "application/pgp-keys",
	"X509":  "application/x-x509-ca-cert",
	"PKCS7": "application/pkcs7-mime",
}

// The canonical vCard 3.0 TYPE value for a media type, kept separate from the
// table above because that one carries aliases and inverting it would not be
// deterministic.
var vcardBinaryTypeParams = map[string]string{
	"image/jpeg":                 "JPEG",
	"image/png":                  "PNG",
	"image/gif":                  "GIF",
	"image/bmp":                  "BMP",
	"image/tiff":                 "TIFF",
	"image/webp":                 "WEBP",
	"image/svg+xml":              "SVG",
	"audio/mpeg":                 "MP3",
	"audio/wav":                  "WAVE",
	"audio/aiff":                 "AIFF",
	"audio/ogg":                  "OGG",
	"application/pgp-keys":       "PGP",
	"application/x-x509-ca-cert": "X509",
	"application/pkcs7-mime":     "PKCS7",
}

var vcardBinaryProperties = map[string]struct{}{
	"PHOTO": {},
	"LOGO":  {},
	"SOUND": {},
	"KEY":   {},
}

// Properties whose value is a date or timestamp, which vCard 3.0 writes in ISO
// 8601 extended form and vCard 4.0 in basic form.
var vcardDateProperties = map[string]struct{}{
	"ANNIVERSARY":   {},
	"BDAY":          {},
	"REV":           {},
	"X-ANNIVERSARY": {},
}

var (
	extendedDateRe     = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})$`)
	extendedMonthDayRe = regexp.MustCompile(`^--(\d{2})-(\d{2})$`)
	extendedTimeRe     = regexp.MustCompile(`^(\d{2}):(\d{2}):(\d{2})(.*)$`)
	extendedZoneRe     = regexp.MustCompile(`^([+-])(\d{2}):(\d{2})$`)
	basicDateRe        = regexp.MustCompile(`^(\d{4})(\d{2})(\d{2})$`)
	basicMonthDayRe    = regexp.MustCompile(`^--(\d{2})(\d{2})$`)
	basicTimeRe        = regexp.MustCompile(`^(\d{2})(\d{2})(\d{2})(.*)$`)
	basicZoneRe        = regexp.MustCompile(`^([+-])(\d{2})(\d{2})$`)
)

// vcardLine is one unfolded content line: an optional group prefix, the
// property name as it was written, its parameters in the order they were
// written, and the raw (still escaped) value. Property names are compared
// case-insensitively but kept verbatim, so conversion never rewrites a
// spelling a client may recognise by sight (Apple's X-ABLabel, say).
type vcardLine struct {
	group  string
	name   string
	params []vcardLineParam
	value  string
}

type vcardLineParam struct {
	name   string
	values []string
}

// convertVCardVersion rewrites raw into the requested vCard version. It reports
// false when the stored data cannot be mapped onto that version at all, which
// is the CARDDAV:supported-address-data-conversion precondition.
func convertVCardVersion(raw, target string) (string, bool) {
	target = strings.TrimSpace(target)
	source, err := extractVCardVersion(raw)
	if err != nil {
		return "", false
	}
	if source == target {
		return raw, true
	}

	var convert func(vcardLine) (vcardLine, bool)
	switch {
	case source == "3.0" && target == "4.0":
		convert = upgradeVCardLine
	case source == "4.0" && target == "3.0":
		convert = downgradeVCardLine
	default:
		return "", false
	}

	lines := ical.UnfoldLines(raw)
	out := make([]string, 0, len(lines))
	var formattedName, structuredName string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parsed, ok := parseVCardLine(line)
		if !ok {
			// Malformed content lines are passed through untouched rather than
			// dropped: the stored copy is the authority over its own content.
			out = append(out, line)
			continue
		}
		converted, keep := convert(parsed)
		if !keep {
			continue
		}
		switch strings.ToUpper(vcardPropertyBaseName(converted.name)) {
		case "FN":
			if formattedName == "" {
				formattedName = converted.value
			}
		case "N":
			if structuredName == "" {
				structuredName = converted.value
			}
		}
		out = append(out, converted.String())
	}
	if len(out) == 0 {
		return "", false
	}

	// Rewriting content lines one at a time cannot produce a property the
	// source version never had, and the two versions do not require the same
	// ones. Deriving the missing one is what keeps the conversion a conversion:
	// refusing instead would hide the resource from the client for good, since
	// the sync token advances either way and the card is never asked for again.
	if target == "3.0" && structuredName == "" && formattedName != "" {
		out = insertBeforeVCardEnd(out, "N:"+structuredNameFromFormatted(formattedName))
	}
	if target == "4.0" && formattedName == "" && structuredName != "" {
		if derived := formattedNameFromStructured(structuredName); derived != "" {
			out = insertBeforeVCardEnd(out, "FN:"+derived)
		}
	}

	converted := strings.Join(out, "\r\n") + "\r\n"
	if err := validateVCardForVersion(converted, target); err != nil {
		// Judged against the card that was stored, not against the target
		// version alone. A card already invalid in its own version is handed
		// back converted as far as it goes: refusing it would hide the resource
		// from the client for good, since the sync token advances either way
		// and the card is never asked for again. A *valid* card that will not
		// convert is the real
		// CARDDAV:supported-address-data-conversion precondition, and with the
		// repairs above nothing should reach it -- which is what makes this
		// worth checking rather than assuming.
		if validateVCardForVersion(raw, source) == nil {
			return "", false
		}
	}
	return converted, true
}

// insertBeforeVCardEnd puts a derived content line where a property belongs,
// which is inside the card rather than after it.
func insertBeforeVCardEnd(lines []string, line string) []string {
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.EqualFold(strings.TrimSpace(lines[i]), "END:VCARD") {
			return append(lines[:i:i], append([]string{line}, lines[i:]...)...)
		}
	}
	return append(lines, line)
}

// structuredNameFromFormatted derives the N value RFC 2426 Section 3.1.2
// requires from the FN a vCard 4.0 card is guaranteed to carry.
//
// The last whitespace-separated word becomes the family name and the rest the
// given name, which is the reading that makes a converted card sort and display
// the way a 3.0 client expects. It is a guess about a name -- no split can be
// derived from FN with certainty -- so it is only ever made when the card has no
// N of its own, and never overwrites one.
func structuredNameFromFormatted(formatted string) string {
	words := strings.Fields(formatted)
	switch len(words) {
	case 0:
		return ";;;;"
	case 1:
		return words[0] + ";;;;"
	default:
		return words[len(words)-1] + ";" + strings.Join(words[:len(words)-1], " ") + ";;;"
	}
}

// formattedNameFromStructured derives the FN RFC 6350 Section 6.2.1 makes
// mandatory from the N components, read in the order a name is spoken rather
// than the order the property stores them.
func formattedNameFromStructured(structured string) string {
	components := splitVCardComponents(structured)
	component := func(i int) string {
		if i < len(components) {
			return strings.TrimSpace(components[i])
		}
		return ""
	}
	// prefix, given, additional, family, suffix
	ordered := []string{component(3), component(1), component(2), component(0), component(4)}
	spoken := make([]string, 0, len(ordered))
	for _, part := range ordered {
		if part != "" {
			spoken = append(spoken, part)
		}
	}
	return strings.Join(spoken, " ")
}

// splitVCardComponents splits a structured property value on its unescaped
// semicolons, leaving every escape sequence in the components untouched.
func splitVCardComponents(value string) []string {
	var components []string
	var current strings.Builder
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			current.WriteByte(value[i])
			if i+1 < len(value) {
				i++
				current.WriteByte(value[i])
			}
		case ';':
			components = append(components, current.String())
			current.Reset()
		default:
			current.WriteByte(value[i])
		}
	}
	return append(components, current.String())
}

// addressDataForVersion returns the data to hand a client that asked for a
// specific vCard version, converting the stored copy when they differ.
func addressDataForVersion(raw, version string) (string, bool) {
	version = strings.TrimSpace(version)
	if version == "" {
		return raw, true
	}
	return convertVCardVersion(raw, version)
}

func upgradeVCardLine(line vcardLine) (vcardLine, bool) {
	name := strings.ToUpper(line.name)
	switch name {
	case "BEGIN", "END":
		return line, true
	case "VERSION":
		line.value = "4.0"
		return line, true
	}
	// Carried and restored verbatim: the value means nothing to this version,
	// so any rewriting below would only corrupt what the other version reads.
	if _, removed := vcard3OnlyProperties[name]; removed {
		line.name = vcardPreservedPropertyPrefix + line.name
		return line, true
	}
	if restoreVCardProperty(&line, name, vcard4OnlyProperties) {
		return line, true
	}

	// vCard 4.0 is always UTF-8 and spells the URI value type "uri".
	line.removeParams("CHARSET")
	line.replaceParamValue("VALUE", "URL", "uri")
	line.replaceParamValue("VALUE", "BINARY", "uri")

	upgradePreferenceParam(&line)
	if name == "EMAIL" {
		line.removeParamValues("TYPE", "INTERNET", "X400")
	}
	upgradeBinaryValue(&line, name)
	if _, dated := vcardDateProperties[name]; dated && !line.hasParamValue("VALUE", "TEXT") {
		line.value = compactVCardDateTime(line.value)
	}
	if name == "GEO" {
		line.value = "geo:" + strings.Replace(line.value, ";", ",", 1)
	}
	if renamed, ok := vcardPropertyUpgrades[name]; ok {
		line.name = renamed
	}
	return line, true
}

func downgradeVCardLine(line vcardLine) (vcardLine, bool) {
	name := strings.ToUpper(line.name)
	switch name {
	case "BEGIN", "END":
		return line, true
	case "VERSION":
		line.value = "3.0"
		return line, true
	}
	// Carried and restored verbatim, as on the upgrade path above.
	if _, added := vcard4OnlyProperties[name]; added {
		line.name = vcardPreservedPropertyPrefix + line.name
		return line, true
	}
	if restoreVCardProperty(&line, name, vcard3OnlyProperties) {
		return line, true
	}

	downgradePreferenceParam(&line)
	downgradeBinaryValue(&line, name)
	if name == "TEL" && strings.HasPrefix(strings.ToLower(line.value), "tel:") {
		line.value = line.value[len("tel:"):]
		line.removeParamValues("VALUE", "URI")
	}
	if _, dated := vcardDateProperties[name]; dated && !line.hasParamValue("VALUE", "TEXT") {
		line.value = expandVCardDateTime(line.value)
	}
	if name == "GEO" {
		line.value = strings.Replace(strings.TrimPrefix(line.value, "geo:"), ",", ";", 1)
	}
	// Parameters RFC 6350 added; RFC 2426 parsers have no meaning for them.
	line.removeParams("ALTID", "PID", "SORT-AS", "MEDIATYPE")
	if renamed, ok := vcardPropertyDowngrades[name]; ok {
		line.name = renamed
	}
	return line, true
}

// upgradePreferenceParam turns the vCard 3.0 TYPE=PREF marker into the vCard
// 4.0 PREF parameter.
func upgradePreferenceParam(line *vcardLine) {
	if !line.hasParamValue("TYPE", "PREF") {
		return
	}
	line.removeParamValues("TYPE", "PREF")
	if len(line.paramValues("PREF")) == 0 {
		line.setParamValues("PREF", []string{"1"})
	}
}

// downgradePreferenceParam turns the vCard 4.0 PREF parameter back into the
// vCard 3.0 TYPE=PREF marker.
func downgradePreferenceParam(line *vcardLine) {
	if len(line.paramValues("PREF")) == 0 {
		return
	}
	line.removeParams("PREF")
	if !line.hasParamValue("TYPE", "PREF") {
		line.setParamValues("TYPE", append(line.paramValues("TYPE"), "PREF"))
	}
}

// upgradeBinaryValue rewrites an inline vCard 3.0 binary value as the data: URI
// vCard 4.0 requires.
func upgradeBinaryValue(line *vcardLine, name string) {
	if _, binary := vcardBinaryProperties[name]; !binary {
		return
	}
	if !line.hasParamValue("ENCODING", "B") && !line.hasParamValue("ENCODING", "BASE64") {
		return
	}
	mediaType, typeValue := vcardMediaTypeFor(line, name)
	line.removeParams("ENCODING", "MEDIATYPE")
	if typeValue != "" {
		line.removeParamValues("TYPE", typeValue)
	}
	line.removeParams("VALUE")
	line.value = "data:" + mediaType + ";base64," + strings.TrimSpace(line.value)
}

// downgradeBinaryValue unpacks a vCard 4.0 data: URI into the inline base64
// value and TYPE parameter vCard 3.0 uses.
func downgradeBinaryValue(line *vcardLine, name string) {
	if _, binary := vcardBinaryProperties[name]; !binary {
		return
	}
	if !strings.HasPrefix(strings.ToLower(line.value), "data:") {
		markVCardURIValue(line)
		return
	}
	header, data, found := strings.Cut(line.value[len("data:"):], ",")
	if !found {
		markVCardURIValue(line)
		return
	}
	if !strings.HasSuffix(strings.ToLower(header), ";base64") {
		// Only base64 payloads can be expressed as a vCard 3.0 inline value;
		// the rest stay the URI they already are.
		markVCardURIValue(line)
		return
	}
	mediaType := header[:len(header)-len(";base64")]
	line.value = data
	line.removeParams("VALUE", "MEDIATYPE")
	line.setParamValues("ENCODING", []string{"b"})
	if typeValue, ok := vcardBinaryTypeParams[strings.ToLower(strings.TrimSpace(mediaType))]; ok {
		line.setParamValues("TYPE", append(line.paramValues("TYPE"), typeValue))
	}
}

// vcardURIValue matches a value written as a URI, which is the only shape that
// may be labelled VALUE=uri.
var vcardURIValue = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*:`)

// markVCardURIValue labels a binary property that carries a URI rather than a
// payload. RFC 2426 Section 4 defaults PHOTO, LOGO, SOUND and KEY to an inline
// binary value, so a vCard 3.0 client reading an unlabelled "https://..." reads
// it as a corrupt image -- while RFC 6350 made the URI the only form, which is
// why nothing in the 4.0 card had to say so.
func markVCardURIValue(line *vcardLine) {
	if !vcardURIValue.MatchString(line.value) {
		return
	}
	line.setParamValues("VALUE", []string{"uri"})
}

// vcardMediaTypeFor resolves the media type of an inline vCard 3.0 binary
// value, returning it with the TYPE value it came from so the caller can drop
// that value from the converted line.
func vcardMediaTypeFor(line *vcardLine, name string) (mediaType, typeValue string) {
	if values := line.paramValues("MEDIATYPE"); len(values) > 0 {
		return values[0], ""
	}
	prefix := "application/"
	switch name {
	case "PHOTO", "LOGO":
		prefix = "image/"
	case "SOUND":
		prefix = "audio/"
	}
	for _, value := range line.paramValues("TYPE") {
		upper := strings.ToUpper(value)
		if known, ok := vcardBinaryMediaTypes[upper]; ok {
			return known, value
		}
		if strings.EqualFold(value, "HOME") || strings.EqualFold(value, "WORK") {
			continue
		}
		return prefix + strings.ToLower(value), value
	}
	return "application/octet-stream", ""
}

func compactVCardDateTime(value string) string {
	date, timeOfDay, hasTime := strings.Cut(value, "T")
	if match := extendedDateRe.FindStringSubmatch(date); match != nil {
		date = match[1] + match[2] + match[3]
	} else if match := extendedMonthDayRe.FindStringSubmatch(date); match != nil {
		date = "--" + match[1] + match[2]
	}
	if !hasTime {
		return date
	}
	if match := extendedTimeRe.FindStringSubmatch(timeOfDay); match != nil {
		zone := match[4]
		if zoneMatch := extendedZoneRe.FindStringSubmatch(zone); zoneMatch != nil {
			zone = zoneMatch[1] + zoneMatch[2] + zoneMatch[3]
		}
		timeOfDay = match[1] + match[2] + match[3] + zone
	}
	return date + "T" + timeOfDay
}

func expandVCardDateTime(value string) string {
	date, timeOfDay, hasTime := strings.Cut(value, "T")
	if match := basicDateRe.FindStringSubmatch(date); match != nil {
		date = match[1] + "-" + match[2] + "-" + match[3]
	} else if match := basicMonthDayRe.FindStringSubmatch(date); match != nil {
		date = "--" + match[1] + "-" + match[2]
	}
	if !hasTime {
		return date
	}
	if match := basicTimeRe.FindStringSubmatch(timeOfDay); match != nil {
		zone := match[4]
		if zoneMatch := basicZoneRe.FindStringSubmatch(zone); zoneMatch != nil {
			zone = zoneMatch[1] + zoneMatch[2] + ":" + zoneMatch[3]
		}
		timeOfDay = match[1] + ":" + match[2] + ":" + match[3] + zone
	}
	return date + "T" + timeOfDay
}

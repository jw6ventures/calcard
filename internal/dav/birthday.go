package dav

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

const birthdayCalendarName = "Birthdays"
const birthdayCalendarDescription = "Contact birthdays from your address books"

func birthdayCalendarHref() string {
	return ensureCollectionHref(fmt.Sprintf("/dav/calendars/%d", birthdayCalendarID))
}

// birthdayCollectionState versions the generated birthday collection. The
// collection is built from every contact the user owns, and the address books
// those contacts live in already carry that state: a book's ctag counts every
// contact inserted, updated and deleted in it, and the number of books catches
// a book removed whole, which takes its contacts with it without touching any
// surviving book. The ctag moves for contacts carrying no birthday too, so the
// collection is re-read more often than its content strictly changes -- the
// error a client can absorb, unlike a change it is never told about.
type birthdayCollectionState struct {
	books     int
	ctagSum   int64
	updatedAt time.Time
	// year is the calendar year the collection is generated in. Whether a
	// stored year counts as a birth year depends on it, so it versions the
	// collection too.
	year int
}

// birthdayGeneratorVersion names how events are generated from contacts. A
// change to that moves every resource's body or href without touching an
// address book, so it is part of the collection's version and a client holding
// a token from before resynchronizes once.
const birthdayGeneratorVersion = 3

// tag is the state rendered as one opaque token. updatedAt is carried alongside
// the counts because a book removed and another added can land on the same pair
// of counts, and it cannot land on the same instant.
func (s birthdayCollectionState) tag() string {
	return fmt.Sprintf("g%d-y%d-%d-%d-%d", birthdayGeneratorVersion, s.year, s.books, s.ctagSum, syncTokenNanos(s.updatedAt))
}

func (s birthdayCollectionState) syncToken() string {
	return buildSyncTokenWithState("cal", birthdayCalendarID, s.updatedAt, s.tag())
}

// birthdayCollectionState reads the version of the generated collection. A
// store with no address books behind it leaves the zero state, which is the
// version of a collection that generates nothing.
func (h *DavServer) birthdayCollectionState(ctx context.Context, userID int64) (birthdayCollectionState, error) {
	state := birthdayCollectionState{year: birthdayGenerationYear()}
	if h == nil || h.store == nil || h.store.AddressBooks == nil {
		return state, nil
	}
	books, err := h.store.AddressBooks.ListByUser(ctx, userID)
	if err != nil {
		return birthdayCollectionState{}, err
	}
	state.books = len(books)
	for i := range books {
		state.ctagSum += books[i].CTag
		if books[i].UpdatedAt.After(state.updatedAt) {
			state.updatedAt = books[i].UpdatedAt
		}
	}
	return state, nil
}

func birthdayCalendarCollection(href, principalHref string, state birthdayCollectionState) response {
	description := birthdayCalendarDescription
	return birthdayCalendarCollectionResponse(href, birthdayCalendarName, store.Calendar{Description: &description}, principalHref, state.syncToken(), state.tag())
}

// birthdayCalendarPrivilegeNames is what the generated birthday collection
// grants its owner. It is read-only and has no stored calendar row, so it is
// both the privilege set the collection advertises and the set the privilege
// checks enforce, rather than two lists that can drift apart.
var birthdayCalendarPrivilegeNames = []string{
	"read", "read-free-busy", "read-acl", "read-current-user-privilege-set",
}

func isBirthdayCalendarTarget(target davTarget) bool {
	if !target.Valid || target.Domain != davPathCalendar {
		return false
	}
	calendarID, err := strconv.ParseInt(target.CollectionSegment, 10, 64)
	return err == nil && calendarID == birthdayCalendarID
}

func isBirthdayCalendarPath(ctx context.Context, rawPath string) bool {
	return isBirthdayCalendarTarget(parsedDAVTarget(ctx, rawPath))
}

func isBirthdayCalendarMutation(method string) bool {
	switch method {
	case http.MethodPut, http.MethodDelete, "MKCOL", "MKCALENDAR", "COPY", "MOVE", "LOCK", "UNLOCK", "ACL":
		return true
	default:
		return false
	}
}

func rejectBirthdayCalendarMutation(w http.ResponseWriter, r *http.Request) bool {
	if r == nil || !isBirthdayCalendarMutation(r.Method) || !isBirthdayCalendarPath(r.Context(), r.URL.Path) {
		return false
	}
	http.Error(w, "birthday calendar is read-only", http.StatusForbidden)
	return true
}

// listBoundedBirthdayContacts reads the contacts the generated collection is
// built from, under the same row budget a stored collection is read with. The
// set spans every address book the user owns, so it is the one report read
// whose size is not bounded by a single collection -- and the one no keyset
// column orders cheaply, which is why it is read whole under a cap rather than
// a page at a time. One row past the budget is enough to know the report cannot
// answer over the complete set.
func (h *DavServer) listBoundedBirthdayContacts(ctx context.Context, userID int64) ([]store.Contact, error) {
	rowLimit := h.reportCandidateRowLimit()
	// A budget turned off is carried as math.MaxInt, which has no room for the
	// extra row. Asking for the budget itself then reads one row short of
	// knowing the set is complete, which no collection can reach anyway.
	readLimit := rowLimit
	if readLimit < math.MaxInt {
		readLimit++
	}
	contacts, err := h.store.Contacts.ListWithBirthdaysByUserLimit(ctx, userID, readLimit)
	if err != nil {
		return nil, err
	}
	if len(contacts) > rowLimit {
		return nil, errTooManyCandidateRows
	}
	return contacts, nil
}

func (h *DavServer) generateBirthdayEvents(ctx context.Context, userID int64) ([]store.Event, error) {
	contacts, err := h.listBoundedBirthdayContacts(ctx, userID)
	if err != nil {
		return nil, err
	}

	currentYear := birthdayGenerationYear()
	var events []store.Event

	for _, c := range contacts {
		if c.Birthday == nil {
			continue
		}

		displayName := "Unknown"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}

		// The href is built from the event UID, so X-CALCARD-CONTACT-UID
		// carries the same escaped spelling rather than the stored UID.
		contactUID := icalSafeUID(c.UID)
		uid := birthdayEventUID(c.AddressBookID, c.UID)

		// No age in the summary: the event recurs yearly, so a baked-in
		// "(turning N)" would be wrong every year after the first.
		summary := fmt.Sprintf("🎂 %s's Birthday", displayName)

		dtstart, rrule := birthdayRecurrence(*c.Birthday, currentYear)
		dtstartStr := dtstart.Format("20060102")

		// Build the iCal event with yearly recurrence
		var sb strings.Builder
		writeFoldedContentLine(&sb, "BEGIN:VCALENDAR")
		writeFoldedContentLine(&sb, "VERSION:2.0")
		writeFoldedContentLine(&sb, "PRODID:-//CalCard//Birthdays//EN")
		writeFoldedContentLine(&sb, "BEGIN:VEVENT")
		writeFoldedContentLine(&sb, "UID:"+uid)
		// DTSTAMP derives from the contact so the generated iCal (and its
		// ETag) stays stable across requests; time.Now() here would force
		// clients to re-download every birthday on every sync.
		dtstamp := c.LastModified.UTC()
		if c.LastModified.IsZero() {
			dtstamp = time.Unix(0, 0).UTC()
		}
		writeFoldedContentLine(&sb, "DTSTAMP:"+dtstamp.Format("20060102T150405Z"))
		writeFoldedContentLine(&sb, "DTSTART;VALUE=DATE:"+dtstartStr)
		writeFoldedContentLine(&sb, "SUMMARY:"+escapeICalText(summary))
		writeFoldedContentLine(&sb, "RRULE:"+rrule)
		writeFoldedContentLine(&sb, "TRANSP:TRANSPARENT") // Free/busy: free time
		writeFoldedContentLine(&sb, "CLASS:PUBLIC")

		// RFC 5545 §3.8.8.2: a non-standard property the server defines for its
		// own use carries a vendor id, so it cannot collide with another
		// implementation's property of the same purpose.
		writeFoldedContentLine(&sb, "X-CALCARD-TYPE:BIRTHDAY")
		writeFoldedContentLine(&sb, "X-CALCARD-CONTACT-UID:"+contactUID)

		writeFoldedContentLine(&sb, "END:VEVENT")
		writeFoldedContentLine(&sb, "END:VCALENDAR")

		rawICAL := sb.String()
		etag := fmt.Sprintf("%x", sha256.Sum256([]byte(rawICAL)))

		events = append(events, store.Event{
			ID:           0, // Virtual event, no DB ID
			CalendarID:   birthdayCalendarID,
			UID:          uid,
			RawICAL:      rawICAL,
			ETag:         etag,
			Summary:      &summary,
			DTStart:      &dtstart,
			DTEnd:        nil,
			AllDay:       true,
			LastModified: c.LastModified,
		})
	}

	return events, nil
}

// escapeICalText escapes a string for use as an RFC 5545 §3.3.11 TEXT value. A
// line break becomes the literal \n escape sequence, and every other control
// character is dropped: a raw CR or LF would end the content line the value is
// written into, leaving the remainder to be read as properties -- or whole
// components -- of its own.
func escapeICalText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', ';', ',':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\r':
			b.WriteString("\\n")
			if i+1 < len(s) && s[i+1] == '\n' {
				i++
			}
		case '\n':
			b.WriteString("\\n")
		default:
			if isICalControlOctet(c) {
				continue
			}
			b.WriteByte(c)
		}
	}
	return b.String()
}

// birthdayEventUID names a contact's birthday resource. A contact is identified
// by its UID within one address book, so the book is part of the name.
func birthdayEventUID(addressBookID int64, contactUID string) string {
	return fmt.Sprintf("birthday-%d-%s@calcard", addressBookID, icalSafeUID(contactUID))
}

// birthdayNoYearStartYear anchors the DTSTART of a year-less birthday. It is a
// leap year so February 29 is a valid start.
const birthdayNoYearStartYear = 1972

// birthdayGenerationYear is the year birth years are judged against.
func birthdayGenerationYear() int {
	return time.Now().UTC().Year()
}

// birthYearKnown reports whether a stored year is a real birth year. The store
// keeps a year-less birthday in store.NoYearBirthdayYear, and clients that
// cannot write one use an early stand-in year of their own (Apple writes
// 1604), so only years from 1900 to the current one count.
func birthYearKnown(year, currentYear int) bool {
	return year >= 1900 && year <= currentYear
}

// birthdayRecurrence returns a birthday's DTSTART and RRULE. DTSTART is the
// birth date, or birthdayNoYearStartYear when the year is not a birth year, so
// the body depends only on the contact and the year it is generated in. A
// February 29 birthday recurs on the last day of February, so it falls on
// February 28 in common years.
func birthdayRecurrence(birthday time.Time, currentYear int) (time.Time, string) {
	year := birthday.Year()
	if !birthYearKnown(year, currentYear) {
		year = birthdayNoYearStartYear
	}
	start := time.Date(year, birthday.Month(), birthday.Day(), 0, 0, 0, 0, time.UTC)
	if birthday.Month() == time.February && birthday.Day() == 29 {
		return start, "FREQ=YEARLY;BYMONTH=2;BYMONTHDAY=-1"
	}
	return start, "FREQ=YEARLY"
}

// icalSafeUID percent-encodes the octets of a stored UID that a TEXT value
// cannot carry unescaped: control octets, the TEXT delimiters , ; \ and the %
// introducer itself, so distinct UIDs keep distinct spellings. Other UIDs are
// returned unchanged, so their hrefs stay stable.
func icalSafeUID(uid string) string {
	escapeFrom := -1
	for i := 0; i < len(uid); i++ {
		if needsICalUIDEscape(uid[i]) {
			escapeFrom = i
			break
		}
	}
	if escapeFrom < 0 {
		return uid
	}
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(uid) + 2)
	b.WriteString(uid[:escapeFrom])
	for i := escapeFrom; i < len(uid); i++ {
		c := uid[i]
		if !needsICalUIDEscape(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0F])
	}
	return b.String()
}

func needsICalUIDEscape(c byte) bool {
	switch c {
	case '%', ',', ';', '\\':
		return true
	}
	return isICalControlOctet(c)
}

// isICalControlOctet reports whether an octet may not appear in a content line.
// RFC 5545 §3.1 admits tab and the printable characters; every other C0 octet,
// and DEL, is excluded. The test is safe to apply octet by octet because no
// octet of a multi-octet UTF-8 sequence falls below 0x80.
func isICalControlOctet(c byte) bool {
	return (c < 0x20 && c != '\t') || c == 0x7F
}

// birthdayCalendarReportResponses runs one REPORT against the virtual birthday
// collection. targetResource names a single generated resource when the
// Request-URI is an object resource rather than the collection.
func (h *DavServer) birthdayCalendarReportResponses(ctx context.Context, user *store.User, principalHref, cleanPath, targetResource string, report reportRequest, request *http.Request) ([]response, string, error) {
	switch report.XMLName.Local {
	case "calendar-multiget", "calendar-query", "sync-collection":
	default:
		// RFC 3253 §3.6: unknown report types must be refused, not answered
		// with a full dump of the collection. Refused ahead of the read as
		// well, so an unknown report neither generates the collection nor
		// answers a capacity failure in place of the refusal it is owed.
		return nil, "", errUnsupportedReport
	}
	state, err := h.birthdayCollectionState(ctx, user.ID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read birthday collection state")
	}
	if report.XMLName.Local == "sync-collection" && report.SyncToken != "" {
		return h.birthdayCalendarSyncFromToken(ctx, user, principalHref, cleanPath, state, report)
	}
	events, err := h.generateBirthdayEvents(ctx, user.ID)
	if err != nil {
		if errors.Is(err, errTooManyCandidateRows) {
			// The row budget refuses the read this collection is generated
			// from, which is a capacity failure of the whole report. §7.8 gives
			// calendar-query the DAV:number-of-matches-within-limits
			// postcondition for one; §7.9 and RFC 6578 give the other two
			// reports none, so they keep the RFC 4918 capacity status.
			if report.XMLName.Local == "calendar-query" {
				return nil, "", errNumberOfMatchesExceeded
			}
			return nil, "", err
		}
		return nil, "", fmt.Errorf("failed to generate birthday events")
	}
	// Response hrefs are built from the collection, so an object-resource
	// Request-URI narrows the candidate set instead of moving the base href.
	collectionPath := cleanPath
	if targetResource != "" {
		events = eventsWithResourceName(events, targetResource)
		if len(events) == 0 {
			// RFC 4791 §7: the Request-URI names no resource here, so there is
			// nothing for the report to run against.
			return nil, "", store.ErrNotFound
		}
		collectionPath = birthdayCalendarHref()
	}

	switch report.XMLName.Local {
	case "calendar-multiget":
		res, err := h.birthdayCalendarMultiGet(ctx, user, events, report.Hrefs, collectionPath, targetResource, report.selector, birthdayCalendarDataProjection(report), request)
		return res, "", err
	case "calendar-query":
		if report.Filter != nil {
			// The collection is generated rather than stored, so it defines no
			// CALDAV:calendar-timezone of its own and §7.3 resolves floating
			// values against the request's CALDAV:timezone, else UTC. Its
			// entries are DTSTART;VALUE=DATE, so the zone decides which instants
			// the implied day covers.
			events, err = applyCalendarFilter(events, report.Filter, reportFloatingZone(report.Timezone, nil))
			if err != nil {
				return nil, "", err
			}
		}
		res, err := h.calendarResourceReportResponses(ctx, user, collectionPath, events, report.selector, birthdayCalendarDataProjection(report))
		return res, "", err
	case "sync-collection":
		collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"
		syncToken := state.syncToken()
		projection := birthdayCalendarDataProjection(report)
		responses := []response{
			birthdayCalendarCollection(collectionHref, principalHref, state),
		}
		resourceResponses := rawCalendarResourceReportResponsesLimit(collectionHref, events, projection, h.multistatusBuildLimit()-len(responses))
		responses = h.appendMultistatusResponses(responses, resourceResponses)
		responses, err = h.finishCalendarReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, projection.requested())
		if err != nil {
			return nil, "", err
		}
		return responses, syncToken, nil
	default:
		// Unreachable: the guard above refuses every other report type.
		return nil, "", errUnsupportedReport
	}
}

// birthdayCalendarSyncFromToken answers a DAV:sync-collection that carries a
// client token. The collection is generated per request and keeps no change
// history, so the only token it can answer is one naming the state it is in
// right now: there is then nothing to report. Any older token is refused under
// RFC 6578 §3.2, and the client resynchronizes against the collection whole --
// the only way a contact removed since the token was issued is reported gone.
func (h *DavServer) birthdayCalendarSyncFromToken(ctx context.Context, user *store.User, principalHref, cleanPath string, state birthdayCollectionState, report reportRequest) ([]response, string, error) {
	info, err := parseSyncToken(report.SyncToken)
	if err != nil || info.Kind != "cal" || info.ID != birthdayCalendarID {
		return nil, "", errInvalidSyncToken
	}
	syncToken := state.syncToken()
	if report.SyncToken != syncToken {
		return nil, "", errInvalidSyncToken
	}
	collectionHref := strings.TrimSuffix(cleanPath, "/") + "/"
	responses := []response{birthdayCalendarCollection(collectionHref, principalHref, state)}
	projection := birthdayCalendarDataProjection(report)
	responses, err = h.finishCalendarReportResponses(ctx, user, responses, propertySelector{Prop: report.Prop}, projection.requested())
	if err != nil {
		return nil, "", err
	}
	return responses, syncToken, nil
}

// birthdayCalendarDataProjection is the §9.6 selection for the generated
// collection. It defines no CALDAV:calendar-timezone of its own, so §7.3 leaves
// the request's CALDAV:timezone as the only source ahead of UTC.
func birthdayCalendarDataProjection(report reportRequest) calendarDataProjection {
	return newCalendarDataProjection(reportCalendarData(report), reportFloatingZone(report.Timezone, nil))
}

func (h *DavServer) birthdayCalendarMultiGet(ctx context.Context, user *store.User, events []store.Event, hrefs []string, collectionPath, targetResource string, selector propertySelector, projection calendarDataProjection, request *http.Request) ([]response, error) {
	// §7.9 owes one DAV:response per href here as it does over a stored
	// collection, so an href list past the limit is refused rather than trimmed.
	if len(hrefs) > h.multigetHrefLimit() {
		return nil, errTooManyHrefs
	}

	eventsByUID := make(map[string]store.Event)
	for _, ev := range events {
		eventsByUID[ev.UID] = ev
	}

	var responses []response
	for _, href := range hrefs {
		resolved, ok := h.resolveCalendarHrefForRequest(href, request)
		uid := resolved.ResourceName
		// The birthday collection is addressed only by its constant ID, never by
		// a slug, so no repository lookup can resolve its segment.
		id, idErr := strconv.ParseInt(resolved.Segment, 10, 64)
		// RFC 4791 §7.9: an unresolvable or out-of-scope href still owes the client a DAV:response.
		if !ok || idErr != nil || id != birthdayCalendarID || !multigetHrefInScope(targetResource, uid) {
			responses = append(responses, response{Href: multiGetFallbackHref(href, resolved.Path, collectionPath), Status: httpStatusNotFound})
			continue
		}
		// An in-scope href names a resource this collection spells one way, so
		// the response carries that spelling rather than the client's.
		responseHref := calendarObjectHref(collectionPath, uid)
		ev, found := eventsByUID[uid]
		if !found {
			responses = append(responses, response{Href: responseHref, Status: httpStatusNotFound})
			continue
		}
		responses = append(responses, rawCalendarResourceReportResponse(responseHref, ev, projection))
	}
	return h.finishCalendarReportResponses(ctx, user, responses, selector, projection.requested())
}

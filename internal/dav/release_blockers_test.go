package dav

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestReleaseBlockerCardSyncACLRevocation(t *testing.T) {
	for _, objectOnly := range []bool{false, true} {
		for _, physicalDelete := range []bool{false, true} {
			t.Run(fmt.Sprintf("objectOnly=%t/delete=%t", objectOnly, physicalDelete), func(t *testing.T) {
				db := newDAVPostgresStore(t)
				ctx := t.Context()
				owner, err := db.Users.UpsertOAuthUser(ctx, "owner", "owner@example.test", "", "")
				if err != nil {
					t.Fatal(err)
				}
				guest, err := db.Users.UpsertOAuthUser(ctx, "guest", "guest@example.test", "", "")
				if err != nil {
					t.Fatal(err)
				}
				book, err := db.AddressBooks.Create(ctx, store.AddressBook{UserID: owner.ID, Name: "Review"})
				if err != nil {
					t.Fatal(err)
				}
				root := fmt.Sprintf("/dav/addressbooks/%d", book.ID)
				href := root + "/alice.vcf"
				_, err = db.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: "alice", ResourceName: "alice", RawVCard: buildVCard("3.0", "UID:alice", "FN:Alice"), ETag: "one"})
				if err != nil {
					t.Fatal(err)
				}
				grantPaths := []string{root}
				if objectOnly {
					grantPaths = []string{root + "/alice", root + "/bob"}
					if _, err := db.Contacts.Upsert(ctx, store.Contact{AddressBookID: book.ID, UID: "bob", ResourceName: "bob", RawVCard: buildVCard("3.0", "UID:bob", "FN:Bob"), ETag: "bob"}); err != nil {
						t.Fatal(err)
					}
				}
				for _, grantPath := range grantPaths {
					if err := db.ACLEntries.SetACL(ctx, grantPath, []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: true, Privilege: "read"}}); err != nil {
						t.Fatal(err)
					}
				}
				h := NewDavServer(Options{Store: db})
				report := func(token string) *httptest.ResponseRecorder {
					req := httptest.NewRequest("REPORT", root+"/", strings.NewReader(syncCollectionBody(token)))
					req.Header.Set("Depth", "0")
					req = req.WithContext(auth.WithUser(req.Context(), guest))
					rr := httptest.NewRecorder()
					if objectOnly {
						// Object-only grants cannot REPORT the collection; exercise its sync evaluator directly.
						current, err := db.AddressBooks.GetByID(ctx, book.ID)
						if err != nil {
							t.Fatal(err)
						}
						responses, next, err := h.addressBookSyncCollection(req.Context(), guest, current, acl.PrincipalHref(guest.ID), root, reportRequest{SyncToken: token})
						if err != nil {
							writeReportError(rr, err)
						} else {
							h.writeReportMultiStatus(rr, req, "sync-collection", responses, next)
						}
					} else {
						h.Report(rr, req)
					}
					return rr
				}
				initial := report("")
				if initial.Code != 207 || !strings.Contains(initial.Body.String(), href) {
					t.Fatalf("initial sync: %d %s", initial.Code, initial.Body.String())
				}
				book, err = db.AddressBooks.GetByID(ctx, book.ID)
				if err != nil {
					t.Fatal(err)
				}
				token := buildSyncToken("card", book.ID, book.UpdatedAt)
				if err := db.ACLEntries.SetACL(ctx, root+"/alice", []store.ACLEntry{{PrincipalHref: acl.PrincipalHref(guest.ID), IsGrant: false, Privilege: "read"}}); err != nil {
					t.Fatal(err)
				}
				if physicalDelete {
					contact, err := db.Contacts.GetByResourceName(ctx, book.ID, "alice")
					if err != nil {
						t.Fatal(err)
					}
					if err := db.DeleteContactAndState(ctx, book.ID, store.ContactDAVResourceState(contact), root+"/alice", nil); err != nil {
						t.Fatal(err)
					}
				}
				incremental := report(token)
				if physicalDelete && !objectOnly {
					if incremental.Code != 207 || !strings.Contains(incremental.Body.String(), href) || !strings.Contains(incremental.Body.String(), "404 Not Found") {
						t.Fatalf("missing deletion: %d %s", incremental.Code, incremental.Body.String())
					}
				} else {
					if incremental.Code != 403 || !strings.Contains(incremental.Body.String(), "valid-sync-token") {
						t.Fatalf("revoked contact must invalidate sync token: %d %s", incremental.Code, incremental.Body.String())
					}
					if strings.Contains(incremental.Body.String(), href) {
						t.Fatal("invalid-token response disclosed unreadable contact")
					}
				}

				full := report("")
				if full.Code != 207 || strings.Contains(full.Body.String(), href) {
					t.Fatalf("full reconciliation: %d %s", full.Code, full.Body.String())
				}
				book, err = db.AddressBooks.GetByID(ctx, book.ID)
				if err != nil {
					t.Fatal(err)
				}
				after := report(buildSyncToken("card", book.ID, book.UpdatedAt))
				if after.Code != 207 {
					t.Fatalf("fresh token rejected: %d %s", after.Code, after.Body.String())
				}

			})
		}
	}
}

func TestReleaseBlockerRDatePeriodQueryAndExpansion(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\nBEGIN:VEVENT\r\nUID:period\r\nDTSTAMP:20240601T000000Z\r\nDTSTART:20240601T090000Z\r\nDTEND:20240601T100000Z\r\nRDATE;VALUE=PERIOD:20240605T090000Z/20240605T120000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	if _, fault := validateCalendarObjectForStorage(raw, &store.CalendarAccess{}); fault != nil {
		t.Fatalf("storage validation: %#v", fault)
	}
	event := store.Event{UID: "period", RawICAL: raw}
	if !eventMatchesFilter(event, calQueryWithTimeRange("20240605T110000Z", "20240605T113000Z"), floatingZone{}) {
		t.Error("calendar-query drops the RDATE occurrence while it is still running")
	}
	candidates, rangeErr := filterFreeBusyCandidatesByTimeRange(freeBusyCandidates([]store.Event{event}, floatingZone{}), &timeRange{Start: "20240605T110000Z", End: "20240605T113000Z"})
	if rangeErr != nil {
		t.Fatal(rangeErr)
	}
	if len(candidates) != 1 {
		t.Error("free-busy prefilter drops the still-running RDATE period")
	}

	got, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: time.Date(2024, 6, 5, 0, 0, 0, 0, time.UTC), End: time.Date(2024, 6, 6, 0, 0, 0, 0, time.UTC)}}, floatingZone{}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "DTEND:20240605T120000Z") {
		t.Errorf("expanded RDATE loses its explicit end: %s", got)
	}
}

func TestReleaseBlockerLegacyRecurrenceExpansionLimit(t *testing.T) {
	raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Review//EN\r\nBEGIN:VEVENT\r\nUID:legacy\r\nDTSTAMP:20240601T000000Z\r\nDTSTART:20240601T000000Z\r\nDTEND:20240601T000030Z\r\nRRULE:FREQ=MINUTELY\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	got, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2024, 6, 2, 0, 0, 0, 0, time.UTC)}}, floatingZone{}))
	if !errors.Is(err, ical.ErrRecurrenceExpansionLimit) {
		t.Fatalf("successful legacy expansion returned only %d of 1440 occurrences", strings.Count(got, "BEGIN:VEVENT"))
	}
}
func TestReleaseBlockerXMLPropertyNameStart(t *testing.T) {
	for _, name := range []string{"1bad", "-bad", ".bad", "\u0300bad"} {
		if validXMLNameToken(name) {
			t.Errorf("invalid XML Name accepted: %q", name)
		}
	}
}

func TestReleaseBlockerInvalidExpandPropertyHTTP(t *testing.T) {
	h, user := expandPropertyServer()
	req := httptest.NewRequest("REPORT", "/dav/", strings.NewReader(`<d:expand-property xmlns:d="DAV:"><d:property name="1bad" namespace="urn:review"/></d:expand-property>`))
	req = req.WithContext(auth.WithUser(req.Context(), user))
	rr := httptest.NewRecorder()
	h.Report(rr, req)
	if rr.Code != 400 {
		t.Fatalf("invalid property name response: %d %s", rr.Code, rr.Body.String())
	}
}

func TestRDatePeriodReports(t *testing.T) {
	for _, period := range []string{"20240605T090000Z/20240605T120000Z", "20240605T090000Z/PT3H"} {
		for _, duration := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/duration=%t", period, duration), func(t *testing.T) {
				ending := "DTEND:20240601T100000Z"
				wantEnd := "DTEND:20240605T120000Z"
				if duration {
					ending = "DURATION:PT1H"
					wantEnd = "DURATION:PT10800S"
				}
				raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:period\r\nDTSTAMP:20240601T000000Z\r\nDTSTART:20240601T090000Z\r\n" + ending + "\r\nRDATE;VALUE=PERIOD:" + period + "\r\nBEGIN:VALARM\r\nACTION:DISPLAY\r\nDESCRIPTION:Reminder\r\nTRIGGER;RELATED=END:-PT15M\r\nEND:VALARM\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
				if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
					t.Fatalf("storage: %+v", fault)
				}
				for _, test := range []struct {
					name, body, want string
					status           int
				}{
					{"query", `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:time-range start="20240605T110000Z" end="20240605T113000Z"/></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`, "period.ics", 207},
					{"freebusy", `<C:free-busy-query xmlns:C="urn:ietf:params:xml:ns:caldav"><C:time-range start="20240605T110000Z" end="20240605T113000Z"/></C:free-busy-query>`, "FREEBUSY:20240605T090000Z/20240605T120000Z", 200},
					{"expand", `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><C:calendar-data><C:expand start="20240605T110000Z" end="20240605T113000Z"/></C:calendar-data></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"/></C:comp-filter></C:filter></C:calendar-query>`, wantEnd, 207},
					{"end alarm", `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:comp-filter name="VALARM"><C:time-range start="20240605T114000Z" end="20240605T115000Z"/></C:comp-filter></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`, "period.ics", 207},
					{"end property", `<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav"><D:prop><D:getetag/></D:prop><C:filter><C:comp-filter name="VCALENDAR"><C:comp-filter name="VEVENT"><C:prop-filter name="DTEND"><C:time-range start="20240605T115900Z" end="20240605T120100Z"/></C:prop-filter></C:comp-filter></C:comp-filter></C:filter></C:calendar-query>`, "period.ics", 207},
				} {
					t.Run(test.name, func(t *testing.T) {
						h := hostileRecurrenceServer()
						h.store.Events = &fakeEventRepo{events: map[string]*store.Event{"1:period": {ID: 1, CalendarID: 1, UID: "period", ResourceName: "period", ETag: "e", RawICAL: raw}}}
						req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(test.body))
						req.Header.Set("Depth", "1")
						rr := httptest.NewRecorder()
						h.Report(rr, req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1})))
						if rr.Code != test.status || !strings.Contains(rr.Body.String(), test.want) {
							t.Fatalf("%d %s; want %s", rr.Code, rr.Body.String(), test.want)
						}
					})
				}
			})
		}
	}
}

func TestExpansionOutputBudget(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, count := range []int{1000, 1001} {
		for _, kind := range []string{"rrule", "unbounded", "rdates", "overrides", "aggregate"} {
			t.Run(fmt.Sprintf("%s/%d", kind, count), func(t *testing.T) {
				var components strings.Builder
				base := "BEGIN:VEVENT\r\nUID:legacy\r\nDTSTART:20240601T000000Z\r\nDTEND:20240601T000030Z\r\n"
				switch kind {
				case "rrule":
					components.WriteString(base + fmt.Sprintf("RRULE:FREQ=MINUTELY;COUNT=%d\r\nEND:VEVENT\r\n", count))
				case "unbounded":
					components.WriteString(base + "RRULE:FREQ=MINUTELY\r\nEND:VEVENT\r\n")
				case "rdates":
					components.WriteString(base)
					for i := 1; i < count; i++ {
						components.WriteString("RDATE:" + start.Add(time.Duration(i)*time.Minute).Format("20060102T150405Z") + "\r\n")
					}
					components.WriteString("END:VEVENT\r\n")
				case "overrides", "aggregate":
					for i := 0; i < count; i++ {
						date := start.Add(time.Duration(i) * time.Minute).Format("20060102T150405Z")
						components.WriteString("BEGIN:VEVENT\r\nUID:legacy\r\nDTSTART:" + date + "\r\nDURATION:PT30S\r\n")
						if kind == "overrides" {
							components.WriteString("RECURRENCE-ID:" + date + "\r\n")
						}
						components.WriteString("END:VEVENT\r\n")
					}
				}
				raw := "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + components.String() + "END:VCALENDAR\r\n"
				end := start.Add(24 * time.Hour)
				if kind == "unbounded" {
					end = start.Add(time.Duration(count) * time.Minute)
				}
				got, err := filterICalendarData(raw, newCalendarDataProjection(&calendarDataEl{Expand: &calendarRange{Start: start, End: end}}, floatingZone{}))
				if count > 1000 {
					if !errors.Is(err, ical.ErrRecurrenceExpansionLimit) {
						t.Fatalf("want expansion limit, got %v", err)
					}
				} else if err != nil || strings.Count(got, "BEGIN:VEVENT") != count {
					t.Fatalf("exact budget: count=%d err=%v", strings.Count(got, "BEGIN:VEVENT"), err)
				}
			})
		}
	}
}

func TestExpandPropertySelectionConstructionErrors(t *testing.T) {
	for _, name := range []string{"1bad", "-bad", ".bad", "\u0300bad"} {
		if _, err := propfindQueryFromExpandProperties([]expandPropertyElement{{Name: name, Namespace: "urn:test"}}); err == nil {
			t.Errorf("%q: selection error discarded", name)
		}
	}
	for _, name := range []string{"valid", "_valid", "évalid", "a1-._\u0300"} {
		if !validXMLNameToken(name) {
			t.Errorf("valid name rejected: %s", name)
		}
	}
}

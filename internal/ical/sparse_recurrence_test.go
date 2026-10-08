package ical

import (
	"crypto/sha256"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

// sparseRecurrenceCases are rules the period scan gives up on, which the
// sparse scan follows day by day through the days their date filters admit.
var sparseRecurrenceCases = []struct {
	name, tzid, dtstart, rrule string
}{
	{"hourly at 03:00", "", "20260105T030000", "FREQ=HOURLY;BYHOUR=3"},
	{"hourly at 03:00, New York", "America/New_York", "20260105T030000", "FREQ=HOURLY;BYHOUR=3"},
	{"every 7 minutes on 29 February", "", "20240229T000000", "FREQ=MINUTELY;INTERVAL=7;BYMONTHDAY=29;BYMONTH=2"},
	{"one second on Mondays that are 29 February", "Europe/Berlin", "20240101T010101", "FREQ=SECONDLY;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO;BYHOUR=1;BYMINUTE=1;BYSECOND=1"},
	{"every third day on the 31st", "", "20260101T090000", "FREQ=DAILY;INTERVAL=3;BYMONTHDAY=31"},
	{"every 5 hours on June Mondays, New York", "America/New_York", "20260601T020000", "FREQ=HOURLY;INTERVAL=5;BYDAY=MO;BYMONTH=6"},
	{"counted hourly at 02:30 in March, New York", "America/New_York", "20260301T023000", "FREQ=HOURLY;BYHOUR=2;BYMINUTE=30;BYMONTH=3;COUNT=40"},
}

func sparseRecurrenceObject(tzid, dtstart, rrule string) (string, time.Time) {
	loc := time.UTC
	key := "DTSTART:" + dtstart + "Z"
	if tzid != "" {
		loc, _ = time.LoadLocation(tzid)
		key = "DTSTART;TZID=" + tzid + ":" + dtstart
	}
	wall, _ := ParseDateTime(dtstart)
	start := WallClockIn(loc, wall)
	if tzid == "" {
		start = wall
	}
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:sparse\r\n" + key + "\r\nRRULE:" + rrule + "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n", start
}

func sparseRecurrenceDigest(t testing.TB, tzid, dtstart, rrule string) string {
	raw, start := sparseRecurrenceObject(tzid, dtstart, rrule)
	periods, err := RecurringBusyPeriods(raw, start, time.Hour, start, start.AddDate(60, 0, 0), math.MaxInt32, nil)
	var b strings.Builder
	fmt.Fprintf(&b, "%d %v\n", len(periods), err)
	for _, period := range periods {
		fmt.Fprintf(&b, "%s/%s\n", period.Start.UTC().Format(time.RFC3339), period.End.UTC().Format(time.RFC3339))
	}
	exceeds, ok := RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
	fmt.Fprintf(&b, "%v %v\n", exceeds, ok)
	wall, _ := ParseDateTime(dtstart)
	latest, found, complete := LatestRecurrenceOnOrBefore(wall, wall.AddDate(60, 0, 0), rrule)
	fmt.Fprintf(&b, "%s %v %v\n", latest.Format(time.RFC3339), found, complete)
	return fmt.Sprintf("%d:%x", len(periods), sha256.Sum256([]byte(b.String())))
}

// sparseRecurrenceDigests are what the expansion, the recurrence-limit check
// and the onset search answered for each sparse rule when every one of them
// built the rule's day mask afresh. Building it once per parsed rule changes
// none of it.
var sparseRecurrenceDigests = map[string]string{
	"hourly at 03:00":                            "21915:e3b8acf630459e2dce83a3a097e952b0365a59483a4f6e4dd196d5d7f23d580a",
	"hourly at 03:00, New York":                  "21915:b54444d57b89e9e4274a4d253afa1d875fe53f8ca0a4077cc9164e32b4aa52cf",
	"every 7 minutes on 29 February":             "3086:f42ab09b2d01aeb853f57b206b10f6e657e1143f7e0e20c63d70a83002f4ece7",
	"one second on Mondays that are 29 February": "3:709c21c1af71cfc2ddd2507571746305bcb3e58982a4e7df497285049106faa2",
	"every third day on the 31st":                "136:bfa648df9313b49d3e3108395f766f94e9406f4c4573b4949a2be30056cd6b95",
	"every 5 hours on June Mondays, New York":    "1234:385b2a9a342d59a2ac3e2c93701ba682f67b7f06772e33de352638b187911969",
	"counted hourly at 02:30 in March, New York": "40:4f8772911e91556b071bedd913e322652be9b59e7ef526a0ad51802a58fc985d",
}

func TestSparseRecurrenceAnswersAreUnchanged(t *testing.T) {
	for _, tt := range sparseRecurrenceCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.tzid != "" {
				requireHostZone(t, tt.tzid)
			}
			if got := sparseRecurrenceDigest(t, tt.tzid, tt.dtstart, tt.rrule); got != sparseRecurrenceDigests[tt.name] {
				t.Errorf("digest = %s, want %s", got, sparseRecurrenceDigests[tt.name])
			}
		})
	}
}

// BenchmarkSparseHourlyRecurrence expands a rule the period scan gives up on,
// 03:00 every day written as an hourly rule, over an open-ended range, which
// the sparse scan follows a day at a time.
func BenchmarkSparseHourlyRecurrence(b *testing.B) {
	raw, start := sparseRecurrenceObject("", "20260105T030000", "FREQ=HOURLY;BYHOUR=3")
	b.ReportAllocs()
	for b.Loop() {
		if _, err := RecurringBusyPeriods(raw, start, time.Hour, start, start.AddDate(60, 0, 0), math.MaxInt32, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSparseHourlyRecurrenceLimitCheck is the recurrence-limit check a
// write of the same rule runs.
func BenchmarkSparseHourlyRecurrenceLimitCheck(b *testing.B) {
	raw, _ := sparseRecurrenceObject("", "20260105T030000", "FREQ=HOURLY;BYHOUR=3;UNTIL=20860105T030000Z")
	b.ReportAllocs()
	for b.Loop() {
		RecurrenceSetExceedsLimit(raw, MaxRecurrenceInstances)
	}
}

// A parsed rule's day mask is the one newRecurrenceDayMask builds, built once
// for the rule and its copies, and a UTC day's start is counted in whole days.
func TestRecurrenceDayMaskIsBuiltOncePerRule(t *testing.T) {
	rule, ok := parseRecurrenceRule("FREQ=HOURLY;BYMONTH=2;BYMONTHDAY=29;BYDAY=MO", time.UTC, nil, nil)
	if !ok {
		t.Fatal("rule not parsed")
	}
	copied := rule
	copied.Count = 3
	first, again := rule.days(), copied.days()
	if !slices.Equal(first.words, newRecurrenceDayMask(rule).words) || first.admitsAny != newRecurrenceDayMask(rule).admitsAny {
		t.Error("the cached mask differs from the one built afresh")
	}
	if &first.words[0] != &again.words[0] {
		t.Error("a copy of the parsed rule built its own mask")
	}
	for dayNumber := recurrenceDayMaskOrigin - daysInGregorianCycle; dayNumber < recurrenceDayMaskOrigin+daysInGregorianCycle; dayNumber += 97 {
		if got, want := civilDayStartUnix(dayNumber, time.UTC), civilDayStart(dayNumber, time.UTC).Unix(); got != want {
			t.Fatalf("civilDayStartUnix(%d) = %d, want %d", dayNumber, got, want)
		}
	}
}

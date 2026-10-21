package ical

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// parseDateTimeByLayouts is ParseDateTime reading every value with time.Parse
// alone.
func parseDateTimeByLayouts(s string) (time.Time, bool) {
	s = normalizeLeapSecond(s)
	for _, format := range icalDateTimeFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// The fast reading of the three basic forms answers as time.Parse does for every
// value: the same instant for those it admits, and a refusal for those it
// refuses, including a day a month lacks, 29 February of a year that is no leap
// year, an hour of 24, and the leap second.
func TestParseDateTimeFastPathAnswersAsTimeParseDoes(t *testing.T) {
	var values []string
	for _, year := range []int{0, 1, 1600, 1601, 1900, 2000, 2023, 2024, 2100, 9999} {
		for month := 0; month <= 13; month++ {
			for _, day := range []int{0, 1, 28, 29, 30, 31, 32} {
				values = append(values, fmt.Sprintf("%04d%02d%02d", year, month, day),
					fmt.Sprintf("%04d%02d%02dT235959", year, month, day), fmt.Sprintf("%04d%02d%02dT000000Z", year, month, day))
			}
		}
	}
	for _, clock := range []string{"000000", "235959", "240000", "236000", "006000", "000060", "123456", "126000", "12345", "1234567", "12:3456"} {
		values = append(values, "20240229T"+clock, "20240229T"+clock+"Z", "20230229T"+clock)
	}
	values = append(values, "", "2024", "2024-02-29", "2024-02-29T10:00:00", "20240229t100000", "20240229T100000z", "20240229 100000",
		"20240229T100000+0100", "2024022a", "2024022٣", "-2240229", "20240229T10000Z", "20240229T100000ZZ")
	random := rand.New(rand.NewPCG(1, 2))
	for range 20000 {
		b := []byte("20240229T123456Z")[:[]int{8, 15, 16}[random.IntN(3)]]
		for range random.IntN(3) {
			b[random.IntN(len(b))] = "0123456789TZ:-a"[random.IntN(15)]
		}
		values = append(values, string(b))
	}
	for _, value := range values {
		want, wantOK := parseDateTimeByLayouts(value)
		got, err := ParseDateTime(value)
		if (err == nil) != wantOK || (wantOK && !got.Equal(want)) {
			t.Fatalf("ParseDateTime(%q) = %v, %v; time.Parse reads it as %v, %t", value, got, err, want, wantOK)
		}
	}
}

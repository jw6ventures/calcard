package ical

import (
	"crypto/sha256"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jw6ventures/calcard/internal/lru"
)

const (
	// maxCachedHostLocations caps the names cached as zones, well above the
	// few hundred the zone database holds. A filesystem that matches names
	// case-insensitively loads every spelling of one, and those arrive from
	// clients.
	maxCachedHostLocations = 4096
	// maxRememberedMisses caps the names remembered as ones the host lacks.
	// They are client-controlled TZIDs, Outlook's "Eastern Standard Time"
	// among them, so the least recently asked about gives way to a new one.
	maxRememberedMisses = 1024
	// maxRememberedMissLength is the longest name remembered as missing as
	// written; a longer one is remembered by its digest (missKey), so the
	// cache holds at most maxRememberedMisses times this many octets. No zone
	// database name comes near it.
	maxRememberedMissLength = 128
)

var hostLocations = newHostLocationCache(time.LoadLocation, maxCachedHostLocations, maxRememberedMisses)

// HostLocation returns the host's definition of the zone tzid names. It is
// safe for concurrent use, and a name is read from the zone database once per
// process rather than once per value resolved; a name the database lacks is
// remembered as missing, within bounds, since each probe for one costs a
// failed lookup in every place the time package searches.
//
// The empty name, which time.LoadLocation reads as UTC, and "Local", the time
// package's name for the server's own zone, do not resolve: no calendar object
// can mean either.
func HostLocation(tzid string) (*time.Location, bool) {
	return hostLocations.lookup(tzid)
}

// hostLocationCache holds the zones load returned, and a least-recently-used
// set of the names it failed on.
type hostLocationCache struct {
	load func(string) (*time.Location, error)

	found      sync.Map // TZID -> *time.Location
	foundCount atomic.Int64
	foundLimit int64

	misses *lru.Cache[string, struct{}]
}

func newHostLocationCache(load func(string) (*time.Location, error), foundLimit, missLimit int) *hostLocationCache {
	return &hostLocationCache{
		load:       load,
		foundLimit: int64(foundLimit),
		// The TZID was read out of a calendar object and shares its memory.
		misses: lru.New[string, struct{}](missLimit, nil, strings.Clone),
	}
}

func (c *hostLocationCache) lookup(tzid string) (*time.Location, bool) {
	if tzid == "" || tzid == "Local" {
		return nil, false
	}
	if cached, ok := c.found.Load(tzid); ok {
		return cached.(*time.Location), true
	}
	if c.missed(tzid) {
		return nil, false
	}
	loc, err := c.load(tzid)
	if err != nil {
		c.rememberMiss(tzid)
		return nil, false
	}
	if c.foundCount.Load() >= c.foundLimit {
		return loc, true
	}
	// The TZID was read out of a calendar object and shares its memory.
	cached, loaded := c.found.LoadOrStore(strings.Clone(tzid), loc)
	if !loaded {
		c.foundCount.Add(1)
	}
	return cached.(*time.Location), true
}

func (c *hostLocationCache) missed(tzid string) bool {
	_, ok := c.misses.Get(missKey(tzid))
	return ok
}

func (c *hostLocationCache) rememberMiss(tzid string) {
	if c.misses.Budget() <= 0 {
		return
	}
	c.misses.Add(missKey(tzid), struct{}{})
}

// missKey is the key a name is remembered as missing under: the name itself,
// or, for one longer than maxRememberedMissLength, a NUL -- which no zone
// database name holds -- and its SHA-256 digest.
func missKey(tzid string) string {
	if len(tzid) <= maxRememberedMissLength {
		return tzid
	}
	digest := sha256.Sum256([]byte(tzid))
	return "\x00" + string(digest[:])
}

func (c *hostLocationCache) missCount() int {
	return c.misses.Len()
}

// wallClockReach bounds how far a wall clock can sit from the instant it names:
// no zone's offset, local mean time included, reaches a day.
const wallClockReach = 24 * 60 * 60

// WallClockIn returns the instant, in loc, that a wall-clock reading names
// there. wall carries the reading as the UTC time with the same fields, as
// ParseDateTime returns it; only its fields are read.
//
// RFC 5545 §3.3.5 settles the readings a transition makes ambiguous, which
// time.Date leaves unspecified: a reading the transition skips is read with
// the offset in force before the gap, and one it repeats is its first
// occurrence, which is likewise the offset before the transition.
func WallClockIn(loc *time.Location, wall time.Time) time.Time {
	reading := time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), wall.Second(), 0, time.UTC).Unix()
	offsetAt := func(unix int64) int64 {
		_, offset := time.Unix(unix, 0).In(loc).Zone()
		return int64(offset)
	}
	at := func(offset int64) time.Time {
		return time.Unix(reading-offset, int64(wall.Nanosecond())).In(loc)
	}
	// Every instant the reading can name lies within a day of it, so the
	// offsets either side of that span are the ones it can be read with.
	before, after := offsetAt(reading-wallClockReach), offsetAt(reading+wallClockReach)
	if before == after {
		if offsetAt(reading-before) == before {
			return at(before)
		}
		// More than one transition within the span; time.Date's choice is
		// as good as any rule this function could state for it.
		return time.Date(wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), wall.Second(), wall.Nanosecond(), loc)
	}
	if offsetAt(reading-before) == before || offsetAt(reading-after) != after {
		// The reading exists before the transition -- its first occurrence
		// when it also exists after -- or falls in the gap.
		return at(before)
	}
	return at(after)
}

// WallClockExists reports whether a wall-clock reading names any instant in
// loc. A reading a transition skips names none, though WallClockIn still
// reads it, at the offset before the gap (RFC 5545 §3.3.5): a recurrence rule
// uses this to find which of its candidates may collide with a neighbor at
// the same instant (§3.8.5.3). wall is read as WallClockIn reads it.
func WallClockExists(loc *time.Location, wall time.Time) bool {
	local := WallClockIn(loc, wall)
	return local.Year() == wall.Year() && local.Month() == wall.Month() && local.Day() == wall.Day() &&
		local.Hour() == wall.Hour() && local.Minute() == wall.Minute() && local.Second() == wall.Second()
}

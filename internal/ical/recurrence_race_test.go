//go:build race

package ical

// raceDetectorEnabled reports that the test binary carries the race detector,
// whose per-access cost makes the scaling tests too slow to be worth running.
const raceDetectorEnabled = true

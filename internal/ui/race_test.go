//go:build race

package ui

// raceDetectorEnabled reports that the test binary carries the race detector,
// whose per-access cost makes the scaling tests too slow to be worth running.
const raceDetectorEnabled = true

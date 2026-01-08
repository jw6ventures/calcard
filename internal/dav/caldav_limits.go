package dav

import (
	"fmt"
	"time"
)

const (
	caldavMinDateTime  = "19000101T000000Z"
	caldavMaxDateTime  = "21001231T235959Z"
	caldavMaxInstances = 1000
	caldavMaxAttendees = 100
)

var (
	// Parsed date limits, validated at package initialization
	caldavMinTime time.Time
	caldavMaxTime time.Time
)

func init() {
	var err error
	caldavMinTime, err = parseICalDateTime(caldavMinDateTime)
	if err != nil {
		panic(fmt.Sprintf("invalid caldavMinDateTime constant: %v", err))
	}
	caldavMaxTime, err = parseICalDateTime(caldavMaxDateTime)
	if err != nil {
		panic(fmt.Sprintf("invalid caldavMaxDateTime constant: %v", err))
	}
}

func caldavDateLimits() (time.Time, time.Time) {
	return caldavMinTime, caldavMaxTime
}

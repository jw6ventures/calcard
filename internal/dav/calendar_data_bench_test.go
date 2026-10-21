package dav

import (
	"fmt"
	"testing"
)

// BenchmarkCalendarDataSelection measures the §9.6.1 selection against one
// resource. The selection is applied to every property of every component of
// every resource a report returns, so what this gate watches is the shape of
// the cost rather than its absolute value: it must stay flat as the selector
// count grows, which is what resolving by name buys over scanning.
func BenchmarkCalendarDataSelection(b *testing.B) {
	for _, selectorCount := range []int{10, 1000, 10000} {
		b.Run(fmt.Sprintf("selectors=%d", selectorCount), func(b *testing.B) {
			selectors := make([]calendarProp, 0, selectorCount+1)
			for i := 0; i < selectorCount; i++ {
				selectors = append(selectors, calendarProp{Name: fmt.Sprintf("X-ABSENT-%d", i)})
			}
			selectors = append(selectors, calendarProp{Name: "UID"})
			projection := newCalendarDataProjection(&calendarDataEl{Comp: &calendarComp{
				Name: "VCALENDAR",
				Comp: []calendarComp{{Name: "VEVENT", Prop: selectors}},
			}}, floatingZone{})

			b.ReportAllocs()
			for b.Loop() {
				filterICalendarData(projectionFixture, projection)
			}
		})
	}
}

package store

import (
	"context"
	"errors"
	"testing"
)

type uidChangeLookupRepository struct {
	EventRepository
	existing   Event
	uidLookups int
}

func (r *uidChangeLookupRepository) GetByResourceName(context.Context, int64, string) (*Event, error) {
	existing := r.existing
	return &existing, nil
}

func (r *uidChangeLookupRepository) GetByUID(context.Context, int64, string) (*Event, error) {
	r.uidLookups++
	return nil, errors.New("UID lookup must not run for a forbidden UID change")
}

func TestPutCalendarObjectRejectsUIDChangeBeforeUIDOwnerLookup(t *testing.T) {
	repository := &uidChangeLookupRepository{existing: Event{
		CalendarID:   1,
		UID:          "original-uid",
		ResourceName: "resource",
	}}

	result, err := putCalendarObjectViaRepository(t.Context(), repository, CalendarObjectWrite{
		CalendarID:   1,
		UID:          "replacement-uid",
		ResourceName: "resource",
	})

	if !errors.Is(err, ErrUIDConflict) {
		t.Fatalf("PutCalendarObject() error = %v, want ErrUIDConflict", err)
	}
	if result == nil || result.Conflict == nil || result.Conflict.UID != "original-uid" {
		t.Fatalf("PutCalendarObject() result = %#v, want original resource conflict", result)
	}
	if repository.uidLookups != 0 {
		t.Fatalf("UID owner lookups = %d, want 0", repository.uidLookups)
	}
}

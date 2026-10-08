package calobject

import (
	"errors"
	"net/http"
	"testing"
)

func TestErrorMessageNamesConditionsAndCause(t *testing.T) {
	tests := []struct {
		name string
		err  *Error
		want string
	}{
		{
			name: "conditions only",
			err:  NewError(http.StatusForbidden, []string{"max-instances", "max-date-time"}, nil),
			want: "invalid calendar object: max-instances, max-date-time",
		},
		{
			name: "with cause",
			err:  NewError(http.StatusForbidden, []string{"valid-calendar-object-resource"}, ErrUndefinedTimezone),
			want: "invalid calendar object: valid-calendar-object-resource: " + ErrUndefinedTimezone.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestErrorUnwrapsToItsCause(t *testing.T) {
	var err error = NewError(http.StatusForbidden, []string{"valid-calendar-object-resource"}, ErrUndefinedTimezone)
	if !errors.Is(err, ErrUndefinedTimezone) {
		t.Fatalf("errors.Is(%v, ErrUndefinedTimezone) = false, want true", err)
	}
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Status != http.StatusForbidden {
		t.Fatalf("errors.As(%v) = %+v, want a 403 *Error", err, refusal)
	}

	if bare := NewError(http.StatusForbidden, []string{"valid-calendar-data"}, nil); bare.Unwrap() != nil {
		t.Fatalf("Unwrap() = %v, want nil for a refusal with no cause", bare.Unwrap())
	}
}

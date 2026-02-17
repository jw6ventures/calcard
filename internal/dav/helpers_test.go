package dav

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestCalendarCurrentUserPrivilegeSet_Writable(t *testing.T) {
	privs := calendarCurrentUserPrivilegeSet(false)
	if privs == nil {
		t.Fatal("expected non-nil privilege set")
	}

	data, err := xml.Marshal(privs)
	if err != nil {
		t.Fatalf("failed to marshal privilege set: %v", err)
	}
	output := string(data)

	// Writable calendars must include write privileges
	for _, expected := range []string{"d:write>", "d:write-content>", "d:write-properties>", "d:bind>", "d:unbind>"} {
		if !strings.Contains(output, expected) {
			t.Errorf("writable calendar missing privilege %q in: %s", expected, output)
		}
	}

	// Must also include read privileges
	if !strings.Contains(output, "d:read") {
		t.Error("writable calendar missing read privilege")
	}
}

func TestCalendarCurrentUserPrivilegeSet_ReadOnly(t *testing.T) {
	privs := calendarCurrentUserPrivilegeSet(true)
	if privs == nil {
		t.Fatal("expected non-nil privilege set")
	}

	data, err := xml.Marshal(privs)
	if err != nil {
		t.Fatalf("failed to marshal privilege set: %v", err)
	}
	output := string(data)

	// Read-only calendars must NOT include write privileges
	for _, forbidden := range []string{"d:write>", "d:write-content>", "d:write-properties>", "d:bind>", "d:unbind>"} {
		if strings.Contains(output, forbidden) {
			t.Errorf("read-only calendar should not have privilege %q in: %s", forbidden, output)
		}
	}

	// Must still include read privileges
	if !strings.Contains(output, "d:read") {
		t.Error("read-only calendar missing read privilege")
	}
	if !strings.Contains(output, "read-free-busy") {
		t.Error("read-only calendar missing read-free-busy privilege")
	}
}

func TestCalendarCollectionResponse_WritableHasNoReadOnlyFlag(t *testing.T) {
	resp := calendarCollectionResponse(
		"/dav/calendars/1/", "Test Calendar", nil, nil,
		"/dav/principals/user@example.com/", "sync-token", "1", false,
	)

	data, err := xml.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}
	output := string(data)

	if strings.Contains(output, "read-only") {
		t.Error("writable calendar should not have cs:read-only property")
	}
	if !strings.Contains(output, "d:write>") {
		t.Error("writable calendar should include write privilege")
	}
}

func TestCalendarCollectionResponse_ReadOnlyHasFlag(t *testing.T) {
	resp := calendarCollectionResponse(
		"/dav/calendars/-1/", "Birthdays", nil, nil,
		"/dav/principals/user@example.com/", "sync-token", "0", true,
	)

	data, err := xml.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal response: %v", err)
	}
	output := string(data)

	if !strings.Contains(output, "read-only") {
		t.Error("read-only calendar should have cs:read-only property")
	}
	if strings.Contains(output, "d:write>") {
		t.Error("read-only calendar should not include write privilege")
	}
}

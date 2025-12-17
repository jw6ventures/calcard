package utils

import (
	"strings"
	"testing"
)

func TestBuildVCard(t *testing.T) {
	tests := []struct {
		name        string
		uid         string
		displayName string
		firstName   string
		lastName    string
		email       string
		phone       string
		birthday    string
		notes       string
		company     string
		wantFields  []string
	}{
		{
			name:        "complete vCard",
			uid:         "test-uid@calcard",
			displayName: "John Doe",
			firstName:   "John",
			lastName:    "Doe",
			email:       "john@example.com",
			phone:       "+1234567890",
			birthday:    "1990-01-15",
			notes:       "Test contact",
			company:     "Acme Corp",
			wantFields: []string{
				"BEGIN:VCARD",
				"VERSION:3.0",
				"UID:test-uid@calcard",
				"FN:John Doe",
				"N:Doe;John;;;",
				"ORG:Acme Corp",
				"EMAIL;TYPE=INTERNET:john@example.com",
				"TEL;TYPE=CELL:+1234567890",
				"BDAY:1990-01-15",
				"NOTE:Test contact",
				"REV:",
				"END:VCARD",
			},
		},
		{
			name:        "minimal vCard",
			uid:         "minimal@calcard",
			displayName: "Jane Smith",
			firstName:   "Jane",
			lastName:    "Smith",
			wantFields: []string{
				"BEGIN:VCARD",
				"VERSION:3.0",
				"UID:minimal@calcard",
				"FN:Jane Smith",
				"N:Smith;Jane;;;",
				"END:VCARD",
			},
		},
		{
			name:        "vCard with birthday no year",
			uid:         "noyear@calcard",
			displayName: "No Year",
			firstName:   "No",
			lastName:    "Year",
			birthday:    "--12-25",
			wantFields: []string{
				"BDAY:--12-25",
			},
		},
		{
			name:        "vCard with special characters",
			uid:         "special@calcard",
			displayName: "Test;User,Name",
			firstName:   "Test;First",
			lastName:    "Test,Last",
			notes:       "Line1\nLine2",
			wantFields: []string{
				"FN:Test\\;User\\,Name",
				"N:Test\\,Last;Test\\;First;;;",
				"NOTE:Line1\\nLine2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard := BuildVCard(tt.uid, tt.displayName, tt.firstName, tt.lastName, tt.email, tt.phone, tt.birthday, tt.notes, tt.company)

			for _, field := range tt.wantFields {
				if !strings.Contains(vcard, field) {
					t.Errorf("BuildVCard() missing expected field: %s\nGot:\n%s", field, vcard)
				}
			}

			// Verify it starts and ends correctly
			if !strings.HasPrefix(vcard, "BEGIN:VCARD\r\n") {
				t.Error("BuildVCard() should start with BEGIN:VCARD")
			}
			if !strings.HasSuffix(vcard, "END:VCARD\r\n") {
				t.Error("BuildVCard() should end with END:VCARD")
			}
		})
	}
}

func TestEscapeVCardValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no special characters",
			input: "Simple text",
			want:  "Simple text",
		},
		{
			name:  "backslash",
			input: "Path\\to\\file",
			want:  "Path\\\\to\\\\file",
		},
		{
			name:  "semicolon",
			input: "Last;First",
			want:  "Last\\;First",
		},
		{
			name:  "comma",
			input: "City,State",
			want:  "City\\,State",
		},
		{
			name:  "newline",
			input: "Line1\nLine2",
			want:  "Line1\\nLine2",
		},
		{
			name:  "multiple special chars",
			input: "Complex;Value,With\\Chars\nAnd newlines",
			want:  "Complex\\;Value\\,With\\\\Chars\\nAnd newlines",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EscapeVCardValue(tt.input)
			if got != tt.want {
				t.Errorf("EscapeVCardValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildVCard_EmailAndPhone(t *testing.T) {
	tests := []struct {
		name  string
		email string
		phone string
		want  string
		skip  string
	}{
		{
			name:  "with email",
			email: "test@example.com",
			want:  "EMAIL;TYPE=INTERNET:test@example.com",
		},
		{
			name: "without email",
			skip: "EMAIL",
		},
		{
			name:  "with phone",
			phone: "+1234567890",
			want:  "TEL;TYPE=CELL:+1234567890",
		},
		{
			name: "without phone",
			skip: "TEL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard := BuildVCard("test-uid", "Test User", "Test", "User", tt.email, tt.phone, "", "", "")

			if tt.want != "" && !strings.Contains(vcard, tt.want) {
				t.Errorf("BuildVCard() should contain %q", tt.want)
			}
			if tt.skip != "" && strings.Contains(vcard, tt.skip+":") {
				t.Errorf("BuildVCard() should not contain %q when field is empty", tt.skip)
			}
		})
	}
}

func TestBuildVCard_Birthday(t *testing.T) {
	tests := []struct {
		name     string
		birthday string
		want     string
	}{
		{
			name:     "full date",
			birthday: "1990-01-15",
			want:     "BDAY:1990-01-15",
		},
		{
			name:     "no year format",
			birthday: "--12-25",
			want:     "BDAY:--12-25",
		},
		{
			name:     "invalid date",
			birthday: "invalid-date",
			want:     "",
		},
		{
			name:     "empty birthday",
			birthday: "",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard := BuildVCard("test-uid", "Test User", "Test", "User", "", "", tt.birthday, "", "")

			if tt.want != "" {
				if !strings.Contains(vcard, tt.want) {
					t.Errorf("BuildVCard() should contain %q", tt.want)
				}
			} else if tt.birthday != "" {
				// For invalid dates, BDAY should not be present
				if strings.Contains(vcard, "BDAY:") && tt.birthday != "--12-25" {
					t.Error("BuildVCard() should not include BDAY for invalid date")
				}
			}
		})
	}
}

func TestBuildVCard_Notes(t *testing.T) {
	tests := []struct {
		name  string
		notes string
		want  string
	}{
		{
			name:  "simple notes",
			notes: "Important contact",
			want:  "NOTE:Important contact",
		},
		{
			name:  "notes with newline",
			notes: "Line 1\nLine 2",
			want:  "NOTE:Line 1\\nLine 2",
		},
		{
			name:  "notes with special chars",
			notes: "Text;with,special\\chars",
			want:  "NOTE:Text\\;with\\,special\\\\chars",
		},
		{
			name:  "empty notes",
			notes: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard := BuildVCard("test-uid", "Test User", "Test", "User", "", "", "", tt.notes, "")

			if tt.want != "" {
				if !strings.Contains(vcard, tt.want) {
					t.Errorf("BuildVCard() should contain %q\nGot:\n%s", tt.want, vcard)
				}
			} else {
				if strings.Contains(vcard, "NOTE:") {
					t.Error("BuildVCard() should not include NOTE when notes are empty")
				}
			}
		})
	}
}

func TestBuildVCard_Structure(t *testing.T) {
	vcard := BuildVCard("uid", "Full Name", "First", "Last", "email@test.com", "123", "2000-01-01", "Notes", "Company")

	// Check line endings
	lines := strings.Split(vcard, "\r\n")
	if len(lines) < 5 {
		t.Errorf("BuildVCard() should have multiple lines, got %d", len(lines))
	}

	// Check required fields are present
	requiredFields := []string{
		"BEGIN:VCARD",
		"VERSION:3.0",
		"UID:",
		"FN:",
		"N:",
		"REV:",
		"END:VCARD",
	}

	for _, field := range requiredFields {
		found := false
		for _, line := range lines {
			if strings.HasPrefix(line, field) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("BuildVCard() missing required field: %s", field)
		}
	}
}

func TestBuildVCard_REVTimestamp(t *testing.T) {
	vcard1 := BuildVCard("uid1", "Test", "T", "User", "", "", "", "", "")
	vcard2 := BuildVCard("uid2", "Test", "T", "User", "", "", "", "", "")

	// Both should have REV field
	if !strings.Contains(vcard1, "REV:") {
		t.Error("BuildVCard() should include REV timestamp")
	}
	if !strings.Contains(vcard2, "REV:") {
		t.Error("BuildVCard() should include REV timestamp")
	}

	// Extract REV values
	extractREV := func(vcard string) string {
		for _, line := range strings.Split(vcard, "\r\n") {
			if strings.HasPrefix(line, "REV:") {
				return line
			}
		}
		return ""
	}

	rev1 := extractREV(vcard1)
	_ = extractREV(vcard2) // Both should have REV, just checking format of one

	// REV should be in format YYYYMMDDTHHMMSSZ
	if !strings.HasSuffix(rev1, "Z") {
		t.Error("REV timestamp should end with Z (UTC)")
	}
	if len(rev1) < len("REV:20250101T000000Z") {
		t.Errorf("REV timestamp seems too short: %s", rev1)
	}
}

func TestBuildVCard_Company(t *testing.T) {
	tests := []struct {
		name    string
		company string
		want    string
	}{
		{
			name:    "with company",
			company: "Acme Corp",
			want:    "ORG:Acme Corp",
		},
		{
			name:    "company with special chars",
			company: "Test;Company,Inc\\More",
			want:    "ORG:Test\\;Company\\,Inc\\\\More",
		},
		{
			name:    "empty company",
			company: "",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vcard := BuildVCard("test-uid", "Test User", "Test", "User", "", "", "", "", tt.company)

			if tt.want != "" {
				if !strings.Contains(vcard, tt.want) {
					t.Errorf("BuildVCard() should contain %q\nGot:\n%s", tt.want, vcard)
				}
			} else {
				if strings.Contains(vcard, "ORG:") {
					t.Error("BuildVCard() should not include ORG when company is empty")
				}
			}
		})
	}
}

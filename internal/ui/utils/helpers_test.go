package utils

import (
	"testing"
)

func TestRandomString(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{"length 8", 8},
		{"length 16", 16},
		{"length 32", 32},
		{"length 1", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RandomString(tt.length)

			if len(result) != tt.length {
				t.Errorf("RandomString(%d) returned string of length %d, want %d", tt.length, len(result), tt.length)
			}

			// Check that it only contains valid characters
			validChars := "abcdefghijklmnopqrstuvwxyz0123456789"
			for _, char := range result {
				found := false
				for _, valid := range validChars {
					if char == valid {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("RandomString() contains invalid character: %c", char)
				}
			}
		})
	}
}

func TestRandomString_Uniqueness(t *testing.T) {
	// Generate multiple random strings and check they're different
	length := 16
	strings := make(map[string]bool)
	iterations := 100

	for i := 0; i < iterations; i++ {
		result := RandomString(length)
		if strings[result] {
			t.Errorf("RandomString() generated duplicate: %s", result)
		}
		strings[result] = true
	}

	if len(strings) != iterations {
		t.Errorf("RandomString() generated %d unique strings out of %d attempts", len(strings), iterations)
	}
}

func TestRandomString_EmptyLength(t *testing.T) {
	result := RandomString(0)
	if len(result) != 0 {
		t.Errorf("RandomString(0) should return empty string, got %q", result)
	}
}

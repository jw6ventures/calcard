package store

import (
	"fmt"
	"strings"
)

func NormalizeCalendarColor(value string) (*string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if len(value) != 7 && len(value) != 9 {
		return nil, fmt.Errorf("invalid calendar color")
	}
	if value[0] != '#' {
		return nil, fmt.Errorf("invalid calendar color")
	}
	for _, r := range value[1:] {
		if !isHexDigit(r) {
			return nil, fmt.Errorf("invalid calendar color")
		}
	}
	normalized := strings.ToUpper(value)
	return &normalized, nil
}

func NormalizeCalendarColorOpaque(value string) (*string, error) {
	normalized, err := NormalizeCalendarColor(value)
	if err != nil || normalized == nil {
		return normalized, err
	}
	if len(*normalized) == 7 {
		opaque := *normalized + "FF"
		return &opaque, nil
	}
	return normalized, nil
}

func isHexDigit(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

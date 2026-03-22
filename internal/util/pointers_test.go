package util

import "testing"

func TestStrPtr(t *testing.T) {
	value := "calcard"
	ptr := StrPtr(value)
	if ptr == nil || *ptr != value {
		t.Fatalf("StrPtr() = %#v", ptr)
	}
}

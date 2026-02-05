package dav

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSafeUnmarshalXML_ValidXML(t *testing.T) {
	type TestStruct struct {
		Name  string `xml:"name"`
		Value string `xml:"value"`
	}

	xmlData := []byte(`<TestStruct><name>test</name><value>123</value></TestStruct>`)
	var result TestStruct

	err := safeUnmarshalXML(xmlData, &result)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if result.Name != "test" {
		t.Errorf("expected Name='test', got: %s", result.Name)
	}
	if result.Value != "123" {
		t.Errorf("expected Value='123', got: %s", result.Value)
	}
}

func TestSafeUnmarshalXML_PreventXXE(t *testing.T) {
	// Attempt to define and use an external entity
	xxePayload := []byte(`<?xml version="1.0"?>
<!DOCTYPE test [
  <!ENTITY xxe SYSTEM "file:///etc/passwd">
]>
<test>&xxe;</test>`)

	type TestStruct struct {
		Content string `xml:",chardata"`
	}

	var result TestStruct
	err := safeUnmarshalXML(xxePayload, &result)

	// The safe unmarshaler should fail to process external entities
	// Since we use xml.HTMLEntity, custom entities should not be resolved
	if err == nil {
		// Even if it doesn't error, the entity should not be expanded
		if strings.Contains(result.Content, "root:") || strings.Contains(result.Content, "/etc/passwd") {
			t.Fatal("XXE vulnerability: external entity was expanded")
		}
	}
}

func TestSafeUnmarshalXML_HTMLEntitiesAllowed(t *testing.T) {
	type TestStruct struct {
		Content string `xml:",chardata"`
	}

	// HTML entities should be allowed
	xmlData := []byte(`<TestStruct>&lt;tag&gt;&amp;&quot;</TestStruct>`)
	var result TestStruct

	err := safeUnmarshalXML(xmlData, &result)
	if err != nil {
		t.Fatalf("expected no error for HTML entities, got: %v", err)
	}

	// HTML entities should be decoded
	if !strings.Contains(result.Content, "<") || !strings.Contains(result.Content, ">") {
		t.Errorf("HTML entities not properly decoded: %s", result.Content)
	}
}

func TestSafeUnmarshalXML_MalformedXML(t *testing.T) {
	type TestStruct struct {
		Name string `xml:"name"`
	}

	malformedXML := []byte(`<TestStruct><name>test</TestStruct>`) // Missing closing tag
	var result TestStruct

	err := safeUnmarshalXML(malformedXML, &result)
	if err == nil {
		t.Error("expected error for malformed XML, got nil")
	}
}

func TestReadDAVBodyTooLarge(t *testing.T) {
	overLimit := bytes.Repeat([]byte("a"), int(maxDAVBodyBytes)+1)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(overLimit))
	w := httptest.NewRecorder()

	_, err := readDAVBody(w, req, maxDAVBodyBytes)
	if err == nil {
		t.Fatal("expected error for oversized body, got nil")
	}
	if err != errRequestTooLarge {
		t.Fatalf("expected errRequestTooLarge, got: %v", err)
	}
}

func TestReadDAVBodyChunked(t *testing.T) {
	payload := "chunked-body"
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(payload))
	req.ContentLength = -1
	w := httptest.NewRecorder()

	body, err := readDAVBody(w, req, maxDAVBodyBytes)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if string(body) != payload {
		t.Fatalf("expected %q, got %q", payload, string(body))
	}
}

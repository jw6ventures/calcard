package dav

import (
	"context"
	"encoding/xml"
	"net/http"

	"github.com/jw6ventures/calcard/internal/store"
)

// RequestContext is the stable request data passed to DAV extensions.
type RequestContext struct {
	Context context.Context
	User    *store.User
	Request *http.Request
	Path    string
	Depth   string
	Body    []byte

	ReportName string
}

// PropfindProperties exposes the stable subset of a PROPFIND response property
// set that extensions can decorate.
type PropfindProperties struct {
	href string
	prop *prop
}

func (p *PropfindProperties) Href() string {
	if p == nil {
		return ""
	}
	return p.href
}

func (p *PropfindProperties) DisplayName() string {
	if p == nil || p.prop == nil {
		return ""
	}
	return p.prop.DisplayName
}

func (p *PropfindProperties) SetDisplayName(name string) {
	if p == nil || p.prop == nil {
		return
	}
	p.prop.DisplayName = name
}

func (p *PropfindProperties) SetGetContentType(contentType string) {
	if p == nil || p.prop == nil {
		return
	}
	p.prop.GetContentType = contentType
}

func (p *PropfindProperties) SetXMLProperty(property XMLProperty) {
	if p == nil || p.prop == nil || property.Name.Local == "" {
		return
	}
	p.prop.setCustomXMLProperty(property)
}

// XMLProperty is a typed, namespaced property extensions can add to PROPFIND
// responses.
type XMLProperty struct {
	Name  xml.Name
	Value any
}

// PutValidation describes a PUT request after core DAV request parsing and
// before the default persistence path stores the resource.
type PutValidation struct {
	Context      context.Context
	User         *store.User
	Request      *http.Request
	Path         string
	ResourceType string
	CollectionID int64
	ResourceName string
	ContentType  string
	Body         []byte
	ETag         string
}

const (
	ResourceTypeCalendarObject = "calendar-object"
	ResourceTypeAddressObject  = "address-object"
)

// ResponseError lets extensions reject a request with a specific HTTP status
// and response body.
type ResponseError struct {
	Status int
	Body   string
}

func (e *ResponseError) Error() string {
	if e == nil {
		return ""
	}
	if e.Body != "" {
		return e.Body
	}
	return http.StatusText(e.Status)
}

func writeResponseError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	if responseErr, ok := err.(*ResponseError); ok {
		status := responseErr.Status
		if status == 0 {
			status = http.StatusBadRequest
		}
		body := responseErr.Body
		if body == "" {
			body = http.StatusText(status)
		}
		http.Error(w, body, status)
		return true
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
	return true
}

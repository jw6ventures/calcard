package dav

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

type extensionFunc func(*Registry)

func (f extensionFunc) RegisterDAV(r *Registry) {
	f(r)
}

func TestServerRegistersDefaultDAVModules(t *testing.T) {
	s := NewDavServer(Options{Config: &config.Config{}, Store: &store.Store{}})

	for _, prefix := range []string{"/dav", "/dav/principals", "/dav/calendars", "/dav/addressbooks"} {
		if !s.davRegistry().HasCollection(prefix) {
			t.Fatalf("default collection %q was not registered", prefix)
		}
	}
}

func TestExtensionHandlesAdditiveReportOnDefaultPath(t *testing.T) {
	reportBody := `<C:schedule-query xmlns:C="urn:ietf:params:xml:ns:caldav"/>`
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterReport("/dav/calendars", "schedule-query", func(w http.ResponseWriter, r *http.Request, ctx RequestContext) bool {
				requestBody, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("reading report body: %v", err)
				}
				if string(requestBody) != reportBody {
					t.Fatalf("request body = %q, want %q", string(requestBody), reportBody)
				}
				if string(ctx.Body) != reportBody {
					t.Fatalf("context body = %q, want %q", string(ctx.Body), reportBody)
				}
				if ctx.ReportName != "schedule-query" {
					t.Fatalf("report name = %q", ctx.ReportName)
				}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write([]byte(ctx.Path))
				return true
			})
		})},
	})

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(reportBody))
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Report(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("REPORT status = %d, want %d", rec.Code, http.StatusAccepted)
	}
	if strings.TrimSpace(rec.Body.String()) != "/dav/calendars/1" {
		t.Fatalf("REPORT body = %q", rec.Body.String())
	}
}

func TestExtensionReportDoesNotOverrideCoreReportByDefault(t *testing.T) {
	reportBody := `<C:calendar-query xmlns:C="urn:ietf:params:xml:ns:caldav"/>`
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterReport("/dav/calendars", "calendar-query", func(w http.ResponseWriter, r *http.Request, ctx RequestContext) bool {
				w.WriteHeader(http.StatusAccepted)
				return true
			})
		})},
	})

	req := httptest.NewRequest("REPORT", "/dav/calendars/1/", strings.NewReader(reportBody))
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Report(rec, req)

	if rec.Code == http.StatusAccepted {
		t.Fatalf("core calendar-query was overridden by additive report registration")
	}
}

func TestExtensionAddsPropfindCollection(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollection("/dav/pro")
			r.RegisterPropfindDecorator("/dav/pro", func(ctx RequestContext, props *PropfindProperties) error {
				props.SetDisplayName("Pro DAV")
				props.SetXMLProperty(XMLProperty{
					Name:  xml.Name{Space: "urn:calcard:pro", Local: "tier"},
					Value: "pro",
				})
				return nil
			})
		})},
	})

	req := httptest.NewRequest("PROPFIND", "/dav/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Propfind(rec, req)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want %d: %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/dav/pro/") {
		t.Fatalf("PROPFIND response did not include extension collection: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Pro DAV") {
		t.Fatalf("PROPFIND response did not include decorated property: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `<tier xmlns="urn:calcard:pro">pro</tier>`) {
		t.Fatalf("PROPFIND response did not include custom XML property: %s", rec.Body.String())
	}
}

func TestExtensionPropfindCustomXMLPropertyCanBeRequestedExplicitly(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollection("/dav/pro")
			r.RegisterPropfindDecorator("/dav/pro", func(ctx RequestContext, props *PropfindProperties) error {
				props.SetXMLProperty(XMLProperty{
					Name:  xml.Name{Space: "urn:calcard:pro", Local: "enabled"},
					Value: true,
				})
				return nil
			})
		})},
	})

	body := `<d:propfind xmlns:d="DAV:" xmlns:p="urn:calcard:pro"><d:prop><p:enabled/></d:prop></d:propfind>`
	req := httptest.NewRequest("PROPFIND", "/dav/pro/", strings.NewReader(body))
	req.Header.Set("Depth", "0")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Propfind(rec, req)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want %d: %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `<enabled xmlns="urn:calcard:pro">true</enabled>`) {
		t.Fatalf("PROPFIND response did not include requested custom XML property: %s", rec.Body.String())
	}
}

func TestExtensionRegisteredCollectionHandlesPropfind(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollection("/dav/pro")
			r.RegisterCollection("/dav/pro/reports")
			r.RegisterPropfindDecorator("/dav/pro", func(ctx RequestContext, props *PropfindProperties) error {
				props.SetDisplayName("Pro DAV")
				return nil
			})
		})},
	})

	req := httptest.NewRequest("PROPFIND", "/dav/pro/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Propfind(rec, req)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want %d: %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/dav/pro/") {
		t.Fatalf("PROPFIND response did not include extension collection: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/dav/pro/reports/") {
		t.Fatalf("PROPFIND response did not include extension child collection: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Pro DAV") {
		t.Fatalf("PROPFIND response did not include decorated property: %s", rec.Body.String())
	}
}

func TestCollectionContributorAddsDynamicCollection(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollectionContributor("/dav", func(ctx RequestContext) ([]Collection, error) {
				return []Collection{{Href: "/dav/pro/dynamic/", Name: "Dynamic"}}, nil
			})
		})},
	})

	req := httptest.NewRequest("PROPFIND", "/dav/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Propfind(rec, req)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want %d: %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/dav/pro/dynamic/") {
		t.Fatalf("PROPFIND response did not include contributed collection: %s", rec.Body.String())
	}
}

func TestCollectionContributorOnlyAppliesToExactParent(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store: &store.Store{
			Calendars: &fakeCalendarRepo{},
		},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollectionContributor("/dav", func(ctx RequestContext) ([]Collection, error) {
				if ctx.Request == nil {
					t.Fatalf("expected PROPFIND request in extension context")
				}
				return []Collection{{Href: "/dav/pro/", Name: "Pro"}}, nil
			})
		})},
	})

	req := httptest.NewRequest("PROPFIND", "/dav/calendars/", nil)
	req.Header.Set("Depth", "1")
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Propfind(rec, req)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want %d: %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "/dav/pro/") {
		t.Fatalf("root extension collection leaked into calendar listing: %s", rec.Body.String())
	}
}

func TestMethodHandlerDoesNotOverrideDefaultDAVPath(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterMethod(http.MethodPut, "/dav/calendars", MethodOptions{Auth: MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
	})

	req := httptest.NewRequest(http.MethodPut, "/dav/calendars/2/event.ics", strings.NewReader(validCalendarObject("event")))
	rec := httptest.NewRecorder()

	s.Put(rec, req)

	if rec.Code == http.StatusAccepted {
		t.Fatalf("method handler bypassed default DAV path checks")
	}
}

func TestMethodHandlerCanExtendDefaultDAVPathWithAdditiveMethod(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterMethod(http.MethodPost, "/dav/calendars", MethodOptions{Auth: MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
	})

	req := httptest.NewRequest(http.MethodPost, "/dav/calendars/1/outbox", nil)
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension POST status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
}

func TestMethodHandlerRequiresAuthenticatedRequest(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollection("/dav/pro")
			r.RegisterMethod(http.MethodGet, "/dav/pro", MethodOptions{Auth: MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
	})

	req := httptest.NewRequest(http.MethodGet, "/dav/pro/resource", nil)
	rec := httptest.NewRecorder()

	s.Get(rec, req)

	if rec.Code == http.StatusAccepted {
		t.Fatalf("extension method handler accepted unauthenticated request")
	}
}

func TestMethodHandlerHandlesAuthenticatedExtensionPath(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollection("/dav/pro")
			r.RegisterMethod(http.MethodGet, "/dav/pro", MethodOptions{Auth: MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
	})

	req := httptest.NewRequest(http.MethodGet, "/dav/pro/resource", nil)
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Get(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension method status = %d, want %d", rec.Code, http.StatusAccepted)
	}
}

func TestHeadFallsBackToExtensionGetHandler(t *testing.T) {
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterCollection("/dav/pro")
			r.RegisterMethod(http.MethodGet, "/dav/pro", MethodOptions{Auth: MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
			})
		})},
	})

	req := httptest.NewRequest(http.MethodHead, "/dav/pro/resource", nil)
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Head(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("extension HEAD status = %d, want %d", rec.Code, http.StatusAccepted)
	}
}

func TestExtensionOptionsAuthPolicyIsExplicit(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		s := NewDavServer(Options{
			Config: &config.Config{},
			Store:  &store.Store{},
			Extensions: []Extension{extensionFunc(func(r *Registry) {
				r.RegisterCollection("/dav/pro")
				r.RegisterMethod(http.MethodOptions, "/dav/pro", MethodOptions{Auth: MethodAuthRequired}, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusAccepted)
				})
			})},
		})

		req := httptest.NewRequest(http.MethodOptions, "/dav/pro", nil)
		rec := httptest.NewRecorder()

		s.Options(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("OPTIONS status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
	})

	t.Run("none", func(t *testing.T) {
		s := NewDavServer(Options{
			Config: &config.Config{},
			Store:  &store.Store{},
			Extensions: []Extension{extensionFunc(func(r *Registry) {
				r.RegisterCollection("/dav/pro")
				r.RegisterMethod(http.MethodOptions, "/dav/pro", MethodOptions{Auth: MethodAuthNone}, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusAccepted)
				})
			})},
		})

		req := httptest.NewRequest(http.MethodOptions, "/dav/pro", nil)
		rec := httptest.NewRecorder()

		s.Options(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("OPTIONS status = %d, want %d", rec.Code, http.StatusAccepted)
		}
	})
}

func TestExtensionPutValidatorRejectsCalendarObject(t *testing.T) {
	calRepo := &fakeCalendarRepo{
		accessible: []store.CalendarAccess{
			{Calendar: store.Calendar{ID: 2, UserID: 1, Name: "Work"}, Editor: true},
		},
		calendars: map[int64]*store.Calendar{
			2: {ID: 2, UserID: 1, Name: "Work"},
		},
	}
	eventRepo := &fakeEventRepo{
		events: map[string]*store.Event{
			"2:event": {CalendarID: 2, UID: "event", ResourceName: "event", RawICAL: validCalendarObject("event"), ETag: "old"},
		},
	}
	s := NewDavServer(Options{
		Config: &config.Config{},
		Store:  &store.Store{Calendars: calRepo, Events: eventRepo},
		Extensions: []Extension{extensionFunc(func(r *Registry) {
			r.RegisterPutValidator("/dav/calendars", func(v PutValidation) error {
				if v.ResourceType != ResourceTypeCalendarObject {
					t.Fatalf("resource type = %q", v.ResourceType)
				}
				return &ResponseError{Status: http.StatusForbidden, Body: "blocked by extension"}
			})
		})},
	})

	req := newCalendarPutRequest("/dav/calendars/2/event.ics", strings.NewReader(validCalendarObject("event")))
	req = req.WithContext(auth.WithUser(context.Background(), &store.User{ID: 1}))
	rec := httptest.NewRecorder()

	s.Put(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("PUT status = %d, want %d: %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "blocked by extension") {
		t.Fatalf("PUT body = %q", rec.Body.String())
	}
	if eventRepo.events["2:event"].ETag != "old" {
		t.Fatalf("validator rejection should prevent persistence")
	}
}

func validCalendarObject(uid string) string {
	return "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:" + uid + "\r\nDTSTART:20260101T120000Z\r\nDTEND:20260101T130000Z\r\nSUMMARY:Test\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
}

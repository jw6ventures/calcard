package dav

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
)

func (h *DavServer) proppatch(w http.ResponseWriter, r *http.Request) {
	h.logger().Trace("Proppatch", "PROPPATCH %s", r.URL.Path)
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}

	cleanPath := path.Clean(r.URL.Path)
	if !h.requireLock(w, r, cleanPath, "resource is locked") {
		return
	}

	// Parse PROPPATCH request body
	body, err := readDAVBody(w, r, maxDAVBodyBytes)
	if err != nil {
		if errors.Is(err, errRequestTooLarge) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		} else {
			http.Error(w, "failed to read body", http.StatusBadRequest)
		}
		return
	}

	var proppatchReq proppatchRequest
	if err := safeUnmarshalXML(body, &proppatchReq); err != nil {
		http.Error(w, "invalid PROPPATCH body", http.StatusBadRequest)
		return
	}

	// Process the property updates
	var responses []response

	if strings.HasPrefix(cleanPath, "/dav/calendars/") {
		resp, err := h.proppatchCalendar(r.Context(), user, cleanPath, &proppatchReq)
		if err != nil {
			if errors.Is(err, errInvalidPath) {
				http.Error(w, err.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		responses = append(responses, resp...)
	} else if strings.HasPrefix(cleanPath, "/dav/addressbooks/") {
		resp, err := h.proppatchAddressBook(r.Context(), user, cleanPath, &proppatchReq)
		if err != nil {
			if errors.Is(err, errInvalidPath) {
				http.Error(w, err.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		responses = append(responses, resp...)
	} else {
		http.Error(w, "unsupported path for PROPPATCH", http.StatusBadRequest)
		return
	}

	writeMultiStatus(w, newMultistatus(responses, ""))
}

func (h *DavServer) proppatchCalendar(ctx context.Context, user *store.User, cleanPath string, req *proppatchRequest) ([]response, error) {
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/calendars"), "/")
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("%w: invalid calendar path", errInvalidPath)
	}

	calID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid calendar id", errInvalidPath)
	}
	if calID == birthdayCalendarID {
		return forbiddenProppatchResponses(cleanPath, req), nil
	}

	calAccess, err := h.loadCalendar(ctx, user, calID)
	if err != nil {
		return nil, err
	}
	if err := h.requireCalendarPrivilege(ctx, user, &calAccess.Calendar, cleanPath, "write-properties"); err != nil {
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   prop{},
				Status: httpStatusForbidden,
			}},
		}}, nil
	}

	var name *string
	var description *string
	var timezone *string
	var color *string
	colorChanged := false

	if req.Set != nil {
		name = req.Set.Prop.DisplayName
		description = req.Set.Prop.CalendarDescription
		timezone = req.Set.Prop.CalendarTimezone
		if req.Set.Prop.CalendarColor != nil {
			colorChanged = true
			color, err = store.NormalizeCalendarColor(*req.Set.Prop.CalendarColor)
			if err != nil {
				return []response{{
					Href: cleanPath,
					Propstat: []propstat{{
						Prop:   prop{CalendarColor: req.Set.Prop.CalendarColor},
						Status: httpStatusForbidden,
					}},
				}}, nil
			}
		}
	}
	if req.Remove != nil && req.Remove.Prop.CalendarColor != nil {
		colorChanged = true
		color = nil
	}

	if name != nil || description != nil || timezone != nil || colorChanged {
		// Use existing name if not being updated
		updateName := calAccess.Name
		if name != nil {
			updateName = *name
		}

		updateDescription := description
		if updateDescription == nil {
			updateDescription = calAccess.Description
		}
		updateTimezone := timezone
		if updateTimezone == nil {
			updateTimezone = calAccess.Timezone
		}
		updateColor := color
		if !colorChanged {
			updateColor = calAccess.Color
		}

		err := h.store.Calendars.UpdateProperties(ctx, calID, updateName, updateDescription, updateTimezone, updateColor)
		if err != nil {
			h.logger().Error("Proppatch", "failed to update calendar properties for calendar %d: %v", calID, err)
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   prop{},
					Status: httpStatusInternalServerError,
				}},
			}}, nil
		}
		// A rename changes name/slug-based path resolution.
		invalidateDAVRequestState(ctx)
	}

	// Return success response
	successProp := prop{}
	if name != nil {
		successProp.DisplayName = *name
	}
	if description != nil {
		successProp.CalendarDescription = *description
	}
	if timezone != nil {
		successProp.CalendarTimezone = timezone
	}
	if colorChanged {
		successProp.CalendarColor = color
		if successProp.CalendarColor == nil {
			successProp.CalendarColor = stringPtr("")
		}
	}

	return []response{{
		Href: cleanPath,
		Propstat: []propstat{{
			Prop:   successProp,
			Status: httpStatusOK,
		}},
	}}, nil
}

func forbiddenProppatchResponses(cleanPath string, req *proppatchRequest) []response {
	return []response{{
		Href: cleanPath,
		Propstat: []propstat{{
			PropNames: requestedProppatchPropertyNames(req),
			Status:    httpStatusForbidden,
		}},
	}}
}

func requestedProppatchPropertyNames(req *proppatchRequest) []xml.Name {
	if req == nil {
		return nil
	}
	var names []xml.Name
	seen := make(map[xml.Name]struct{})
	appendName := func(name xml.Name) {
		if name.Local == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	appendProp := func(value proppatchProp) {
		for _, property := range []struct {
			present bool
			name    xml.Name
		}{
			{value.DisplayName != nil, xml.Name{Local: "d:displayname"}},
			{value.ResourceType != nil, xml.Name{Local: "d:resourcetype"}},
			{value.CalendarDescription != nil, xml.Name{Local: "cal:calendar-description"}},
			{value.CalendarTimezone != nil, xml.Name{Local: "cal:calendar-timezone"}},
			{value.CalendarColor != nil, xml.Name{Local: "ical:calendar-color"}},
			{value.AddressBookDesc != nil, xml.Name{Local: "card:addressbook-description"}},
			{value.SupportedAddressData != nil, xml.Name{Local: "card:supported-address-data"}},
			{value.AddressBookMaxResourceSize != nil, xml.Name{Local: "card:max-resource-size"}},
			{value.SupportedCollationSet != nil, xml.Name{Local: "card:supported-collation-set"}},
		} {
			if property.present {
				appendName(property.name)
			}
		}
		for _, name := range value.CustomXML {
			appendName(name)
		}
	}
	if req.Set != nil {
		appendProp(req.Set.Prop)
	}
	if req.Remove != nil {
		appendProp(req.Remove.Prop)
	}
	return names
}

func (h *DavServer) proppatchAddressBook(ctx context.Context, user *store.User, cleanPath string, req *proppatchRequest) ([]response, error) {
	parts := strings.Split(strings.TrimPrefix(cleanPath, "/dav/addressbooks"), "/")
	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("%w: invalid address book path", errInvalidPath)
	}

	bookID, ok, err := h.resolveAddressBookID(ctx, user, strings.TrimSpace(parts[1]))
	if err != nil {
		if errors.Is(err, errAmbiguousAddressBook) {
			return nil, errAmbiguousAddressBook
		}
		return nil, fmt.Errorf("%w: invalid address book id", errInvalidPath)
	}
	if !ok {
		return nil, fmt.Errorf("%w: invalid address book id", errInvalidPath)
	}

	book, err := h.getAddressBook(ctx, bookID)
	if err != nil {
		return nil, err
	}
	if err := h.requireAddressBookPrivilege(ctx, user, book, cleanPath, "write-properties"); err != nil {
		if errors.Is(err, errForbidden) || err == store.ErrNotFound {
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   prop{},
					Status: httpStatusForbidden,
				}},
			}}, nil
		}
		return nil, err
	}

	// Extract properties to update
	var name *string
	var description *string
	var protectedProp prop
	var hasProtected bool

	if req.Set != nil {
		name = req.Set.Prop.DisplayName
		description = req.Set.Prop.AddressBookDesc
		if req.Set.Prop.SupportedAddressData != nil {
			protectedProp.SupportedAddressData = supportedAddressDataProp()
			hasProtected = true
		}
		if req.Set.Prop.AddressBookMaxResourceSize != nil {
			protectedProp.AddressBookMaxResourceSize = strconv.FormatInt(maxDAVBodyBytes, 10)
			hasProtected = true
		}
		if req.Set.Prop.SupportedCollationSet != nil {
			protectedProp.SupportedCollationSet = supportedCollationSetProp()
			hasProtected = true
		}
	}

	successProp := prop{}
	if name != nil {
		successProp.DisplayName = *name
	}
	if description != nil {
		successProp.AddressBookDesc = *description
	}

	if hasProtected {
		failedProp := protectedProp
		if name != nil {
			failedProp.DisplayName = *name
		}
		if description != nil {
			failedProp.AddressBookDesc = *description
		}
		return []response{{
			Href: cleanPath,
			Propstat: []propstat{{
				Prop:   failedProp,
				Status: httpStatusForbidden,
			}},
		}}, nil
	}

	// Update the address book
	if name != nil || description != nil {
		updateName := book.Name
		if name != nil {
			updateName = *name
		}

		err := h.store.AddressBooks.UpdateProperties(ctx, bookID, updateName, description)
		if err != nil {
			status := httpStatusInternalServerError
			if errors.Is(err, store.ErrConflict) {
				status = httpStatusConflict
			}
			h.logger().Error("Proppatch", "failed to update address book properties for book %d: %v", bookID, err)
			return []response{{
				Href: cleanPath,
				Propstat: []propstat{{
					Prop:   successProp,
					Status: status,
				}},
			}}, nil
		}
		// A rename changes name-based path resolution.
		invalidateDAVRequestState(ctx)
	}

	return []response{{
		Href: cleanPath,
		Propstat: []propstat{{
			Prop:   successProp,
			Status: httpStatusOK,
		}},
	}}, nil
}

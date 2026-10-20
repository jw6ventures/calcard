package ui

import (
	"errors"
	"net/http"

	httperrors "github.com/jw6ventures/calcard/internal/http/errors"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// eventFormFlash is the flash text for an event form that the form parsers or
// the event builder refused. It names the field and what is wrong with it in
// words for the person filling the form in; the refusal's own text, which is
// written for logs, goes to the server log.
func eventFormFlash(r *http.Request, err error) string {
	httperrors.LogInfo(r, "event form refused: "+err.Error())

	var property *utils.PropertyError
	switch {
	case errors.Is(err, utils.ErrInvalidStart):
		return "The start date or time is not valid."
	case errors.Is(err, utils.ErrInvalidEnd):
		return "The end date or time is not valid."
	case errors.Is(err, utils.ErrEndBeforeStart):
		return "The end must not be before the start."
	case errors.Is(err, utils.ErrInvalidDayOfMonth):
		return "The days of the month must be numbers from 1 to 31 or -31 to -1, separated by commas."
	case errors.Is(err, utils.ErrInvalidFrequency):
		return "The repeat frequency is not valid."
	case errors.As(err, &property):
		if message, ok := eventPropertyMessages[property.Name]; ok {
			return message
		}
	}
	return "The event could not be saved because some of its details are not valid."
}

// eventPropertyMessages describe a refused property by the field the editor
// shows it in.
var eventPropertyMessages = map[string]string{
	"DTSTART":       "The start date or time is not valid.",
	"DTEND":         "The end date or time is not valid.",
	"RECURRENCE-ID": "The occurrence date is not valid.",
	"STATUS":        "The status is not valid.",
	"CLASS":         "The visibility is not valid.",
	"TRANSP":        "The availability is not valid.",
}

const (
	// eventChangedElsewhereMessage is the flash text for a write whose form was
	// built from a version of the event that is no longer the stored one.
	eventChangedElsewhereMessage = "This event was changed elsewhere since you opened it. Nothing was changed. Reload the page to see the current version, then try again."

	// eventPageOutOfDateMessage is the flash text for a write whose form names
	// no version of the event, as a stale copy of a page does not.
	eventPageOutOfDateMessage = "This page is out of date, so the event was not changed. Reload the page and try again."
)

// The flash texts for a write refused because a CalDAV client holds a lock the
// write would otherwise have to submit the token of.
const (
	eventLockedMessage          = "This event or its calendar is locked by a CalDAV client. Try again once it has been unlocked."
	eventMoveLockedMessage      = "This event, its calendar or the destination calendar is locked by a CalDAV client. Try again once it has been unlocked."
	calendarLockedMessage       = "This calendar is locked by a CalDAV client. Try again once it has been unlocked."
	calendarDeleteLockedMessage = "This calendar or an event in it is locked by a CalDAV client. Try again once it has been unlocked."
)

// eventKeptChangingMessage is the flash text for an event write given up
// because another client kept changing the event or its access under it.
func eventKeptChangingMessage(action string) string {
	return "This event kept changing elsewhere and could not be " + action + ". Reload the page and try again."
}

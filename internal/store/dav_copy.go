package store

import (
	"context"
	"database/sql"
	"errors"
)

func copyEventTx(ctx context.Context, tx queryExecContext, fromCalendarID, toCalendarID int64, uid, destResourceName, newETag string) (*Event, error) {
	const selectQ = `SELECT id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified FROM events WHERE calendar_id=$1 AND uid=$2`
	src, err := scanEvent(tx.QueryRowContext(ctx, selectQ, fromCalendarID, uid).Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if destResourceName == "" {
		destResourceName = src.ResourceName
		if destResourceName == "" {
			destResourceName = src.UID
		}
	}

	const existingDestQ = `SELECT resource_name FROM events WHERE calendar_id=$1 AND uid=$2`
	var existingDestResourceName string
	switch err := tx.QueryRowContext(ctx, existingDestQ, toCalendarID, src.UID).Scan(&existingDestResourceName); {
	case err == nil:
		if existingDestResourceName != "" && existingDestResourceName != destResourceName {
			return nil, ErrConflict
		}
	case errors.Is(err, sql.ErrNoRows):
		existingDestResourceName = ""
	default:
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE calendar_id=$1 AND resource_name=$2 AND uid<>$3`, toCalendarID, destResourceName, src.UID); err != nil {
		return nil, err
	}
	if existingDestResourceName != "" && existingDestResourceName != destResourceName {
		if _, err := tx.ExecContext(ctx, `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('event', $1, $2, $3)`, toCalendarID, src.UID, existingDestResourceName); err != nil {
			return nil, err
		}
	}

	const insertQ = `
INSERT INTO events (calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, recurrence_start, recurrence_until, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW())
ON CONFLICT (calendar_id, uid) DO UPDATE SET
    resource_name=EXCLUDED.resource_name,
    raw_ical=EXCLUDED.raw_ical,
    etag=EXCLUDED.etag,
    summary=EXCLUDED.summary,
    description=EXCLUDED.description,
    location=EXCLUDED.location,
    dtstart=EXCLUDED.dtstart,
    dtend=EXCLUDED.dtend,
    all_day=EXCLUDED.all_day,
    recurrence_start=EXCLUDED.recurrence_start,
    recurrence_until=EXCLUDED.recurrence_until,
    last_modified=NOW()
RETURNING id, calendar_id, uid, resource_name, raw_ical, etag, summary, description, location, dtstart, dtend, all_day, last_modified`
	recurrenceStart, recurrenceUntil := recurrenceBoundsFromICal(src.RawICAL)
	event, err := scanEvent(tx.QueryRowContext(ctx, insertQ,
		toCalendarID, src.UID, destResourceName, src.RawICAL, newETag,
		src.Summary, src.Description, src.Location, src.DTStart, src.DTEnd,
		src.AllDay, recurrenceStart, recurrenceUntil,
	).Scan)
	if err != nil {
		if isEventResourceNameConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	return &event, nil
}

func copyContactTx(ctx context.Context, tx queryExecContext, fromAddressBookID, toAddressBookID int64, uid, destResourceName, newETag string) (*Contact, error) {
	const selectQ = `SELECT id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified FROM contacts WHERE address_book_id=$1 AND uid=$2`
	src, err := scanContact(tx.QueryRowContext(ctx, selectQ, fromAddressBookID, uid).Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if destResourceName == "" {
		destResourceName = src.ResourceName
		if destResourceName == "" {
			destResourceName = src.UID
		}
	}

	const existingDestQ = `SELECT resource_name FROM contacts WHERE address_book_id=$1 AND uid=$2`
	var existingDestResourceName string
	switch err := tx.QueryRowContext(ctx, existingDestQ, toAddressBookID, src.UID).Scan(&existingDestResourceName); {
	case err == nil:
		if existingDestResourceName != "" && existingDestResourceName != destResourceName {
			return nil, ErrConflict
		}
	case errors.Is(err, sql.ErrNoRows):
		existingDestResourceName = ""
	default:
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM contacts WHERE address_book_id=$1 AND resource_name=$2 AND uid<>$3`, toAddressBookID, destResourceName, src.UID); err != nil {
		return nil, err
	}
	if existingDestResourceName != "" && existingDestResourceName != destResourceName {
		if _, err := tx.ExecContext(ctx, `INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name) VALUES ('contact', $1, $2, $3)`, toAddressBookID, src.UID, existingDestResourceName); err != nil {
			return nil, err
		}
	}

	const insertQ = `
INSERT INTO contacts (address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
ON CONFLICT (address_book_id, uid) DO UPDATE SET
    resource_name=EXCLUDED.resource_name,
    raw_vcard=EXCLUDED.raw_vcard,
    etag=EXCLUDED.etag,
    display_name=EXCLUDED.display_name,
    primary_email=EXCLUDED.primary_email,
    birthday=EXCLUDED.birthday,
    last_modified=NOW()
RETURNING id, address_book_id, uid, resource_name, raw_vcard, etag, display_name, primary_email, birthday, last_modified`
	contact, err := scanContact(tx.QueryRowContext(ctx, insertQ,
		toAddressBookID, src.UID, destResourceName, src.RawVCard, newETag,
		src.DisplayName, src.PrimaryEmail, src.Birthday,
	).Scan)
	if err != nil {
		if isContactResourceNameConflict(err) {
			return nil, ErrConflict
		}
		return nil, err
	}
	return &contact, nil
}

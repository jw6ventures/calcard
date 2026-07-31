-- v1.1.10: persist the CALDAV:calendar-description language and the
-- per-collection CALDAV:supported-calendar-component-set.
--
-- description_lang holds the xml:lang attribute RFC 4918 section 4.3 requires a
-- server to return with the property value it was set with. NULL means the
-- description carries no language tag.
--
-- supported_components holds the component names a calendar collection accepts,
-- as set by MKCALENDAR. NULL means the collection imposes no restriction of its
-- own and the server default applies.

ALTER TABLE calendars ADD COLUMN IF NOT EXISTS description_lang TEXT;
ALTER TABLE calendars ADD COLUMN IF NOT EXISTS supported_components TEXT[];

UPDATE application SET value = 'v1.1.10' WHERE key = 'version';

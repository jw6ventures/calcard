-- Track CalDAV resource names separately from UID
ALTER TABLE events ADD COLUMN resource_name TEXT;

UPDATE events
SET resource_name = uid
WHERE resource_name IS NULL;

ALTER TABLE events ALTER COLUMN resource_name SET NOT NULL;
ALTER TABLE events ADD CONSTRAINT events_calendar_resource_name_unique UNIQUE (calendar_id, resource_name);

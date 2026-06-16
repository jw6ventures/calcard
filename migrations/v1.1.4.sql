-- v1.1.4: promote event description and location into columns so the UI and REST API
-- can filter/search on them efficiently without scanning raw iCalendar blobs.

ALTER TABLE events ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS location TEXT;

-- Best-effort backfill from stored iCalendar data. Newly written and synced
-- events populate these columns directly; this fills pre-existing rows so they
-- are searchable without waiting for a re-write. The nested replace() calls
-- mirror unescapeICalValue() so backfilled values match app-written ones.
UPDATE events
SET
    description = NULLIF(
        replace(replace(replace(replace(replace(
            substring(regexp_replace(raw_ical, E'\r?\n[ \t]', '', 'g')
                from E'(?:^|\n)DESCRIPTION[^:\r\n]*:([^\r\n]*)'),
        E'\\n', E'\n'), E'\\N', E'\n'), E'\\,', ','), E'\\;', ';'), E'\\\\', E'\\'),
    ''),
    location = NULLIF(
        replace(replace(replace(replace(replace(
            substring(regexp_replace(raw_ical, E'\r?\n[ \t]', '', 'g')
                from E'(?:^|\n)LOCATION[^:\r\n]*:([^\r\n]*)'),
        E'\\n', E'\n'), E'\\N', E'\n'), E'\\,', ','), E'\\;', ';'), E'\\\\', E'\\'),
    '')
WHERE description IS NULL AND location IS NULL;

UPDATE application SET value = 'v1.1.4' WHERE key = 'version';

-- v1.1.7: precompute recurrence_start/recurrence_until so calendar-query
-- time-range bounds are indexable for recurring events.
--
-- A recurring event stores only its first instance's dtstart/dtend, so a naive
-- "ends at or after range start" bound wrongly drops events whose first instance
-- predates the range but which recur into it. Previously worked around
-- this with a raw text superset that pulled every recurring row regardless of
-- age. This column replaces that with an indexed predicate:
--
--     COALESCE(recurrence_until, dtend, dtstart) >= range_start
--     COALESCE(recurrence_start, dtstart) <= range_end
--
-- recurrence_start holds the first recurring instance start and recurrence_until
-- holds the end of the last recurring instance. Existing recurring rows are
-- backfilled to safe sentinels (1900-01-01 / 9999-12-31) and gain precise values
-- when next rewritten. They are NULL for non-recurring events, so COALESCE falls
-- back to the original dtstart/dtend behavior.

ALTER TABLE events ADD COLUMN IF NOT EXISTS recurrence_start TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN IF NOT EXISTS recurrence_until TIMESTAMPTZ;

UPDATE events
    SET recurrence_start = '1900-01-01T00:00:00Z',
        recurrence_until = '9999-12-31T23:59:59Z'
    WHERE (recurrence_start IS NULL OR recurrence_until IS NULL)
      AND (
          EXISTS (
              SELECT 1
              FROM regexp_matches(events.raw_ical, $re$BEGIN:VEVENT([[:space:][:print:]]*?)END:VEVENT$re$, 'gi') AS component(match)
              WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE)[;:]$re$
          )
          OR EXISTS (
              SELECT 1
              FROM regexp_matches(events.raw_ical, $re$BEGIN:VTODO([[:space:][:print:]]*?)END:VTODO$re$, 'gi') AS component(match)
              WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE)[;:]$re$
          )
          OR EXISTS (
              SELECT 1
              FROM regexp_matches(events.raw_ical, $re$BEGIN:VJOURNAL([[:space:][:print:]]*?)END:VJOURNAL$re$, 'gi') AS component(match)
              WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE)[;:]$re$
          )
	      );

CREATE INDEX IF NOT EXISTS idx_events_recurrence_start
    ON events (calendar_id, COALESCE(recurrence_start, dtstart));

CREATE INDEX IF NOT EXISTS idx_events_recurrence_until
    ON events (calendar_id, COALESCE(recurrence_until, dtend, dtstart));

UPDATE application SET value = 'v1.1.7' WHERE key = 'version';

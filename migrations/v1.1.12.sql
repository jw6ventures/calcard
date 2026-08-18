-- v1.1.12: convert the two time-range expression indexes to the expressions
-- ListForCalendarFiltered now uses, replacing the v1.1.7 pair
--
--     COALESCE(recurrence_until, dtend, dtstart) >= range_start
--     COALESCE(recurrence_start, dtstart)        <= range_end
--
-- An infinity sentinel replaces the NULL each COALESCE could yield, and dtstart
-- leaves the upper-bound expression. Both changes keep a row the columns cannot
-- bound in the candidate set instead of dropping it before the RFC 4791 §9.9
-- test can judge it; the reasoning is on the predicates in
-- internal/store/postgres.go.
--
-- The indexes have to be recreated because an expression index only applies
-- when it matches the predicate verbatim.

DROP INDEX IF EXISTS idx_events_recurrence_start;
DROP INDEX IF EXISTS idx_events_recurrence_until;

CREATE INDEX IF NOT EXISTS idx_events_recurrence_start
    ON events (calendar_id, COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz));

CREATE INDEX IF NOT EXISTS idx_events_recurrence_until
    ON events (calendar_id, COALESCE(recurrence_until, dtend, 'infinity'::timestamptz));

UPDATE application SET value = 'v1.1.12' WHERE key = 'version';

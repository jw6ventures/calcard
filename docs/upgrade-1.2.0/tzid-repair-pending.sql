-- Events the v1.2.0 TZID repair left as stored.
--
-- Lists the id, calendar and resource name of every event still carrying a
-- DTSTART, DTEND, DUE, RECURRENCE-ID, EXDATE or RDATE outside every VTIMEZONE
-- whose TZID parameter, read as the server reads it, is untrimmed or is UTC on
-- a local date-time: the lines the repair reports in its WARNING. DAVx5 may
-- show these events at the device's local time; saving each from a client, or
-- rewriting the line by hand, settles it.
--
-- The repair reads every line the same way; see migrations/v1.2.0.sql.
WITH pattern AS (
    SELECT '^' || space || '+|' || space || '+$' AS trim,
           ';' || space || '*TZID' || space || '*=(?:' || space || '|"' || space || '|"[^"\r\n]*(?:' || space || '"|"' || space || ')|[^";:\r\n]*'
               || space || '[;:]|(?:UTC|"' || space || '*UTC' || space || '*")' || space || '*[;:])' AS candidate,
           ';' || space || '*TZID' || space || '*=' AS tzid
    FROM (VALUES ('[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]')) AS white(space)
)
SELECT events.id, events.calendar_id, events.resource_name
FROM events
CROSS JOIN pattern
WHERE regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') ~* pattern.candidate
  AND EXISTS (
      -- Each logical line, unfolded, and the component a BEGIN or END line
      -- names.
      WITH line AS (
          SELECT split.position, unfolded.line AS unfolded,
                 CASE WHEN unfolded.line ~* '^BEGIN:' THEN 'B' WHEN unfolded.line ~* '^END:' THEN 'E' END AS kind,
                 CASE WHEN unfolded.line ~* '^BEGIN:' THEN upper(regexp_replace(substr(unfolded.line, 7), pattern.trim, '', 'g'))
                      WHEN unfolded.line ~* '^END:' THEN upper(regexp_replace(substr(unfolded.line, 5), pattern.trim, '', 'g'))
                 END AS component
          FROM regexp_split_to_table(events.raw_ical, '(?<=\n)(?![ \t])|(?<=\r)(?![\n \t])')
              WITH ORDINALITY AS split(text, position)
          CROSS JOIN LATERAL (
              SELECT regexp_replace(split.text, E'(?:\\r\\n?|\\n)[ \\t]|(?:\\r\\n|\\r|\\n)$', '', 'g') AS line
          ) AS unfolded
      ),
      -- The VTIMEZONE a line follows is the last one begun at or before it. It
      -- runs to the first END naming it, or naming a component already open
      -- where it began, which closes it with everything opened inside it, as
      -- ical.ReadComponents reads it; an END naming a component that is not open
      -- closes nothing. An END line names a component open where the VTIMEZONE
      -- began exactly when the line naming that component before it is a BEGIN
      -- ahead of the VTIMEZONE: nothing named so has begun or ended since.
      -- This misreads one shape no client writes: a component nested in another
      -- of the same name. After the inner one's END the outer one is still open,
      -- and its END closes a VTIMEZONE begun in between, as the reader has it,
      -- but the line before that END naming the component is the inner END, so
      -- here the VTIMEZONE runs on.
      began AS (
          SELECT line.*,
                 max(line.position) FILTER (WHERE line.kind = 'B' AND line.component = 'VTIMEZONE')
                     OVER (ORDER BY line.position) AS timezone,
                 lag(line.kind) OVER named AS previous_kind,
                 lag(line.position) OVER named AS previous_position
          FROM line
          WINDOW named AS (PARTITION BY line.component ORDER BY line.position)
      ),
      closing AS (
          SELECT began.*,
                 began.kind = 'E' AND began.timezone IS NOT NULL
                     AND (began.component = 'VTIMEZONE'
                          OR (began.previous_kind = 'B' AND began.previous_position < began.timezone)) AS closes
          FROM began
      ),
      scoped AS (
          SELECT closing.position, closing.unfolded,
                 COALESCE(closing.timezone > max(closing.position) FILTER (WHERE closing.closes) OVER (ORDER BY closing.position),
                          closing.timezone IS NOT NULL) AS in_timezone
          FROM closing
      )
      SELECT 1
      FROM scoped
      -- The key runs to the first colon outside quotes, and the TZID is its
      -- first parameter named TZID.
      CROSS JOIN LATERAL (SELECT substring(scoped.unfolded FROM '^(?:[^:"]|"[^"]*")*') AS key) AS key_part
      CROSS JOIN LATERAL (
          SELECT substr(match.parts[1], strpos(match.parts[1], '=') + 1) AS written,
                 regexp_replace(CASE WHEN trimmed.value LIKE '"%"' THEN substr(trimmed.value, 2, length(trimmed.value) - 2)
                                     ELSE trimmed.value END, pattern.trim, '', 'g') AS name
          FROM regexp_matches(key_part.key, ';(?:[^;"]|"[^"]*")*', 'g') WITH ORDINALITY AS match(parts, position)
          CROSS JOIN LATERAL (
              SELECT regexp_replace(substr(match.parts[1], strpos(match.parts[1], '=') + 1), pattern.trim, '', 'g') AS value
          ) AS trimmed
          WHERE strpos(match.parts[1], '=') > 0
            AND upper(regexp_replace(split_part(substr(match.parts[1], 2), '=', 1), pattern.trim, '', 'g')) = 'TZID'
          ORDER BY match.position
          LIMIT 1
      ) AS tzid
      WHERE NOT scoped.in_timezone
        AND scoped.unfolded ~* pattern.tzid
        AND substr(scoped.unfolded, length(key_part.key) + 1, 1) = ':'
        AND upper(regexp_replace(substring(key_part.key FROM '^[^;]*'), pattern.trim, '', 'g'))
            IN ('DTSTART', 'DTEND', 'DUE', 'RECURRENCE-ID', 'EXDATE', 'RDATE')
        AND (tzid.written NOT IN (tzid.name, '"' || tzid.name || '"')
             OR (tzid.name = 'UTC'
                 AND substr(scoped.unfolded, length(key_part.key) + 2) ~ '[0-9]{8}T[0-9]{6}(?![0-9Z])'))
  )
ORDER BY events.id;

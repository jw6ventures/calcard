-- v1.2.0: consolidated upgrade from every v1.1.x release.
-- Apply with the application stopped or during a maintenance window.
--
-- The v1.1.6, v1.1.7 and v1.1.9 releases ship migrations only through
-- v1.1.4.sql yet stamp the database with their own version, and the runner
-- applies only the files above that stamp. The stamp therefore does not say
-- which of the v1.1.6-v1.1.9 schema changes an installation holds, so this file
-- makes every one of them, each idempotently, before anything that reads them.

-- The indexes this upgrade retires go first, so the rewrites below do not
-- rebuild them and the backfills do not maintain them row by row.
-- idx_acl_unique gives way to idx_acl_resource_order, and idx_events_calendar_id
-- and idx_contacts_address_book_id are leading-column prefixes of the keyset
-- indexes built below and serve no lookup those cannot.
DROP INDEX IF EXISTS idx_acl_unique;
DROP INDEX IF EXISTS idx_events_calendar_id;
DROP INDEX IF EXISTS idx_contacts_address_book_id;

-- v1.1.6: normalized, indexable join keys for the object-level ACL checks.
-- resource_path_norm and object_acl_path drop the trailing .ics/.vcf extension
-- so a grant stored with or without it lines up by plain equality. They are
-- STORED generated columns: adding one rewrites its table to fill the existing
-- rows, and every later write keeps it correct with no application changes.
-- idx_events_object_acl_path is built after the recurrence repair below, and
-- idx_acl_principal_grant_norm after the ace_order backfill.

ALTER TABLE acl_entries
    ADD COLUMN IF NOT EXISTS resource_path_norm TEXT
    GENERATED ALWAYS AS (regexp_replace(resource_path, '\.(ics|vcf)$', '', 'i')) STORED;

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS object_acl_path TEXT
    GENERATED ALWAYS AS ('/dav/calendars/' || calendar_id::text || '/' || regexp_replace(resource_name, '\.ics$', '', 'i')) STORED;

-- v1.1.7: recurrence_start and recurrence_until bound a recurring resource's
-- instances so calendar-query time ranges can use an index; both are NULL for a
-- resource that does not recur. The recurrence repair below fills them for the
-- stored rows, and their indexes are built after it.

ALTER TABLE events ADD COLUMN IF NOT EXISTS recurrence_start TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN IF NOT EXISTS recurrence_until TIMESTAMPTZ;

-- v1.1.8: persistent DAV dead properties and scoped ACL lookup indexes.
-- idx_acl_resource_principal is built after the ace_order backfill below.

CREATE TABLE IF NOT EXISTS dav_dead_properties (
    resource_path TEXT NOT NULL,
    namespace_uri TEXT NOT NULL,
    local_name TEXT NOT NULL,
    inner_xml TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (resource_path, namespace_uri, local_name)
);

CREATE INDEX IF NOT EXISTS idx_dav_dead_properties_resource
    ON dav_dead_properties (resource_path);

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS object_acl_path TEXT
    GENERATED ALWAYS AS ('/dav/addressbooks/' || address_book_id::text || '/' || regexp_replace(resource_name, '\.vcf$', '', 'i')) STORED;

CREATE INDEX IF NOT EXISTS idx_contacts_object_acl_path
    ON contacts (object_acl_path);

-- v1.1.9: display names from the OAuth profile.

ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT '';

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

-- v1.1.11: ordered WebDAV ACLs and Digest credentials for newly issued app
-- passwords. The digest columns hold AES-256-GCM ciphertext under a key derived
-- from APP_SESSION_SECRET, not the bare HA1: an HA1 authenticates its holder
-- without the password, so a database read must not yield usable credentials.

-- The backfill runs only on the upgrade that introduces ace_order. An
-- installation already carrying the column carries orderings an ACL request
-- established, and reordering those would discard them. Recording the column's
-- absence before the ALTER is what distinguishes the two; the whole block is
-- one statement, so it cannot commit the column without the backfill. The table
-- is named through regclass so it resolves against search_path exactly as the
-- ALTER below does.
--
-- Denials sort ahead of grants because the evaluation rule changes with this
-- column. Unordered ACLs were read deny-first -- a denial anywhere suppressed
-- every grant -- while an ordered ACL is read first-match (RFC 3744 section
-- 5.5.2). Ordering by age alone would let a DAV:all grant written before a
-- user-specific denial answer first and admit a principal the installation had
-- denied.
--
-- The partition is resource_path_norm, the column the object-level predicates
-- join on, so an ACE stored as /path/x.ics and one stored as /path/x order
-- against each other here exactly as they are evaluated. The resulting
-- ace_order values are not contiguous within a single resource_path, which
-- nothing requires: the ACL method rewrites every position on write, and the
-- reader groups on a change of position rather than on its value.
DO $$
DECLARE
    backfill_needed BOOLEAN;
BEGIN
    SELECT NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'acl_entries'::regclass
          AND attname = 'ace_order'
          AND NOT attisdropped
    ) INTO backfill_needed;

    ALTER TABLE acl_entries ADD COLUMN IF NOT EXISTS ace_order INTEGER NOT NULL DEFAULT 0;

    IF backfill_needed THEN
        WITH ordered AS (
            SELECT id, ROW_NUMBER() OVER (PARTITION BY resource_path_norm ORDER BY is_grant, created_at, id) - 1 AS position
            FROM acl_entries
        )
        UPDATE acl_entries AS entry
        SET ace_order = ordered.position
        FROM ordered
        WHERE entry.id = ordered.id;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_acl_principal_grant_norm
    ON acl_entries (principal_href, is_grant, resource_path_norm);

CREATE INDEX IF NOT EXISTS idx_acl_resource_principal
    ON acl_entries (resource_path, principal_href);

CREATE INDEX IF NOT EXISTS idx_acl_resource_order ON acl_entries(resource_path, ace_order, id);

ALTER TABLE app_passwords ADD COLUMN IF NOT EXISTS digest_md5_ha1 TEXT;
ALTER TABLE app_passwords ADD COLUMN IF NOT EXISTS digest_sha256_ha1 TEXT;

-- Consumed Digest nonce counts. RFC 7616 section 3.4 makes the (nonce, nc) pair
-- single-use, and the nonce this server issues is signed with a key derived
-- from the configured session secret, so it verifies on any instance holding
-- that secret and across a restart. Holding the consumed counts in process
-- memory therefore left captured credentials replayable at any other instance,
-- and at the same one after a restart, for the nonce lifetime. The primary key
-- is the uniqueness constraint: the insert that loses the race changes no row,
-- and that is what identifies a replay.
CREATE TABLE IF NOT EXISTS digest_nonce_counts (
    token_id    BIGINT NOT NULL REFERENCES app_passwords(id) ON DELETE CASCADE,
    nonce       TEXT NOT NULL,
    nonce_count BIGINT NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (token_id, nonce, nonce_count)
);

CREATE INDEX IF NOT EXISTS idx_digest_nonce_counts_expiry
    ON digest_nonce_counts (expires_at);


-- Operator note. Run this with the application stopped or during a maintenance
-- window. The migration runner executes this whole file in one transaction, and
-- every lock below is held until it commits. Every ALTER TABLE takes ACCESS
-- EXCLUSIVE on its table, as does every DROP INDEX or DROP TRIGGER that finds
-- its object; an IF EXISTS drop that finds nothing takes no lock. CREATE INDEX
-- takes SHARE and CREATE TRIGGER SHARE ROW EXCLUSIVE, which block writes but not
-- reads. In practice acl_entries, events, contacts, users, calendars and
-- app_passwords are closed from their first ALTER TABLE, which runs on every
-- upgrade path whether or not it finds anything to add. address_books and
-- deleted_resources are closed to writes from their CREATE TRIGGER, and to
-- reads as well when the file runs again and its DROP TRIGGER finds the
-- trigger. A closed table makes a SELECT wait rather than fall back to another
-- plan. A run that fails part way rolls back entirely and can simply be
-- repeated; the statements are individually idempotent as well.
--
-- The duration is therefore what matters operationally. It is mostly the
-- recurrence repair, plus the index builds and the birthday backfill, and on an
-- installation that lacks the generated ACL join keys, the rewrite of
-- acl_entries, events and contacts that adding them costs. The server does not
-- open its listener until migrations finish, so on a container deployment it
-- is startup time and has to fit the probe budget.
--
-- Check that budget against the table before upgrading. The repair measured
-- about 30 seconds per hundred thousand events when two fifths are recurring and
-- bodies are about 3 KB with a VTIMEZONE, and about 70 seconds with 10 KB
-- bodies; it grows with both. The whole file, run from a v1.1.9 release
-- install, where it also adds the generated columns, measured about 40 seconds
-- per hundred thousand of those 3 KB events. The Helm chart's default startup
-- budget is ten minutes
-- (startupProbe.periodSeconds 10 x failureThreshold 60). A run that overruns it
-- is killed rather than allowed to finish, and because the file is one
-- transaction the kill rolls it back: the container then restarts and begins
-- the same work again, so the deployment does not fail loudly, it never comes
-- up. Above roughly half a million events, or a quarter of a million with large
-- bodies, raise startupProbe.failureThreshold first, or apply the file by hand
-- as below.
--
-- On a deployment that cannot take that pause, run the file's statements by hand
-- outside a transaction, using DROP INDEX CONCURRENTLY and CREATE INDEX
-- CONCURRENTLY for each index statement and checking pg_index.indisvalid
-- afterwards -- concurrent builds can fail and leave an invalid index behind,
-- which then has to be dropped by hand.

-- Every events index that reads recurrence_start or recurrence_until is dropped
-- ahead of the repair and built after it, so building each once over the
-- repaired rows replaces maintaining it per repaired row. idx_events_object_acl_path
-- is built after the repair for the same reason where the upgrade is what adds
-- it. The repair's updates still fire trg_events_touch_last_modified, so they
-- rewrite last_modified and maintain every index that remains.
--
-- The two time-range expression indexes are built on the expressions
-- ListForCalendarFiltered filters on:
--
--     COALESCE(recurrence_until, dtend, 'infinity'::timestamptz)   >= range_start
--     COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz) <= range_end
--
-- The infinity sentinels keep a row the columns cannot bound in the candidate
-- set, where the RFC 4791 §9.9 test can judge it; the reasoning is on the
-- predicates in internal/store/postgres.go. An expression index only applies
-- when it matches the predicate verbatim, so the v1.1.7 pair, built on other
-- expressions, is replaced.
--
-- idx_events_calendar_keyset exists only on a database that ran a v1.2.0
-- release candidate, which built it narrower than it is built below.
DROP INDEX IF EXISTS idx_events_recurrence_start;
DROP INDEX IF EXISTS idx_events_recurrence_until;
DROP INDEX IF EXISTS idx_events_calendar_keyset;

-- Fill the recurrence bounds of every stored resource that recurs and has none.
--
-- A resource recurs when a VEVENT, VTODO or VJOURNAL in it carries an RRULE,
-- an RDATE or a RECURRENCE-ID. A resource made solely of detached instances --
-- RECURRENCE-ID components with neither property anywhere -- is recurring too:
-- ical.ConservativeRecurrenceBounds counts a RECURRENCE-ID, so every write
-- stores bounds for one. A row left with NULL bounds -- written before the
-- columns existed, or a detached-only resource that an RRULE/RDATE-only
-- backfill passed over -- would have the candidate filter COALESCE to its first
-- component's dtstart/dtend and drop the resource before the recurrence matcher
-- can look at the other instances.
--
-- Only NULL bounds are filled, so a precise value written by a later PUT is
-- never replaced and the statement can be run again. The sentinels match
-- ical.RecurrenceStartSentinel and ical.RecurrenceUntilSentinel: deliberately
-- open-ended, so a repaired row is a candidate for every range and the in-memory
-- RFC 4791 Section 9.9 pass makes the decision.
--
-- The body is unfolded before anything reads it, because RFC 5545 section 3.1
-- lets a fold fall anywhere in a content line, including inside a component or
-- property name. A line break is CRLF, bare LF or bare CR, as ical.UnfoldLines
-- reads them. It is unfolded once, as a function scan whose column every test
-- reads: PostgreSQL does not eliminate a repeated subexpression, so naming the
-- unfold inside each test would unfold the whole body again per test.
--
-- The last branch of the CASE decides the row set. The non-greedy component
-- match pairs each BEGIN with the END that closes it, and its cost grows faster
-- than the body length, so two cheaper tests run ahead of it and each admits
-- every row the one after it would. The first is a plain search for the property
-- names. The second is a greedy, linear match from the first BEGIN of a
-- component to the last END of one, which covers every character the non-greedy
-- match could capture; it rejects the common shape the first cannot, a VTIMEZONE
-- whose DST rules are RRULEs around a non-recurring event. A VTIMEZONE between
-- two events lies inside that span, which is why the non-greedy match remains.
-- Both filters test the property name unanchored, so each plainly admits
-- everything the anchored test after it can accept.
--
-- CASE rather than AND'd conditions because it evaluates its branches in order;
-- the planner costs the two regex subplans identically and would otherwise keep
-- them only in the order they happen to be written.
--
-- The component bodies are matched with ".", which without the n flag matches
-- every character, newlines included, under any collation. POSIX bracket
-- classes follow the collation and under C admit ASCII only.
UPDATE events
    SET recurrence_start = COALESCE(recurrence_start, '1900-01-01T00:00:00Z'),
        recurrence_until = COALESCE(recurrence_until, '9999-12-31T23:59:59Z')
    WHERE (recurrence_start IS NULL OR recurrence_until IS NULL)
      AND EXISTS (
          SELECT 1
          FROM regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') AS unfolded(body)
          WHERE CASE
              WHEN unfolded.body !~* $re$(RRULE|RDATE|RECURRENCE-ID)[;:]$re$ THEN FALSE
              WHEN NOT (
                  EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:VEVENT(.*)END:VEVENT$re$, 'gi') AS span(match)
                      WHERE span.match[1] ~* $re$(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:VTODO(.*)END:VTODO$re$, 'gi') AS span(match)
                      WHERE span.match[1] ~* $re$(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:VJOURNAL(.*)END:VJOURNAL$re$, 'gi') AS span(match)
                      WHERE span.match[1] ~* $re$(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
                  )
              ) THEN FALSE
              ELSE
                  EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:VEVENT(.*?)END:VEVENT$re$, 'gi') AS component(match)
                      WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:VTODO(.*?)END:VTODO$re$, 'gi') AS component(match)
                      WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:VJOURNAL(.*?)END:VJOURNAL$re$, 'gi') AS component(match)
                      WHERE component.match[1] ~* $re$(^|\r|\n)(RRULE|RDATE|RECURRENCE-ID)[;:]$re$
                  )
          END
      );

CREATE INDEX IF NOT EXISTS idx_events_recurrence_start
    ON events (calendar_id, COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz));

CREATE INDEX IF NOT EXISTS idx_events_recurrence_until
    ON events (calendar_id, COALESCE(recurrence_until, dtend, 'infinity'::timestamptz));

CREATE INDEX IF NOT EXISTS idx_events_object_acl_path
    ON events (object_acl_path);

-- Keyset indexes for the reads the DAV reports page a collection with:
--
--     WHERE <collection>=$1 AND (<collection>, id) > ($1, $2) [AND <filter>]
--     ORDER BY <collection>, id LIMIT $n
--
-- The cursor is a row comparison because the primary key cannot use one as an
-- index bound. Spelled "id > $2 ORDER BY id", the primary key answers the
-- ORDER BY too, and the planner prices that scan assuming the collection's rows
-- are spread evenly through the id range; for a collection holding a large share
-- of the table -- a bulk import, or any collection created after the table had
-- grown -- it walks and discards most of the table per page. The collection
-- equality stays beside the row comparison and is not redundant: the comparison
-- alone also admits every row of every collection whose id is higher.
--
-- The trailing columns are the predicates those reads narrow on, so a page
-- filters on the index tuple rather than visiting the heap for rows it discards.
--
-- deleted_resources keeps the plain id cursor: its lookup index leads on
-- deleted_at, which is the more selective bound for every sync that reads it.
CREATE INDEX IF NOT EXISTS idx_events_calendar_keyset ON events (
    calendar_id, id, last_modified,
    COALESCE(recurrence_until, dtend, 'infinity'::timestamptz),
    COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz));

-- A v1.2.0 release candidate built this as (address_book_id, id) only.
DROP INDEX IF EXISTS idx_contacts_book_keyset;
CREATE INDEX IF NOT EXISTS idx_contacts_book_keyset ON contacts (address_book_id, id, last_modified);

CREATE INDEX IF NOT EXISTS idx_deleted_resources_keyset
    ON deleted_resources (resource_type, collection_id, id);

-- Commit-ordered sync stamps.
--
-- A sync token is its collection's updated_at, and sync-collection reports a
-- member whose last_modified, or a tombstone whose deleted_at, is later than the
-- token. That only holds while the stamps order the way the writes commit.
-- NOW() is the time the transaction started, so a writer that started before
-- another but committed after it stamped its change behind a token the other's
-- commit had already let a client take, and the change was never reported.
--
-- Every member write locks its collection's row before it writes, so a stamp
-- read from clock_timestamp() inside the write, and kept above the collection's
-- current updated_at, is later than every token handed out before the write
-- can commit. The collection's own updated_at only moves forward, by at least a
-- microsecond per change, so two changes never share a token.
CREATE OR REPLACE FUNCTION touch_last_modified()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_TABLE_NAME = 'events' THEN
        NEW.last_modified = GREATEST(clock_timestamp(),
            (SELECT updated_at FROM calendars WHERE id = NEW.calendar_id) + interval '1 microsecond');
    ELSE
        NEW.last_modified = GREATEST(clock_timestamp(),
            (SELECT updated_at FROM address_books WHERE id = NEW.address_book_id) + interval '1 microsecond');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_events_touch_last_modified ON events;
CREATE TRIGGER trg_events_touch_last_modified
BEFORE INSERT OR UPDATE ON events
FOR EACH ROW EXECUTE FUNCTION touch_last_modified();

DROP TRIGGER IF EXISTS trg_contacts_touch_last_modified ON contacts;
CREATE TRIGGER trg_contacts_touch_last_modified
BEFORE INSERT OR UPDATE ON contacts
FOR EACH ROW EXECUTE FUNCTION touch_last_modified();

-- Any change to a collection row moves its sync token, whatever updated_at the
-- statement itself wrote.
CREATE OR REPLACE FUNCTION stamp_collection_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = GREATEST(clock_timestamp(), OLD.updated_at + interval '1 microsecond');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_calendars_stamp_updated_at ON calendars;
CREATE TRIGGER trg_calendars_stamp_updated_at
BEFORE UPDATE ON calendars
FOR EACH ROW EXECUTE FUNCTION stamp_collection_updated_at();

DROP TRIGGER IF EXISTS trg_address_books_stamp_updated_at ON address_books;
CREATE TRIGGER trg_address_books_stamp_updated_at
BEFORE UPDATE ON address_books
FOR EACH ROW EXECUTE FUNCTION stamp_collection_updated_at();

CREATE OR REPLACE FUNCTION increment_calendar_ctag()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE calendars SET ctag = ctag + 1 WHERE id = OLD.calendar_id;
        INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name)
        VALUES ('event', OLD.calendar_id, OLD.uid, OLD.resource_name);
        RETURN OLD;
    ELSE
        UPDATE calendars SET ctag = ctag + 1 WHERE id = NEW.calendar_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION increment_address_book_ctag()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE address_books SET ctag = ctag + 1 WHERE id = OLD.address_book_id;
        INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name)
        VALUES ('contact', OLD.address_book_id, OLD.uid, OLD.resource_name);
        RETURN OLD;
    ELSE
        UPDATE address_books SET ctag = ctag + 1 WHERE id = NEW.address_book_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- A tombstone moves its collection's sync token and takes the new token as its
-- own stamp: a client holding an older token is told of the removal, and one
-- holding the new token already has a state without the resource. The
-- collection may already be gone when its members are removed with it.
CREATE OR REPLACE FUNCTION stamp_deleted_resource()
RETURNS TRIGGER AS $$
DECLARE
    stamped TIMESTAMPTZ;
BEGIN
    IF NEW.resource_type = 'event' THEN
        UPDATE calendars SET updated_at = clock_timestamp() WHERE id = NEW.collection_id
        RETURNING updated_at INTO stamped;
    ELSIF NEW.resource_type = 'contact' THEN
        UPDATE address_books SET updated_at = clock_timestamp() WHERE id = NEW.collection_id
        RETURNING updated_at INTO stamped;
    END IF;
    NEW.deleted_at = COALESCE(stamped, clock_timestamp());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_deleted_resources_stamp ON deleted_resources;
CREATE TRIGGER trg_deleted_resources_stamp
BEFORE INSERT ON deleted_resources
FOR EACH ROW EXECUTE FUNCTION stamp_deleted_resource();

-- Re-derive the birthdays earlier releases stored in the wrong year.
--
-- A year-less BDAY (--MM-DD) was stored in year 1, which is not a leap year, so
-- --02-29 became March 1. An Apple card, which cannot write a year-less date,
-- writes a stand-in year and names it in X-APPLE-OMIT-YEAR, and that stand-in
-- was stored as though it were the birth year. Both belong in year 4
-- (store.NoYearBirthdayYear).
--
-- The birthday is read back from the card the way store.parseVCardFields reads
-- it, so the row ends up as a write of the same card would leave it: content
-- lines unfolded across CRLF, LF or CR breaks; the first BDAY line decides,
-- whatever its group; a VALUE=text BDAY is no date; a date-time keeps its date;
-- the date is one of YYYY-MM-DD, YYYYMMDD, --MM-DD or --MMDD and must exist,
-- a year-less February 29 included; and a year equal to the first numeric
-- X-APPLE-OMIT-YEAR value is no year. A card whose first BDAY does not parse
-- keeps the birthday it has.
--
-- Only rows still in year 1, and rows whose year is the card's omitted year,
-- are touched, and only when the derived date differs, so a birthday genuinely
-- dated in year 1 and a row already moved stay as they are and the statement
-- can run again. Each update fires the contacts triggers, which move the
-- address book's CTag and sync token and stamp the contact after the old token,
-- so clients pick up the corrected birthday.
--
-- Each card is unfolded, split and matched once, in LATERAL function scans, and
-- only the first BDAY line is parsed further. OFFSET 0 keeps the derived steps
-- from being flattened into the expressions that read them, which would
-- evaluate each regex again for every reference.
UPDATE contacts AS contact
SET birthday = derived.birthday
FROM (
    SELECT candidate.id, parsed.birthday, bday.omitted_year
    FROM contacts AS candidate
    CROSS JOIN LATERAL regexp_replace(candidate.raw_vcard, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') AS unfolded(body)
    CROSS JOIN LATERAL (
        SELECT property.parts[1] AS params, btrim(property.parts[2], E' \t\013\f') AS value
        FROM regexp_split_to_table(unfolded.body, E'\\r\\n?|\\n') WITH ORDINALITY AS line(text, position)
        CROSS JOIN LATERAL regexp_match(line.text,
            $re$^[ \t]*(?:[^;:"]*\.)?BDAY[ \t]*((?:;(?:[^";:]|"[^"]*")*)*):(.*)$re$, 'i') AS property(parts)
        WHERE property.parts IS NOT NULL
        ORDER BY line.position
        LIMIT 1
    ) AS first_bday
    CROSS JOIN LATERAL regexp_match(first_bday.value,
        $re$^(?:([0-9]{4})-([0-9]{2})-([0-9]{2})|([0-9]{4})([0-9]{2})([0-9]{2})|--([0-9]{2})-([0-9]{2})|--([0-9]{2})([0-9]{2}))(?:[Tt][0-9]{2}[0-9:.,Zz+-]*)?$$re$) AS date(parts)
    CROSS JOIN LATERAL (
        SELECT EXISTS (
                   SELECT 1
                   FROM regexp_matches(first_bday.params, $re$;[ \t]*VALUE[ \t]*=((?:[^";]|"[^"]*")*)$re$, 'gi') AS param(match)
                   CROSS JOIN LATERAL regexp_split_to_table(param.match[1], ',') AS item(value)
                   WHERE lower(btrim(btrim(item.value, E' \t'), '"')) = 'text'
               ) AS is_text,
               (
                   SELECT btrim(btrim(item.value, E' \t'), '"')::numeric
                   FROM regexp_matches(first_bday.params, $re$;[ \t]*X-APPLE-OMIT-YEAR[ \t]*=((?:[^";]|"[^"]*")*)$re$, 'gi') WITH ORDINALITY AS param(match, position)
                   CROSS JOIN LATERAL regexp_split_to_table(param.match[1], ',') WITH ORDINALITY AS item(value, position)
                   WHERE btrim(btrim(item.value, E' \t'), '"') ~ '^[0-9]+$'
                   ORDER BY param.position, item.position
                   LIMIT 1
               ) AS omitted_year
        OFFSET 0
    ) AS bday
    CROSS JOIN LATERAL (
        SELECT COALESCE(date.parts[1], date.parts[4])::int AS year,
               COALESCE(date.parts[2], date.parts[5], date.parts[7], date.parts[9])::int AS month,
               COALESCE(date.parts[3], date.parts[6], date.parts[8], date.parts[10])::int AS day
        OFFSET 0
    ) AS fields
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN date.parts IS NULL OR bday.is_text THEN NULL
            WHEN fields.month NOT BETWEEN 1 AND 12 OR fields.day < 1 THEN NULL
            -- Validated in its own year, or in a leap year when it has none
            -- (year 0, which PostgreSQL cannot store, is one as well).
            WHEN EXTRACT(MONTH FROM make_date(CASE WHEN COALESCE(fields.year, 0) = 0 THEN 2000 ELSE fields.year END, fields.month, 1)
                                    + (fields.day - 1)) <> fields.month THEN NULL
            WHEN fields.year IS NULL OR fields.year = bday.omitted_year THEN make_date(4, fields.month, fields.day)
            WHEN fields.year = 0 THEN NULL
            ELSE make_date(fields.year, fields.month, fields.day)
        END AS birthday
        OFFSET 0
    ) AS parsed
    WHERE candidate.birthday IS NOT NULL
      AND ((candidate.birthday >= DATE '0001-01-01' AND candidate.birthday < DATE '0002-01-01')
           OR unfolded.body ~* 'X-APPLE-OMIT-YEAR')
) AS derived
WHERE contact.id = derived.id
  AND derived.birthday IS NOT NULL
  AND derived.birthday <> contact.birthday
  AND ((contact.birthday >= DATE '0001-01-01' AND contact.birthday < DATE '0002-01-01')
       OR EXTRACT(YEAR FROM contact.birthday) = derived.omitted_year);

UPDATE application SET value = 'v1.2.0' WHERE key = 'version';

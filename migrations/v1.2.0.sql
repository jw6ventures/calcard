-- v1.2.0: consolidated upgrade from v1.1.9.
-- Apply with the application stopped or during a maintenance window.

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

DROP INDEX IF EXISTS idx_acl_unique;
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

-- v1.2.0-rc7: index the keyset reads the DAV reports page a collection with.
--
-- Every report that scans a collection reads it as
--
--     WHERE <collection>=$1 AND id>$2 [AND <timestamp> > $3] ORDER BY id LIMIT $4
--
-- and no index ordered a single collection by id. The planner answered that
-- with the primary key, walking the whole table from afterID and discarding
-- every row belonging to another collection: for a collection whose rows sit in
-- one contiguous id range -- a bulk import, or any collection created after the
-- table had grown -- one 256-row page of a 220,000-row table discarded 200,000
-- rows and cost 21 ms. With these indexes the same page is an index scan of
-- about 0.09 ms.
CREATE INDEX IF NOT EXISTS idx_events_calendar_keyset
    ON events (calendar_id, id);

CREATE INDEX IF NOT EXISTS idx_contacts_book_keyset
    ON contacts (address_book_id, id);

CREATE INDEX IF NOT EXISTS idx_deleted_resources_keyset
    ON deleted_resources (resource_type, collection_id, id);

-- Both are leading-column prefixes of the keyset indexes above, so they no
-- longer serve a lookup the new indexes cannot.
DROP INDEX IF EXISTS idx_events_calendar_id;
DROP INDEX IF EXISTS idx_contacts_address_book_id;

-- v1.2.0-rc8: make the keyset indexes the plan the DAV paged reads actually get,
-- and let them filter on the index rather than on the heap.
--
-- v1.2.0-rc7 added (collection, id) indexes for the reads that page a
-- collection. They are the right indexes; they were not reliably chosen. With
-- the cursor spelled "collection=$1 AND id>$2 ORDER BY id" the primary key
-- answers the ORDER BY too, and the planner costs a pkey scan by assuming the
-- collection's rows are spread evenly through the id range. For a collection
-- holding a large fraction of the table -- a bulk import, or any collection
-- created after the table had grown -- that assumption is wrong in the
-- expensive direction. Measured on PostgreSQL 16.14 over 320,000 events with a
-- 100,000-row collection at the end of the id range, one 256-row first page was
-- an events_pkey scan discarding 220,300 rows: 36.17 ms filtered by a
-- time-range, 28.37 ms for an incremental sync one day back and 40.58 ms for one
-- ten years back. Extended statistics do not move it, and neither does naming
-- the collection in the ORDER BY: the planner folds an equality-constrained
-- column out of the sort key.
--
-- The reads now spell the cursor as a row comparison,
-- "(collection, id) > ($1, $2)", which the primary key cannot use as an index
-- bound at all. The same pages become 0.12 ms, 0.09 ms and 0.15 ms. The
-- collection equality stays beside it and is not redundant: the row comparison
-- alone also admits every row of every collection whose id is higher.
--
-- The trailing columns are the predicates those reads narrow on. Carried in the
-- index they are evaluated against the index tuple, so a page stops visiting
-- the heap for rows it will discard.
--
-- Two shapes pay for it, both a selective predicate losing the index that suited
-- it exactly: a narrow time-range can no longer reach
-- idx_events_recurrence_until, so 0.029 ms becomes 4.57 ms, and an incremental
-- sync whose token is near the present can no longer reach
-- idx_events_last_modified or idx_contacts_last_modified, so 0.08 ms becomes
-- 3.5-4.6 ms. The worst case over every shape falls from 40.58 ms to 4.57 ms.
-- Both costs are far inside any request budget, and trading them for a tail that
-- much shorter is the right way round for reads whose whole purpose is to be
-- bounded. An unfiltered first page was already reaching these indexes before
-- the change and is unaffected either way.
--
-- deleted_resources is deliberately unchanged: its lookup index leads on
-- deleted_at, which is the more selective bound for every sync that reads it,
-- and both shapes measured under a millisecond either way.
--
-- Operator note. Each rebuild below drops an index the DAV paged reads depend on
-- and builds it again, so run this with the application stopped or during a
-- maintenance window: a plain CREATE INDEX holds a SHARE lock that blocks writes
-- to the table for the build, and between the DROP and the CREATE those reads
-- fall back to the primary-key scans this migration exists to stop. The
-- statements are idempotent, so a run that fails part way can be repeated. On a
-- deployment that cannot take the write pause, replace each pair with
-- DROP INDEX CONCURRENTLY and CREATE INDEX CONCURRENTLY, outside a transaction
-- and checking pg_index.indisvalid afterwards -- concurrent builds can fail and
-- leave an invalid index behind, which then has to be dropped by hand.
DROP INDEX IF EXISTS idx_events_calendar_keyset;
CREATE INDEX IF NOT EXISTS idx_events_calendar_keyset ON events (
    calendar_id, id, last_modified,
    COALESCE(recurrence_until, dtend, 'infinity'::timestamptz),
    COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz));

DROP INDEX IF EXISTS idx_contacts_book_keyset;
CREATE INDEX IF NOT EXISTS idx_contacts_book_keyset ON contacts (address_book_id, id, last_modified);

UPDATE application SET value = 'v1.2.0' WHERE key = 'version';

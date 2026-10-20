-- v1.2.0: consolidated upgrade from every v1.1.x release.
-- Apply with the application stopped or during a maintenance window.
--
-- The v1.1.6, v1.1.7 and v1.1.9 releases ship migrations only through
-- v1.1.4.sql yet stamp the database with their own version, and the runner
-- applies only the files above that stamp. The stamp therefore does not say
-- which of the v1.1.6-v1.1.9 schema changes an installation holds, so this file
-- makes every one of them, each idempotently, before anything that reads them.

-- The indexes this upgrade retires or builds again go first, so the rewrites
-- below do not rebuild them and the backfills and repairs do not maintain them
-- row by row. idx_acl_unique gives way to idx_acl_resource_order, and
-- idx_events_calendar_id and idx_contacts_address_book_id are leading-column
-- prefixes of the keyset indexes built below and serve no lookup those cannot.
-- The three stamp indexes give way to the change-stream indexes built below,
-- which add the id to their stamp and serve every read the stamp alone did;
-- idx_deleted_resources_keyset, which a v1.2.0 release candidate built, is
-- superseded the same way. The recurrence and keyset indexes are built again
-- below, after the repairs that write the columns they read: the v1.1.7 pair
-- on other expressions, and the keyset pair narrower, where a release
-- candidate built them.
DROP INDEX IF EXISTS idx_acl_unique;
DROP INDEX IF EXISTS idx_events_calendar_id;
DROP INDEX IF EXISTS idx_contacts_address_book_id;
DROP INDEX IF EXISTS idx_events_last_modified;
DROP INDEX IF EXISTS idx_contacts_last_modified;
DROP INDEX IF EXISTS idx_deleted_resources_lookup;
DROP INDEX IF EXISTS idx_deleted_resources_keyset;
DROP INDEX IF EXISTS idx_events_recurrence_start;
DROP INDEX IF EXISTS idx_events_recurrence_until;
DROP INDEX IF EXISTS idx_events_calendar_keyset;
DROP INDEX IF EXISTS idx_contacts_book_keyset;

-- v1.1.6: indexable join keys for the object-level ACL checks. The resource
-- identity repair below adds them, as events.object_acl_path and
-- contacts.object_acl_path, once it has rewritten the ACL entries they join.

-- v1.1.7: recurrence_start and recurrence_until bound a recurring resource's
-- instances so calendar-query time ranges can use an index; both are NULL for a
-- resource that does not recur. The recurrence repair below fills them for the
-- stored rows, and their indexes are built after it.

ALTER TABLE events ADD COLUMN IF NOT EXISTS recurrence_start TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN IF NOT EXISTS recurrence_until TIMESTAMPTZ;

-- v1.1.8: persistent DAV dead properties and scoped ACL lookup indexes.
-- idx_acl_resource_principal is built after the ace_order backfill below, and
-- contacts.object_acl_path by the resource identity repair.

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
-- The partition is resource_path: the entries at one path are one resource's
-- ACL. The resource identity repair below decides how entries stored under
-- another spelling of a member's name take part in that member's ACL.
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
            SELECT id, ROW_NUMBER() OVER (PARTITION BY resource_path ORDER BY is_grant, created_at, id) - 1 AS position
            FROM acl_entries
        )
        UPDATE acl_entries AS entry
        SET ace_order = ordered.position
        FROM ordered
        WHERE entry.id = ordered.id;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Resource identity: one state path per stored resource.
--
-- A member's ACL entries, locks and dead properties are stored at its state
-- path: its collection's canonical path and its stored resource name, exactly,
-- or its UID where it has no name (store.MemberStatePath). RFC 3986 section
-- 6.2.2.1 makes X, X.ics and X.ICS three resources, yet the v1.1.x releases
-- and the v1.2.0 release candidates decided a member's privileges from the
-- entries at other spellings of its name as well, each reader from its own
-- set. With "the extension" meaning .ics in a calendar and .vcf in an address
-- book, matched in any case, and white space being what strings.TrimSpace
-- removes:
--
--   * a DAV request read the member's path less trailing white space, with
--     the extension removed where it ends the name and leaves something
--     before it, and that path with the extension added in lower case;
--   * a GET, and a v1.1.x contact listing, read the same for the stored name
--     with its last extension of any kind removed first, so a.b read a and
--     a.ics;
--   * a DAV listing, the REST API and the UI read the stored name, trimmed,
--     and that name with the extension removed where it has it, or added in
--     lower case where it has not; a name that is nothing but the extension
--     read its collection's entries;
--   * the v1.1.x SQL deciders read the stored name, that name with a
--     lower-case .ics removed, and that with .ics added;
--   * the release candidates' SQL deciders read every path that, less one
--     .ics or .vcf in any case, is the stored name less the extension in any
--     case.
--
-- Every reader honoured a denial at any path it read. This repair rewrites the
-- ACL so that a member's decisions read its own entries only, and replaces the
-- release candidates' join columns, which strip the extension, with ones that
-- keep the name as it is stored. It runs once: until events.object_acl_path is
-- generated as db.sql generates it, which this statement installs. For each
-- member, and each other path any of those readers read for it:
--
--   * a deny entry stored there is copied to the member, ahead of the
--     member's own entries, so it still refuses what any reader refused;
--   * a grant entry at a member path is carried to the member, after its own
--     entries, only where no member is stored under that path, no other
--     member's decisions read it, and the member's DAV requests read it, so
--     the entry decided this member's requests and this member's alone; a
--     grant only another reader applied never decided a write;
--   * any other grant at a member path stays with the member stored there,
--     or goes; either way it no longer reaches this member, and is reported.
--
-- Copied denials take positions ahead of the member's first and carried
-- grants positions after its last, so its own entries keep theirs; each keeps
-- the order of the readers above, then of the paths, then its own. A denial
-- ahead of the member's own grant can refuse what the grant allowed; that
-- errs towards refusing. Entries at a member path no member is stored under
-- but some member read have then been carried to every member they applied
-- to, and are removed. Each entry removed, and each grant withheld from a
-- member that read it, is first kept in acl_entries_v120_removed, with the
-- reason and the members that read it; the table is created only when there
-- is such an entry.
--
-- A member whose decisions this changes -- one that read entries at another
-- path, or holds entries of its own that a DAV request or GET read past -- is
-- stamped as changed, and the event and contact repairs below move each
-- collection holding one once, so a client syncing it, a sharee's among them,
-- reads again what it may now access.
--
-- Entries and dead properties at a member path no member is stored under or
-- read are left in place and reported; creating a member under that name
-- clears them, as it clears whatever ACL and dead properties are stored at
-- the path it takes. Dead properties are not rewritten: a member's own are at
-- its own path, and the releases whose collection COPY merged the dead
-- properties of every spelling into the copy wrote them at the copy's path.
-- A lock was always taken at the state path of the name its request bound,
-- so none is rewritten either; one stored at a path another member's DAV
-- requests read covered that member as well, and now covers only the name it
-- was taken on, which is reported. Also reported: members whose state paths
-- coincide (one stored without a name, at its UID, beside one stored under
-- that name), which share their state; and members whose stored names the
-- releases' path handling resolved to another path -- a name with a "." or
-- ".." segment, an empty one, a trailing "/", a "?", a "#" or a
-- percent-encoded octet -- which no request reaches as itself, and whose
-- other paths the repair reads without that resolution. At a member path that
-- path.Clean changes, which DAV requests cleaned before reading but the SQL
-- deciders read as stored, the denials stay; the grants, dead properties and
-- locks, which only DAV requests would have applied, are removed: a lock there
-- would refuse its collection's DELETE and MOVE with a token no request can
-- submit.
--
-- Each report is a WARNING, which the server log records.
--
-- Every join below is on equal paths, so each step is linear in the rows it
-- reads.
DO $identity$
DECLARE
    repair_needed BOOLEAN;
    copied_denials BIGINT;
    carried_grants BIGINT;
    changed_members BIGINT;
    changed_sample TEXT;
    removed_entries BIGINT;
    removed_denials BIGINT;
    removed_carried BIGINT;
    removed_sample TEXT;
    withheld_grants BIGINT;
    withheld_sample TEXT;
    unread_entries BIGINT;
    unread_entry_sample TEXT;
    unread_properties BIGINT;
    unread_property_sample TEXT;
    narrowed_locks BIGINT;
    narrowed_lock_sample TEXT;
    shared_paths BIGINT;
    shared_sample TEXT;
    resolved_names BIGINT;
    resolved_sample TEXT;
    resolved_locks BIGINT;
    resolved_entries BIGINT;
    resolved_properties BIGINT;
    resolved_state_sample TEXT;
BEGIN
    SELECT NOT EXISTS (
        SELECT 1
        FROM pg_attribute a
        JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = 'events'::regclass
          AND a.attname = 'object_acl_path'
          AND NOT a.attisdropped
          AND pg_get_expr(d.adbin, d.adrelid) LIKE '%NULLIF%'
    ) INTO repair_needed;

    IF repair_needed THEN
        CREATE TEMPORARY TABLE v120_members ON COMMIT DROP AS
        SELECT stored.collection || '/' || stored.name AS own, stored.*
        FROM (
            SELECT '/dav/calendars/' || calendar_id::text AS collection,
                   COALESCE(NULLIF(resource_name, ''), uid) AS name, resource_name, '.ics' AS ext
            FROM events
            UNION ALL
            SELECT '/dav/addressbooks/' || address_book_id::text,
                   COALESCE(NULLIF(resource_name, ''), uid), resource_name, '.vcf'
            FROM contacts
        ) AS stored;
        ANALYZE v120_members;

        -- Each path a reader read for a member, ranked in the order of the
        -- readers above. An extension is the last dot of the name's last
        -- segment and what follows it, as Go's path.Ext reads it; it is the
        -- collection kind's when it folds to it as strings.EqualFold folds,
        -- under which U+017F folds to s.
        CREATE TEMPORARY TABLE v120_spellings ON COMMIT DROP AS
        SELECT DISTINCT ON (read.member, read.spelling) read.member, read.spelling, read.rank
        FROM (
            SELECT m.own AS member, path.spelling, path.rank
            FROM v120_members m
            CROSS JOIN LATERAL (
                SELECT regexp_replace(m.name, $re$[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+$$re$, '') AS request,
                       regexp_replace(m.name, $re$^[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+$$re$, '', 'g') AS listed
                OFFSET 0
            ) AS trimmed
            CROSS JOIN LATERAL (
                SELECT request.stem AS request_stem, request.own_ext AS request_own_ext,
                       listed.stem AS listed_stem, listed.own_ext AS listed_own_ext
                FROM (SELECT left(trimmed.request, length(trimmed.request) - COALESCE(length(substring(trimmed.request FROM $re$\.[^/.]*$$re$)), 0)) AS stem,
                             COALESCE(translate(lower(substring(trimmed.request FROM $re$\.[^/.]*$$re$)), 'ſ', 's') = m.ext, FALSE) AS own_ext) AS request,
                     (SELECT left(trimmed.listed, length(trimmed.listed) - COALESCE(length(substring(trimmed.listed FROM $re$\.[^/.]*$$re$)), 0)) AS stem,
                             COALESCE(translate(lower(substring(trimmed.listed FROM $re$\.[^/.]*$$re$)), 'ſ', 's') = m.ext, FALSE) AS own_ext) AS listed
                OFFSET 0
            ) AS split
            -- The DAV request's base, and the base a GET read from the name
            -- less its last extension.
            CROSS JOIN LATERAL (
                SELECT CASE WHEN split.request_own_ext AND split.request_stem <> '' THEN split.request_stem ELSE trimmed.request END AS request,
                       CASE WHEN split.request_stem <> '' THEN split.request_stem ELSE trimmed.request END AS stored
                OFFSET 0
            ) AS named
            CROSS JOIN LATERAL (
                SELECT named.request,
                       left(named.request, length(named.request) - COALESCE(length(substring(named.request FROM $re$\.[^/.]*$$re$)), 0)) <> '' AS request_extended,
                       CASE WHEN COALESCE(translate(lower(substring(named.stored FROM $re$\.[^/.]*$$re$)), 'ſ', 's') = m.ext, FALSE)
                                 AND left(named.stored, length(named.stored) - length(substring(named.stored FROM $re$\.[^/.]*$$re$))) <> ''
                            THEN left(named.stored, length(named.stored) - length(substring(named.stored FROM $re$\.[^/.]*$$re$)))
                            ELSE named.stored
                       END AS stored
                OFFSET 0
            ) AS base
            CROSS JOIN LATERAL (VALUES
                (m.collection || '/' || base.request, 1),
                (CASE WHEN base.request_extended THEN m.collection || '/' || base.request || m.ext END, 2),
                (m.collection || '/' || base.stored, 3),
                (CASE WHEN left(base.stored, length(base.stored) - COALESCE(length(substring(base.stored FROM $re$\.[^/.]*$$re$)), 0)) <> ''
                      THEN m.collection || '/' || base.stored || m.ext END, 4),
                (m.collection || '/' || trimmed.listed, 5),
                (CASE WHEN NOT split.listed_own_ext THEN m.collection || '/' || trimmed.listed || m.ext
                      WHEN split.listed_stem <> '' THEN m.collection || '/' || split.listed_stem
                      ELSE m.collection END, 6),
                (CASE WHEN split.listed_own_ext AND split.listed_stem = '' THEN m.collection || '/' END, 6),
                (CASE WHEN m.ext = '.ics' THEN m.collection || '/' || m.resource_name END, 7),
                (CASE WHEN m.ext = '.ics' THEN m.collection || '/' || regexp_replace(m.resource_name, '\.ics$', '') END, 8),
                (CASE WHEN m.ext = '.ics' THEN m.collection || '/' || regexp_replace(m.resource_name, '\.ics$', '') || '.ics' END, 9)
            ) AS path(spelling, rank)
            WHERE path.spelling IS NOT NULL
            UNION ALL
            SELECT m.own, entry.resource_path, 10
            FROM v120_members m
            JOIN (
                SELECT DISTINCT resource_path, regexp_replace(resource_path, '\.(ics|vcf)$', '', 'i') AS norm
                FROM acl_entries
                WHERE resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/'
            ) AS entry
              ON entry.norm = m.collection || '/' || regexp_replace(m.resource_name, CASE m.ext WHEN '.ics' THEN '\.ics$' ELSE '\.vcf$' END, '', 'i')
        ) AS read
        WHERE read.spelling <> read.member
        ORDER BY read.member, read.spelling, read.rank;

        -- Only paths that hold entries matter; reader_count is how many
        -- members' decisions read each, owned whether a member is stored
        -- under it, member_path whether it is a member's path at all rather
        -- than its collection's, and carries whether its grants go to the
        -- member: a member path no member is stored under, read for this
        -- member alone, and read by its DAV requests.
        CREATE TEMPORARY TABLE v120_aliased ON COMMIT DROP AS
        SELECT read.*, read.member_path AND NOT read.owned AND read.reader_count = 1 AND read.rank <= 2 AS carries
        FROM (
            SELECT s.member, s.spelling, s.rank,
                   count(*) OVER (PARTITION BY s.spelling) AS reader_count,
                   o.own IS NOT NULL AS owned,
                   s.spelling ~ '^/dav/(calendars|addressbooks)/[0-9]+/.' AS member_path
            FROM v120_spellings s
            LEFT JOIN (SELECT DISTINCT own FROM v120_members) AS o ON o.own = s.spelling
            WHERE s.spelling IN (SELECT resource_path FROM acl_entries)
        ) AS read;
        ANALYZE v120_aliased;

        IF EXISTS (
            SELECT 1
            FROM v120_aliased a
            JOIN acl_entries e ON e.resource_path = a.spelling
            WHERE a.member_path AND (NOT a.owned OR e.is_grant)
        ) OR EXISTS (
            SELECT 1 FROM acl_entries
            WHERE is_grant AND ('/' || substring(resource_path FROM '^/dav/(?:calendars|addressbooks)/[0-9]+/(.+)$') || '/') ~ '//|/\.\.?/'
        ) THEN
            CREATE TABLE IF NOT EXISTS acl_entries_v120_removed (
                id BIGINT NOT NULL,
                resource_path TEXT NOT NULL,
                principal_href TEXT NOT NULL,
                is_grant BOOLEAN NOT NULL,
                privilege TEXT NOT NULL,
                ace_order INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                still_stored BOOLEAN NOT NULL,
                reason TEXT NOT NULL,
                read_by TEXT[] NOT NULL
            );
            INSERT INTO acl_entries_v120_removed
            SELECT e.id, e.resource_path, e.principal_href, e.is_grant, e.privilege, e.ace_order, e.created_at,
                   path.owned,
                   CASE
                       WHEN path.owned THEN 'grant withheld from the members that read it at another spelling of their names; the member stored here keeps it'
                       WHEN NOT e.is_grant THEN 'denial copied to every member that read it, then removed'
                       WHEN path.carries THEN 'grant carried to the one member whose DAV requests read it, then removed'
                       WHEN path.reader_count > 1 THEN 'grant withheld from the several members that read it, then removed'
                       ELSE 'grant withheld from the member that read it, whose DAV requests did not, then removed'
                   END,
                   path.readers
            FROM (
                SELECT spelling, bool_or(owned) AS owned, max(reader_count) AS reader_count, bool_or(carries) AS carries,
                       array_agg(member ORDER BY member) AS readers
                FROM v120_aliased
                WHERE member_path
                GROUP BY spelling
            ) AS path
            JOIN acl_entries e ON e.resource_path = path.spelling
            WHERE NOT path.owned OR e.is_grant;
            INSERT INTO acl_entries_v120_removed
            SELECT id, resource_path, principal_href, is_grant, privilege, ace_order, created_at, FALSE,
                   'grant stored at a path that resolves to another, which no DAV request read, then removed', '{}'
            FROM acl_entries
            WHERE is_grant AND ('/' || substring(resource_path FROM '^/dav/(?:calendars|addressbooks)/[0-9]+/(.+)$') || '/') ~ '//|/\.\.?/';
        END IF;

        WITH carried AS (
            SELECT a.member, a.rank, a.spelling, e.id, e.principal_href, e.is_grant, e.privilege, e.ace_order, e.created_at
            FROM v120_aliased a
            JOIN acl_entries e ON e.resource_path = a.spelling
            WHERE NOT e.is_grant OR a.carries
        ),
        placed AS (
            SELECT c.*,
                   row_number() OVER (PARTITION BY c.member, c.is_grant ORDER BY c.rank, c.spelling, c.ace_order, c.id) AS place,
                   count(*) OVER (PARTITION BY c.member, c.is_grant) AS places
            FROM carried c
        ),
        inserted AS (
            INSERT INTO acl_entries (resource_path, principal_href, is_grant, privilege, ace_order, created_at)
            SELECT p.member, p.principal_href, p.is_grant, p.privilege,
                   CASE
                       WHEN p.is_grant THEN COALESCE(own.last, -1) + p.place
                       ELSE COALESCE(own.first, 0) - p.places - 1 + p.place
                   END,
                   p.created_at
            FROM placed p
            LEFT JOIN (
                SELECT resource_path, min(ace_order) AS first, max(ace_order) AS last FROM acl_entries GROUP BY resource_path
            ) AS own ON own.resource_path = p.member
            ORDER BY p.member, p.is_grant, p.place
            RETURNING resource_path, is_grant
        )
        SELECT count(*) FILTER (WHERE NOT is_grant), count(*) FILTER (WHERE is_grant), count(DISTINCT resource_path),
               (SELECT string_agg(path, ', ' ORDER BY path) FROM (SELECT DISTINCT resource_path AS path FROM inserted ORDER BY 1 LIMIT 50) AS sample)
        FROM inserted
        INTO copied_denials, carried_grants, changed_members, changed_sample;

        WITH withheld AS (
            SELECT a.spelling || ' -> ' || a.member AS pair
            FROM v120_aliased a
            JOIN acl_entries e ON e.resource_path = a.spelling
            WHERE a.member_path AND e.is_grant AND NOT a.carries
        )
        SELECT count(*), (SELECT string_agg(pair, ', ' ORDER BY pair) FROM (SELECT DISTINCT pair FROM withheld ORDER BY 1 LIMIT 50) AS sample)
        FROM withheld
        INTO withheld_grants, withheld_sample;

        WITH removed AS (
            DELETE FROM acl_entries e
            USING (SELECT spelling, bool_or(carries) AS carries FROM v120_aliased WHERE member_path AND NOT owned GROUP BY spelling) AS unowned
            WHERE e.resource_path = unowned.spelling
            RETURNING e.resource_path, e.is_grant, unowned.carries
        )
        SELECT count(*), count(*) FILTER (WHERE NOT is_grant), count(*) FILTER (WHERE is_grant AND carries),
               (SELECT string_agg(path, ', ' ORDER BY path) FROM (SELECT DISTINCT resource_path AS path FROM removed ORDER BY 1 LIMIT 50) AS sample)
        FROM removed
        INTO removed_entries, removed_denials, removed_carried, removed_sample;

        -- State at a member path that path.Clean changes -- a name with a "."
        -- or ".." segment, an empty one, or a trailing "/". A DAV request
        -- cleaned the path to another before it read any state, and so did
        -- the DAV listing, the REST API and the UI. The SQL deciders, the
        -- v1.1.x ones, the release candidates' and 1.2.0's own, read the
        -- member's ACL there as stored, so its denials stay. Its grants, which
        -- no DAV request read and no write was decided by, go, kept in
        -- acl_entries_v120_removed, as do its dead properties, which only DAV
        -- requests read. Its locks go: one would refuse a DELETE or MOVE of
        -- its collection that no request can submit its token for.
        WITH resolved AS (
            SELECT 'lock' AS kind, resource_path FROM locks
            WHERE ('/' || substring(resource_path FROM '^/dav/(?:calendars|addressbooks)/[0-9]+/(.+)$') || '/') ~ '//|/\.\.?/'
        ),
        removed_locks AS (
            DELETE FROM locks l USING (SELECT DISTINCT resource_path FROM resolved) AS r
            WHERE l.resource_path = r.resource_path
            RETURNING l.resource_path
        ),
        removed_entries AS (
            DELETE FROM acl_entries
            WHERE is_grant AND ('/' || substring(resource_path FROM '^/dav/(?:calendars|addressbooks)/[0-9]+/(.+)$') || '/') ~ '//|/\.\.?/'
            RETURNING resource_path
        ),
        removed_properties AS (
            DELETE FROM dav_dead_properties
            WHERE ('/' || substring(resource_path FROM '^/dav/(?:calendars|addressbooks)/[0-9]+/(.+)$') || '/') ~ '//|/\.\.?/'
            RETURNING resource_path
        )
        SELECT (SELECT count(*) FROM removed_locks), (SELECT count(*) FROM removed_entries), (SELECT count(*) FROM removed_properties),
               (SELECT string_agg(path, ', ' ORDER BY path) FROM (
                    SELECT resource_path AS path FROM removed_locks
                    UNION SELECT resource_path FROM removed_entries
                    UNION SELECT resource_path FROM removed_properties
                    ORDER BY 1 LIMIT 50) AS sample)
        INTO resolved_locks, resolved_entries, resolved_properties, resolved_state_sample;

        -- A member path is a calendar's or an address book's path followed by
        -- a name; the collections' own entries, and a pending collection's
        -- locks, are not member paths.
        SELECT count(*), (SELECT string_agg(path, ', ' ORDER BY path) FROM (
                   SELECT DISTINCT e.resource_path AS path
                   FROM acl_entries e
                   LEFT JOIN (SELECT DISTINCT own FROM v120_members) AS o ON o.own = e.resource_path
                   WHERE e.resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/.' AND o.own IS NULL
                   ORDER BY 1 LIMIT 50) AS sample)
        FROM acl_entries e
        LEFT JOIN (SELECT DISTINCT own FROM v120_members) AS o ON o.own = e.resource_path
        WHERE e.resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/.' AND o.own IS NULL
        INTO unread_entries, unread_entry_sample;

        SELECT count(*), (SELECT string_agg(path, ', ' ORDER BY path) FROM (
                   SELECT DISTINCT p.resource_path AS path
                   FROM dav_dead_properties p
                   LEFT JOIN (SELECT DISTINCT own FROM v120_members) AS o ON o.own = p.resource_path
                   WHERE p.resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/.' AND o.own IS NULL
                   ORDER BY 1 LIMIT 50) AS sample)
        FROM dav_dead_properties p
        LEFT JOIN (SELECT DISTINCT own FROM v120_members) AS o ON o.own = p.resource_path
        WHERE p.resource_path ~ '^/dav/(calendars|addressbooks)/[0-9]+/.' AND o.own IS NULL
        INTO unread_properties, unread_property_sample;

        SELECT count(*), (SELECT string_agg(path, ', ' ORDER BY path) FROM (
                   SELECT DISTINCT l.resource_path AS path
                   FROM locks l
                   JOIN (SELECT DISTINCT spelling FROM v120_spellings WHERE rank <= 2) AS s ON s.spelling = l.resource_path
                   WHERE l.expires_at > now()
                   ORDER BY 1 LIMIT 50) AS sample)
        FROM locks l
        JOIN (SELECT DISTINCT spelling FROM v120_spellings WHERE rank <= 2) AS s ON s.spelling = l.resource_path
        WHERE l.expires_at > now()
        INTO narrowed_locks, narrowed_lock_sample;

        SELECT count(*), string_agg(own, ', ' ORDER BY own) FILTER (WHERE place <= 50)
        FROM (
            SELECT own, row_number() OVER (ORDER BY own) AS place
            FROM v120_members
            GROUP BY own
            HAVING count(*) > 1
        ) AS shared
        INTO shared_paths, shared_sample;

        SELECT count(*), string_agg(own, ', ' ORDER BY own) FILTER (WHERE place <= 50)
        FROM (
            SELECT own, row_number() OVER (ORDER BY own) AS place
            FROM v120_members
            WHERE name ~ '[?#]|%[0-9A-Fa-f]{2}'
               OR ('/' || regexp_replace(name, $re$[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]+$$re$, '') || '/') ~ '//|/\.\.?/'
        ) AS resolved
        INTO resolved_names, resolved_sample;

        IF changed_members > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: copied % deny and % grant ACL entries from other spellings of their names to % members (first 50: %)',
                copied_denials, carried_grants, changed_members, changed_sample;
        END IF;
        IF removed_entries > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: removed % ACL entries stored under no member''s name: % denials copied to every member that read them, % grants carried to the one member whose DAV requests read each, and % grants no member keeps; acl_entries_v120_removed keeps each (first 50 paths: %)',
                removed_entries, removed_denials, removed_carried, removed_entries - removed_denials - removed_carried, removed_sample;
        END IF;
        IF withheld_grants > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: % ACL grants no longer apply to a member they reached only through another spelling of its name; acl_entries_v120_removed keeps each (first 50, spelling -> member: %)',
                withheld_grants, withheld_sample;
        END IF;
        IF resolved_locks + resolved_entries + resolved_properties > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: removed % locks, % ACL grants and % dead properties stored at member paths that resolve to another path, which no DAV request read; the denials there stay, and acl_entries_v120_removed keeps the grants (first 50 paths: %)',
                resolved_locks, resolved_entries, resolved_properties, resolved_state_sample;
        END IF;
        IF unread_entries > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: left % ACL entries at member paths no member is stored under (first 50 paths: %)',
                unread_entries, unread_entry_sample;
        END IF;
        IF unread_properties > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: left % dead properties at member paths no member is stored under (first 50 paths: %)',
                unread_properties, unread_property_sample;
        END IF;
        IF narrowed_locks > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: % active locks now cover only the name they were taken on, not the other member whose name they also spelled (first 50 paths: %)',
                narrowed_locks, narrowed_lock_sample;
        END IF;
        IF shared_paths > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: % state paths are each the path of more than one member, one stored without a name at its UID and one stored under that name, which share their ACL entries, locks and dead properties (first 50: %)',
                shared_paths, shared_sample;
        END IF;
        IF resolved_names > 0 THEN
            RAISE WARNING 'v1.2.0 resource identity repair: % members have stored names that resolve to another path, with a "." or ".." segment, an empty one, a trailing "/", a "?", a "#" or a percent-encoded octet; no request reaches them as themselves (first 50: %)',
                resolved_names, resolved_sample;
        END IF;
    END IF;

    -- A release candidate built these columns stripping the extension.
    -- PostgreSQL 16 cannot change a generated column's expression, so one
    -- built otherwise than db.sql builds it is dropped and added again;
    -- dropping a column drops the indexes on it.
    ALTER TABLE acl_entries DROP COLUMN IF EXISTS resource_path_norm;
    IF EXISTS (
        SELECT 1
        FROM pg_attribute a
        JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = 'events'::regclass
          AND a.attname = 'object_acl_path'
          AND NOT a.attisdropped
          AND pg_get_expr(d.adbin, d.adrelid) NOT LIKE '%NULLIF%'
    ) THEN
        ALTER TABLE events DROP COLUMN object_acl_path;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_attribute a
        JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = 'contacts'::regclass
          AND a.attname = 'object_acl_path'
          AND NOT a.attisdropped
          AND pg_get_expr(d.adbin, d.adrelid) NOT LIKE '%NULLIF%'
    ) THEN
        ALTER TABLE contacts DROP COLUMN object_acl_path;
    END IF;

    ALTER TABLE events
        ADD COLUMN IF NOT EXISTS object_acl_path TEXT
        GENERATED ALWAYS AS ('/dav/calendars/' || calendar_id::text || '/' || COALESCE(NULLIF(resource_name, ''), uid)) STORED;
    ALTER TABLE contacts
        ADD COLUMN IF NOT EXISTS object_acl_path TEXT
        GENERATED ALWAYS AS ('/dav/addressbooks/' || address_book_id::text || '/' || COALESCE(NULLIF(resource_name, ''), uid)) STORED;

    -- The members whose decisions changed: those that read entries at
    -- another path, and those holding entries of their own that the DAV
    -- request or GET reader passed over for another path. The stamp is
    -- trg_*_touch_last_modified's; the collection moves follow the event and
    -- contact repairs, which find the members by it.
    IF repair_needed THEN
        CREATE TEMPORARY TABLE v120_changed ON COMMIT DROP AS
        SELECT member FROM v120_aliased
        UNION
        SELECT s.member
        FROM v120_spellings s
        WHERE s.rank IN (1, 3)
          AND s.member IN (SELECT resource_path FROM acl_entries);
        ALTER TABLE events DISABLE TRIGGER trg_events_increment_ctag;
        ALTER TABLE contacts DISABLE TRIGGER trg_contacts_increment_ctag;
        UPDATE events SET last_modified = last_modified
            WHERE object_acl_path IN (SELECT member FROM v120_changed);
        UPDATE contacts SET last_modified = last_modified
            WHERE object_acl_path IN (SELECT member FROM v120_changed);
        ALTER TABLE events ENABLE TRIGGER trg_events_increment_ctag;
        ALTER TABLE contacts ENABLE TRIGGER trg_contacts_increment_ctag;
    END IF;
END
$identity$;
-- End of the resource identity repair.
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_contacts_object_acl_path
    ON contacts (object_acl_path);

CREATE INDEX IF NOT EXISTS idx_acl_principal_grant_path
    ON acl_entries (principal_href, is_grant, resource_path);

CREATE INDEX IF NOT EXISTS idx_acl_resource_principal
    ON acl_entries (resource_path, principal_href);

-- Serves a range over every path beneath a collection whatever the database's
-- collation: text_pattern_ops compares byte-wise, which a range on a path
-- prefix needs and the default btree on resource_path does not give under a
-- linguistic collation.
CREATE INDEX IF NOT EXISTS idx_acl_resource_path_pattern
    ON acl_entries (resource_path text_pattern_ops);

CREATE INDEX IF NOT EXISTS idx_acl_resource_order ON acl_entries(resource_path, ace_order, id);

-- Serves the scans for every lock beneath a collection, a LIKE on the path
-- prefix, as idx_acl_resource_path_pattern serves the ACL's.
CREATE INDEX IF NOT EXISTS idx_locks_resource_path_pattern
    ON locks (resource_path text_pattern_ops);

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
-- upgrade path whether or not it finds anything to add. deleted_resources is
-- closed from the DROP INDEX of the stamp index the file replaces.
-- address_books is closed to writes from its CREATE TRIGGER, and to reads as
-- well when the file runs again and its DROP TRIGGER finds the trigger. A
-- closed table makes a SELECT wait rather than fall back to another plan. A run
-- that fails part way rolls back entirely and can simply be repeated; the
-- statements are individually idempotent as well.
--
-- The duration is therefore what matters operationally. It is mostly the
-- recurrence, event-end and TZID repairs, plus the index builds and the
-- birthday backfill, and the rewrite of events and contacts that adding the
-- generated ACL join keys costs, or replacing them on a database a release
-- candidate migrated. The server does not open its listener until migrations
-- finish, so on a container deployment it is startup time and has to fit the
-- probe budget.
--
-- Check that budget against the table before upgrading. The whole file, run
-- from a v1.1.9 release install, where it also adds the generated columns,
-- measured on 20,000 events across 50 calendars, two fifths recurring, a
-- quarter with an ACL entry of their own, and each with a VTIMEZONE: about 30
-- seconds per hundred thousand events with 3 KB bodies and 70 with 10 KB
-- bodies, whether their TZIDs are IANA names or Windows names. The TZID repair
-- reads and rewrites a body only where a TZID needs it, at about 0.65
-- milliseconds an event with 3 KB bodies and 1.25 with 10 KB: for events that
-- all carry Exchange's TZID=UTC the whole file took 95 seconds per hundred
-- thousand with 3 KB bodies and 190 with 10 KB. It grows with the number of events
-- and their size, not with how they are spread across calendars: the time was
-- the same with every event in one calendar as across 500. The Helm chart's
-- default startup budget is ten minutes
-- (startupProbe.periodSeconds 10 x failureThreshold 60). A run that overruns it
-- is killed rather than allowed to finish, and because the file is one
-- transaction the kill rolls it back: the container then restarts and begins
-- the same work again, so the deployment does not fail loudly, it never comes
-- up. Above roughly 400,000 events, or 200,000 with large bodies, raise
-- startupProbe.failureThreshold first, or apply the file by hand as below;
-- those figures leave a third of the budget spare even where every event
-- carries TZID=UTC.
--
-- On a deployment that cannot take that pause, run the file's statements by hand
-- outside a transaction, using DROP INDEX CONCURRENTLY and CREATE INDEX
-- CONCURRENTLY for each index statement and checking pg_index.indisvalid
-- afterwards -- concurrent builds can fail and leave an invalid index behind,
-- which then has to be dropped by hand. Stop every server instance first all
-- the same: the resource identity repair runs once, and a 1.1.x server still
-- running writes ACL entries under the spellings it has just rewritten, which
-- nothing repairs afterwards; and on a database a release candidate migrated,
-- the repair drops acl_entries.resource_path_norm and replaces the
-- object_acl_path columns, which that server's queries read.

-- Every events index that reads recurrence_start or recurrence_until was
-- dropped at the top of the file and is built after the two recurrence
-- repairs, so building each once over the repaired rows replaces maintaining
-- it per repaired row. idx_events_object_acl_path is built after the repairs
-- for the same reason where the upgrade adds or replaces its column. The
-- repairs' updates still fire trg_events_touch_last_modified, so they rewrite
-- last_modified and maintain every index that remains.

-- The repairs below can update most rows of events, and every update fires
-- trg_events_increment_ctag, which updates the row of the event's calendar.
-- Each of those writes another version of that one calendars row, which the
-- next has to reach past the versions before it, so the cost grows with the
-- square of the rows repaired in one calendar. The trigger is therefore off
-- while the repairs run, and each calendar they changed is moved once after
-- them. The repairs and both trigger changes are one statement, so the trigger
-- is never left off, even when the file is run by hand outside a transaction.
--
-- The repaired rows, and the members the resource identity repair stamped,
-- are found by their stamp. trg_events_touch_last_modified
-- stays on, and both the function an earlier release installed (NOW()) and the
-- one this file installs below (clock_timestamp(), kept past the calendar's
-- token) stamp a row at or after the start of the transaction that updates
-- it. A row stamped later than that for any other reason only costs its
-- calendar a move it did not need.
--
-- The move writes updated_at itself because on an earlier release's schema no
-- trigger on calendars does; stamp_collection_updated_at, where it is
-- installed, writes the same value. It leaves the sync token at or past every
-- stamp the repairs wrote, so a client that syncs from it is not told of the
-- repaired rows again, while one holding an older token is, and the CTag move
-- tells a client that compares CTags to look again.
DO $repairs$
DECLARE
    tzid_rewritten_events BIGINT;
    tzid_untouched_lines BIGINT;
    tzid_untouched_events BIGINT;
    tzid_untouched_ids BIGINT[];
BEGIN
ALTER TABLE events DISABLE TRIGGER trg_events_increment_ctag;

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
-- component to the end of the body, which covers every character the
-- non-greedy match could capture; it rejects the common shape the first cannot,
-- a VTIMEZONE whose DST rules are RRULEs ahead of a non-recurring event. A
-- VTIMEZONE after an event lies inside that span, which is why the non-greedy
-- match remains. Both filters test the component and property names
-- unanchored, so each plainly admits everything the anchored test after it can
-- accept.
--
-- The deciding match reads a component's lines as ical.ReadComponents and
-- ical.ConservativeRecurrenceBounds read them, with white space being what
-- strings.TrimSpace removes (the bracket class: ASCII white space, U+0085,
-- U+00A0, U+1680, U+2000-U+200A, U+2028, U+2029, U+202F, U+205F and U+3000):
--   - a BEGIN or END is a content line of its own starting with BEGIN: or END:,
--     and names the component with nothing but white space around the name;
--   - a component ends at its END line, or, when it has none, at the end of
--     the body, where the reader closes what is still open;
--   - a property line names RRULE, RDATE or RECURRENCE-ID after any leading
--     white space, followed by its parameters or, after any white space, by
--     its colon; an RDATE holding no colon counts as well.
-- The same text inside a value -- a DESCRIPTION quoting END:VEVENT, or a
-- calendar description naming BEGIN:VEVENT ahead of a VTIMEZONE -- neither
-- ends a component early nor opens one, and a longer name such as VEVENTX is
-- another component. The match reaches beyond the reader in three ways, each
-- of which only bounds a row that needed none: it reads the lines of a
-- component nested in the one it matches (a VALARM's) as that component's; a
-- component the reader closes at the END of an enclosing one runs on to the
-- end of the body; and it counts an RRULE with an empty value and a
-- RECURRENCE-ID the reader cannot resolve. It falls short of the reader in one
-- shape no client writes: a component nested in another of the same name,
-- whose END the match takes for the outer one's.
--
-- Each anchor is a lookaround constraint, not a quantified atom: PostgreSQL
-- takes a regular expression's preference for the longest or shortest match
-- from its first quantified atom, so white space matched ahead of the ".*?"
-- would turn the whole match greedy and pair a BEGIN with the last END in the
-- body. The component's name is therefore looked ahead to, and the captured
-- text starts with it.
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
              WHEN unfolded.body !~* $re$(RRULE|RDATE|RECURRENCE-ID)$re$ THEN FALSE
              WHEN NOT (
                  EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VEVENT(.*)$re$, 'gi') AS span(match)
                      WHERE span.match[1] ~* $re$(RRULE|RDATE|RECURRENCE-ID)$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VTODO(.*)$re$, 'gi') AS span(match)
                      WHERE span.match[1] ~* $re$(RRULE|RDATE|RECURRENCE-ID)$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$BEGIN:[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VJOURNAL(.*)$re$, 'gi') AS span(match)
                      WHERE span.match[1] ~* $re$(RRULE|RDATE|RECURRENCE-ID)$re$
                  )
              ) THEN FALSE
              ELSE
                  EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$(?<![^\r\n])BEGIN:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VEVENT[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))(.*?)(?:(?<![^\r\n])END:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VEVENT[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))|$)$re$, 'gi') AS component(match)
                      WHERE component.match[1] ~* $re$(^|\r|\n)[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:(?:RRULE|RECURRENCE-ID)(?:;|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*:)|RDATE(?:;|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?::|\r|\n|$)))$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$(?<![^\r\n])BEGIN:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VTODO[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))(.*?)(?:(?<![^\r\n])END:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VTODO[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))|$)$re$, 'gi') AS component(match)
                      WHERE component.match[1] ~* $re$(^|\r|\n)[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:(?:RRULE|RECURRENCE-ID)(?:;|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*:)|RDATE(?:;|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?::|\r|\n|$)))$re$
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM regexp_matches(unfolded.body, $re$(?<![^\r\n])BEGIN:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VJOURNAL[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))(.*?)(?:(?<![^\r\n])END:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VJOURNAL[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))|$)$re$, 'gi') AS component(match)
                      WHERE component.match[1] ~* $re$(^|\r|\n)[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:(?:RRULE|RECURRENCE-ID)(?:;|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*:)|RDATE(?:;|[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?::|\r|\n|$)))$re$
                  )
          END
      );

-- Reopen every finite recurrence_until the series it bounds can outlast.
--
-- ical.ConservativeRecurrenceBounds writes a finite bound only for a rule with
-- UNTIL, or with COUNT, FREQ=DAILY or WEEKLY and no BY parts, and the sentinel
-- for every other. Two of those series run past a bound read from the rule
-- alone. An instance whose local start time a timezone transition skips is
-- read at the offset before the gap (RFC 5545 section 3.3.5), and where that
-- reading names the instant of the next instance, as across a skipped day, the
-- two are one instance (section 3.8.5.3), so a COUNT series whose DTSTART
-- names a zone can reach its last instance a step or more later than the rule
-- alone says.
-- And UNTIL is inclusive, so a date UNTIL on a timed series admits that day's
-- instances, up to a day past the day's start. Neither can be left to the
-- candidate filter's 48-hour margin, which covers the zones the column and the
-- RFC 4791 section 9.9 test read a time in (maxStoredInstantSkew in
-- internal/dav).
--
-- RFC 5545 section 3.8.5.3 also makes the DTSTART the first instance even when
-- the rule's UNTIL falls before it, which a bound read as UNTIL plus the
-- instance's length leaves out: such a bound lies before the end of that
-- instance, by however far UNTIL precedes the DTSTART. A finite bound is never
-- earlier than the start or the end of any component of the object, so a row
-- whose bound is earlier than its stored dtstart or dtend holds one of these.
-- GREATEST skips a NULL dtend, so a row whose dtend is earlier than its
-- dtstart -- a DTEND RFC 5545 refuses, repaired below -- is judged by its
-- dtstart. A row whose DURATION decides its end stores no dtend, and its bound
-- can fall short by more than the 48-hour margin while still lying past its
-- dtstart, so its bound is also compared with its dtstart plus each DURATION
-- the body carries. An alarm's or an override's DURATION is read too, which
-- can only reopen a bound that did not need it. A DURATION is cast to an
-- interval only when it has the RFC 5545 form, with no more than six digits to
-- a number so the cast cannot overflow; any other DURATION reopens the bound.
--
-- A finite bound is reset to the sentinel when it is earlier than the row's
-- dtstart or dtend; when the row stores no dtend and the bound is earlier
-- than its dtstart plus a DURATION the body carries; when the body carries a
-- DAILY or WEEKLY rule with COUNT and a DTSTART with a TZID; or when it
-- carries a rule with a date UNTIL and a DTSTART with a time of day outside
-- every VTIMEZONE, whose observances always carry one. The tests read the
-- whole body rather than one component, which can only reset a bound that
-- needed none: the row stays a candidate for every range, and the next write
-- of the resource stores its exact bound. Only
-- finite bounds are read, so the statement can run again, and a body whose row
-- holds none, or whose bound the columns already show to be short, is never
-- unfolded.
--
-- The body is unfolded as in the repair above. Names and values match without
-- regard to case, and the parts of a rule and the parameters of a property in
-- any order. A parameter value may be quoted and hold a colon or a semicolon, so
-- a TZID parameter is looked for anywhere on its DTSTART line. A negated class
-- such as [^\r\n] admits every other character under any collation; it is the
-- named classes that a C collation confines to ASCII. Stripping the VTIMEZONEs
-- costs a non-greedy match, so the CASE runs it only for a body the cheap tests
-- have not settled. A VTIMEZONE is stripped from its own BEGIN line to its own
-- END line, each read as the recurrence repair above reads a component's: a
-- VTIMEZONE named inside a value would otherwise strip the DTSTART after it.
-- One without an END line is not stripped: what it holds then stays a
-- candidate for a time of day, which can only reopen a bound.
UPDATE events
    SET recurrence_until = '9999-12-31T23:59:59Z'
    WHERE recurrence_until <> '9999-12-31T23:59:59Z'
      AND (recurrence_until < GREATEST(dtstart, dtend) OR EXISTS (
          SELECT 1
          FROM regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') AS unfolded(body)
          WHERE CASE
              WHEN events.dtend IS NULL AND events.dtstart IS NOT NULL AND EXISTS (
                  SELECT 1
                  FROM regexp_matches(unfolded.body, $re$(?:^|\r|\n)DURATION(?:;[^\r\n]*)?:[ \t]*([^\r\n]*)$re$, 'gi') AS duration(value)
                  WHERE CASE
                      WHEN duration.value[1] ~* $re$^\+?P(?:[0-9]{1,6}W|[0-9]{1,6}D(?:T(?=[0-9])(?:[0-9]{1,6}H)?(?:[0-9]{1,6}M)?(?:[0-9]{1,6}S)?)?|T(?=[0-9])(?:[0-9]{1,6}H)?(?:[0-9]{1,6}M)?(?:[0-9]{1,6}S)?)[ \t]*$$re$
                          THEN events.recurrence_until < events.dtstart + upper(btrim(duration.value[1], E'+ \t'))::interval
                      ELSE TRUE
                  END
              ) THEN TRUE
              WHEN unfolded.body ~* $re$(^|\r|\n)RRULE(?=[^\r\n]*[;:][ \t]*COUNT[ \t]*=)[^\r\n]*[;:][ \t]*FREQ[ \t]*=[ \t]*(DAILY|WEEKLY)$re$
                  AND unfolded.body ~* $re$(^|\r|\n)DTSTART;[^\r\n]*TZID[ \t]*=$re$ THEN TRUE
              WHEN unfolded.body !~* $re$(^|\r|\n)RRULE[^\r\n]*[;:][ \t]*UNTIL[ \t]*=[ \t]*[0-9]{8}[ \t]*(;|\r|\n|$)$re$ THEN FALSE
              ELSE regexp_replace(unfolded.body, $re$(?<![^\r\n])BEGIN:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VTIMEZONE[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$)).*?(?<![^\r\n])END:(?=[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*VTIMEZONE[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]*(?:\r|\n|$))$re$, '', 'gi')
                  ~* $re$(^|\r|\n)DTSTART[;:][^\r\n]*[0-9]{8}T$re$
          END
      ));

-- Store the end an event occupies where its DTEND cannot end it.
--
-- RFC 5545 requires DTEND later than DTSTART. Earlier releases stored the DTEND
-- as written whatever it said, but the reports read an event whose DTEND is not
-- later -- a DATE DTEND on or before a DATE DTSTART, or a DATE-TIME DTEND before
-- a DATE-TIME DTSTART -- as though it had none: a DATE start occupies its day,
-- a DATE-TIME start is the instant it names, and a DURATION, where the event
-- carries one, sets its length. A dtend before dtstart drops such a row from
-- the time-range narrowing on COALESCE(recurrence_until, dtend, ...) before the
-- RFC 4791 section 9.9 test sees it. Each row gets the end a write of the same
-- body stores (ical.StoredEventEnd): dtstart plus a day for an all-day event,
-- dtstart itself for a timed one, and none where the body carries a DURATION,
-- which leaves the row a candidate for every range.
--
-- all_day records whether the DTSTART the columns were read from is a DATE; it
-- is the column every release has written that way. A DATE's dtstart is its UTC
-- midnight, so its day ends 24 hours later. The DURATION test reads the whole
-- unfolded body, an alarm's DURATION included, which can only clear a dtend a
-- write would have kept. The new dtend is never earlier than the one it
-- replaces, and never earlier than dtstart, so no row the narrowing kept
-- before is dropped by it. Only rows still in one of these shapes are read, and
-- none is left in one, so the statement can run again.
UPDATE events
    SET dtend = CASE
        WHEN regexp_replace(raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') ~* $re$(^|\r|\n)DURATION[;:]$re$ THEN NULL
        WHEN all_day THEN dtstart + interval '24 hours'
        ELSE dtstart
    END
    WHERE dtstart IS NOT NULL
      AND dtend IS NOT NULL
      AND (dtend < dtstart OR (all_day AND dtend = dtstart));

-- Store the end an event without a DTEND occupies.
--
-- Earlier releases stored no dtend for an event without a DTEND, which kept the
-- row a candidate for every range. A write stores the end the reports read such
-- an event with (ical.StoredEventEnd): dtstart plus a day for an all-day event,
-- dtstart itself for a timed one, and none where a DURATION decides its end.
--
-- A row whose body carries a DTEND keeps no dtend: the release that wrote it
-- could not read that DTEND, so the end it names is unknown here, and only the
-- RFC 4791 section 9.9 test, which reads it, can bound the row. Both tests
-- read the whole unfolded body -- an alarm's DURATION or an override's DTEND
-- included -- which can only leave a row without the dtend a write would
-- store, a candidate for every range as it was. A row given a dtend leaves the
-- set this reads, so the statement can run again.
UPDATE events
    SET dtend = CASE WHEN all_day THEN dtstart + interval '24 hours' ELSE dtstart END
    WHERE dtstart IS NOT NULL
      AND dtend IS NULL
      AND regexp_replace(raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') !~* $re$(^|\r|\n)(DTEND|DURATION)[;:]$re$;

-- Write the zone of each stored time in the form DAVx5 reads as the server
-- does.
--
-- DAVx5 parses calendar data with ical4j under relaxed parsing, and loses the
-- zone of two TZID spellings the server reads correctly, so the device shows
-- the time at its own local time instead:
--   - ical4j's PropertyBuilder.build deletes every parameter whose value is
--     exactly UTC, compared with regard to case once one pair of enclosing
--     quotes is removed, leaving the local time floating;
--   - ical4j keeps a parameter value's white space, so " Europe/Berlin" names
--     no zone it knows, and DAVx5 drops the TZID and floats the time.
-- The server trims a TZID with strings.TrimSpace and resolves UTC through the
-- host's zone database or the object's own definition.
--
-- The rule:
--   - It reads the DTSTART, DTEND, DUE, RECURRENCE-ID, EXDATE and RDATE lines
--     outside every VTIMEZONE: the properties RFC 5545 qualifies with a TZID.
--     An observance's DTSTART is a local time by definition, and ical4j
--     ignores a TZID inside a VTIMEZONE.
--   - A line's TZID is its first parameter, outside quotes, whose name is TZID
--     in any case with white space around the name ignored. Its value is read
--     as ical.PropertyParam and every TZID reader read it: white space
--     trimmed, one pair of enclosing quotes removed, white space trimmed
--     again. White space is what strings.TrimSpace removes, the class
--     [\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000] once
--     line breaks are gone.
--   - A TZID whose value is UTC, compared without regard to case, on a line
--     whose value holds a local DATE-TIME (YYYYMMDDTHHMMSS with no Z) is
--     removed, and the DATE-TIME is written with Z appended: the instant the
--     server read, which every reader reads as UTC. A DATE and a value
--     already in UTC are left, since no zone places them.
--   - Any other TZID written as anything but its trimmed value, bare or in
--     quotes, is written as that value, in quotes if it was quoted.
--
-- The repair keeps every octet the rule does not change, so it rewrites only a
-- line whose rewrite is an edit of that line alone: one that is not folded,
-- whose parameters are each a name of letters, digits and hyphens, "=", and
-- values that are either quoted or hold no quote, ";", ":" or ",", of which
-- exactly one is named TZID with a single value, and which for UTC holds one
-- DATE-TIME and no VALUE but DATE-TIME. A TZID is therefore trimmed on an
-- EXDATE or RDATE list or period as on any other line, while UTC on one is
-- left: its rewrite would append Z to each of its values. Every other line the
-- rule applies to -- a folded line, UTC on an EXDATE or RDATE list or period,
-- a parameter in another form -- is left as stored and reported. Lines in two
-- more cases are left and reported:
--   - PUT holds an override's RECURRENCE-ID to the form and TZID of its
--     series' DTSTART, so where a DTSTART or RECURRENCE-ID naming UTC cannot
--     be rewritten, none of the body's DTSTARTs and RECURRENCE-IDs naming UTC
--     is.
--   - A definition of its own, whose trimmed TZID is UTC in any case and whose
--     offsets are not all zero, reads a local time naming that very spelling
--     as another instant than the UTC form names, so a value whose trimmed
--     TZID is spelled exactly as such a definition is keeps its TZID rather
--     than losing it for a Z. A value naming UTC in another spelling is
--     answered by no definition, every reader takes it for UTC itself, and it
--     is rewritten. DAVx5 keeps a TZID's white space whatever defines the
--     zone, so a held line is still trimmed, but is reported regardless of
--     whether trimming changed it: ical4j drops a parameter named exactly UTC
--     whether or not a same-named VTIMEZONE is present, so DAVx5 floats the
--     time this held zone defines however the line reads, and only a client
--     resaving it, or the zone's own definition changing, settles it.
-- A VTIMEZONE named UTC stays, used or not: RFC 5545 section 3.6.5 asks for a
-- definition of every TZID an object uses, not that every definition be used,
-- the PUT validator leaves an unused one alone, and DAVx5 discards a
-- definition of a zone Java knows.
--
-- A write of the rewritten body derives the columns a write of the stored one
-- does, so they are left as they are: the UTC form names the instant the
-- local time named, and the readers of dtstart, dtend and recurrence_start
-- trim the TZID they resolve. The one reading that does not trim, the local
-- times a zone skips, only lengthens a DAILY or WEEKLY COUNT series past a
-- finite recurrence_until, and the reopening above has already opened every
-- such bound whose DTSTART carries a TZID. Each rewritten row gets the ETag
-- PUT, the REST API and the UI give the octets they store, the lowercase hex
-- SHA-256 of their UTF-8, and is stamped by trg_events_touch_last_modified,
-- so the move below reaches its calendar.
--
-- The events rewritten and the lines left as stored are counted in a WARNING,
-- with the ids of the events holding the lines left, which PostgreSQL writes to
-- its log and the server to its own. A run that finds nothing to do says so in
-- a NOTICE. A run over rows already rewritten changes nothing, and reports the
-- same lines.
--
-- Cost: every body is unfolded and searched once for a TZID parameter whose
-- value is UTC, or has white space at an end, inside its quotes or out: every
-- body the rule changes or reports carries one, and a body naming zones
-- correctly, IANA's names, Windows names with spaces inside them and names
-- such as "(UTC+01:00) Amsterdam" included, does not. A body that has one
-- is read on its own, in a lateral subquery, so each sort and window covers
-- its lines alone and the cost stays linear in the table. It is split into
-- its logical lines; only a line that can be a BEGIN or END, a TZID parameter,
-- or a VTIMEZONE's TZID or offset is unfolded and matched, and only a line
-- naming a TZID parameter is parsed. The body is joined back from its lines
-- in order, so an unchanged line keeps its octets. No expression here uses a
-- capturing group: PostgreSQL evaluates one an order of magnitude more slowly
-- than a plain match. pattern spells out the white space class once for the
-- expressions that use it.
WITH pattern AS (
    SELECT '^' || space || '+|' || space || '+$' AS trim,
           ';' || space || '*TZID' || space || '*=(?:' || space || '|"' || space || '|"[^"\r\n]*(?:' || space || '"|"' || space || ')|[^";:\r\n]*'
               || space || '[;:]|(?:UTC|"' || space || '*UTC' || space || '*")' || space || '*[;:])' AS candidate,
           ';' || space || '*TZID' || space || '*=' AS tzid,
           '^' || space || '*TZID' || space || '*[;:]' AS timezone_name,
           '^' || space || '*(?:TZID|TZOFFSET(?:FROM|TO))' || space || '*[;:]' AS timezone_property,
           '^' || space || '*TZOFFSET(?:FROM|TO)' || space || '*[;:]' AS timezone_offset,
           '^' || space || '*[+-]0000(?:00)?' || space || '*$' AS zero_offset
    FROM (VALUES ('[\t\v\f \x85\xa0\x1680\x2000-\x200a\x2028\x2029\x202f\x205f\x3000]')) AS white(space)
),
repaired AS (
    SELECT events.id, body.text AS body, body.untouched_lines
    FROM events
    CROSS JOIN pattern
    CROSS JOIN LATERAL (
        -- A logical line runs to a line break that no space or tab follows,
        -- so it holds its folds and ends with its own break.
        WITH line AS (
            SELECT split.position, split.text,
                   CASE WHEN initial.letter = 'B' AND unfolded.line ~* '^BEGIN:' THEN 'B'
                        WHEN initial.letter = 'E' AND unfolded.line ~* '^END:' THEN 'E'
                   END AS kind,
                   CASE WHEN initial.letter = 'B' AND unfolded.line ~* '^BEGIN:' THEN upper(regexp_replace(substr(unfolded.line, 7), pattern.trim, '', 'g'))
                        WHEN initial.letter = 'E' AND unfolded.line ~* '^END:' THEN upper(regexp_replace(substr(unfolded.line, 5), pattern.trim, '', 'g'))
                   END AS component,
                   CASE WHEN (initial.parameters AND unfolded.line ~* pattern.tzid)
                          OR (initial.timezone_property AND unfolded.line ~* pattern.timezone_property)
                        THEN unfolded.line
                   END AS unfolded
            FROM regexp_split_to_table(events.raw_ical, $re$(?<=\n)(?![ \t])|(?<=\r)(?![\n \t])$re$)
                WITH ORDINALITY AS split(text, position)
            -- What a line can be, read off octets a fold cannot move: its
            -- first character, and whether it holds a ";" and an "=" at all.
            -- Only a line that can be a BEGIN or END, a TZID parameter's or a
            -- VTIMEZONE's TZID or offset is unfolded and matched; a name
            -- starting with white space strings.TrimSpace removes counts as
            -- either of the last two.
            CROSS JOIN LATERAL (
                SELECT upper(left(split.text, 1)) AS letter,
                       strpos(split.text, ';') > 0 AND strpos(split.text, '=') > 0 AS parameters,
                       upper(left(split.text, 1)) = 'T' OR ascii(upper(left(split.text, 1))) NOT BETWEEN 65 AND 90 AS timezone_property
                OFFSET 0
            ) AS initial
            CROSS JOIN LATERAL (
                SELECT CASE
                    WHEN NOT (initial.letter IN ('B', 'E') OR initial.parameters OR initial.timezone_property) THEN NULL
                    WHEN strpos(ending.content, E'\r') + strpos(ending.content, E'\n') = 0 THEN ending.content
                    ELSE regexp_replace(ending.content, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g')
                END AS line
                FROM (SELECT CASE
                    WHEN right(split.text, 2) = E'\r\n' THEN left(split.text, -2)
                    WHEN right(split.text, 1) IN (E'\r', E'\n') THEN left(split.text, -1)
                    ELSE split.text
                END AS content) AS ending
                OFFSET 0
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
            SELECT closing.position, closing.text, closing.unfolded, closing.timezone, closing.kind, closing.component,
                   COALESCE(closing.timezone > max(closing.position) FILTER (WHERE closing.closes) OVER (ORDER BY closing.position),
                            closing.timezone IS NOT NULL) AS in_timezone
            FROM closing
        ),
        -- The lines of each VTIMEZONE, each with how deep it stands inside it:
        -- 1 for a line of the VTIMEZONE itself, 2 for a line of a component
        -- directly inside it, and so on. A BEGIN opens a level and an END
        -- closes one, counted as ical.ReadComponents counts them for every
        -- well-formed definition; an END naming no open component, which the
        -- reader skips, is counted here, so a definition holding one misreads.
        zone_depth AS (
            SELECT scoped.position, scoped.timezone, scoped.unfolded, scoped.kind,
                   sum(CASE scoped.kind WHEN 'B' THEN 1 WHEN 'E' THEN -1 ELSE 0 END)
                       OVER (PARTITION BY scoped.timezone ORDER BY scoped.position) AS depth
            FROM scoped
            WHERE scoped.in_timezone
        ),
        -- ... and the line that opened the component directly inside the
        -- VTIMEZONE that each line is in, if any.
        zone_child AS (
            SELECT zone_depth.*,
                   max(zone_depth.position) FILTER (WHERE zone_depth.kind = 'B' AND zone_depth.depth = 2)
                       OVER (PARTITION BY zone_depth.timezone ORDER BY zone_depth.position) AS child
            FROM zone_depth
        ),
        -- The names of the zones the body defines away from zero that are
        -- UTC, compared without regard to case: each a VTIMEZONE both named
        -- UTC, by a TZID of its own, and carrying an offset other than zero
        -- on a STANDARD or DAYLIGHT directly inside it, as readers read a
        -- definition; an offset line elsewhere in it, in a component of some
        -- other name or nested deeper, places nothing. A name is spelled as
        -- its trimmed TZID. Readers look a value's TZID up among the
        -- definitions by its trimmed spelling, the first VTIMEZONE of that
        -- spelling answering, and take a name no definition answers for UTC
        -- itself, so a value is held exactly when its trimmed TZID is one of
        -- these names. Computed ahead of applies, which reads it to decide
        -- whether a value naming UTC is rewritten to its UTC form or only
        -- trimmed.
        zone_held AS MATERIALIZED (
            SELECT COALESCE(array_agg(chosen.name), ARRAY[]::text[]) AS names
            FROM (
                SELECT DISTINCT ON (zone.name) zone.name, zone.moves
                FROM (
                    SELECT line.timezone,
                           (array_agg(regexp_replace(definition.value, pattern.trim, '', 'g') ORDER BY line.position)
                               FILTER (WHERE line.depth = 1 AND line.unfolded ~* pattern.timezone_name))[1] AS name,
                           COALESCE(bool_or(line.depth = 2 AND opener.component IN ('STANDARD', 'DAYLIGHT')
                               AND line.unfolded ~* pattern.timezone_offset
                               AND definition.value !~ pattern.zero_offset), FALSE) AS moves
                    FROM zone_child AS line
                    LEFT JOIN scoped AS opener ON opener.position = line.child
                    -- Each property's value follows the first colon outside
                    -- quotes, as for the lines the rule applies to.
                    CROSS JOIN LATERAL (SELECT substring(line.unfolded FROM $re$^(?:[^:"]|"[^"]*")*$re$) AS key OFFSET 0) AS definition_key
                    CROSS JOIN LATERAL (
                        SELECT CASE WHEN substr(line.unfolded, length(definition_key.key) + 1, 1) = ':'
                                    THEN substr(line.unfolded, length(definition_key.key) + 2)
                               END AS value
                        OFFSET 0
                    ) AS definition
                    GROUP BY line.timezone
                ) AS zone
                WHERE zone.name IS NOT NULL
                ORDER BY zone.name, zone.timezone
            ) AS chosen
            WHERE chosen.moves AND upper(chosen.name) = 'UTC'
        ),
        -- Each line the rule applies to, read as the server reads it, and
        -- whether it has the form the repair rewrites.
        applies AS (
            SELECT scoped.position, property.name, params.utc, params.held,
                   property.written_name || params.rebuilt || ':' || property.value
                       || CASE WHEN params.utc AND NOT params.held THEN 'Z' ELSE '' END
                       || substr(scoped.text, length(scoped.unfolded) + 1) AS rewritten,
                   left(scoped.text, length(scoped.unfolded)) = scoped.unfolded
                       AND substr(scoped.text, length(scoped.unfolded) + 1) IN ('', E'\r\n', E'\r', E'\n')
                       AND property.written_name ~* '^(?:DTSTART|DTEND|DUE|RECURRENCE-ID|EXDATE|RDATE)$'
                       AND params.strict
                       AND params.tzids = 1
                       AND params.written ~ $re$^(?:"[^"]*"|[^";:,]*)$$re$
                       -- UTC held by a zone of the object's own (zone_held) is
                       -- only trimmed, never given a "Z", so the value's own
                       -- shape -- which only the "Z" rewrite has to respect --
                       -- does not bound whether such a line is rewritable.
                       AND (NOT params.utc OR params.held
                            OR (property.value ~ '^[0-9]{8}T[0-9]{6}$' AND params.date_time)) AS rewritable
            FROM scoped
            CROSS JOIN zone_held
            -- The key runs to the first colon outside quotes, as
            -- ical.SplitContentLine reads it.
            CROSS JOIN LATERAL (SELECT substring(scoped.unfolded FROM $re$^(?:[^:"]|"[^"]*")*$re$) AS key OFFSET 0) AS key_part
            CROSS JOIN LATERAL (
                SELECT substring(key_part.key FROM '^[^;]*') AS written_name,
                       upper(regexp_replace(substring(key_part.key FROM '^[^;]*'), pattern.trim, '', 'g')) AS name,
                       substr(scoped.unfolded, length(key_part.key) + 2) AS value
                OFFSET 0
            ) AS property
            CROSS JOIN LATERAL (
                SELECT (array_agg(param.written ORDER BY param.position) FILTER (WHERE param.tzid))[1] AS written,
                       (array_agg(param.effective = ANY(zone_held.names) ORDER BY param.position) FILTER (WHERE param.tzid))[1] AS held,
                       (array_agg(upper(param.effective) = 'UTC' ORDER BY param.position) FILTER (WHERE param.tzid))[1]
                           AND property.value ~ '[0-9]{8}T[0-9]{6}(?![0-9Z])' AS utc,
                       (array_agg(param.written NOT IN (param.effective, '"' || param.effective || '"') ORDER BY param.position)
                           FILTER (WHERE param.tzid))[1] AS untrimmed,
                       count(*) FILTER (WHERE param.tzid) AS tzids,
                       COALESCE(bool_and(param.text ~ $re$^[A-Za-z0-9-]+=(?:"[^"]*"|[^";:,]*)(?:,(?:"[^"]*"|[^";:,]*))*$$re$), TRUE) AS strict,
                       COALESCE(bool_and(upper(param.name) <> 'VALUE' OR upper(param.written) IN ('DATE-TIME', '"DATE-TIME"')), TRUE) AS date_time,
                       string_agg(CASE
                           WHEN NOT param.tzid THEN ';' || param.text
                           WHEN upper(param.effective) = 'UTC' AND NOT (param.effective = ANY(zone_held.names))
                                AND property.value ~ '[0-9]{8}T[0-9]{6}(?![0-9Z])' THEN ''
                           WHEN param.written LIKE '"%' THEN ';' || param.name || '="' || param.effective || '"'
                           ELSE ';' || param.name || '=' || param.effective
                       END, '' ORDER BY param.position) AS rebuilt
                FROM (
                    SELECT match.position, substr(match.parts[1], 2) AS text,
                           split_part(substr(match.parts[1], 2), '=', 1) AS name,
                           substr(match.parts[1], strpos(match.parts[1], '=') + 1) AS written,
                           strpos(match.parts[1], '=') > 0
                               AND upper(regexp_replace(split_part(substr(match.parts[1], 2), '=', 1), pattern.trim, '', 'g')) = 'TZID' AS tzid,
                           regexp_replace(CASE WHEN trimmed.value LIKE '"%"' THEN substr(trimmed.value, 2, length(trimmed.value) - 2)
                                               ELSE trimmed.value END, pattern.trim, '', 'g') AS effective
                    FROM regexp_matches(key_part.key, $re$;(?:[^;"]|"[^"]*")*$re$, 'g') WITH ORDINALITY AS match(parts, position)
                    CROSS JOIN LATERAL (
                        SELECT regexp_replace(substr(match.parts[1], strpos(match.parts[1], '=') + 1), pattern.trim, '', 'g') AS value
                        OFFSET 0
                    ) AS trimmed
                    OFFSET 0
                ) AS param
            ) AS params
            WHERE NOT scoped.in_timezone
              AND scoped.unfolded ~* pattern.tzid
              AND substr(scoped.unfolded, length(key_part.key) + 1, 1) = ':'
              AND property.name IN ('DTSTART', 'DTEND', 'DUE', 'RECURRENCE-ID', 'EXDATE', 'RDATE')
              AND (params.utc OR params.untrimmed)
        ),
        held AS MATERIALIZED (
            SELECT
                -- A DTSTART or RECURRENCE-ID naming UTC that cannot be
                -- rewritten.
                EXISTS (
                    SELECT 1
                    FROM applies
                    WHERE applies.utc AND NOT applies.held AND applies.name IN ('DTSTART', 'RECURRENCE-ID') AND NOT applies.rewritable
                ) AS identity
        ),
        -- The lines rewritten: those of the form the repair rewrites, less a
        -- DTSTART or RECURRENCE-ID the series' own identity holds back
        -- (held.identity), unless zone_held holds its name: zone_held already
        -- decided, inside applies.rewritten itself, whether such a line gets
        -- its UTC form or only a trim, and a trim changes no line's form, so
        -- the series' identity has nothing to keep of it.
        rewrite AS (
            SELECT applies.position, applies.rewritten
            FROM applies, held
            WHERE applies.rewritable
              AND (applies.name NOT IN ('DTSTART', 'RECURRENCE-ID') OR NOT applies.utc OR applies.held OR NOT held.identity)
        ),
        -- The lines DAVx5 may still show wrong once this runs: one the repair
        -- cannot rewrite at all, or one naming UTC that zone_held holds to
        -- a definition of that name -- trimmed above, perhaps, but ical4j drops a
        -- parameter named exactly UTC whether or not a same-named VTIMEZONE
        -- is present, so the line floats regardless -- or one a DTSTART or
        -- RECURRENCE-ID elsewhere in the series keeps in its own form.
        reported AS (
            SELECT applies.position
            FROM applies, held
            WHERE NOT applies.rewritable
               OR (applies.utc AND applies.held)
               OR (applies.utc AND NOT applies.held AND applies.name IN ('DTSTART', 'RECURRENCE-ID') AND held.identity)
        )
        SELECT string_agg(COALESCE(rewrite.rewritten, scoped.text), '' ORDER BY scoped.position) AS text,
               (SELECT count(*) FROM reported) AS untouched_lines
        FROM scoped
        LEFT JOIN rewrite ON rewrite.position = scoped.position
    ) AS body
    WHERE regexp_replace(events.raw_ical, E'(?:\\r\\n?|\\n)[ \\t]', '', 'g') ~* pattern.candidate
),
rewritten AS (
    UPDATE events
    SET raw_ical = repaired.body,
        etag = encode(sha256(convert_to(repaired.body, 'UTF8')), 'hex')
    FROM repaired
    WHERE events.id = repaired.id
      AND events.raw_ical <> repaired.body
    RETURNING events.id
)
SELECT (SELECT count(*) FROM rewritten),
       COALESCE(sum(repaired.untouched_lines), 0),
       count(*) FILTER (WHERE repaired.untouched_lines > 0),
       array_agg(repaired.id ORDER BY repaired.id) FILTER (WHERE repaired.untouched_lines > 0)
FROM repaired
INTO tzid_rewritten_events, tzid_untouched_lines, tzid_untouched_events, tzid_untouched_ids;

IF tzid_untouched_lines > 0 THEN
    RAISE WARNING 'v1.2.0 TZID repair: rewrote % events, and left % content lines in % events as stored',
        tzid_rewritten_events, tzid_untouched_lines, tzid_untouched_events
        USING DETAIL = 'events.id: ' || array_to_string(tzid_untouched_ids[1:1000], ', ')
                       || CASE WHEN tzid_untouched_events > 1000
                               THEN format(' and %s more', tzid_untouched_events - 1000) ELSE '' END,
              HINT = 'DAVx5 may show these events at the device''s local time. Each line is folded, '
                     'names UTC on an EXDATE or RDATE list or period, spells its parameters in a form '
                     'the repair does not rewrite, or is held with its series or by a definition of UTC '
                     'that moves the clock. Saving an event from a client rewrites it.';
ELSIF tzid_rewritten_events > 0 THEN
    RAISE WARNING 'v1.2.0 TZID repair: rewrote % events, and left 0 content lines in 0 events as stored',
        tzid_rewritten_events;
ELSE
    RAISE NOTICE 'v1.2.0 TZID repair: rewrote 0 events, and left 0 content lines in 0 events as stored';
END IF;

ALTER TABLE events ENABLE TRIGGER trg_events_increment_ctag;

UPDATE calendars
    SET ctag = ctag + 1,
        updated_at = GREATEST(clock_timestamp(), updated_at + interval '1 microsecond')
    WHERE id IN (SELECT calendar_id FROM events WHERE last_modified >= now());
END
$repairs$;

-- The two time-range expression indexes are built on the expressions
-- ListForCalendarFiltered filters on:
--
--     COALESCE(recurrence_until, dtend, 'infinity'::timestamptz)   >= range_start
--     COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz) <= range_end
--
-- The infinity sentinels keep a row the columns cannot bound in the candidate
-- set, where the RFC 4791 §9.9 test can judge it; the reasoning is on the
-- predicates in internal/store/postgres.go. An expression index only applies
-- when it matches the predicate verbatim, which is why the v1.1.7 pair, built
-- on other expressions, was dropped.
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
CREATE INDEX IF NOT EXISTS idx_events_calendar_keyset ON events (
    calendar_id, id,
    COALESCE(recurrence_until, dtend, 'infinity'::timestamptz),
    COALESCE(recurrence_start, dtstart, '-infinity'::timestamptz));

CREATE INDEX IF NOT EXISTS idx_contacts_book_keyset ON contacts (address_book_id, id);

-- sync-collection reads a collection's changes in stream order, member rows by
-- (last_modified, id) and tombstones by (deleted_at, id), a page at a time from
-- a cursor; internal/store/sync_changes.go carries why the id is part of the
-- position. The events index is built here, after the event repairs, and the
-- contacts index after the birthday repair at the end of the file.
CREATE INDEX IF NOT EXISTS idx_events_change_stream ON events (calendar_id, last_modified, id);

CREATE INDEX IF NOT EXISTS idx_deleted_resources_change_stream
    ON deleted_resources (resource_type, collection_id, deleted_at, id);

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

-- An event write moves its calendar's CTag, except while its transaction has
-- set calcard.defer_calendar_ctag to that calendar's id. A transaction does
-- that around writes of many members of one calendar and then moves the CTag
-- once itself: each update of one row within a transaction leaves a version
-- that later reads of the row step over, so a move per member would make the
-- write quadratic in its member count. Any other value defers nothing, and
-- set_config(..., true) ends the setting with the transaction.
CREATE OR REPLACE FUNCTION increment_calendar_ctag()
RETURNS TRIGGER AS $$
DECLARE
    deferred TEXT := current_setting('calcard.defer_calendar_ctag', true);
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF deferred IS DISTINCT FROM OLD.calendar_id::text THEN
            UPDATE calendars SET ctag = ctag + 1 WHERE id = OLD.calendar_id;
        END IF;
        INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name)
        VALUES ('event', OLD.calendar_id, OLD.uid, OLD.resource_name);
        RETURN OLD;
    ELSE
        IF deferred IS DISTINCT FROM NEW.calendar_id::text THEN
            UPDATE calendars SET ctag = ctag + 1 WHERE id = NEW.calendar_id;
        END IF;
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
-- can run again. Each update fires trg_contacts_touch_last_modified, which
-- stamps the contact after the old token, and each address book the repair
-- changed is then moved once, as the calendars are after the event repairs and
-- for the same reasons, so clients pick up the corrected birthday.
--
-- Each card is unfolded, split and matched once, in LATERAL function scans, and
-- only the first BDAY line is parsed further. OFFSET 0 keeps the derived steps
-- from being flattened into the expressions that read them, which would
-- evaluate each regex again for every reference.
DO $birthdays$
BEGIN
ALTER TABLE contacts DISABLE TRIGGER trg_contacts_increment_ctag;

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

ALTER TABLE contacts ENABLE TRIGGER trg_contacts_increment_ctag;

UPDATE address_books
    SET ctag = ctag + 1,
        updated_at = GREATEST(clock_timestamp(), updated_at + interval '1 microsecond')
    WHERE id IN (SELECT address_book_id FROM contacts WHERE last_modified >= now());
END
$birthdays$;

CREATE INDEX IF NOT EXISTS idx_contacts_change_stream ON contacts (address_book_id, last_modified, id);

-- The rewrites above leave events, contacts and acl_entries with columns no
-- statistics describe, and the first queries after the upgrade plan on them.
ANALYZE events, contacts, acl_entries;

UPDATE application SET value = 'v1.2.0' WHERE key = 'version';

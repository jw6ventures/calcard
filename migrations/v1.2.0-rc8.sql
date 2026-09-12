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

UPDATE application SET value = 'v1.2.0-rc8' WHERE key = 'version';

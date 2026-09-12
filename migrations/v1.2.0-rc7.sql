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

UPDATE application SET value = 'v1.2.0-rc7' WHERE key = 'version';

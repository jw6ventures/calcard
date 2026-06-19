-- v1.1.5: speed up object-level ACL visibility checks (calendars.list_accessible
-- and get_accessible). These previously matched ACL resource paths against events
-- with a per-row regexp_replace, which is unindexable and forced a scan of every
-- event in each calendar on PROPFIND of /dav/calendars/.
--
-- Introduce normalized, indexable join keys on both sides. resource_path_norm and
-- object_acl_path drop the trailing .ics/.vcf extension so a grant stored with or
-- without it lines up via plain equality. They are STORED generated columns, so
-- adding them backfills existing rows and keeps the normalization correct on every
-- future write with no application changes.

ALTER TABLE acl_entries
    ADD COLUMN IF NOT EXISTS resource_path_norm TEXT
    GENERATED ALWAYS AS (regexp_replace(resource_path, '\.(ics|vcf)$', '', 'i')) STORED;

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS object_acl_path TEXT
    GENERATED ALWAYS AS ('/dav/calendars/' || calendar_id::text || '/' || regexp_replace(resource_name, '\.ics$', '', 'i')) STORED;

CREATE INDEX IF NOT EXISTS idx_acl_principal_grant_norm
    ON acl_entries (principal_href, is_grant, resource_path_norm);

CREATE INDEX IF NOT EXISTS idx_events_object_acl_path
    ON events (object_acl_path);

UPDATE application SET value = 'v1.1.5' WHERE key = 'version';

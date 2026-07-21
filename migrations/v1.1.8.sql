-- v1.1.8: persistent DAV dead properties and scoped ACL lookup indexes.

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

CREATE INDEX IF NOT EXISTS idx_acl_resource_principal
    ON acl_entries (resource_path, principal_href);

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS object_acl_path TEXT
    GENERATED ALWAYS AS ('/dav/addressbooks/' || address_book_id::text || '/' || regexp_replace(resource_name, '\.vcf$', '', 'i')) STORED;

CREATE INDEX IF NOT EXISTS idx_contacts_object_acl_path
    ON contacts (object_acl_path);

UPDATE application SET value = 'v1.1.8' WHERE key = 'version';

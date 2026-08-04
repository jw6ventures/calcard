-- v1.1.11: ordered WebDAV ACLs and Digest credentials for newly issued app
-- passwords. The digest columns hold AES-256-GCM ciphertext under a key derived
-- from APP_SESSION_SECRET, not the bare HA1: an HA1 authenticates its holder
-- without the password, so a database read must not yield usable credentials.

ALTER TABLE acl_entries ADD COLUMN IF NOT EXISTS ace_order INTEGER NOT NULL DEFAULT 0;

WITH ordered AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY resource_path ORDER BY created_at, id) - 1 AS position
    FROM acl_entries
)
UPDATE acl_entries AS entry
SET ace_order = ordered.position
FROM ordered
WHERE entry.id = ordered.id;

DROP INDEX IF EXISTS idx_acl_unique;
CREATE INDEX IF NOT EXISTS idx_acl_resource_order ON acl_entries(resource_path, ace_order, id);

ALTER TABLE app_passwords ADD COLUMN IF NOT EXISTS digest_md5_ha1 TEXT;
ALTER TABLE app_passwords ADD COLUMN IF NOT EXISTS digest_sha256_ha1 TEXT;

UPDATE application SET value = 'v1.1.11' WHERE key = 'version';

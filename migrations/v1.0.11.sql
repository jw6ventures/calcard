-- v1.0.11: WebDAV LOCK/UNLOCK and ACL support, contact resource names

-- Lock storage for WebDAV Class 2/3 compliance
CREATE TABLE IF NOT EXISTS locks (
    id BIGSERIAL PRIMARY KEY,
    token TEXT NOT NULL UNIQUE,
    resource_path TEXT NOT NULL,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    lock_scope TEXT NOT NULL DEFAULT 'exclusive',
    lock_type TEXT NOT NULL DEFAULT 'write',
    depth TEXT NOT NULL DEFAULT '0',
    owner_info TEXT,
    timeout_seconds INT NOT NULL DEFAULT 3600,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_locks_resource ON locks(resource_path);
CREATE INDEX IF NOT EXISTS idx_locks_expires ON locks(expires_at);

-- ACL entries for WebDAV access control (RFC 3744)
CREATE TABLE IF NOT EXISTS acl_entries (
    id BIGSERIAL PRIMARY KEY,
    resource_path TEXT NOT NULL,
    principal_href TEXT NOT NULL,
    is_grant BOOLEAN NOT NULL DEFAULT TRUE,
    privilege TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_acl_resource ON acl_entries(resource_path);
CREATE INDEX IF NOT EXISTS idx_acl_principal ON acl_entries(principal_href);
DROP INDEX IF EXISTS idx_acl_unique;
CREATE UNIQUE INDEX IF NOT EXISTS idx_acl_unique ON acl_entries(resource_path, principal_href, privilege, is_grant);

-- Add resource_name to contacts for path-based lookup (mirrors events table pattern)
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS resource_name TEXT NOT NULL DEFAULT '';
UPDATE contacts SET resource_name = uid WHERE resource_name = '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_resource_name ON contacts(address_book_id, resource_name);

DO $$
DECLARE
    rec RECORD;
    candidate TEXT;
    suffix INT;
BEGIN
    FOR rec IN
        SELECT id, user_id, name
        FROM (
            SELECT id, user_id, name,
                   ROW_NUMBER() OVER (PARTITION BY user_id, LOWER(name) ORDER BY id) AS rn
            FROM address_books
        ) ranked
        WHERE rn > 1
        ORDER BY user_id, LOWER(name), id
    LOOP
        suffix := 1;
        LOOP
            candidate := rec.name || ' (' || rec.id::TEXT;
            IF suffix > 1 THEN
                candidate := candidate || '-' || suffix::TEXT;
            END IF;
            candidate := candidate || ')';

            EXIT WHEN NOT EXISTS (
                SELECT 1
                FROM address_books
                WHERE user_id = rec.user_id
                  AND id <> rec.id
                  AND LOWER(name) = LOWER(candidate)
            );

            suffix := suffix + 1;
        END LOOP;

        RAISE NOTICE 'Renaming duplicate address book for user %, "%" -> "%"', rec.user_id, rec.name, candidate;
        UPDATE address_books
        SET name = candidate
        WHERE id = rec.id;
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_address_books_user_name_lower ON address_books(user_id, LOWER(name));

-- Ensure contact deletion tombstones preserve the resource name
DROP FUNCTION IF EXISTS increment_address_book_ctag() CASCADE;

CREATE FUNCTION increment_address_book_ctag()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = OLD.address_book_id;
        INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name)
        VALUES ('contact', OLD.address_book_id, OLD.uid, OLD.resource_name);
        RETURN OLD;
    ELSE
        UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = NEW.address_book_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_contacts_increment_ctag ON contacts;
CREATE TRIGGER trg_contacts_increment_ctag
AFTER INSERT OR UPDATE OR DELETE ON contacts
FOR EACH ROW EXECUTE FUNCTION increment_address_book_ctag();

-- Bump application version
UPDATE application SET value = 'v1.0.11' WHERE key = 'version';

-- CalCard baseline schema (flattened migrations)

CREATE TABLE IF NOT EXISTS application (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO application (key, value)
VALUES ('version', 'v1.0.12')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Initial schema for CalCard
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    oauth_subject TEXT NOT NULL UNIQUE,
    primary_email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE calendars (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    color TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    calendar_id BIGINT NOT NULL REFERENCES calendars(id) ON DELETE CASCADE,
    uid TEXT NOT NULL,
    raw_ical TEXT NOT NULL,
    etag TEXT NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (calendar_id, uid)
);

CREATE TABLE address_books (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE contacts (
    id BIGSERIAL PRIMARY KEY,
    address_book_id BIGINT NOT NULL REFERENCES address_books(id) ON DELETE CASCADE,
    uid TEXT NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    raw_vcard TEXT NOT NULL,
    etag TEXT NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (address_book_id, uid)
);

CREATE TABLE app_passwords (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    label TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NULL,
    revoked_at TIMESTAMPTZ NULL,
    last_used_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_events_calendar_id ON events(calendar_id);
CREATE INDEX idx_contacts_address_book_id ON contacts(address_book_id);
CREATE INDEX idx_app_passwords_user_id ON app_passwords(user_id);

-- Automatically keep last_modified columns fresh for updates.
CREATE OR REPLACE FUNCTION touch_last_modified()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_touch_last_modified
BEFORE UPDATE ON events
FOR EACH ROW EXECUTE FUNCTION touch_last_modified();

CREATE TRIGGER trg_contacts_touch_last_modified
BEFORE UPDATE ON contacts
FOR EACH ROW EXECUTE FUNCTION touch_last_modified();

-- Scalability and usability improvements
CREATE INDEX idx_users_primary_email ON users(primary_email);

ALTER TABLE calendars ADD COLUMN description TEXT;
ALTER TABLE calendars ADD COLUMN timezone TEXT;
ALTER TABLE calendars ADD COLUMN ctag BIGINT NOT NULL DEFAULT 1;
ALTER TABLE calendars ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE address_books ADD COLUMN description TEXT;
ALTER TABLE address_books ADD COLUMN ctag BIGINT NOT NULL DEFAULT 1;
ALTER TABLE address_books ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

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

ALTER TABLE events ADD COLUMN summary TEXT;
ALTER TABLE events ADD COLUMN dtstart TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN dtend TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN all_day BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX idx_events_dtstart ON events(calendar_id, dtstart);
CREATE INDEX idx_events_last_modified ON events(calendar_id, last_modified DESC);

ALTER TABLE contacts ADD COLUMN display_name TEXT;
ALTER TABLE contacts ADD COLUMN primary_email TEXT;

CREATE INDEX idx_contacts_display_name ON contacts(address_book_id, display_name);
CREATE INDEX idx_contacts_last_modified ON contacts(address_book_id, last_modified DESC);

UPDATE contacts SET resource_name = uid WHERE resource_name = '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_resource_name ON contacts(address_book_id, resource_name);

CREATE TABLE deleted_resources (
    id BIGSERIAL PRIMARY KEY,
    resource_type TEXT NOT NULL,
    collection_id BIGINT NOT NULL,
    uid TEXT NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deleted_resources_lookup ON deleted_resources(resource_type, collection_id, deleted_at);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    user_agent TEXT,
    ip_address TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_expires_at ON sessions(expires_at);

CREATE OR REPLACE FUNCTION increment_calendar_ctag()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = OLD.calendar_id;
        INSERT INTO deleted_resources (resource_type, collection_id, uid)
        VALUES ('event', OLD.calendar_id, OLD.uid);
        RETURN OLD;
    ELSE
        UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = NEW.calendar_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_increment_ctag
AFTER INSERT OR UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION increment_calendar_ctag();

CREATE OR REPLACE FUNCTION increment_address_book_ctag()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = OLD.address_book_id;
        INSERT INTO deleted_resources (resource_type, collection_id, uid)
        VALUES ('contact', OLD.address_book_id, OLD.uid);
        RETURN OLD;
    ELSE
        UPDATE address_books SET ctag = ctag + 1, updated_at = NOW() WHERE id = NEW.address_book_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_contacts_increment_ctag
AFTER INSERT OR UPDATE OR DELETE ON contacts
FOR EACH ROW EXECUTE FUNCTION increment_address_book_ctag();

-- Add birthday field to contacts for birthdays calendar feature
ALTER TABLE contacts ADD COLUMN birthday DATE;

CREATE INDEX idx_contacts_birthday ON contacts(address_book_id, birthday) WHERE birthday IS NOT NULL;
CREATE INDEX idx_contacts_birthday_user ON contacts(birthday) WHERE birthday IS NOT NULL;

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

-- Add slug column for MKCALENDAR path mapping
ALTER TABLE calendars ADD COLUMN slug TEXT;

-- Track CalDAV resource names separately from UID
ALTER TABLE events ADD COLUMN resource_name TEXT;

UPDATE events
SET resource_name = uid
WHERE resource_name IS NULL;

ALTER TABLE events ALTER COLUMN resource_name SET NOT NULL;
ALTER TABLE events ADD CONSTRAINT events_calendar_resource_name_unique UNIQUE (calendar_id, resource_name);

-- Enforce case-insensitive uniqueness for calendar slugs.
UPDATE calendars SET slug = LOWER(slug) WHERE slug IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS calendars_slug_ci_unique ON calendars (user_id, LOWER(slug)) WHERE slug IS NOT NULL;

-- Track deleted resource names for CalDAV/CardDAV sync
ALTER TABLE deleted_resources ADD COLUMN resource_name TEXT;

UPDATE deleted_resources
SET resource_name = uid
WHERE resource_name IS NULL;

ALTER TABLE deleted_resources ALTER COLUMN resource_name SET NOT NULL;

-- Update calendar deletion trigger to capture resource_name
DROP FUNCTION IF EXISTS increment_calendar_ctag() CASCADE;

CREATE FUNCTION increment_calendar_ctag()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = OLD.calendar_id;
        INSERT INTO deleted_resources (resource_type, collection_id, uid, resource_name)
        VALUES ('event', OLD.calendar_id, OLD.uid, OLD.resource_name);
        RETURN OLD;
    ELSE
        UPDATE calendars SET ctag = ctag + 1, updated_at = NOW() WHERE id = NEW.calendar_id;
        RETURN NEW;
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_events_increment_ctag ON events;
CREATE TRIGGER trg_events_increment_ctag
AFTER INSERT OR UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION increment_calendar_ctag();

-- Update address book deletion trigger to include resource_name
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

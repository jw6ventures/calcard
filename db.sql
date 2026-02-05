-- CalCard baseline schema (flattened migrations)

CREATE TABLE IF NOT EXISTS application (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO application (key, value)
VALUES ('version', 'v1.0.5')
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

-- Shared calendars
CREATE TABLE calendar_shares (
    calendar_id BIGINT NOT NULL REFERENCES calendars(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    granted_by BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    editor BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (calendar_id, user_id)
);

CREATE INDEX idx_calendar_shares_user_id ON calendar_shares(user_id);

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
        VALUES ('contact', OLD.address_book_id, OLD.uid, OLD.uid);
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

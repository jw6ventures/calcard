-- Scalability and usability improvements

-- 1. Add missing index on users.primary_email for authentication lookups
CREATE INDEX idx_users_primary_email ON users(primary_email);

-- 2. Add CTag and updated_at to calendars for efficient sync token computation
ALTER TABLE calendars ADD COLUMN description TEXT;
ALTER TABLE calendars ADD COLUMN timezone TEXT;
ALTER TABLE calendars ADD COLUMN ctag BIGINT NOT NULL DEFAULT 1;
ALTER TABLE calendars ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- 3. Add description and CTag to address_books
ALTER TABLE address_books ADD COLUMN description TEXT;
ALTER TABLE address_books ADD COLUMN ctag BIGINT NOT NULL DEFAULT 1;
ALTER TABLE address_books ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- 4. Add parsed/indexed fields to events for server-side queries
ALTER TABLE events ADD COLUMN summary TEXT;
ALTER TABLE events ADD COLUMN dtstart TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN dtend TIMESTAMPTZ;
ALTER TABLE events ADD COLUMN all_day BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX idx_events_dtstart ON events(calendar_id, dtstart);
CREATE INDEX idx_events_last_modified ON events(calendar_id, last_modified DESC);

-- 5. Add parsed/indexed fields to contacts for server-side queries
ALTER TABLE contacts ADD COLUMN display_name TEXT;
ALTER TABLE contacts ADD COLUMN primary_email TEXT;

CREATE INDEX idx_contacts_display_name ON contacts(address_book_id, display_name);
CREATE INDEX idx_contacts_last_modified ON contacts(address_book_id, last_modified DESC);

-- 6. Create tombstone table for tracking deleted resources (required for proper sync)
CREATE TABLE deleted_resources (
    id BIGSERIAL PRIMARY KEY,
    resource_type TEXT NOT NULL,  -- 'event' or 'contact'
    collection_id BIGINT NOT NULL,  -- calendar_id or address_book_id
    uid TEXT NOT NULL,
    deleted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_deleted_resources_lookup ON deleted_resources(resource_type, collection_id, deleted_at);

-- 7. Create sessions table for database-backed session management
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

-- 8. Trigger to increment calendar ctag when events change
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

-- 9. Trigger to increment address_book ctag when contacts change
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

-- 10. Cleanup job helper: delete old tombstones (run periodically, e.g., via pg_cron)
-- DELETE FROM deleted_resources WHERE deleted_at < NOW() - INTERVAL '30 days';



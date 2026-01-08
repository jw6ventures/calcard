-- Track deleted resource names for CalDAV/CardDAV sync
ALTER TABLE deleted_resources ADD COLUMN resource_name TEXT;

UPDATE deleted_resources
SET resource_name = uid
WHERE resource_name IS NULL;

ALTER TABLE deleted_resources ALTER COLUMN resource_name SET NOT NULL;

-- Update calendar deletion trigger to capture resource_name
-- Explicitly drop and recreate for clarity and safety
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

-- Recreate trigger since DROP FUNCTION CASCADE removed it
DROP TRIGGER IF EXISTS trg_events_increment_ctag ON events;
CREATE TRIGGER trg_events_increment_ctag
AFTER INSERT OR UPDATE OR DELETE ON events
FOR EACH ROW EXECUTE FUNCTION increment_calendar_ctag();

-- Update address book deletion trigger to include resource_name
-- Explicitly drop and recreate for clarity and safety
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

-- Recreate trigger since DROP FUNCTION CASCADE removed it
DROP TRIGGER IF EXISTS trg_contacts_increment_ctag ON contacts;
CREATE TRIGGER trg_contacts_increment_ctag
AFTER INSERT OR UPDATE OR DELETE ON contacts
FOR EACH ROW EXECUTE FUNCTION increment_address_book_ctag();

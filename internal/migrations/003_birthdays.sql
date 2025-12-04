-- Add birthday field to contacts for birthdays calendar feature

-- 1. Add birthday column to contacts table
ALTER TABLE contacts ADD COLUMN birthday DATE;

-- 2. Create index for efficient birthday queries
CREATE INDEX idx_contacts_birthday ON contacts(address_book_id, birthday) WHERE birthday IS NOT NULL;

-- 3. Create index for user-wide birthday lookups (for dynamic birthdays calendar)
CREATE INDEX idx_contacts_birthday_user ON contacts(birthday)
    WHERE birthday IS NOT NULL;


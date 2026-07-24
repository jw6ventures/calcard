-- v1.1.9: persist display names from the OAuth profile.

ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT NOT NULL DEFAULT '';
ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT '';

UPDATE application SET value = 'v1.1.9' WHERE key = 'version';

-- v1.3.0: per-user first day of the week for the calendar grids. 0 is Sunday,
-- as JavaScript's Date.getDay() and Go's time.Weekday number it, so every
-- existing user keeps the Sunday-first layout they had.

ALTER TABLE users ADD COLUMN IF NOT EXISTS week_start SMALLINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'users'::regclass AND conname = 'users_week_start_check'
    ) THEN
        ALTER TABLE users ADD CONSTRAINT users_week_start_check CHECK (week_start BETWEEN 0 AND 6);
    END IF;
END $$;

UPDATE application SET value = 'v1.3.0' WHERE key = 'version';

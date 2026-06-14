-- v1.0.13: track completion of the first-login welcome tour

ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_completed_at TIMESTAMPTZ NULL;

UPDATE application SET value = 'v1.0.13' WHERE key = 'version';

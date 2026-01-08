-- Enforce case-insensitive uniqueness for calendar slugs.
UPDATE calendars SET slug = LOWER(slug) WHERE slug IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS calendars_slug_ci_unique ON calendars (user_id, LOWER(slug)) WHERE slug IS NOT NULL;

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
    user_id BIGINT NOT NULL REFERENCES users(id),
    name TEXT NOT NULL,
    color TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    calendar_id BIGINT NOT NULL REFERENCES calendars(id),
    uid TEXT NOT NULL,
    raw_ical TEXT NOT NULL,
    etag TEXT NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (calendar_id, uid)
);

CREATE TABLE address_books (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE contacts (
    id BIGSERIAL PRIMARY KEY,
    address_book_id BIGINT NOT NULL REFERENCES address_books(id),
    uid TEXT NOT NULL,
    raw_vcard TEXT NOT NULL,
    etag TEXT NOT NULL,
    last_modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (address_book_id, uid)
);

CREATE TABLE app_passwords (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
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

-- TODO: add triggers for updated timestamps and cascading deletes.

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

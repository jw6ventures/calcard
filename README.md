# CalCard

CalCard is a self-hosted CalDAV/CardDAV server scaffolded in Go. It exposes DAV endpoints, a minimal HTML admin UI, and an OAuth-centric authentication model with app passwords for DAV clients.

## Features
- WebDAV foundation with CalDAV and CardDAV hooks.
- OAuth-only web UI sessions plus per-user app passwords for DAV Basic Auth.
- PostgreSQL schema and repository layer for users, calendars, address books, events, contacts, and app passwords.
- Minimal server-rendered HTML pages for dashboard, calendars, address books, and app password management.
- Docker-friendly entrypoint via `cmd/server`.

## Configuration
Environment variables (prefix `APP_`):
- `APP_LISTEN_ADDR` (default `:8080`)
- `APP_BASE_URL` (e.g., `https://dav.example.com`)
- `APP_DB_DSN` (PostgreSQL DSN)
- `APP_OAUTH_CLIENT_ID`, `APP_OAUTH_CLIENT_SECRET`, `APP_OAUTH_ISSUER_URL`, `APP_OAUTH_REDIRECT_PATH`
- `APP_SESSION_SECRET` (signing key for sessions)

Database migrations live in `internal/migrations/001_init.sql`.

## Status
The server focuses on clear structure and extensibility. OAuth token exchange, CSRF, and DAV REPORT depth semantics are stubbed for follow-up work, but interfaces and storage primitives are ready for integration.

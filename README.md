# CalCard
CalCard is a self-hosted CalDAV/CardDAV server written in Go. It exposes DAV endpoints, a minimal HTML UI, and an OAuth-centric authentication model with app passwords for DAV clients.

## Installing
I am publishing a docker image publically at registry.jw6.us/public/calcard:beta

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

## Connecting a CalDAV/CardDAV client
- Sign in to the web UI once (via OAuth) before configuring a DAV client so the server can bootstrap your default calendar and address book.
- Generate an app-password in the WebUI to use in your DAV client
- Start service discovery from the DAV root at `<base-url>/dav` (recommended) or from the collection homes at `/dav/calendars/` and `/dav/addressbooks/`. Calendar collections live at `/dav/calendars/<calendar-id>/` (numeric IDs are visible in the web UI and PROPFIND responses).
- Authenticate with HTTP Basic Auth using your **primary email address** as the username and the generated **App Password** as the password. Other identifiers (display names, OAuth subject, etc.) are not accepted.
- Create and manage App Passwords from the web UI at `/app-passwords` after signing in through OAuth. Passwords can be revoked at any time; make sure the one you use is not expired or revoked.

## Health probes
- Liveness: `GET /healthz` returns immediately when the HTTP server is running, without touching dependencies.
- Readiness: `GET /readyz` checks connectivity to critical dependencies (currently PostgreSQL via `Store.HealthCheck`) and returns `503 Service Unavailable` until they are reachable.
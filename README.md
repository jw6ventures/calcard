# CalCard
CalCard is a self-hosted CalDAV/CardDAV server written in Go. It exposes DAV endpoints, a web UI, and requires OIDC authentication with app passwords for DAV clients.

## Installing
### Docker (Recommended)
I am publishing the following docker images publically:
| Image                                 	        | Branch     	| Notes                       	|
|---------------------------------------	        |------------	|-----------------------------	|
| docker pull ghcr.io/jw6ventures/calcard:latest 	| main       	| Automatic build after merge 	|
| docker pull ghcr.io/jw6ventures/calcard:beta   	| develop    	| Automatic build after merge 	|
| docker pull ghcr.io/jw6ventures/calcard:latest 	| tag/v1.0.0 	|                             	|

### Linux Installs
In the future I will publish a linux binary

## Features
- CalDAV and CardDAV server.
- OAuth-only web UI sessions plus per-user app passwords for DAV Basic Auth.
- PostgreSQL schema and repository layer for users, calendars, address books, events, contacts, and app passwords.
- Minimal server-rendered HTML pages for dashboard, calendars, address books, and app password management.

## Configuration
Environment variables:
| Name | Required | Notes |
| --- | --- | --- |
| `APP_LISTEN_ADDR` | false | (Default `:8080`) Bind address|
| `APP_BASE_URL` | false | (Default: `http://localhost:8080`) The URL that users will access for example: `https://calcard.example.com` |
| `APP_DB_DSN` | true | PostgreSQL DSN |
| `APP_OAUTH_CLIENT_ID` | true | Provided from IDP |
| `APP_OAUTH_CLIENT_SECRET` | true | Provided from IDP |
| `APP_OAUTH_ISSUER_URL` | true | Provided from IDP |
| `APP_OAUTH_DISCOVERY_URL` | true | Provided from IDP |
| `APP_SESSION_SECRET` | true | Must be at least 32 characters long |
| `APP_TRUSTED_PROXIES` | false | If none are specified, CalCard trusts all proxies - Not recommended for pubic environments |


## Connecting a CalDAV/CardDAV client
- Sign in to the web UI
- Generate an app-password to use in your DAV client
- Start service discovery from the DAV root at `<base-url>/dav` (recommended) or from the collection homes at `/dav/calendars/` and `/dav/addressbooks/`. Calendar collections live at `/dav/calendars/<calendar-id>/` (numeric IDs are visible in the web UI and PROPFIND responses).
- Authenticate with HTTP Basic Auth using your **primary email address** as the username and the generated **App Password** as the password. Other identifiers (display names, OAuth subject, etc.) are not accepted.
- Create and manage App Passwords from the web UI at `/app-passwords` after signing in through OAuth. Passwords can be revoked at any time; make sure the one you use is not expired or revoked.

## Health probes
- Liveness: `GET /healthz` returns immediately when the HTTP server is running, without touching dependencies.
- Readiness: `GET /readyz` checks connectivity to critical dependencies and returns `503 Service Unavailable` until they are reachable.

## License
CalCard is dual-licensed:
- **AGPLv3** for open-source use
- **Commercial License** for proprietary or hosted SaaS use

If you run CalCard as a network service and do not provide full corresponding
source code to users interacting with it, you must obtain a commercial license.

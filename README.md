# CalCard

CalCard is a self-hosted CalDAV/CardDAV server written in Go. It exposes DAV endpoints, a web UI, and requires OIDC authentication with the ability to generate revokable app passwords for DAV clients.

## Installing

Copy the .env.template file from the root of this repository, rename to .env, and modify the values to match your environment.

### Docker (Recommended)

| Image                                  | Branch     	| Notes                       	|
|--------------------------------------- |------------	|-----------------------------	|
| ghcr.io/jw6ventures/calcard:latest 	 | main       	| Latest stable release. 	|
| ghcr.io/jw6ventures/calcard:beta   	 | develop    	| Pre-release.	|
| ghcr.io/jw6ventures/calcard:v1.0.x 	 | tag/v1.0.x 	| Refer to github release for latest patch version |

#### Docker Run

```docker run -p 8080 --env-file .env ghcr.io/jw6ventures/calcard:latest```
You'll also need a postgres 16 server:

#### Docker Compose

```
services:
  postgres:
    image: postgres:16
    restart: unless-stopped
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: app
    volumes:
      - postgres_data:/var/lib/postgresql/data

  app:
    image: ghcr.io/jw6ventures/calcard:latest
    restart: unless-stopped
    depends_on:
      - postgres
    env_file:
      - .env
    ports:
      - "8081:8080"

volumes:
  postgres_data:
```

### Linux Installs
A linux binary is published as a github release for each version. You'll need a postgres 16 server.
```
source .env
./calcard-linux-amd64
```

## Features
- CalDAV and CardDAV server.
- OAuth-only web UI sessions plus per-user app passwords for DAV Basic Auth.
- PostgreSQL schema and repository layer for users, calendars, address books, events, contacts, and app passwords.
- Web interface for dashboard, calendars, address books, and app password management.

## Configuration
Environment variables:
| Name | Required | Notes |
| --- | --- | --- |
| `APP_LISTEN_ADDR` | false | (Default `:8080`) Bind address|
| `APP_BASE_URL` | false | (Default: `http://localhost:8080`) The URL that users will access for example: `https://calcard.example.com` |
| `APP_DB_DSN` | true | PostgreSQL DSN (ex. `postgres://postgres:postgres@postgres:5432/app?sslmode=disable` )|
| `APP_OAUTH_CLIENT_ID` | true | Provided from IDP |
| `APP_OAUTH_CLIENT_SECRET` | true | Provided from IDP |
| `APP_OAUTH_ISSUER_URL` | one of two | Provided from IDP. Used if `APP_OAUTH_DISCOVERY_URL` is not set. |
| `APP_OAUTH_DISCOVERY_URL` | one of two | Provided from IDP. Overrides `APP_OAUTH_ISSUER_URL` when set. |
| `APP_SESSION_SECRET` | true | Must be at least 32 characters long (ex. openssl rand -base64 32) |
| `APP_TRUSTED_PROXIES` | false | If none are specified, CalCard trusts all proxies - Not recommended for public environments |


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

CalCard is licensed under the [Source First License](https://sourcefirst.com/).

Commercial use requires a commercial agreement with JW6 Ventures LLC - [License Request Form](https://jw6ventures.atlassian.net/helpcenter/CSM/contact-us/9a6bbea8-202d-498f-8462-52c2ee8ab09e).

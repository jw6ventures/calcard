# CalCard 📅

CalCard makes it easy to self-host and share calendars, reminders, and contacts.

## Features

* **Full CalDAV/CardDAV RFC compliance** - Works seamlessly with your devices. Supports iOS, Android, macOS, Windows, and Linux.
* **Self-hosted** - You own your data.
* **Single sign-on (SSO)** - Sign into your existing identity service to access the website and manage your CalCard account.
* **App passwords** - Generate passwords to connect devices to your account.
* **Shared calendars** - Share calendars with other users.

## Prerequisites

CalCard requires:

* Any OAuth 2.0 compatible identity service. CalCard has been tested to be compatible with:
    - [Authentik](https://goauthentik.io/)
    - [Keycloak](https://www.keycloak.org/)
* A PostgresQL database. If you don't have one, see the install instructions below for your environment.

## Install

* [Docker (Recommended)](#docker-recommended)
* [Kubernetes](#kubernetes)

### Docker (Recommended)

Copy the .env.template file from the root of this repository, rename to .env, and modify the values to match your environment.

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

### Kubernetes

The Kubernetes Helm chart is published to GHCR as an OCI artifact at `ghcr.io/jw6ventures/calcard-helm`.

1. Create a values file with your configuration. See the [default values](./deploy/helm/calcard/values.yaml).
2. Install or upgrade:

```
helm upgrade --install calcard oci://ghcr.io/jw6ventures/calcard-helm -f values.yaml
```

Notes:
- The ingress host is derived from `app.baseUrl` (APP_BASE_URL) when `ingress.host` is empty.
- `app.baseUrl` is required for the chart to render.
- Postgres is disabled by default. Set `postgres.enabled=true` to deploy Postgres with a 500Mi PVC (see `postgres.persistence.size`).
- When using an existing database secret, set `app.db.existingSecret.name`. The secret must contain `APP_DB_DSN`, `APP_DB_USER`, and `APP_DB_PASSWORD` by default, or the keys configured under `app.db.existingSecret`.
- When using an external database without `app.db.existingSecret.name`, set `app.db.host`.
- Secrets are stored in Kubernetes Secrets (`APP_OAUTH_CLIENT_SECRET`, `APP_SESSION_SECRET`, `APP_DB_DSN`, `APP_DB_USER`, `APP_DB_PASSWORD`) unless the corresponding existing-secret settings are used.

### Linux Installs
A linux binary is published as a github release for each version. You'll need a postgres 16 server.
```
source .env
./calcard-linux-amd64
```

## Configuration
Environment variables:
| Name | Required | Notes |
| --- | --- | --- |
| `APP_LISTEN_ADDR` | false | (Default `:8080`) Bind address|
| `APP_BASE_URL` | false | (Default: `http://localhost:8080`) The URL that users will access for example: `https://calcard.example.com` |
| `APP_DB_DSN` | true | PostgreSQL DSN (ex. `postgres://postgres:postgres@postgres:5432/app?sslmode=disable` ). Required unless you provide `APP_DB_HOST`, `APP_DB_NAME`, `APP_DB_USER`, and `APP_DB_PASSWORD`. |
| `APP_DB_HOST` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_NAME` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_USER` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_PASSWORD` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_PORT` | false | (Default `5432`) Used when not providing `APP_DB_DSN`. |
| `APP_DB_SSLMODE` | false | (Default `disable`) Used when not providing `APP_DB_DSN`. |
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

Commercial use requires a commercial agreement with JW6 Ventures LLC - [License Request Form](https://jw6ventures.atlassian.net/helpcenter/CSM/contact-us/9a6bbea8-202d-498f-8462-52c2ee8ab09e). Pricing is dependent on organization size, many small business qualify for $0/yr license costs. 

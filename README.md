# CalCard - Community Edition 📅

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

| Image | Published from | Notes |
| --- | --- | --- |
| `ghcr.io/jw6ventures/calcard:latest` | Stable release tags (`vX.Y.Z`), and manual workflow runs on `main` | Latest stable release. |
| `ghcr.io/jw6ventures/calcard:beta` | The `develop` branch and pre-release tags (`vX.Y.Z-rcN`, `-betaN`, `-alphaN`) | Pre-release. Moves on every `develop` build. |
| `ghcr.io/jw6ventures/calcard:vX.Y.Z` | That release tag | Pinned release. See the GitHub release for the latest patch version. |
| `ghcr.io/jw6ventures/calcard:vX.Y.Z-rcN` | That pre-release tag (likewise `-betaN`, `-alphaN`) | Pinned pre-release. |

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
- The app Deployment uses the `Recreate` strategy: an upgrade stops the running pod before the new one starts, and the new pod applies schema migrations before it serves. Expect downtime on every upgrade for the migration plus startup.
- `replicaCount` must be `0` or `1`. Every pod runs the schema migration on startup and the migration runner takes no lock, so the chart (and its values schema) refuses anything else. An upgrade of a release that set `replicaCount` above 1 fails until it is set back to 1. `kubectl scale` and a HorizontalPodAutoscaler bypass the chart, so do not use either on this Deployment. Set `0` to stop the app without uninstalling the release.
- Migrations run before the listener opens, under a startup probe. The default budget is `startupProbe.periodSeconds` 10 x `startupProbe.failureThreshold` 60 = 10 minutes. A migration that overruns it is killed and rolled back, and the pod restarts into the same work and never becomes ready. Above roughly 500,000 events (or 250,000 when event bodies are large, around 10 KB), raise `startupProbe.failureThreshold` before upgrading (for example `180` for 30 minutes).
- `priorityClassName` schedules the app pods with an existing PriorityClass, making them a later candidate for eviction under node pressure. Leave it empty for the cluster default.

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
| `APP_COMMUNITY_URL` | false | (Default: `https://github.com/jw6ventures/calcard/issues`) Link used by the "Reach out to the community" buttons on the Help page and welcome tour |
| `APP_DB_DSN` | true | PostgreSQL DSN (ex. `postgres://postgres:postgres@postgres:5432/app?sslmode=disable` ). Required unless you provide `APP_DB_HOST`, `APP_DB_NAME`, `APP_DB_USER`, and `APP_DB_PASSWORD`. |
| `APP_DB_HOST` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_NAME` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_USER` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_PASSWORD` | true | Required when not providing `APP_DB_DSN`. |
| `APP_DB_PORT` | false | (Default `5432`) Used when not providing `APP_DB_DSN`. |
| `APP_DB_SSLMODE` | false | (Default `disable`) Used when not providing `APP_DB_DSN`. |
| `APP_DB_MAX_OPEN_CONNS` | false | (Default `25`) Maximum number of open database connections. |
| `APP_DB_MAX_IDLE_CONNS` | false | (Default `10`) Maximum number of idle database connections kept in the pool. |
| `APP_DB_CONN_MAX_LIFETIME` | false | (Default `30m`) Maximum lifetime of a database connection before it is recycled (Go duration, ex. `30m`). |
| `APP_OAUTH_CLIENT_ID` | true | Provided from IDP |
| `APP_OAUTH_CLIENT_SECRET` | true | Provided from IDP |
| `APP_OAUTH_ISSUER_URL` | one of two | Provided from IDP. Used if `APP_OAUTH_DISCOVERY_URL` is not set. |
| `APP_OAUTH_DISCOVERY_URL` | one of two | Provided from IDP. Overrides `APP_OAUTH_ISSUER_URL` when set. |
| `APP_OAUTH_REDIRECT_PATH` | false | (Default `/auth/callback`) OAuth callback path, appended to `APP_BASE_URL`. Must match the redirect URI registered with the IDP. Must be a clean absolute path that no other CalCard route uses (not `/`, and not under `/auth`, `/dav`, `/api`, `/calendars` and so on); startup fails otherwise. |
| `APP_SESSION_SECRET` | true | Must be at least 32 characters long (ex. openssl rand -base64 32) |
| `APP_TRUSTED_PROXIES` | false | If none are specified, CalCard trusts all proxies - Not recommended for public environments |
| `APP_HTTP_READ_TIMEOUT` | false | (Default `15s`) Maximum duration for reading an entire request, including the body (Go duration). |
| `APP_HTTP_WRITE_TIMEOUT` | false | (Default `15s`) Maximum duration before timing out writes of the response (Go duration). |
| `APP_HTTP_IDLE_TIMEOUT` | false | (Default `60s`) Maximum time to wait for the next request on a keep-alive connection (Go duration). |
| `APP_TRAFFIC_CAPTURE_FILE` | false | Enables diagnostic request capture when set to a JSONL file path. Captures method, path, headers, request body (up to 10 MiB), client IP, timing offset, and response status. Basic credentials are replaced with `${CALCARD_DAV_BASIC_AUTH}` and cookies/tokens are redacted, but calendar, contact, and other body data remains sensitive. The file is appended with `0600` permissions; move or remove it before starting a distinct capture session. |
| `APP_PPROF_ENABLED` | false | (Default `false`) Exposes `net/http/pprof` profiling on a dedicated debug listener (`APP_PPROF_ADDR`). The endpoints leak runtime internals and can be used to DoS the process - keep the listener on loopback and reach it via an SSH tunnel. |
| `APP_PPROF_ADDR` | false | (Default `127.0.0.1:6060`) Bind address for the pprof debug listener when `APP_PPROF_ENABLED` is true. Keep on loopback. |
| `APP_PROMETHEUS_ENDPOINT_ENABLED` | false | (Default `false`) Serves Prometheus metrics at `/metrics` on the main listener. |
| `LOG_LEVEL` | false | (Default `Info`) Minimum log level: `Trace`, `Debug`, `Info`, `Warn`, `Error`, or `Fatal`. Case-sensitive; any other value falls back to `Info`. |

Boolean variables accept `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`.

CalDAV/CardDAV resource limits. Each bounds the work one request can provoke; a request past a limit is refused rather than answered with a partial result. Integer limits must be non-negative, and `0` turns that limit off.
| Name | Required | Notes |
| --- | --- | --- |
| `APP_DAV_PROPFIND_INFINITY_ENABLED` | false | (Default `true`) Allow `Depth: infinity` PROPFIND. Disable where deep listings are a DoS concern. |
| `APP_DAV_DIGEST_ENABLED` | false | (Default `false`) Offer and accept HTTP Digest on `/dav`. See [Connecting a CalDAV/CardDAV client](#connecting-a-caldavcarddav-client). |
| `APP_DAV_MAX_MULTISTATUS_RESPONSES` | false | (Default `10000`) `DAV:response` elements one multistatus may carry. |
| `APP_DAV_MAX_MULTISTATUS_BYTES` | false | (Default `67108864`) Encoded size of one multistatus response. |
| `APP_DAV_MAX_FILTER_ELEMENTS` | false | (Default `100`) Elements one `CALDAV:filter` or `CARDDAV:filter` may carry. |
| `APP_DAV_MAX_CARDDAV_QUERY_BYTES` | false | (Default `65536`) Combined serialized `CARDDAV:filter` and `CARDDAV:address-data` bytes per REPORT. |
| `APP_DAV_MAX_ADDRESS_DATA_PROPERTIES` | false | (Default `100`) `CARDDAV:address-data` property selectors per REPORT. |
| `APP_DAV_MAX_REPORT_ELEMENT_DEPTH` | false | (Default `20`) Element nesting one REPORT body may reach. |
| `APP_DAV_MAX_MULTIGET_HREFS` | false | (Default `5000`) `DAV:href` elements one calendar-multiget may carry. |
| `APP_DAV_MAX_REPORT_CANDIDATE_ROWS` | false | (Default `50000`) Stored resources one report will read and parse while looking for matches. |
| `APP_DAV_SYNC_HISTORY_RETENTION` | false | (Default `2160h`, 90 days; Go duration) How far back sync deletion history is kept. A sync token older than this is refused with `DAV:valid-sync-token` and the client re-syncs in full. `0` prunes nothing. Widening it re-admits tokens whose deletions were already pruned, so widen only when no client holds a token older than the previous window. |


## Connecting a CalDAV/CardDAV client
- Sign in to the web UI
- Generate an app-password to use in your DAV client
- Start service discovery from the DAV root at `<base-url>/dav` (recommended) or from the collection homes at `/dav/calendars/` and `/dav/addressbooks/`. Calendar collections live at `/dav/calendars/<calendar-id>/` (numeric IDs are visible in the web UI and PROPFIND responses).
- Authenticate with HTTP Basic over HTTPS, or HTTP Digest (SHA-256 or MD5) where the server offers it, using your **primary email address** as the username and the generated **App Password** as the password. Other identifiers (display names, OAuth subject, etc.) are not accepted.
- When TLS terminates at a reverse proxy, forward `X-Forwarded-Proto: https` and include the proxy's peer address or CIDR in `APP_TRUSTED_PROXIES`. Basic authentication is rejected when CalCard cannot verify that the request used HTTPS.
- Digest is off by default and is enabled with `APP_DAV_DIGEST_ENABLED=true`. Digest verifies against a stored HA1, which is password-equivalent for the DAV realm. CalCard seals it with a key derived from `APP_SESSION_SECRET`, so a copy of the database plus a copy of the configuration recovers it, while a bcrypt token hash on its own is worth nothing to whoever holds the database. Leaving Digest off keeps app passwords stored as bcrypt hashes alone: no HA1 is computed or written, not when a password is issued and not when one authenticates. Any HA1 an earlier run stored is cleared in the background right after startup and again every hour, so a pass that fails (for example while the database is unreachable) is retried rather than dropped; an app password that authenticates over Basic in the meantime has its HA1 cleared at that moment.
- Turning Digest on does not require replacing app passwords. One issued from then on gets its HA1 when it is issued; an existing one gets it the first time it authenticates over Basic, which CalCard accepts only over HTTPS -- a bcrypt hash cannot be converted into an HA1, but a successful Basic authentication supplies the password itself. If DAV is served without HTTPS, existing passwords cannot catch up this way; revoke and reissue any that need Digest. While Digest is on, `/app-passwords` marks each active password **Digest ready** or **Basic only** so you can see which have caught up; clients holding a **Basic only** password keep working over Basic in the meantime.
- Keep `APP_SESSION_SECRET` stable and identical on every instance, because it protects stored Digest credentials. Rotating it leaves every stored HA1 unreadable: those passwords go back to **Basic only** and the server logs once that it could not open them. Over HTTPS, each is re-sealed the next time it authenticates over Basic; without HTTPS, Basic is refused, so affected app passwords must be revoked and reissued. Instances running with different secrets keep re-sealing each other's credentials.
- Correcting proxy forwarding restores Basic access without replacing passwords.
- Create and manage App Passwords from the web UI at `/app-passwords` after signing in through OAuth. Passwords can be revoked at any time; make sure the one you use is not expired or revoked.

## Upgrading to 1.2.0
- The v1.2.0 migration runs as one transaction and holds table locks on `events`, `contacts` and the other tables it alters while it repairs recurrence data and rebuilds indexes; those tables are closed to reads and writes until it finishes. On an installation of v1.1.7 or earlier, or of v1.1.9, it also makes the v1.1.6–v1.1.9 schema changes those releases did not ship, which rewrites `events`, `contacts` and `acl_entries` once. Its duration is startup time, so check the startup-probe budget first (see the [Kubernetes](#kubernetes) notes). A run that fails part way rolls back and can be repeated. `migrations/v1.2.0.sql` describes how to apply it by hand with concurrent index builds where that pause is not acceptable.
- ACLs and dead properties stored for a DAV resource whose name contains a percent-encoded sequence (`%XX`, e.g. `team%20notes.ics`) were keyed under a doubly-decoded spelling of its path and no longer apply after the upgrade. They are not re-keyed automatically, because a sibling with the decoded name may exist. Re-apply the ACLs and dead properties for such resources after upgrading. Locks on them expire on their own.
- A database that has run `v1.2.0-rc9` or `v1.2.0-rc10`, whether installed fresh or upgraded to it, is already stamped as `v1.2.0`, so the release will not run the final `migrations/v1.2.0.sql` on it and it lacks that file's sync-stamp triggers and birthday backfill. Apply the file from the `v1.2.0` release once by hand, with the application stopped: `psql -1 -f migrations/v1.2.0.sql` against the CalCard database. The file is idempotent. Databases that never ran rc9 or rc10 migrate automatically.
- The Helm chart now ships a `values.schema.json`, and `helm install`/`upgrade` refuses values of the wrong type that older charts accepted:
  - `replicaCount` must be the integer `0` or `1`. A quoted `"1"`, `--set-string replicaCount=1`, and `replicaCount: null` are refused.
  - `startupProbe.periodSeconds` and `startupProbe.failureThreshold` must be unquoted integers of at least 1.
  - `ingress.enabled`, `ingress.tls.enabled`, `postgres.enabled` and `postgres.persistence.enabled` must be real booleans, not the strings `"true"`/`"false"` (so not `--set-string`).
  - `image.tag`, `priorityClassName` and the other name fields must be strings. Quote a numeric tag in a values file (`tag: "123"`) or use `--set-string image.tag=123`.
  - `image.pullPolicy` must be `Always`, `IfNotPresent` or `Never`, and `service.type` must be `ClusterIP`, `NodePort` or `LoadBalancer`.

## Health probes
- Liveness: `GET /healthz` returns immediately when the HTTP server is running, without touching dependencies.
- Readiness: `GET /readyz` checks connectivity to critical dependencies and returns `503 Service Unavailable` until they are reachable.

## License

CalCard Community Edition is licensed using the GNU Affero General Public License (AGPL).

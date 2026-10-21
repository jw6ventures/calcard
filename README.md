# CalCard

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

Copy the .env.example file from the root of this repository, rename to .env, and modify the values to match your environment.

| Image | Published from | Notes |
| --- | --- | --- |
| `ghcr.io/jw6ventures/calcard:latest` | Stable release tags (`vX.Y.Z`), and manual workflow runs on `main` | Latest stable release. |
| `ghcr.io/jw6ventures/calcard:beta` | The `develop` branch and pre-release tags (`vX.Y.Z-rcN`, `-betaN`, `-alphaN`) | Pre-release. Moves on every `develop` build. |
| `ghcr.io/jw6ventures/calcard:vX.Y.Z` | That release tag | Pinned release. See the GitHub release for the latest patch version. |
| `ghcr.io/jw6ventures/calcard:vX.Y.Z-rcN` | That pre-release tag (likewise `-betaN`, `-alphaN`) | Pinned pre-release. |

#### Docker Run

```docker run -p 8080:8080 --env-file .env ghcr.io/jw6ventures/calcard:latest```
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
- The app uses the `Recreate` strategy, so expect a short outage on every upgrade while the new pod updates the database and starts.
- `replicaCount` must be `0` or `1`, because every pod updates the database when it starts. Don't scale the Deployment with `kubectl scale` or an autoscaler. Set `0` to stop the app without uninstalling it.
- Database updates must finish within the startup probe's budget, 10 minutes by default (`startupProbe.periodSeconds` × `startupProbe.failureThreshold`). If one runs longer, the pod restarts and never becomes ready. For a large database, raise `startupProbe.failureThreshold` before upgrading. See [Upgrading to 1.2.0](#upgrading-to-120).
- `priorityClassName` schedules the app pods with an existing PriorityClass, making them a later candidate for eviction under node pressure. Leave it empty for the cluster default.

### Linux Installs
A linux binary is published as a github release for each version. You'll need a postgres 16 server.
```
set -a; . ./.env; set +a
./calcard-linux-amd64
```

## Upgrading to 1.2.0
Read this before upgrading from 1.1.x.

### Before you upgrade
- **Back up your database.** 1.2.0 changes your data, and there's no downgrade. To go back to 1.1.x, restore the backup. Don't start 1.1.x against an upgraded database, because upgrading again won't repair what it writes.
- **Put HTTPS in front of CalCard.** DAV clients can no longer sign in over plain HTTP. See [Basic authentication requires HTTPS](#basic-authentication-requires-https).
- **Check `APP_BASE_URL`.** It must match the public URL your clients use, including the port. If it's wrong behind a proxy, moving, copying and editing locked items can fail.
- **Kubernetes:** see [Helm](#helm) below.

### The first start
The first start updates the database before CalCard starts serving. This takes about 30 seconds per 100,000 events, and longer when events are large.

When it's done, check CalCard's log for lines starting with `PostgreSQL WARNING:`. They list anything the upgrade changed or couldn't fix on its own.

If you run `migrations/v1.2.0.sql` yourself, stop every CalCard instance first.

### Sharing permissions on events and contacts
1.1.x sometimes applied the permissions set on one event or contact to others with a similar name (`X`, `X.ics` and `X.ICS`). 1.2.0 treats each as its own item. The upgrade:
- keeps every deny that applied before;
- removes grants that no longer belong to a single item, and saves them in the `acl_entries_v120_removed` table;
- has clients re-sync the items whose access changed.

If someone lost access they should have, share the item with them again, then drop the table. The upgrade's warnings also list items with unusual names (containing `?`, `#`, `%` escapes, or `.` and `..` segments) for you to look over.

### Events stored in UTC
DAVx5 showed times stored with `TZID=UTC` at the device's local time instead of UTC. The upgrade rewrites these times as plain UTC, and every new save is stored the same way. Clients re-sync the events it changes.

A few unusual events can't be rewritten automatically. The upgrade's warning lists them, and saving one from any client fixes it.

### Basic authentication requires HTTPS
DAV clients and the REST API now use Basic authentication only over HTTPS. Over plain HTTP, such as on localhost or your LAN, clients aren't asked for a password and report a failed login.

CalCard doesn't serve HTTPS itself. Put it behind a reverse proxy, or the Helm chart's ingress (`ingress.tls.enabled`), that:
- terminates TLS;
- sends `X-Forwarded-Proto: https` (nginx: `proxy_set_header X-Forwarded-Proto $scheme;`);
- connects from an address listed in `APP_TRUSTED_PROXIES`.

Once the proxy is set up, existing app passwords work again. There's no need to replace them.

If you must keep plain HTTP, set `APP_DAV_INSECURE_BASIC_ENABLED=true` (Helm: `app.dav.insecureBasicEnabled`). This isn't recommended: app passwords and all your data cross the network unencrypted, and CalCard logs a warning on every start.

### Helm
- **Large databases:** the first start must finish within the startup probe budget. Above about 400,000 events (200,000 if your events are large), set `startupProbe.failureThreshold` to `180` (30 minutes) before upgrading.
- **The Service must be `ClusterIP` when the ingress is on.** The chart refuses to render otherwise, because a NodePort or LoadBalancer beside the ingress would expose plain HTTP. Set `service.type: ClusterIP`, or turn off `ingress.enabled` and terminate TLS yourself.
- **Releases first installed with Helm 4** fail to upgrade with `spec.strategy.rollingUpdate: Forbidden`. Run this one upgrade with `--server-side=false`. Later upgrades need no flag.
  ```
  helm upgrade calcard oci://ghcr.io/jw6ventures/calcard-helm --reuse-values --server-side=false
  ```

### Other changes
- **Locks apply everywhere.** When a CalDAV or CardDAV client locks an event, contact, calendar or address book, the web UI and REST API can't change it until the lock is released. The REST API answers `423 Locked`, and imports skip locked items. Owners can still unshare people, and people can still leave a shared calendar or address book, whatever is locked.
- **No more lost edits.** The web UI saves an event only if nobody changed it since you opened it. Reload any page left open during the upgrade before saving. The REST API applies `If-Match` in the same step as the write.
- **Long UIDs.** A UID longer than 1,024 bytes is refused. Older events with a longer UID can still be deleted, copied and moved, but not edited in the web UI or REST API.
- **Floating times.** The web calendar shows events with no time zone at their written time, wherever you are, and keeps them that way when you edit them. Editing a series you organize increments its `SEQUENCE`, so attendees' clients pick up the change.
- **Daylight saving.** An occurrence in the hour skipped when clocks spring forward moves later: `02:30` becomes `03:30`. 1.1.x moved it earlier.
- **Week-numbered rules.** Yearly rules with `BYWEEKNO` follow ISO week numbering, so around New Year they may differ from other calendar software.
- **Slow searches stop.** A calendar search (REPORT) stops after `APP_DAV_REPORT_TIMEOUT`, 10 seconds by default. A calendar with very heavy recurring events may get partial results, and the web UI marks the series it couldn't list in full.
- **PROPFIND without `Depth`** is treated as `Depth: infinity`, as RFC 4918 requires. 1.1.x treated it as `Depth: 1`. Set `APP_DAV_PROPFIND_INFINITY_ENABLED=false` to refuse these requests.

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
| `APP_TRUSTED_PROXIES` | false | Comma-separated IPs/CIDRs of the reverse proxies that connect to CalCard directly. See [Running behind a reverse proxy](#running-behind-a-reverse-proxy). If none are specified, CalCard trusts all proxies - Not recommended for public environments |
| `APP_HTTP_READ_TIMEOUT` | false | (Default `15s`) Maximum duration for reading an entire request, including the body (Go duration). |
| `APP_HTTP_WRITE_TIMEOUT` | false | (Default `15s`) Maximum duration before timing out writes of the response (Go duration). |
| `APP_HTTP_IDLE_TIMEOUT` | false | (Default `60s`) Maximum time to wait for the next request on a keep-alive connection (Go duration). |
| `APP_TRAFFIC_CAPTURE_FILE` | false | Enables diagnostic request capture when set to a JSONL file path. Captures method, path, headers, request body (up to 10 MiB), client IP, timing offset, and response status. Basic credentials are replaced with `${CALCARD_DAV_BASIC_AUTH}` and cookies/tokens are redacted, but calendar, contact, and other body data remains sensitive. The file is appended with `0600` permissions; move or remove it before starting a distinct capture session. |
| `APP_PPROF_ENABLED` | false | (Default `false`) Exposes `net/http/pprof` profiling on a dedicated debug listener (`APP_PPROF_ADDR`). The endpoints leak runtime internals and can be used to DoS the process - keep the listener on loopback and reach it via an SSH tunnel. |
| `APP_PPROF_ADDR` | false | (Default `127.0.0.1:6060`) Bind address for the pprof debug listener when `APP_PPROF_ENABLED` is true. Keep on loopback. |
| `APP_PROMETHEUS_ENDPOINT_ENABLED` | false | (Default `false`) Serves Prometheus metrics at `/metrics` on the main listener. |
| `LOG_LEVEL` | false | (Default `Info`) Minimum log level: `Trace`, `Debug`, `Info`, `Warn`, `Error`, or `Fatal`. Case-sensitive; any other value falls back to `Info`. |

Boolean variables accept `1`/`true`/`yes`/`on` and `0`/`false`/`no`/`off`.

### Running behind a reverse proxy
A request from a peer listed in `APP_TRUSTED_PROXIES` is attributed to the rightmost `X-Forwarded-For` hop that no listed proxy wrote. Rate limiting, the address recorded for a session and the traffic capture all use that address. A request from any other peer is attributed to the peer, and its forwarded headers are ignored. Each trusted proxy must:
- Append the address it received the request from to `X-Forwarded-For`. nginx does this with `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;`, HAProxy with `option forwardfor`, and Traefik and Caddy by default. A proxy that passes the client's `X-Forwarded-For` through without appending lets the client choose its address, as does nginx's `proxy_set_header X-Real-IP $remote_addr;` on its own.
- Overwrite or strip `X-Real-IP` and `True-Client-IP` from clients. CalCard reads `X-Real-IP` only when a request carries no `X-Forwarded-For` hop, so a proxy that writes neither header passes the client's own claim through. CalCard does not read `True-Client-IP`.

A hop CalCard cannot read as an IP address, with or without a port, ends the walk: the request is attributed to the nearest trusted proxy, such as nginx writing `unix:` for a client on a socket. With `APP_TRUSTED_PROXIES` empty, every peer is trusted and the leftmost `X-Forwarded-For` entry, which any client can write, is used.

CalDAV/CardDAV resource limits. Each bounds the work one request can provoke; a request past a limit is refused rather than answered with a partial result. Integer limits must be non-negative, and `0` turns that limit off.
| Name | Required | Notes |
| --- | --- | --- |
| `APP_DAV_PROPFIND_INFINITY_ENABLED` | false | (Default `true`) Allow `Depth: infinity` PROPFIND. Disable where deep listings are a DoS concern. |
| `APP_DAV_DIGEST_ENABLED` | false | (Default `false`) Offer and accept HTTP Digest on `/dav` and `/api`. See [Connecting a CalDAV/CardDAV client](#connecting-a-caldavcarddav-client). |
| `APP_DAV_INSECURE_BASIC_ENABLED` | false | (Default `false`) Accept HTTP Basic on `/dav` and `/api` over plain HTTP. Not recommended: app passwords and all data cross the network unencrypted, and CalCard logs a warning on every start. See [Basic authentication requires HTTPS](#basic-authentication-requires-https). |
| `APP_DAV_MAX_MULTISTATUS_RESPONSES` | false | (Default `10000`) `DAV:response` elements one multistatus may carry. |
| `APP_DAV_MAX_MULTISTATUS_BYTES` | false | (Default `67108864`) Encoded size of one multistatus response. |
| `APP_DAV_MAX_FILTER_ELEMENTS` | false | (Default `100`) Elements one `CALDAV:filter` or `CARDDAV:filter` may carry. |
| `APP_DAV_MAX_CARDDAV_QUERY_BYTES` | false | (Default `65536`) Combined serialized `CARDDAV:filter` and `CARDDAV:address-data` bytes per REPORT. |
| `APP_DAV_MAX_ADDRESS_DATA_PROPERTIES` | false | (Default `100`) `CARDDAV:address-data` property selectors per REPORT. |
| `APP_DAV_MAX_REQUESTED_PROPERTIES` | false | (Default `1000`) Distinct property names the `DAV:prop` selectors of one PROPFIND or REPORT may request (the `DAV:property` elements of `DAV:expand-property` count too). A PROPFIND past it is refused with 413, a REPORT with 400. |
| `APP_DAV_MAX_REPORT_ELEMENT_DEPTH` | false | (Default `20`) Element nesting one REPORT body may reach. |
| `APP_DAV_MAX_MULTIGET_HREFS` | false | (Default `5000`) `DAV:href` elements one calendar-multiget may carry. |
| `APP_DAV_MAX_REPORT_CANDIDATE_ROWS` | false | (Default `50000`) Stored resources one report will read and parse while looking for matches. |
| `APP_DAV_SYNC_HISTORY_RETENTION` | false | (Default `2160h`, 90 days; Go duration) How far back sync deletion history is kept. A sync token older than this is refused with `DAV:valid-sync-token` and the client re-syncs in full. `0` prunes nothing. Widening it re-admits tokens whose deletions were already pruned, so widen only when no client holds a token older than the previous window. |
| `APP_DAV_REPORT_TIMEOUT` | false | (Default `10s`; Go duration) How long one REPORT may run before it is stopped and refused. Keep it below `APP_HTTP_WRITE_TIMEOUT`. `0` turns it off. |


## Connecting a CalDAV/CardDAV client
- Sign in to the web UI
- Create an app password at `/app-passwords`. You can revoke it at any time.
- Start service discovery from the DAV root at `<base-url>/dav` (recommended) or from the collection homes at `/dav/calendars/` and `/dav/addressbooks/`. Calendar collections live at `/dav/calendars/<calendar-id>/` (numeric IDs are visible in the web UI and PROPFIND responses).
- Sign in with your **primary email address** as the username and the **App Password** as the password. Other identifiers (display names, OAuth subject, etc.) are not accepted.
- Clients use HTTP Basic over HTTPS. Behind a reverse proxy, send `X-Forwarded-Proto: https` and list the proxy in `APP_TRUSTED_PROXIES`. See [Basic authentication requires HTTPS](#basic-authentication-requires-https).

### Digest authentication
HTTP Digest (SHA-256 or MD5) is off by default. Turn it on with `APP_DAV_DIGEST_ENABLED=true`.
- Digest needs a password-equivalent secret for each app password. CalCard stores it encrypted with a key from `APP_SESSION_SECRET`, so anyone with both your database and your configuration can recover it. With Digest off, CalCard stores only bcrypt hashes and clears any Digest secrets left from earlier runs.
- New app passwords work with Digest right away. Existing ones catch up the next time they sign in with Basic. `/app-passwords` shows each as **Digest ready** or **Basic only**. Where Basic isn't allowed, create new passwords instead.
- Some clients always choose Digest when it's offered and won't fall back to Basic, so a **Basic only** password won't work for them. Create a new password for those clients, or sign in once with the old one from a client that uses Basic.
- Keep `APP_SESSION_SECRET` the same across restarts and on every instance. If it changes, every password goes back to **Basic only** until it signs in with Basic again.

## Health probes
- Liveness: `GET /healthz` returns immediately when the HTTP server is running, without touching dependencies.
- Readiness: `GET /readyz` checks connectivity to critical dependencies and returns `503 Service Unavailable` until they are reachable.

## License

CalCard is licensed using the GNU Affero General Public License (AGPL).

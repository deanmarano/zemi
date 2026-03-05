---
title: 'Configuration Reference'
sidebar_label: 'Configuration'
---

# Configuration Reference

All configuration is via environment variables, fully compatible with the original Bemi.

## Source Database

The database to track changes on.

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `127.0.0.1` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `postgres` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | `postgres` |

## Replication

| Variable | Description | Default |
|----------|-------------|---------|
| `SLOT_NAME` | Logical replication slot name | `zemi` |
| `PUBLICATION_NAME` | Publication name | `zemi` |

The slot and publication are created automatically on first run. If they already exist, they are reused.

:::tip Migration from Bemi
If migrating from the original Bemi, set `SLOT_NAME` and `PUBLICATION_NAME` to match your existing values (default was `bemi`) to resume from the same WAL position.
:::

## Destination Database

By default, changes are written to the same database being tracked. Set these to write to a separate database:

| Variable | Description | Default |
|----------|-------------|---------|
| `DEST_DB_HOST` | Destination host | same as `DB_HOST` |
| `DEST_DB_PORT` | Destination port | same as `DB_PORT` |
| `DEST_DB_NAME` | Destination database | same as `DB_NAME` |
| `DEST_DB_USER` | Destination user | same as `DB_USER` |
| `DEST_DB_PASSWORD` | Destination password | same as `DB_PASSWORD` |

## SSL/TLS

Zemi supports encrypted connections to PostgreSQL via SSL/TLS. This is required for most cloud-hosted databases (AWS RDS, Supabase, Neon, etc.).

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_SSL_MODE` | SSL mode for source database | `disable` |
| `DB_SSL_ROOT_CERT` | Path to CA certificate file | system CA bundle |
| `DEST_DB_SSL_MODE` | SSL mode for destination database | same as `DB_SSL_MODE` |
| `DEST_DB_SSL_ROOT_CERT` | CA cert path for destination | same as `DB_SSL_ROOT_CERT` |

### SSL Modes

- **`disable`** (default) — no encryption. Use for local/trusted networks.
- **`require`** — encrypted connection, but no certificate verification. Protects against passive eavesdropping but not man-in-the-middle attacks.
- **`verify-ca`** — encrypted connection + verify the server's certificate is signed by a trusted CA. Use with `DB_SSL_ROOT_CERT` for self-signed certificates.
- **`verify-full`** — same as `verify-ca` plus hostname verification. The server certificate's Common Name (or Subject Alternative Name) must match the `DB_HOST`.

### CA Certificates

For `verify-ca` and `verify-full` modes, Zemi needs CA certificates to verify the server:

- **System CA bundle** (default): Automatically loaded from the operating system's trust store (works on Linux and macOS).
- **Custom CA file**: Set `DB_SSL_ROOT_CERT` to the path of a PEM-encoded CA certificate file. This is useful for self-signed certificates or private CAs.

```bash
# Use system CA bundle
DB_SSL_MODE=verify-ca ./zig-out/bin/zemi

# Use custom CA certificate
DB_SSL_MODE=verify-ca DB_SSL_ROOT_CERT=/etc/ssl/custom-ca.crt ./zig-out/bin/zemi
```

:::tip Cloud Databases
Most cloud PostgreSQL providers (AWS RDS, Google Cloud SQL, Azure Database) use certificates signed by well-known CAs included in system trust stores. Set `DB_SSL_MODE=verify-full` for maximum security:

```bash
DB_HOST=mydb.abc123.us-east-1.rds.amazonaws.com \
DB_SSL_MODE=verify-full \
./zig-out/bin/zemi
```
:::

:::note Docker
The Docker image includes system CA certificates from Alpine Linux, so `verify-ca` and `verify-full` work out of the box with well-known CAs (AWS RDS, Google Cloud SQL, etc.). For self-signed or private CA certificates, mount the CA file and set `DB_SSL_ROOT_CERT`:

```bash
docker run --rm \
  -v /path/to/ca.crt:/ca.crt:ro \
  -e DB_SSL_MODE=verify-ca \
  -e DB_SSL_ROOT_CERT=/ca.crt \
  -e DB_HOST=mydb.example.com \
  ...
  ghcr.io/deanmarano/zemi:latest
```
:::

## Table Filtering

| Variable | Description | Default |
|----------|-------------|---------|
| `TABLES` | Comma-separated list of tables to track | all tables |

Examples:

```bash
# Track only users and orders tables
TABLES=users,orders ./zig-out/bin/zemi

# Track all tables (default)
./zig-out/bin/zemi
```

When `TABLES` is set, the publication is created for only those tables. When unset, a `FOR ALL TABLES` publication is created.

:::note
The `changes` table is always excluded from tracking to prevent recursive WAL events.
:::

## Operational

| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Log verbosity: `debug`, `info`, `warn`, `error` | `info` |
| `HEALTH_PORT` | TCP port for health check endpoint | disabled |
| `SHUTDOWN_TIMEOUT` | Seconds to wait for graceful shutdown | `30` |
| `CLEANUP_ON_SHUTDOWN` | Drop replication slot and publication on graceful shutdown | `false` |

### Log Levels

- **`error`** — only errors and fatal conditions
- **`warn`** — warnings and errors (reconnection attempts, transient failures)
- **`info`** — normal operation (startup, config, periodic summaries, shutdown)
- **`debug`** — verbose output (every WAL message, every SQL query, every change)

### Health Check

When `HEALTH_PORT` is set, Zemi starts a minimal HTTP server that responds with `200 OK` on any request:

```bash
HEALTH_PORT=4005 ./zig-out/bin/zemi

# Test:
curl -s http://localhost:4005/
# HTTP/1.1 200 OK
```

This is useful for Docker `HEALTHCHECK`, Kubernetes liveness probes, or load balancer health checks.

### Graceful Shutdown

On receiving `SIGTERM` or `SIGINT`:

1. Zemi stops accepting new WAL messages
2. Persists any buffered changes
3. Sends a final StandbyStatusUpdate to PostgreSQL (acknowledges the last processed WAL position)
4. If `CLEANUP_ON_SHUTDOWN` is enabled, drops the replication slot and publication
5. Closes all connections
6. Exits

If a second signal is received before shutdown completes, Zemi exits immediately.

#### Cleanup on Shutdown

When `CLEANUP_ON_SHUTDOWN=true`, Zemi drops the replication slot and publication during graceful shutdown. This is useful for:

- **Ephemeral environments** (CI, staging, preview deployments) where you don't want leftover slots consuming WAL
- **Clean teardown** when decommissioning a Zemi instance

Accepts `true`, `1`, or `yes` (case-sensitive).

:::warning
With cleanup enabled, stopping and restarting Zemi creates a **new** replication slot. Any WAL changes that occurred while Zemi was stopped will be missed. Only enable this if you don't need continuous change tracking across restarts.
:::

## Example: Minimal

```bash
DB_HOST=localhost DB_NAME=myapp DB_USER=postgres DB_PASSWORD=secret ./zig-out/bin/zemi
```

## Example: Full Configuration

```bash
DB_HOST=source-db.example.com \
DB_PORT=5432 \
DB_NAME=production \
DB_USER=replication_user \
DB_PASSWORD=secret \
DB_SSL_MODE=verify-full \
DEST_DB_HOST=audit-db.example.com \
DEST_DB_NAME=audit \
DEST_DB_USER=audit_writer \
DEST_DB_PASSWORD=audit_secret \
DEST_DB_SSL_MODE=verify-full \
SLOT_NAME=zemi_prod \
PUBLICATION_NAME=zemi_prod \
TABLES=users,orders,payments \
LOG_LEVEL=info \
HEALTH_PORT=4005 \
SHUTDOWN_TIMEOUT=60 \
CLEANUP_ON_SHUTDOWN=false \
./zig-out/bin/zemi
```

## Example: Docker

```bash
docker run --rm \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=myapp \
  -e DB_USER=postgres \
  -e DB_PASSWORD=secret \
  -e DB_SSL_MODE=require \
  -e HEALTH_PORT=4005 \
  -e TABLES=users,orders \
  -p 4005:4005 \
  ghcr.io/deanmarano/zemi:latest
```

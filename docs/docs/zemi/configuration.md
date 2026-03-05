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
4. Closes all connections
5. Exits

If a second signal is received before shutdown completes, Zemi exits immediately.

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
DEST_DB_HOST=audit-db.example.com \
DEST_DB_NAME=audit \
DEST_DB_USER=audit_writer \
DEST_DB_PASSWORD=audit_secret \
SLOT_NAME=zemi_prod \
PUBLICATION_NAME=zemi_prod \
TABLES=users,orders,payments \
LOG_LEVEL=info \
HEALTH_PORT=4005 \
SHUTDOWN_TIMEOUT=60 \
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
  -e HEALTH_PORT=4005 \
  -e TABLES=users,orders \
  -p 4005:4005 \
  ghcr.io/deanmarano/zemi:latest
```

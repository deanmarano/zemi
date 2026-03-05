# Zemi

A fork of [BemiHQ/bemi-io](https://github.com/BemiHQ/bemi-io), rewritten from scratch in Zig.

Zemi automatically tracks every database change (INSERT, UPDATE, DELETE, TRUNCATE) with 100% reliability. It connects to PostgreSQL's [Write-Ahead Log](https://www.postgresql.org/docs/current/wal-intro.html) (WAL) and implements [Change Data Capture](https://en.wikipedia.org/wiki/Change_data_capture) (CDC). Non-invasive by design, it operates in the background without altering your existing database tables.

Zemi replaces the original Bemi's multi-runtime architecture (Java/Debezium + Go/NATS + TypeScript/Node.js) with a **single statically-linked Zig binary** that directly implements the PostgreSQL logical replication protocol.

## Benchmarks

Real measurements comparing Zemi to the original Bemi:

| Metric | Original Bemi | Zemi | Improvement |
|--------|--------------|------|-------------|
| **Docker image** | 3.23 GB | 1.04 MB | **3,100x smaller** |
| **Binary size** | N/A (3 runtimes) | 3.7 MB | single static binary |
| **Memory (RSS)** | 300-500+ MB | 2.8 MB | **~150x less** |
| **Startup time** | 30-60 seconds | <1 ms | **instant** |
| **Processes** | 4+ (sh, java, nats, node) | 1 | single process |
| **Runtime deps** | JRE, Node.js, NATS, pnpm, MikroORM | 0 | zero dependencies |

### How we measured

- **Docker image size**: `docker images` after building both images on `linux/amd64`
- **Memory**: `ps -o rss=` during active WAL replication against a live PostgreSQL instance
- **Startup time**: `/usr/bin/time` measuring wall clock from exec to first log output (Zemi: `0.00 real`)
- **Original Bemi memory**: NATS server alone uses 19 MB RSS before Debezium JVM even starts; at steady state with all three runtimes active, 300-500+ MB is typical

## Quick Start

### Docker

```bash
docker run --rm \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT=5432 \
  -e DB_NAME=mydb \
  -e DB_USER=postgres \
  -e DB_PASSWORD=secret \
  ghcr.io/deanmarano/zemi:latest
```

### From Source

Requires [Zig 0.14.1](https://ziglang.org/download/) (or use `asdf install` with the included `.tool-versions`).

```bash
# Build
zig build

# Run
DB_HOST=127.0.0.1 DB_NAME=mydb DB_USER=postgres DB_PASSWORD=secret \
  ./zig-out/bin/zemi

# Run tests (42 unit tests)
zig build test
```

### Pre-built Binaries

Download from [GitHub Releases](https://github.com/deanmarano/bemi-io/releases) for your platform:

| Binary | Platform | Size |
|--------|----------|------|
| `zemi-x86_64-linux` | Linux x86_64 (static) | 3.7 MB |
| `zemi-aarch64-linux` | Linux ARM64 (static) | 3.8 MB |
| `zemi-x86_64-macos` | macOS Intel | 510 KB |
| `zemi-aarch64-macos` | macOS Apple Silicon | 497 KB |

All Linux binaries are statically linked with zero runtime dependencies.

## Prerequisites

A PostgreSQL database (14+) with logical replication enabled:

```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this change
```

To track both "before" and "after" states on data changes:

```sql
ALTER TABLE [tracked_table_name] REPLICA IDENTITY FULL;
```

## Configuration

All configuration is via environment variables, fully compatible with the original Bemi.

### Required

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | `127.0.0.1` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `postgres` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | `postgres` |

### Replication

| Variable | Description | Default |
|----------|-------------|---------|
| `SLOT_NAME` | Logical replication slot name | `zemi` |
| `PUBLICATION_NAME` | Publication name | `zemi` |

### Destination Database

By default, changes are written to the same database being tracked. Set these to write to a separate database:

| Variable | Description | Default |
|----------|-------------|---------|
| `DEST_DB_HOST` | Destination host | same as `DB_HOST` |
| `DEST_DB_PORT` | Destination port | same as `DB_PORT` |
| `DEST_DB_NAME` | Destination database | same as `DB_NAME` |
| `DEST_DB_USER` | Destination user | same as `DB_USER` |
| `DEST_DB_PASSWORD` | Destination password | same as `DB_PASSWORD` |

### Operational

| Variable | Description | Default |
|----------|-------------|---------|
| `LOG_LEVEL` | Log verbosity: `debug`, `info`, `warn`, `error` | `info` |
| `TABLES` | Comma-separated list of tables to track (empty = all) | all tables |
| `HEALTH_PORT` | TCP port for health check endpoint (empty = disabled) | disabled |
| `SHUTDOWN_TIMEOUT` | Seconds to wait for graceful shutdown | `30` |

### Health Check

When `HEALTH_PORT` is set, a minimal HTTP health endpoint responds on that port:

```bash
HEALTH_PORT=4005 ./zig-out/bin/zemi

# In another terminal:
curl http://localhost:4005/
# Returns: HTTP 200 OK
```

## Architecture

```
PostgreSQL WAL ──> zemi (single Zig binary) ──> PostgreSQL (changes table)
```

The original Bemi requires four processes across three runtimes:

```
PostgreSQL WAL ──> Debezium (Java) ──> NATS (Go) ──> Worker (Node.js) ──> PostgreSQL
```

### Why This Is Better

**Fewer moving parts.** The original Bemi chains four processes together. Each one is a point of failure. Debezium reads the WAL and publishes to NATS. NATS queues messages. A Node.js worker consumes from NATS and writes to PostgreSQL. If any link in that chain goes down, changes are delayed or lost until recovery.

Zemi reads the WAL and writes to PostgreSQL. That's it. One process, one connection in, one connection out.

**No JVM.** Debezium runs on the JVM, which means a 30-60 second startup time, 300+ MB of baseline memory, and garbage collection pauses. Zemi starts in under a millisecond and uses 2.8 MB of memory.

**No message broker.** NATS adds operational complexity (JetStream configuration, stream management, consumer groups) for what is fundamentally a single-producer, single-consumer pipeline. Zemi processes changes in-memory as they arrive from the WAL.

**Deterministic resource usage.** Zig has no garbage collector and no hidden allocations. Memory usage is stable and predictable under load, not subject to GC pauses or heap growth.

**Trivial deployment.** One static binary. Copy it anywhere and run it. No `package.json`, no `pom.xml`, no runtime installation. The Docker image is 1 MB because it's literally just the binary on a `scratch` base.

### Internal Components

```
+-----------------------------------------------------+
|                    zemi binary                        |
|                                                       |
|  +----------+  +----------+  +----------+            |
|  | protocol |  | decoder  |  | storage  |            |
|  |          |  |          |  |          |            |
|  | PG wire  |  | pgoutput |  | changes  |            |
|  | protocol |->| parsing  |->| table    |            |
|  | + repl   |  | + context|  | persist  |            |
|  +----------+  +----------+  +----------+            |
|                                                       |
|  +----------+  +----------+  +----------+            |
|  |connection|  |  config  |  |  health  |            |
|  |          |  |          |  |          |            |
|  | TCP +    |  | env vars |  | HTTP     |            |
|  | auth     |  | + valid  |  | /health  |            |
|  +----------+  +----------+  +----------+            |
+-----------------------------------------------------+
```

- **protocol.zig** -- PostgreSQL wire protocol encoding/decoding, replication messages, MD5 auth
- **connection.zig** -- TCP connection management, startup/auth handshake (MD5 + SCRAM-SHA-256), simple query protocol
- **scram.zig** -- SCRAM-SHA-256 authentication (RFC 5802), PBKDF2, HMAC-SHA-256, SASL message building
- **replication.zig** -- Logical replication stream, slot/publication management, WAL streaming
- **decoder.zig** -- `pgoutput` logical decoding plugin parser, relation cache, context stitching
- **storage.zig** -- Change persistence, schema migration, JSON serialization, retry logic
- **config.zig** -- Environment variable parsing, validation
- **health.zig** -- TCP health check server
- **main.zig** -- Entry point, signal handling, reconnection loop, graceful shutdown

## Building

### Default Build (native target, debug)

```bash
zig build
```

### Release Build (native target, optimized)

```bash
zig build -Doptimize=ReleaseSafe
```

### Cross-Compile All Targets

```bash
zig build release
# Outputs:
#   zig-out/x86_64-linux/zemi
#   zig-out/aarch64-linux/zemi
#   zig-out/x86_64-macos/zemi
#   zig-out/aarch64-macos/zemi
```

### Cross-Compile Single Target

```bash
zig build -Dtarget=x86_64-linux-musl -Doptimize=ReleaseSafe
```

### Docker

```bash
# Build for current platform
docker build -t zemi .

# Build for specific platform
docker build --platform linux/amd64 -t zemi .
```

## Changes Table Schema

Zemi creates a `changes` table in the destination database with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `uuid` | Unique change identifier |
| `database` | `text` | Source database name |
| `schema` | `text` | Source schema name |
| `table` | `text` | Source table name |
| `operation` | `text` | `INSERT`, `UPDATE`, `DELETE`, or `TRUNCATE` |
| `before` | `jsonb` | Row state before the change (null for INSERT) |
| `after` | `jsonb` | Row state after the change (null for DELETE) |
| `context` | `jsonb` | Application context (from ORM packages) |
| `primary_key` | `text` | Primary key value |
| `committed_at` | `timestamptz` | Transaction commit time |
| `position` | `bigint` | WAL position (LSN as numeric) |
| `queued_at` | `timestamptz` | Time the change was queued |
| `created_at` | `timestamptz` | Time the change was persisted |

This schema is identical to the original Bemi -- existing queries work without modification.

## Use Cases

- **Audit Trails** -- compliance logs for customer support and external customers
- **Time Travel** -- retrieve historical data without event sourcing
- **Troubleshooting** -- identify root causes of application issues
- **Change Reversion** -- revert changes or rollback API request side effects
- **Distributed Tracing** -- track changes across distributed systems
- **Trend Analysis** -- gain insights into historical data changes

## Compatibility

Zemi maintains full wire compatibility with the original Bemi:

- Same `changes` table schema -- existing queries work without modification
- Same environment variables -- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- Same ORM package support -- existing [ORM packages](https://docs.bemi.io/#supported-orms) work without changes
- Same `_bemi` context protocol -- ORM packages send context via `pg_logical_emit_message` with the `_bemi` prefix, and Zemi stitches it onto changes identically

## Migration from Bemi

If you're currently running the original Bemi and want to switch to Zemi:

1. **Stop** the existing Bemi worker (Debezium + NATS + Node.js)
2. **Note** the current replication slot name (default: `bemi`)
3. **Set** `SLOT_NAME` and `PUBLICATION_NAME` to match your existing values
4. **Deploy** Zemi with the same environment variables
5. Zemi picks up from where Bemi left off (same replication slot)

**Rollback:** Stop Zemi, restart the old Bemi worker. The replication slot is shared.

**What changes:** Single binary replaces four processes, Docker image drops from 3.2 GB to 1 MB.

**What stays the same:** `changes` table schema, environment variables, ORM packages, replication slot.

## Troubleshooting

### "replication slot does not exist"

The slot is created automatically on first run. If it was manually dropped, restart Zemi and it will recreate it.

### "publication does not exist"

The publication is created automatically (`FOR ALL TABLES` or scoped to `TABLES` if set). Restart Zemi to recreate.

### No changes appearing

1. Verify `wal_level = logical`: `SHOW wal_level;`
2. Verify the publication exists: `SELECT * FROM pg_publication;`
3. Verify the slot exists: `SELECT * FROM pg_replication_slots;`
4. Check logs with `LOG_LEVEL=debug`

### Authentication

Zemi supports both **MD5** and **SCRAM-SHA-256** password authentication. SCRAM-SHA-256 is the default in PostgreSQL 16+ and is recommended for production use. SSL/TLS is not yet implemented.

## License

Distributed under the terms of the [SSPL-1.0 License](/LICENSE).

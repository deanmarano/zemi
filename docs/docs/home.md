---
slug: /
---

# Zemi

Zemi is an automatic PostgreSQL change tracking system. It connects to your database's [Write-Ahead Log](https://www.postgresql.org/docs/current/wal-intro.html) (WAL) and captures every data change (INSERT, UPDATE, DELETE, TRUNCATE) in real-time, persisting them to a queryable `changes` table.

A from-scratch rewrite of [Bemi](https://github.com/BemiHQ/bemi-io) in [Zig](https://ziglang.org/), Zemi replaces the original multi-runtime architecture (Java/Debezium + Go/NATS + TypeScript/Node.js) with a **single statically-linked binary**.

## At a Glance

| Metric | Original Bemi | Zemi | Improvement |
|--------|--------------|------|-------------|
| **Docker image** | 3.23 GB | 1.04 MB | **3,100x smaller** |
| **Memory (RSS)** | 300-500+ MB | 2.8 MB | **~150x less** |
| **Startup time** | 30-60 seconds | &lt;1 ms | **instant** |
| **Throughput** | ~97 changes/s | ~2,000 changes/s | **~20x faster** |
| **Latency (p50)** | ~977 ms | ~75 ms | **~13x faster** |
| **Processes** | 4+ | 1 | single process |

See [Benchmarks](zemi/benchmarks) for full details.

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

```bash
# Requires Zig 0.14.1 (or use: asdf install)
zig build
DB_HOST=127.0.0.1 DB_NAME=mydb DB_USER=postgres DB_PASSWORD=secret ./zig-out/bin/zemi
```

### Pre-built Binaries

Download from [GitHub Releases](https://github.com/deanmarano/zemi/releases):

| Binary | Platform | Size |
|--------|----------|------|
| `zemi-x86_64-linux` | Linux x86_64 (static) | 3.7 MB |
| `zemi-aarch64-linux` | Linux ARM64 (static) | 3.8 MB |
| `zemi-x86_64-macos` | macOS Intel | 510 KB |
| `zemi-aarch64-macos` | macOS Apple Silicon | 497 KB |

## Prerequisites

PostgreSQL 14+ with logical replication enabled:

```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this change
```

See the [hosting platform guides](postgresql/source-database) for provider-specific instructions (AWS RDS, Supabase, Neon, etc.).

To track both "before" and "after" states on data changes:

```sql
ALTER TABLE [tracked_table_name] REPLICA IDENTITY FULL;
```

## How It Works

1. Zemi connects to PostgreSQL's logical replication stream
2. It decodes WAL changes using the `pgoutput` plugin
3. If your app uses a [supported ORM package](orms/prisma), application context (user ID, request ID, etc.) is automatically stitched onto each change
4. Changes are persisted to a `changes` table in the destination database

```
PostgreSQL WAL --> zemi (single binary) --> PostgreSQL (changes table)
```

See [Architecture](zemi/architecture) for the full technical deep-dive.

## Use Cases

- **Audit Trails** -- compliance logs for customer support and external customers
- **Time Travel** -- retrieve historical data without event sourcing
- **Troubleshooting** -- identify root causes of application issues
- **Change Reversion** -- revert changes or rollback API request side effects
- **Distributed Tracing** -- track changes across distributed systems
- **Trend Analysis** -- gain insights into historical data changes

## Supported ORM Packages

Zemi is fully compatible with all Bemi ORM packages for automatic context stitching:

#### JavaScript/TypeScript

* **[Prisma](orms/prisma)**
* **[Drizzle](orms/drizzle)**
* **[TypeORM](orms/typeorm)**
* **[Supabase JS](orms/supabase-js)**
* **[MikroORM](orms/mikro-orm)**

#### Ruby

* **[Ruby on Rails](orms/rails)**

#### Python

* **[SQLAlchemy](orms/sqlalchemy)**
* **[Django](orms/django)**

## Changes Table Schema

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

## Migrating from Bemi?

Zemi is a drop-in replacement. See the [Migration Guide](zemi/migration).

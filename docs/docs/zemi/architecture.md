---
title: 'Architecture'
sidebar_label: 'Architecture'
---

# Architecture

## The Problem with the Original Architecture

The original Bemi chains four processes across three runtimes:

```
PostgreSQL WAL ──> Debezium (Java) ──> NATS (Go) ──> Worker (Node.js) ──> PostgreSQL
```

Each component exists for a reason, but together they create significant complexity:

- **Debezium** is a general-purpose CDC framework. It supports dozens of databases and output formats. Bemi only needs PostgreSQL logical replication, which is a small fraction of what Debezium provides.
- **NATS** is a distributed messaging system with JetStream persistence, consumer groups, and cluster support. Bemi uses it as a simple queue between two processes on the same machine.
- **The Node.js worker** consumes from NATS, stitches context from `_bemi` messages, and writes to PostgreSQL via MikroORM.

The result: a 3.2 GB Docker image, 300+ MB memory usage, 30–60 second startup, and four processes to monitor.

## Zemi's Architecture

```
PostgreSQL WAL ──> zemi (single Zig binary) ──> PostgreSQL (changes table)
```

One process. One connection in (replication), one connection out (persistence). No intermediaries.

### Internal Components

```
┌─────────────────────────────────────────────────────┐
│                    zemi binary                        │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ protocol │  │ decoder  │  │ storage  │           │
│  │          │  │          │  │          │           │
│  │ PG wire  │──│ pgoutput │──│ changes  │           │
│  │ protocol │  │ parsing  │  │ table    │           │
│  │ + repl   │  │ + context│  │ persist  │           │
│  └──────────┘  └──────────┘  └──────────┘           │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │connection│  │  config  │  │  health  │           │
│  │          │  │          │  │          │           │
│  │ TCP +    │  │ env vars │  │ HTTP     │           │
│  │ auth     │  │ + valid  │  │ /health  │           │
│  └──────────┘  └──────────┘  └──────────┘           │
└─────────────────────────────────────────────────────┘
```

### Source Files

| File | Purpose |
|------|---------|
| `src/protocol.zig` | PostgreSQL wire protocol encoding/decoding, replication messages, MD5 authentication (10 tests) |
| `src/connection.zig` | TCP connection management, startup/auth handshake (MD5 + SCRAM-SHA-256), simple query protocol |
| `src/scram.zig` | SCRAM-SHA-256 authentication (RFC 5802), PBKDF2, HMAC-SHA-256, SASL messages (5 tests) |
| `src/replication.zig` | Logical replication stream, slot/publication management, WAL streaming |
| `src/decoder.zig` | `pgoutput` logical decoding plugin parser, relation cache, context stitching (18 tests) |
| `src/storage.zig` | Change persistence, schema migration, JSON serialization, retry logic (10 tests) |
| `src/config.zig` | Environment variable parsing, validation (4 tests) |
| `src/health.zig` | TCP health check server (1 test) |
| `src/main.zig` | Entry point, signal handling, reconnection loop, graceful shutdown |

**Total: 47 unit tests**

### Data Flow

1. **Connection**: Zemi connects to PostgreSQL using the wire protocol, performs authentication (MD5 or SCRAM-SHA-256, auto-detected), and enters replication mode.

2. **Publication & Slot**: On first run, Zemi creates a logical replication slot and publication (either `FOR ALL TABLES` or scoped to `TABLES` env var). On subsequent runs, it reuses the existing slot and resumes from the last acknowledged position.

3. **WAL Streaming**: PostgreSQL sends WAL changes via the `pgoutput` logical decoding plugin inside `CopyData` messages. Each message is either a keepalive or contains logical replication data (Begin, Relation, Insert, Update, Delete, Commit, etc.).

4. **Decoding**: The decoder parses `pgoutput` messages, maintains a relation cache (mapping relation IDs to table/column metadata), and groups changes into transactions. If a `_bemi`-prefixed logical message is present in the transaction, its content is captured as context.

5. **Context Stitching**: At commit time, any captured `_bemi` context is stamped onto all changes in the transaction. This is how ORM packages attach application-level metadata (user ID, request ID, etc.) to database changes.

6. **Persistence**: Changes are batch-inserted into the `changes` table using multi-row `INSERT ... ON CONFLICT DO NOTHING`. The `changes` table itself is filtered out to prevent recursive WAL events.

7. **Acknowledgment**: After successful persistence, Zemi sends a StandbyStatusUpdate to PostgreSQL confirming the WAL position, allowing PostgreSQL to reclaim WAL space.

### Error Handling

- **Transient errors** (connection refused, connection reset, broken pipe): retried up to 3 times with exponential backoff (100ms, 200ms, 400ms)
- **Permanent errors** (authentication failure, SQL syntax error): fail immediately
- **Replication disconnection**: reconnect with exponential backoff (2s, 4s, 8s, ... up to 60s)
- **Graceful shutdown**: on SIGTERM/SIGINT, sends final StandbyStatusUpdate, closes connections. Second signal forces immediate exit.

### Why Not Just Use Debezium Directly?

Debezium is a powerful, general-purpose CDC framework, but for this specific use case:

- Zemi starts in &lt;1ms vs. 30–60 seconds (JVM startup)
- Zemi uses 2.8 MB vs. 300+ MB memory
- Zemi is a single static binary vs. a Java application requiring a JRE
- Zemi implements only what's needed: PostgreSQL logical replication with `pgoutput`
- No need for Kafka/NATS/Kinesis — changes go directly to the destination

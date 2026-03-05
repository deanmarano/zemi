# Bemi Zig Rewrite Plan

This project is a fork of [BemiHQ/bemi-io](https://github.com/BemiHQ/bemi-io), rewritten from the ground up in Zig. The goal is to replace the current multi-runtime architecture (Java/Debezium + Go/NATS + TypeScript/Node.js) with a single, statically-linked native binary.

## Motivation

The original Bemi architecture requires three separate runtimes (JVM, Go, Node.js) packaged together in a Docker container. This creates significant operational complexity, a large resource footprint, and makes the system harder to deploy and debug. A Zig rewrite offers:

- **Single binary deployment** -- no JVM, no Node.js, no Go runtime
- **Minimal resource usage** -- no garbage collector overhead from three separate runtimes
- **Simpler operations** -- one process to monitor, one thing to deploy
- **Direct PostgreSQL integration** -- native logical replication protocol instead of routing through Debezium and NATS
- **Cross-compilation** -- build for any target from any host

## Current Architecture (Being Replaced)

```
PostgreSQL WAL
    │
    ▼
Debezium (Java) ──► NATS JetStream (Go) ──► Bemi Worker (TypeScript)
                                                    │
                                                    ▼
                                              PostgreSQL (changes table)
```

## Target Architecture

```
PostgreSQL WAL
    │
    ▼
bemi (single Zig binary)
    │
    ▼
PostgreSQL (changes table)
```

The Zig binary will directly implement the PostgreSQL logical replication protocol, eliminating the need for Debezium and NATS entirely.

## Phases

The rewrite is broken into six phases, each with its own detailed plan in [docs/phases/](./phases/).

### [Phase 1: PostgreSQL Protocol Foundation](./phases/phase-1-pg-protocol.md)

Implement the PostgreSQL wire protocol and logical replication streaming. This is the core foundation -- the ability to connect to PostgreSQL, create a replication slot, and receive WAL changes over the streaming replication protocol.

### [Phase 2: Logical Decoding and Change Parsing](./phases/phase-2-logical-decoding.md)

Parse the `pgoutput` logical decoding messages into structured change records. Handle all message types (Begin, Relation, Insert, Update, Delete, Truncate, Commit) and maintain relation metadata for decoding column values.

### [Phase 3: Change Persistence](./phases/phase-3-change-persistence.md)

Implement the downstream PostgreSQL connection for writing parsed changes to the `changes` table. Handle schema migrations, conflict resolution (idempotent writes), and transaction management.

### [Phase 4: Context Stitching](./phases/phase-4-context-stitching.md)

Implement the context stitching logic that pairs application-level context (sent via ORM packages through PostgreSQL logical messages) with the corresponding data changes using transaction IDs. This includes the buffering and matching logic.

### [Phase 5: Operational Concerns](./phases/phase-5-operations.md)

Add configuration management, logging, health checks, graceful shutdown, signal handling, and metrics. Make the binary production-ready with proper error handling, reconnection logic, and position tracking for crash recovery.

### [Phase 6: Testing, Packaging, and Migration](./phases/phase-6-testing-packaging.md)

Comprehensive testing strategy, build and release pipeline, Docker image creation, documentation, and a migration guide for existing Bemi users transitioning to the Zig version.

## Compatibility Goals

- **Wire-compatible `changes` table schema** -- existing queries against the `changes` table should work without modification
- **Same environment variable interface** -- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` remain the configuration mechanism
- **ORM package compatibility** -- existing ORM packages that send context via PostgreSQL logical messages should work without changes
- **Drop-in replacement** -- users should be able to swap the Docker image and have it work

## Non-Goals (for initial release)

- Kafka/NATS output support (the original architecture used these as internal plumbing; users don't need them)
- Multi-database support beyond PostgreSQL
- GUI or web interface
- Cloud platform integration (Bemi Cloud is a separate product)

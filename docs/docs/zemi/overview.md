---
title: 'Zemi — Zig Rewrite of Bemi'
sidebar_label: 'Zemi Overview'
slug: '/zemi'
---

# Zemi

Zemi is a from-scratch rewrite of [Bemi](/) in [Zig](https://ziglang.org/). It replaces the original multi-runtime architecture (Java/Debezium + Go/NATS + TypeScript/Node.js) with a **single statically-linked binary** that directly implements the PostgreSQL logical replication protocol.

Same functionality. Same `changes` table. Same ORM packages. **3,100x smaller.**

## At a Glance

| Metric | Original Bemi | Zemi | Improvement |
|--------|--------------|------|-------------|
| **Docker image** | 3.23 GB | 1.04 MB | **3,100x smaller** |
| **Binary size** | N/A (3 runtimes) | 3.7 MB | single static binary |
| **Memory (RSS)** | 300–500+ MB | 2.8 MB | **~150x less** |
| **Startup time** | 30–60 seconds | &lt;1 ms | **instant** |
| **Processes** | 4+ (sh, java, nats, node) | 1 | single process |
| **Runtime deps** | JRE, Node.js, NATS, pnpm, MikroORM | 0 | zero dependencies |
| **Unit tests** | — | 42 | built-in Zig test runner |

## Why Rewrite?

The original Bemi works, but the architecture has significant operational overhead:

1. **Debezium** reads the WAL and publishes to NATS. It runs on the JVM, which means 30–60 second cold starts, 300+ MB baseline memory, and garbage collection pauses.

2. **NATS** queues messages between Debezium and the worker. It's a full-featured message broker for what is fundamentally a single-producer, single-consumer pipeline.

3. **A Node.js worker** consumes from NATS and writes to PostgreSQL. It uses MikroORM for database access.

That's four processes, three runtimes, and a 3.2 GB Docker image for a conceptually simple job: read WAL changes, write them to a table.

Zemi does the same job with one process, zero dependencies, and a 1 MB Docker image.

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

Download from [GitHub Releases](https://github.com/deanmarano/bemi-io/releases):

| Binary | Platform | Size |
|--------|----------|------|
| `zemi-x86_64-linux` | Linux x86_64 (static) | 3.7 MB |
| `zemi-aarch64-linux` | Linux ARM64 (static) | 3.8 MB |
| `zemi-x86_64-macos` | macOS Intel | 510 KB |
| `zemi-aarch64-macos` | macOS Apple Silicon | 497 KB |

## Compatibility

Zemi is a **drop-in replacement** for the original Bemi worker:

- **Same `changes` table schema** — existing queries work without modification
- **Same environment variables** — `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- **Same ORM packages** — all [supported ORM packages](/#supported-orms) work without changes
- **Same replication slot** — Zemi picks up exactly where Bemi left off
- **Same `_bemi` context protocol** — ORM packages send context via `pg_logical_emit_message`, and Zemi stitches it identically

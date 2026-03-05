# AGENTS.md — Zemi Codebase Guide

## What is Zemi?

Zemi is a from-scratch Zig rewrite of [BemiHQ/bemi-io](https://github.com/BemiHQ/bemi-io), an automatic PostgreSQL change tracking system. It captures every data change (INSERT, UPDATE, DELETE, TRUNCATE) via PostgreSQL's Write-Ahead Log (WAL) using the logical replication protocol and persists them to a `changes` table.

The original Bemi used three runtimes: Java/Debezium for CDC, Go/NATS as a message broker, and TypeScript/Node.js for the worker that writes to the database. Zemi replaces all three with a **single statically-linked Zig binary** that directly implements the PostgreSQL wire protocol and logical replication.

**Repository**: https://github.com/deanmarano/zemi
**Local checkout path**: The directory name is still `bemi-io` from the original fork, but the project is "Zemi" everywhere else.
**License**: SSPL-1.0

## Quick Reference

| Item | Value |
|------|-------|
| Language | Zig 0.14.1 |
| Version manager | asdf (`.tool-versions`) |
| Build system | `build.zig` (Zig's native build system) |
| Binary name | `zemi` |
| Entry point | `src/main.zig` |
| Test command | `zig build test` |
| Format check | `zig fmt --check src/ build.zig` |
| E2E tests | `./test/e2e.sh` (requires Docker or `--no-docker` with PostgreSQL running) |
| CI | `.github/workflows/build.yml` |
| Docker | `docker build -t zemi .` |
| Cross-compile | `zig build release` (4 targets) |

## Architecture Overview

```
PostgreSQL WAL ──> zemi (single binary) ──> PostgreSQL (changes table)
```

Internally, Zemi is a pipeline:

```
┌─────────────┐    ┌──────────────┐    ┌─────────┐    ┌──────────┐    ┌─────────┐
│ connection   │───>│ replication   │───>│ decoder  │───>│ storage   │───>│ health   │
│ + protocol   │    │ (WAL stream)  │    │ (pgoutput│    │ (changes  │    │ (HTTP    │
│ + scram      │    │               │    │  parser) │    │  table)   │    │  server) │
│ + tls        │    │               │    │          │    │           │    │          │
└─────────────┘    └──────────────┘    └─────────┘    └──────────┘    └─────────┘
        │                                                                    │
        └──────────────── config ────────────────────────────────────────────┘
```

## Source Files

All Zig source lives in `src/`. The codebase is ~5,000 lines with 50+ unit tests.

### Dependency Layers

```
Layer 0 (no src/ deps):  protocol.zig, scram.zig, config.zig, health.zig
Layer 1:                 connection.zig  (protocol + scram + config + tls)
Layer 2:                 decoder.zig     (protocol)
                         replication.zig (protocol + connection + config)
                         storage.zig     (connection + config + decoder + protocol)
Layer 3:                 main.zig        (everything)
```

### File-by-File Guide

**`src/protocol.zig`** (~755 lines, 9 tests)
PostgreSQL v3 wire protocol implementation. All the low-level message parsing and building.
- `MessageReader` / `MessageWriter` — cursor-based binary reader/writer (big-endian, as PG requires)
- `BackendMessage` / `FrontendMessage` — message type enums matching PG protocol bytes
- `AuthType` — authentication type enum (ok, cleartext, md5, sasl, sasl_continue, sasl_final)
- `PgError` — structured PostgreSQL error with severity, code, message, detail, hint
- `XLogData` / `PrimaryKeepalive` / `ReplicationMessage` — replication stream message types
- Free functions: `buildStartupMessage`, `buildQueryMessage`, `computeMd5Password`, `parseErrorFields`, `parseDataRow`, `parseReplicationMessage`, `buildStandbyStatusUpdate`, `parseLsn`, `formatLsn`, `pgEpochMicroseconds`
- Key detail: PostgreSQL uses big-endian byte order and a PG epoch of 2000-01-01 (not Unix epoch). The offset is `946_684_800_000_000` microseconds.

**`src/connection.zig`** (~600 lines, 0 tests)
TCP connection management with SSL/TLS negotiation and the full startup/authentication handshake.
- `Connection` struct — holds TCP stream, 64KB read buffer, server params, optional `ScramClient` state, optional `tls.Client` and `Certificate.Bundle` for SSL
- `connect()` — opens TCP, optionally negotiates SSL (SSLRequest → TLS handshake), sends startup message, handles auth (MD5 or SCRAM-SHA-256), processes ParameterStatus/BackendKeyData until ReadyForQuery
- `negotiateSsl()` — sends PostgreSQL SSLRequest message, reads server response byte (`'S'` = SSL ok, `'N'` = refused)
- `performTlsHandshake()` — loads CA bundle (system or custom via `ssl_root_cert`), initializes `tls.Client` with appropriate verification options
- `tlsRead()` / `tlsWriteAll()` — TLS-aware I/O wrappers that transparently handle encrypted/unencrypted connections; maps TLS-specific errors to stream-level errors
- `query()` / `exec()` — simple query protocol (sends SQL, reads until ReadyForQuery)
- `readMessage()` — reads one raw backend message with buffer management
- `readCopyData()` — reads CopyData for replication streaming
- `close()` — cleans up TLS client and CA bundle if present
- Key detail: Uses a 64KB stack-allocated read buffer with compact-on-refill. `CopyBothResponse` triggers early return from `query()` to enter replication streaming mode. SSL negotiation happens before the startup message per PostgreSQL protocol spec. The `tls.Client` is ~16KB due to its internal cipher buffer. `allow_truncation_attacks` is set to `true` because PostgreSQL doesn't always send TLS `close_notify`.

**`src/scram.zig`** (~417 lines, 5 tests)
SCRAM-SHA-256 authentication (RFC 5802) for PostgreSQL 14+.
- `ScramClient` — state machine: `buildClientFirst()` → `buildClientFinal()` → `verifyServerFinal()`
- Uses Zig stdlib crypto: `HmacSha256`, `Sha256`, `pbkdf2`, `base64`
- `buildSaslInitialResponse()` / `buildSaslResponse()` — PostgreSQL SASL message builders
- Key detail: `"biws"` is base64 of `"n,,"` (GS2 channel binding header). The `initWithNonce()` constructor allows deterministic test vectors.

**`src/replication.zig`** (~218 lines, 0 tests)
Replication slot and WAL stream management.
- `ReplicationStream` — wraps a `Connection` opened with `replication="database"`
- `identifySystem()` — `IDENTIFY_SYSTEM` command
- `createSlotIfNotExists()` — `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput NOEXPORT_SNAPSHOT` (idempotent; catches "slot already exists" errors)
- `startReplication()` — `START_REPLICATION SLOT ... LOGICAL ...` with `proto_version '1'`, `publication_names`, and `messages 'true'`
- `poll()` — reads one CopyData, returns XLogData or handles keepalive (auto-responds when `reply_requested`)
- `confirmLsn()` — advances the flushed LSN (monotonically increasing only)
- `ensurePublication()` — creates publication via a separate non-replication connection
- Key detail: `messages 'true'` in START_REPLICATION options is required for `_bemi` context messages to be forwarded via pgoutput.

**`src/decoder.zig`** (~1,488 lines, 18 tests)
pgoutput logical decoding message parser and transaction state machine.
- `PgOutputMessageType` — enum for all 10 pgoutput message types (Begin, Commit, Relation, Insert, Update, Delete, Truncate, Origin, Type, Message)
- `RelationCache` — caches relation metadata (column definitions) keyed by relation_id; owns all string data (duped from parse buffers)
- `Decoder` — main decoder struct; maintains transaction state (`in_transaction`, `current_xid`, accumulated changes, context)
- `decode()` — parses one pgoutput message. Returns `null` for non-commit messages; returns owned `[]Change` on Commit.
- `Change` — the final output struct: primary_key, before/after (as `[]NamedValue`), database, schema, table, operation, committed_at, transaction_id, position, context
- **Context stitching**: Logical decoding messages with prefix `_bemi` (transactional only) are captured as `transaction_context` and stamped onto all changes in the same transaction at commit time. This `_bemi` prefix is part of the wire protocol used by ORM packages — do NOT rename it.
- Key detail: The decoder uses two-layer ownership. The parser returns slices into the read buffer (zero-copy). `decode()` then dupes all strings it needs for `Change` records that outlive the buffer.

**`src/storage.zig`** (~543 lines, 10 tests)
Change persistence to PostgreSQL with schema migration and retry logic.
- `Storage` — wraps a `Connection` to the destination database
- `init()` — connects and runs idempotent migrations (CREATE TABLE IF NOT EXISTS, indexes, unique constraint)
- `persistChanges()` — multi-row INSERT with `ON CONFLICT DO NOTHING` for deduplication; retries transient errors up to 3 times with exponential backoff (100ms base)
- JSON serialization: `appendJsonObject()` / `appendJsonString()` build JSONB literals with proper escaping
- The `changes` table schema has 14 columns including UUID PK, JSONB before/after/context, GIN indexes, and a unique constraint on (position, table, schema, database, operation) for deduplication.
- Key detail: The `changes` table itself must be filtered from tracking (in `main.zig`) to prevent infinite recursive WAL events.

**`src/config.zig`** (~280 lines, 7 tests)
Environment variable configuration.
- `SslMode` enum — `disable`, `require`, `verify_ca`, `verify_full` with `fromString()`/`toString()` conversion
- `Config` struct — all settings with defaults. Slot and publication names default to `"zemi"`.
- `fromEnv()` — reads 20 environment variables (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSL_MODE, DB_SSL_ROOT_CERT, SLOT_NAME, PUBLICATION_NAME, DEST_DB_*, LOG_LEVEL, TABLES, SHUTDOWN_TIMEOUT, HEALTH_PORT)
- `getDestSslMode()` / `getDestSslRootCert()` — destination SSL settings with fallback to source DB settings
- `shouldTrackTable()` — comma-separated table filter with whitespace trimming
- `validate()` — returns first validation error message or null
- Destination DB fields (`DEST_DB_*`) fall back to source DB fields when unset.

**`src/health.zig`** (~89 lines, 1 test)
HTTP health check server for container orchestration (Docker HEALTHCHECK, Kubernetes liveness probes).
- `HealthServer` — binds to TCP port, runs accept loop in a background thread, responds with HTTP 200 "ok"
- Supports ephemeral port 0 (OS-assigned) via `getsockname()`
- Uses `std.c.accept()` instead of `std.posix.accept()` to avoid unreachable panic on ENOTSOCK during shutdown
- Graceful stop via atomic flag + self-connect trick to unblock the blocking `accept()`

**`src/main.zig`** (~300 lines, 1 test block importing all modules)
Application entry point and orchestration.
- Sets up `GeneralPurposeAllocator`, installs POSIX signal handlers (SIGINT/SIGTERM), reads config, optionally starts health server
- `runReplicationLoop()` — ensures publication, opens storage, opens replication stream, starts streaming, decodes pgoutput, filters changes (skips `changes` table + untracked tables), persists to storage, confirms LSN, sends periodic status updates (60s)
- Runtime log level filtering: `std_options.log_level` is `.debug` at comptime; actual filtering happens in `runtimeLogFn` based on config
- Graceful shutdown: first SIGTERM sets atomic flag checked in main loop; second signal forces `process.exit(1)`
- Exponential backoff reconnection: 1, 2, 4, 8, 16, 32, 60 seconds (capped)

## Build System

**`build.zig`** — defines three build steps:
- `zig build` — debug build of `zemi` executable (links libc for `std.c.accept` in health server)
- `zig build test` — runs all unit tests via `src/main.zig` test imports
- `zig build release` — cross-compiles ReleaseSafe for 4 targets: x86_64-linux-musl, aarch64-linux-musl, x86_64-macos, aarch64-macos

**`build.zig.zon`** — package manifest. Name `.zemi`, fingerprint `0x8fe75a3956de2e68`, version `0.1.0`, minimum Zig `0.14.0`.

## Testing

### Unit Tests (48 tests)

Run with `zig build test`. All tests are pulled in via `src/main.zig`'s test block which `@import`s all modules. Tests are in-file (Zig convention).

| File | Tests | What's Tested |
|------|-------|---------------|
| protocol.zig | 9 | MessageReader, message builders, MD5 auth, LSN parse/format, replication message parsing |
| decoder.zig | 18 | All 10 pgoutput message types, RelationCache, full transaction decode, primary key extraction, context stitching (4 scenarios) |
| storage.zig | 10 | SQL building, escaping, JSON serialization, timestamp conversion, error classification |
| scram.zig | 5 | Full SCRAM exchange, nonce mismatch, signature verification, SASL message formats |
| config.zig | 7 | Table filtering, whitespace trimming, validation, SSL mode parsing, dest SSL fallback |
| health.zig | 1 | HTTP response format |

**Important**: Zig's test runner treats `log.err` calls as test failures. Error-path tests must use `log.warn` instead.

### E2E Integration Tests (25 assertions, 10 test groups)

Run with `./test/e2e.sh` (uses Docker Compose) or `./test/e2e.sh --no-docker` (expects PostgreSQL already running on ports 5433, 5434, and 5435).

The test script:
1. Starts three PostgreSQL 16 instances: MD5 (port 5433), SCRAM-SHA-256 (port 5434), and SSL-enabled (port 5435)
2. Builds Zemi from source
3. Runs 10 test groups covering: connection, INSERT/UPDATE/DELETE tracking, data correctness, table filtering, context stitching, SCRAM-SHA-256 auth, SSL/TLS connections (require + verify-ca)
4. Each test starts Zemi as a background process, performs SQL operations, waits, then queries the `changes` table

**`docker-compose.test.yml`** — three PostgreSQL 16 Alpine services:
- `postgres` on port 5433 with MD5 auth, `wal_level=logical`
- `postgres-scram` on port 5434 with SCRAM-SHA-256, `wal_level=logical`
- `postgres-ssl` on port 5435 with MD5 auth + SSL enabled (self-signed certs built via `test/Dockerfile.postgres-ssl`)

## CI Pipeline

**`.github/workflows/build.yml`** — triggers on push to main, PRs, and version tags.

| Job | What It Does |
|-----|-------------|
| `test` | `zig fmt --check`, `zig build test`, `zig build` |
| `cross-compile` | 4-target matrix build with binary size reporting to job summary |
| `e2e` | Integration tests against MD5 + SCRAM + SSL PostgreSQL service containers |
| `docker-check` | PR-only Docker build smoke test (no push) |
| `release` | On `v*` tags: build all targets, create GitHub Release with binaries + checksums |
| `docker` | On tags/main: multi-platform Docker push to ghcr.io |

## Docker

**`Dockerfile`** — multi-stage build:
1. Alpine 3.20 + Zig 0.14.1 → cross-compiles static musl binary
2. `FROM scratch` — final image contains the `zemi` binary + CA certificates (~1 MB)

Supports both amd64 and arm64 via `TARGETARCH` build arg. CA certificates from Alpine are included for SSL/TLS `verify-ca`/`verify-full` modes.

## Important Conventions and Gotchas

### The `_bemi` Prefix Must Be Preserved
The wire protocol uses `_bemi` as the prefix for logical decoding messages that carry context from ORM packages (Prisma, Rails, TypeORM, etc.). These packages emit `SELECT pg_logical_emit_message(true, '_bemi', '...')`. This is part of the wire protocol, **not the project name**. Do NOT rename `_bemi` to `_zemi` — it would break all existing ORM packages.

### Default Names Changed to `zemi`
The default replication slot name and publication name are `"zemi"` (was `"bemi"` in the original). This only affects new deployments. Migration docs tell users to set `SLOT_NAME` and `PUBLICATION_NAME` to their original values.

### PostgreSQL Wire Protocol is Big-Endian
All integer read/write operations in `protocol.zig` use `.big` endianness. PG timestamps use a PG-specific epoch (2000-01-01), offset by `946_684_800_000_000` microseconds from Unix epoch.

### Zig 0.14.1 Specifics
- `build.zig.zon` requires a `fingerprint` field — changing the package `.name` requires a new fingerprint
- `std_options.log_level` is comptime-only — runtime log level filtering needs a custom `std_options.logFn`
- `std.posix.accept()` marks ENOTSOCK as `unreachable`, causing panics when closing a listening socket from another thread — use `std.c.accept()` instead (in `health.zig`)
- Zig's test runner treats `log.err` calls as test failures — use `log.warn` in error-path tests
- Zig download URL format: `zig-{arch}-linux-{version}.tar.xz` (NOT `zig-linux-{arch}-{version}.tar.xz`)

### PostgreSQL 16 Defaults to SCRAM-SHA-256
Docker Compose and CI must explicitly configure `POSTGRES_HOST_AUTH_METHOD: md5` and `POSTGRES_INITDB_ARGS: "--auth-host=md5"` for MD5 test instances. The SCRAM instance uses `scram-sha-256` for both.

### `wal_level=logical` Requires a PostgreSQL Restart
`ALTER SYSTEM SET wal_level = 'logical'` takes effect only after a full PostgreSQL restart (`docker restart`), not just `pg_reload_conf()`. This matters in CI where service containers need reconfiguration.

### Memory Ownership in the Decoder
The decoder has two-layer ownership:
1. **Parse layer** (`parsePgOutputMessage`) returns slices into the read buffer — zero-copy, no allocations
2. **Decode layer** (`Decoder.decode`) dupes all strings it needs for `Change` records that outlive the buffer

The `RelationCache` also dupes all string data from relation messages since the parse buffer is reused.

### Changes Table Feedback Loop Prevention
The `changes` table must be filtered from tracking in the main replication loop. Without this, every INSERT into `changes` generates a WAL event, which gets decoded and inserted into `changes`, creating an infinite loop.

### Idempotent Operations
- Slot creation catches "already exists" errors and continues
- Table migration uses `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`
- Change persistence uses `ON CONFLICT DO NOTHING` for deduplication

## Directory Structure

```
.
├── src/                        # Zig source (the Zemi binary)
│   ├── main.zig                  # Entry point, orchestration
│   ├── protocol.zig              # PG wire protocol
│   ├── connection.zig            # TCP + TLS + auth handshake
│   ├── scram.zig                 # SCRAM-SHA-256 auth
│   ├── replication.zig           # WAL streaming
│   ├── decoder.zig               # pgoutput parser + context stitching
│   ├── storage.zig               # Change persistence + migrations
│   ├── config.zig                # Environment variable config
│   └── health.zig                # HTTP health check server
├── test/
│   ├── e2e.sh                  # E2E integration test script
│   └── Dockerfile.postgres-ssl # SSL-enabled PostgreSQL for testing
├── docs/                       # Docusaurus documentation site
│   ├── docs/
│   │   ├── zemi/                 # Zemi-specific docs (5 pages)
│   │   │   ├── overview.md
│   │   │   ├── benchmarks.md
│   │   │   ├── architecture.md
│   │   │   ├── configuration.md
│   │   │   └── migration.md
│   │   └── ...                   # Original Bemi docs (unchanged)
│   ├── docusaurus.config.ts
│   ├── sidebars.ts
│   └── ...
├── docs/phases/                # Rewrite phase planning docs
│   ├── phase-1-pg-protocol.md
│   ├── phase-2-logical-decoding.md
│   ├── phase-3-change-persistence.md
│   ├── phase-4-context-stitching.md
│   ├── phase-5-operations.md
│   └── phase-6-testing-packaging.md
├── docs/zig-rewrite-plan.md    # High-level 6-phase plan
├── core/                       # Original Bemi TypeScript core (unchanged)
├── worker/                     # Original Bemi TypeScript worker (unchanged)
├── build.zig                   # Zig build configuration
├── build.zig.zon               # Zig package manifest
├── Dockerfile                  # Multi-stage: Alpine+Zig → scratch
├── docker-compose.test.yml     # Test PostgreSQL instances (MD5 + SCRAM + SSL)
├── .github/workflows/build.yml # CI/CD pipeline
├── .tool-versions              # asdf: zig 0.14.1
├── Makefile                    # Original Bemi build targets
└── README.md                   # Project documentation
```

## Original Bemi Code

The `core/` and `worker/` directories contain the original TypeScript/Node.js code from the BemiHQ/bemi-io project. These are **not modified** as part of the Zemi rewrite and remain for reference. The `Makefile` also contains original Bemi build targets.

## Current State (as of last session)

### Completed
- All 6 phases of the Zig rewrite (protocol, decoding, persistence, context stitching, operations, testing/packaging)
- SCRAM-SHA-256 authentication (`src/scram.zig` + `src/connection.zig` updates)
- SSL/TLS support (`src/connection.zig` — SSLRequest negotiation, TLS handshake, TLS-aware I/O wrappers)
- SSL configuration (`src/config.zig` — `SslMode` enum, `DB_SSL_MODE`, `DB_SSL_ROOT_CERT`, dest fallbacks)
- E2E integration tests (25 assertions across 10 test groups including SCRAM and SSL)
- Full CI pipeline with cross-compilation, E2E (MD5 + SCRAM + SSL), Docker, and release automation
- Docusaurus documentation (5 Zemi pages + updated site config)
- Rename from Bemi to Zemi throughout

### Potential Future Work
- **TRUNCATE tracking verification** — pgoutput truncate messages are parsed but not E2E tested
- **Observability/metrics** — Prometheus endpoint for monitoring
- **Graceful slot/publication cleanup on shutdown**
- **Connection pooling for storage** — currently single connection

## How to Work on This Codebase

### Adding a New Feature
1. Identify which layer the feature belongs to (see dependency layers above)
2. Implement in the appropriate `src/*.zig` file with unit tests
3. Run `zig build test` to verify
4. Run `zig fmt src/ build.zig` to format
5. If it changes behavior, update E2E tests in `test/e2e.sh`
6. Update `README.md` and relevant docs in `docs/docs/zemi/`

### Running Locally
```bash
# Start test PostgreSQL
docker compose -f docker-compose.test.yml up -d

# Build and run
DB_HOST=127.0.0.1 DB_PORT=5433 DB_NAME=zemi_test DB_USER=postgres DB_PASSWORD=postgres zig build run

# Run unit tests
zig build test

# Run E2E tests
./test/e2e.sh

# Format
zig fmt src/ build.zig
```

### Debugging Tips
- Set `LOG_LEVEL=debug` for verbose output (shows all WAL messages, SQL queries, etc.)
- The health server on `HEALTH_PORT` confirms the process is running and accepting connections
- Use `pg_stat_replication` and `pg_replication_slots` PostgreSQL views to check slot status
- LSN values are logged in `X/Y` format (upper 32 bits / lower 32 bits)

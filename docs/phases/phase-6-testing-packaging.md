# Phase 6: Testing, Packaging, and Migration

## Objective

Build confidence in the rewrite through comprehensive testing, create distributable packages, and provide a clear migration path for existing Bemi users.

## Key Deliverables

### 6.1 Testing Strategy

**Unit Tests**
- Wire protocol message encoding/decoding
- `pgoutput` message parsing for all message types
- Tuple data decoding for various PostgreSQL types
- Context stitching logic and buffer management
- Configuration parsing and validation
- JSON serialization of change records

**Integration Tests**
- End-to-end: make a database change, verify it appears in the `changes` table
- All operation types: INSERT, UPDATE, DELETE, TRUNCATE
- `REPLICA IDENTITY FULL` vs. default (key-only) for updates and deletes
- Context stitching: send context via `pg_logical_emit_message`, verify it's paired
- Large transactions (thousands of rows in a single transaction)
- Schema changes during replication (add/drop column)
- Crash recovery: kill the process, restart, verify no data loss or duplication
- Multiple tables tracked simultaneously

**Compatibility Tests**
- Verify `changes` table schema matches the original exactly
- Verify context from each supported ORM package is captured correctly
- Verify queries from the original documentation work against the new `changes` table

**Performance Tests**
- Throughput: changes per second under sustained load
- Latency: time from commit to change appearing in the `changes` table
- Memory usage under sustained load
- Behavior under backpressure (changes arriving faster than they can be persisted)

### 6.2 Build and Release

- Zig build system (`build.zig`) with clear build targets
- Cross-compilation targets:
  - `x86_64-linux` (primary -- server/container deployment)
  - `aarch64-linux` (ARM servers, Raspberry Pi)
  - `x86_64-macos` and `aarch64-macos` (development)
- Static linking for zero-dependency deployment
- GitHub Actions CI pipeline:
  - Run tests on every push
  - Build release binaries for all targets
  - Publish to GitHub Releases

### 6.3 Docker Image

- Minimal Docker image (scratch or distroless base + static binary)
- Target image size: under 20MB (compared to the current ~1GB with JVM + Node.js + NATS)
- Same environment variable interface as the original
- Health check instruction in Dockerfile

### 6.4 Documentation

- Updated README with Zig-specific build and run instructions
- Architecture documentation explaining the simplified design
- Configuration reference
- Troubleshooting guide
- Performance characteristics and tuning

### 6.5 Migration Guide

For existing Bemi users transitioning from the original to the Zig version:

- **What changes:** single binary replaces three processes, Docker image is much smaller
- **What stays the same:** `changes` table schema, environment variables, ORM packages
- **Migration steps:**
  1. Stop the existing Bemi worker
  2. Note the current replication slot position
  3. Deploy the new binary with the same environment variables
  4. The new binary picks up from where the old one left off (same replication slot)
- **Rollback:** stop the new binary, restart the old worker (replication slot is shared)

## Technical Notes

- Zig has a built-in test runner (`zig test`) that supports unit tests within source files
- For integration tests, use a PostgreSQL instance in Docker (via CI or local development)
- The `zig build` system supports defining custom build steps for running integration tests
- Static linking in Zig is the default for most targets -- no extra work needed

## Verification

- All unit tests pass
- All integration tests pass against PostgreSQL 14, 15, 16, and 17
- Binary runs on all target platforms
- Docker image starts and works with the same environment variables as the original
- Migration from the original Bemi to the Zig version preserves existing change data

## Dependencies

- Phases 1-5 (complete working system)

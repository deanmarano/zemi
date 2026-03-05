# Phase 5: Operational Concerns

## Objective

Make the binary production-ready with proper configuration, logging, error handling, monitoring, and lifecycle management. This phase transforms a working prototype into something you'd trust to run 24/7.

## Key Deliverables

### 5.1 Configuration

- Environment variable parsing matching the original interface:
  - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` -- source database
  - `DESTINATION_DB_HOST`, etc. -- optional separate destination for the `changes` table (defaults to source)
  - `SLOT_NAME` -- replication slot name (default: `bemi`)
  - `PUBLICATION_NAME` -- PostgreSQL publication name
  - `TABLES` -- comma-separated list of tables to track (default: all)
  - `LOG_LEVEL` -- DEBUG, INFO, WARN, ERROR
- Optional config file (TOML or similar) for more complex setups
- Validation of required settings on startup with clear error messages

### 5.2 Logging

- Structured logging (JSON format option for production, human-readable for development)
- Log levels: DEBUG, INFO, WARN, ERROR
- Key events to log:
  - Startup and configuration summary
  - Replication slot creation/attachment
  - Connection established/lost/reconnected
  - Changes processed (periodic summary, not per-change at INFO level)
  - Errors with full context
  - Shutdown initiated and completed

### 5.3 Error Handling and Recovery

- Categorize errors: transient (retry) vs. permanent (fail)
- Replication connection: automatic reconnection with exponential backoff
- Write connection: retry failed writes, circuit breaker for persistent failures
- Handle PostgreSQL replication slot conflicts (another consumer attached)
- Handle WAL position gaps (slot was advanced externally)
- Proper error propagation -- don't swallow errors silently

### 5.4 Graceful Shutdown

- Handle SIGTERM and SIGINT signals
- On shutdown:
  1. Stop accepting new WAL data
  2. Flush buffered changes to the database
  3. Update the flushed LSN position
  4. Send final StandbyStatusUpdate
  5. Close connections cleanly
- Configurable shutdown timeout (default: 30 seconds)

### 5.5 Health and Metrics

- Health check endpoint (HTTP or simple TCP) for container orchestration
- Key metrics to expose:
  - Replication lag (current WAL position vs. last flushed position)
  - Changes processed (total and per-table)
  - Buffer size (pending changes)
  - Connection status (up/down, reconnect count)
  - Processing latency (time from WAL commit to change persisted)
- Optional Prometheus metrics endpoint

### 5.6 Table Filtering

- Support tracking all tables or a specific subset
- Support exclude patterns (track all except certain tables)
- Filter at the replication level using PostgreSQL publications where possible
- Fall back to application-level filtering for more complex rules

## Technical Notes

- Zig's `std.os` provides signal handling capabilities
- For the HTTP health check, a minimal TCP listener is sufficient -- no need for a full HTTP framework
- Replication lag can be calculated by comparing the server's current WAL position (from keepalive messages) with the last flushed position

## Verification

- Binary starts with only environment variables (no config file required)
- Invalid configuration produces clear, actionable error messages
- Recovery after PostgreSQL restart (connection lost and regained)
- Graceful shutdown flushes all pending changes
- Health check responds correctly under normal operation and during issues
- Table filtering works correctly (only tracked tables produce changes)

## Dependencies

- Phases 1-4 (all core functionality)

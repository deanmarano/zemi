# Phase 3: Change Persistence

## Objective

Implement the downstream PostgreSQL connection for writing parsed change records to the `changes` table. This phase makes changes durable -- the system can now capture WAL changes and persist them.

## Background

The original Bemi worker uses MikroORM to write changes to a `changes` table in PostgreSQL. We need to replicate this behavior using direct PostgreSQL queries from Zig, including schema setup (migrations) and idempotent conflict handling.

## Key Deliverables

### 3.1 Standard PostgreSQL Query Connection

- Reuse the wire protocol from Phase 1 but in standard query mode (not replication mode)
- This is a separate connection from the replication connection
- Implement the extended query protocol (Parse, Bind, Execute) for parameterized queries
- Connection to the same or a different PostgreSQL database for storing changes

### 3.2 Schema Management

Create and manage the `changes` table schema:

```sql
CREATE TABLE IF NOT EXISTS changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    primary_key TEXT,
    before JSONB,
    after JSONB,
    context JSONB,
    database TEXT NOT NULL,
    schema TEXT NOT NULL,
    "table" TEXT NOT NULL,
    operation TEXT NOT NULL,
    committed_at TIMESTAMPTZ,
    queued_at TIMESTAMPTZ,
    transaction_id BIGINT,
    position TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- GIN indexes on `context`, `before`, `after` columns for JSON querying
- Unique constraint on `(position, "table", schema, database, operation)` for idempotency
- Migration tracking table (`_bemi_migrations`) for schema versioning
- Run migrations on startup, creating tables if they don't exist

### 3.3 Change Insertion

- Batch insert changes using `INSERT ... ON CONFLICT DO NOTHING`
- Parameterized queries to prevent SQL injection
- JSON serialization of before/after/context fields
- Proper timestamp handling (committed_at from WAL, queued_at from processing time)

### 3.4 Position Tracking

- Track the last successfully flushed LSN position
- Persist position to survive restarts (can use a simple metadata table or file)
- On startup, resume replication from the last flushed position
- Coordinate position updates between the replication connection (StandbyStatusUpdate) and persistence

## Technical Notes

- The write connection should use the extended query protocol for prepared statements, which avoids re-parsing SQL on every insert
- Batch inserts (multi-row INSERT) are more efficient than individual inserts
- The unique constraint ensures that replaying WAL from an earlier position (after crash recovery) doesn't create duplicate records
- Consider a small write buffer to batch changes before flushing

## Verification

- `changes` table is created automatically on first run
- Migrations run idempotently (safe to re-run)
- INSERT, UPDATE, DELETE operations are persisted with correct before/after data
- Duplicate changes are silently ignored (idempotent writes)
- After restart, replication resumes from the correct position without data loss or duplication

## Dependencies

- Phase 1 (PostgreSQL Protocol Foundation) -- wire protocol implementation
- Phase 2 (Logical Decoding) -- structured change records to persist

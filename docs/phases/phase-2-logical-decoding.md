# Phase 2: Logical Decoding and Change Parsing

## Objective

Parse the raw `pgoutput` logical decoding messages received over the replication stream into structured change records. This phase produces a system that understands all PostgreSQL logical replication message types and can extract before/after row states, operation types, and relation metadata.

## Background

The `pgoutput` plugin (PostgreSQL's built-in logical decoding output plugin) emits a defined set of binary messages within the WAL stream. Each message has a type byte followed by type-specific fields. These messages describe transaction boundaries, table schemas, and row-level changes.

## Key Deliverables

### 2.1 Message Type Parsing

Implement parsers for all `pgoutput` message types:

- **Begin** (`B`) -- Transaction start: final LSN, commit timestamp, transaction ID
- **Relation** (`R`) -- Table metadata: relation ID, namespace, table name, column definitions (name, type OID, type modifier)
- **Insert** (`I`) -- New row: relation ID, tuple data
- **Update** (`U`) -- Changed row: relation ID, optional old tuple (when `REPLICA IDENTITY FULL`), new tuple
- **Delete** (`D`) -- Removed row: relation ID, old tuple (key or full depending on replica identity)
- **Truncate** (`T`) -- Table truncation: relation IDs, truncate options
- **Commit** (`C`) -- Transaction end: flags, commit LSN, end LSN, commit timestamp
- **Origin** (`O`) -- Replication origin: commit LSN, origin name
- **Type** (`Y`) -- Custom type definitions
- **Logical Decoding Message** (`M`) -- Application messages (used for context stitching in Phase 4)

### 2.2 Tuple Data Decoding

- Parse the tuple data format: number of columns, then per-column a type flag (`n` for null, `u` for unchanged, `t` for text) followed by the value
- Decode PostgreSQL type OIDs to appropriate Zig types (text, int, bool, timestamp, jsonb, uuid, etc.)
- Maintain a relation cache: `pgoutput` sends `Relation` messages before the first change to each table, and these must be cached to decode subsequent row data

### 2.3 Change Record Construction

Build structured change records from the parsed messages:

```
Change {
    primary_key: []const u8,
    before: ?JsonObject,       // null for INSERT
    after: ?JsonObject,        // null for DELETE
    context: ?JsonObject,      // populated in Phase 4
    database: []const u8,
    schema: []const u8,
    table: []const u8,
    operation: enum { CREATE, UPDATE, DELETE, TRUNCATE },
    committed_at: Timestamp,
    queued_at: Timestamp,
    transaction_id: u32,
    position: []const u8,      // LSN as string
}
```

### 2.4 Transaction Grouping

- Group changes by transaction (between Begin and Commit messages)
- Track transaction IDs for context stitching in Phase 4
- Handle large transactions that may span many messages

## Technical Notes

- `pgoutput` messages use big-endian byte order
- The relation cache must persist across transactions (a Relation message is only sent once per table per session, or when the table schema changes)
- Column values are sent as text representations by default in `pgoutput`
- The primary key must be extracted from the relation metadata and the tuple data

## Verification

- All `pgoutput` message types are correctly parsed
- INSERT, UPDATE, DELETE operations produce correct before/after JSON
- UPDATE with `REPLICA IDENTITY FULL` includes both before and after states
- Relation metadata is cached and reused across transactions
- Large transactions with many row changes are handled correctly

## Dependencies

- Phase 1 (PostgreSQL Protocol Foundation) -- provides the raw WAL byte stream

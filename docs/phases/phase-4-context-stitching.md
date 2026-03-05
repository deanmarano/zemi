# Phase 4: Context Stitching

## Objective

Implement the context stitching logic that pairs application-level context with database-level changes using transaction IDs. This is the feature that distinguishes Bemi from raw CDC tools -- it answers "who made this change and why."

## Background

Bemi's ORM packages (for Prisma, Rails, TypeORM, SQLAlchemy, etc.) inject application context into PostgreSQL using `pg_logical_emit_message()`. This creates a logical decoding message that appears in the WAL stream alongside the data changes. The context message and data changes share the same transaction ID, which allows them to be paired.

Context messages are prefixed with `_bemi` and contain JSON with application-specific data (user ID, API endpoint, worker name, etc.).

## Key Deliverables

### 4.1 Logical Message Detection

- Identify `Logical Decoding Message` (`M`) messages in the WAL stream (already parsed in Phase 2)
- Filter for messages with the `_bemi` prefix
- Parse the JSON payload of context messages

### 4.2 Transaction-Based Matching

- When a context message arrives within a transaction (between Begin and Commit), associate it with that transaction ID
- When data changes arrive in the same transaction, attach the context to each change record
- Handle the case where context arrives before or after the data changes within the same transaction

### 4.3 Buffering Strategy

The original implementation uses a `FetchedRecordBuffer` because messages may arrive out of order (via NATS). In the Zig rewrite, since we read the WAL directly, messages arrive in WAL order within a transaction. However, buffering is still needed because:

- A transaction's changes arrive between Begin and Commit
- Context may appear at any point within the transaction
- We should wait for Commit before flushing to ensure we have the complete context

Implementation:

- Buffer all changes and context messages for in-flight transactions (keyed by transaction ID)
- On Commit, stitch context with changes and flush
- Set a maximum buffer size/timeout to handle very large transactions
- Flush changes without context if no matching context message exists

### 4.4 Context Schema

The context JSON should be stored in the `context` column of the `changes` table:

```json
{
    "user_id": "123",
    "endpoint": "/api/users",
    "method": "PATCH"
}
```

The exact structure is application-defined -- Bemi stores it as opaque JSONB.

## Technical Notes

- `pg_logical_emit_message(true, '_bemi', '{"user_id": "123"}')` -- the first argument (`true`) means the message is transactional (appears within the transaction boundaries in the WAL)
- Non-transactional messages (`false`) would appear outside transaction boundaries and can't be stitched by transaction ID
- The WAL guarantees ordering within a transaction, so all messages (context + data changes) for a transaction appear between Begin and Commit

## Verification

- Context messages sent via `pg_logical_emit_message` are captured
- Context is correctly paired with data changes in the same transaction
- Changes without context have a null context field
- Multiple changes in the same transaction all receive the same context
- The system handles transactions with only data changes (no context) gracefully
- The system handles transactions with only context messages (no data changes) gracefully

## Dependencies

- Phase 2 (Logical Decoding) -- logical message parsing
- Phase 3 (Change Persistence) -- writing stitched changes to the database

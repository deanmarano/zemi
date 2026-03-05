# Phase 1: PostgreSQL Protocol Foundation

## Objective

Implement the PostgreSQL wire protocol client and logical replication streaming connection. This phase produces a binary that can connect to a PostgreSQL server, negotiate authentication, create a replication slot, and receive raw WAL data over the streaming replication protocol.

## Background

PostgreSQL exposes two protocol modes: the standard query protocol (for SQL) and the streaming replication protocol (for WAL replication). The replication protocol is a subset that uses the same wire format but a different set of messages. Debezium handles this today via the Java PostgreSQL JDBC driver's replication API. We need to implement this directly.

## Key Deliverables

### 1.1 PostgreSQL Wire Protocol Client

- Implement the [PostgreSQL v3 frontend/backend protocol](https://www.postgresql.org/docs/current/protocol.html)
- Message framing: 1-byte type identifier + 4-byte length + payload
- Startup message and authentication flow (MD5, SCRAM-SHA-256, trust)
- Simple query protocol (for running setup SQL like `CREATE_REPLICATION_SLOT`)
- SSL/TLS negotiation
- Error and notice message handling

### 1.2 Replication Connection

- Open a replication connection using the `replication=database` parameter
- `IDENTIFY_SYSTEM` command to get system identifier and timeline
- `CREATE_REPLICATION_SLOT` with `pgoutput` logical decoding plugin
- `START_REPLICATION` command with LSN position
- Handle `CopyData` messages containing WAL data
- Send `StandbyStatusUpdate` messages to report flushed LSN positions (keepalive)

### 1.3 Connection Management

- Connection pooling is not needed for the replication connection (single long-lived connection)
- Automatic reconnection with exponential backoff on connection loss
- Proper cleanup of replication slots on shutdown

## Technical Notes

- The replication protocol sends WAL data inside `CopyData` (type `d`) messages
- Within `CopyData`, the first byte distinguishes `XLogData` (`w`), `Primary Keepalive` (`k`), and `Standby Status Update` (`r`)
- LSN (Log Sequence Number) values are 64-bit integers representing byte positions in the WAL
- The `pgoutput` plugin is PostgreSQL's built-in logical decoding output plugin (available since PG 10)

## Verification

- Binary can connect to a local PostgreSQL instance
- Binary can create and drop a replication slot
- Binary receives raw WAL bytes when changes are made to tracked tables
- Binary correctly responds to keepalive messages to maintain the connection
- Connection recovers after PostgreSQL restart

## Dependencies

None -- this is the foundation phase.

## Estimated Scope

This is the largest and most technically complex phase, as it requires implementing the PostgreSQL wire protocol from scratch. Consider using Zig's `std.net` for TCP and `std.crypto` for authentication.

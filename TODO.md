# Zemi TODO — Closing the Gap with Bemi/Debezium

Areas where the original Bemi (Debezium + NATS + Node.js) implementation is
stronger than Zemi, and what we can do about each.

---

## 1. Large Transaction Memory (Actionable)

**Problem:** The `Decoder` accumulates all changes for a transaction in an
unbounded `ArrayList(Change)` (`src/decoder.zig:520`). A single transaction
that modifies millions of rows (e.g. `UPDATE large_table SET x = x + 1`) will
consume unbounded memory and potentially OOM the process. Debezium has
configurable strategies for this.

**Current behavior:** Changes are collected between Begin and Commit, then
returned as a single owned slice on Commit. No limit, no streaming, no
back-pressure signal to the replication stream.

**Plan:**
- [ ] Add a `MAX_TRANSACTION_CHANGES` config (env var) with a sensible default
      (e.g. 100,000). When exceeded, either:
  - (a) Flush accumulated changes to storage mid-transaction (streaming mode),
        accepting that a partial transaction could be visible if Zemi crashes, or
  - (b) Skip the oversized transaction and log a warning with the XID/LSN, or
  - (c) Spill to disk (temp file) and replay on commit.
- [ ] Option (a) is simplest and matches what most users want — changes are
      idempotent via `ON CONFLICT DO NOTHING` anyway. Implement this first.
- [ ] Add a `zemi_transaction_changes_flushed_early` counter to metrics.

---

## 2. TOASTed Column Handling (Actionable)

**Problem:** PostgreSQL's pgoutput sends unchanged TOASTed columns with a `'u'`
(unchanged) marker. Zemi parses this correctly (`src/decoder.zig:226`) but
serializes unchanged columns as `null` in the JSONB output
(`src/storage.zig:348`), making them indistinguishable from actual NULL values.

**Current behavior:** An UPDATE that changes only column A on a row where column
B is a large TOASTed text will produce `"B": null` in the `after` JSONB, even
though B is non-null and unchanged.

**Plan:**
- [ ] Omit unchanged columns from the JSON object entirely (don't include the
      key). This is the most common convention — "if it's not in `after`, it
      didn't change." This matches Debezium's default behavior.
- [ ] Update the `appendJsonObject()` function in `src/storage.zig` to skip
      `.unchanged` values instead of writing `null`.
- [ ] Add a unit test with a TOASTed column scenario.
- [ ] Document the behavior in `docs/docs/zemi/configuration.md`.

---

## 3. Data Type Fidelity (Actionable)

**Problem:** All column values are serialized as JSON strings regardless of
their PostgreSQL type. The integer `42` becomes `"42"`, the boolean `true`
becomes `"true"`, and JSON/JSONB columns get double-encoded. Debezium has
comprehensive type-specific conversion.

**Current behavior:** `type_oid` is parsed and stored in `RelationCache`
(`src/decoder.zig:479`) but never used. `appendJsonObject()` in
`src/storage.zig:332-352` treats every value as a string.

**Plan:**
- [ ] Use `type_oid` to map well-known PostgreSQL OIDs to appropriate JSON
      types. Start with the most impactful:
  - `int2/int4/int8` (OIDs 21, 23, 20) → JSON number
  - `float4/float8` (OIDs 700, 701) → JSON number
  - `bool` (OID 16) → JSON boolean (`true`/`false`)
  - `json/jsonb` (OIDs 114, 3802) → raw JSON (embed directly, no wrapping quotes)
  - `numeric` (OID 1700) → JSON string (to preserve precision)
- [ ] Leave everything else as JSON strings — this is safe and correct.
- [ ] Add unit tests for each type mapping.
- [ ] This is a **breaking change** for existing consumers that parse `"42"` as
      a string. Consider a `JSON_TYPE_COERCION=true|false` config flag, defaulting
      to `false` initially.

---

## 4. Backpressure and Buffering (Actionable)

**Problem:** The pipeline is fully synchronous: `poll() → decode() →
persistChanges() → confirmLsn()`. If the destination DB is slow, the
replication stream stalls and source WAL accumulates. Bemi had NATS as a
message broker buffer between CDC and the writer.

**Current behavior:** Zero buffering. A slow `persistChanges()` blocks `poll()`,
causing replication lag to grow. If persistence takes >60s, PostgreSQL may kill
the replication connection due to `wal_sender_timeout`.

**Plan:**
- [ ] Add a bounded in-memory queue between decode and persist. The decode loop
      reads WAL and enqueues committed change batches. A separate persist thread
      dequeues and writes to storage.
- [ ] Queue depth becomes the backpressure mechanism — when full, the decode
      loop blocks on enqueue, which naturally slows WAL consumption.
- [ ] Send periodic standby status updates from the decode thread (not gated on
      persistence completing) to prevent `wal_sender_timeout` disconnects.
- [ ] Add `zemi_queue_depth` gauge and `zemi_queue_full_stalls_total` counter
      to metrics.
- [ ] **Simpler alternative first:** Just move the standby status update to a
      separate timer thread so replication keepalives aren't blocked by slow
      persistence. This alone prevents the `wal_sender_timeout` disconnection
      without adding a full queue.

---

## 5. Replication Error Classification (Actionable)

**Problem:** The replication connection has no fine-grained error handling. All
errors from `runReplicationLoop()` are retried indefinitely with exponential
backoff (`src/main.zig:134-155`). Permanent errors (slot dropped, auth changed,
publication removed) are retried forever rather than being surfaced as fatal.

**Current behavior:** Storage has `isTransientError()` with targeted retry
(`src/storage.zig:201-212`). Replication has no such classification — the outer
loop tears down everything and rebuilds from scratch on any error.

**Plan:**
- [ ] Add `isTransientError()` to replication error handling in `main.zig`.
      Classify as **permanent**: auth failures, slot not found (after creation
      attempt), publication creation failures due to permissions.
- [ ] Add a max-retry limit for permanent errors (e.g. 3 attempts) before
      exiting with a clear error message.
- [ ] Add a `zemi_replication_errors_total` counter with a `type` label
      (transient vs permanent).
- [ ] Keep indefinite retry for genuinely transient errors (connection reset,
      timeout, etc.) — this is correct behavior.

---

## 6. Schema Evolution Edge Cases (Low Priority)

**Problem:** While `RelationCache` correctly handles schema changes (replaces
old entry on new Relation message, `src/decoder.zig:462-497`), it hasn't been
stress-tested against complex DDL sequences: rapid `ALTER TABLE` under load,
column type changes, table renames, etc.

**Current behavior:** Works correctly per pgoutput contract — PostgreSQL sends a
Relation message before any data message for a changed schema. The cache updates
atomically.

**Plan:**
- [ ] Add E2E tests for schema changes during active replication:
  - `ALTER TABLE ADD COLUMN` while INSERTs are happening
  - `ALTER TABLE DROP COLUMN`
  - `ALTER TABLE ALTER COLUMN TYPE`
- [ ] These are likely already handled correctly, but tests prove it.

---

## 7. Production Battle-Testing (Ongoing)

**Problem:** Debezium has years of production use across thousands of companies.
Zemi has zero production deployments. No amount of testing fully substitutes for
real-world traffic patterns, failure modes, and edge cases.

**Inherent tradeoff.** This only improves with time and adoption.

**Plan:**
- [ ] Run Zemi alongside Bemi in shadow mode (same source DB, separate slot,
      separate destination) and compare outputs.
- [ ] Publish a "known limitations" section in the docs.
- [ ] Set up long-running soak tests that run for days/weeks.

---

## 8. Ecosystem and Extensibility (Inherent Tradeoff)

**Problem:** Debezium supports many sinks (Kafka, S3, Elasticsearch, etc.),
transforms, and has a plugin ecosystem. Zemi only writes to a PostgreSQL
`changes` table. If you want changes routed elsewhere, you modify Zig source.

**Inherent tradeoff.** This is a conscious design choice — Zemi optimizes for
the single-binary, zero-config experience at the cost of flexibility.

**Plan:**
- [ ] Consider a webhook/HTTP sink as a second output option (post-v1.0).
- [ ] Consider stdout/JSONL output mode for piping to other tools.
- [ ] For now, users who need Kafka/S3/etc. should use Debezium directly.

---

## 9. Debugging and Observability Tooling (Inherent Tradeoff)

**Problem:** JVM ecosystem has mature debugging tools (JMX, heap dumps, thread
dumps, profilers). Zig has limited tooling and a small community. When something
goes wrong in production, debugging Zemi is harder.

**Inherent tradeoff.** Single binary means fewer moving parts to debug, but the
tools are less mature.

**Plan:**
- [ ] Ensure Prometheus metrics cover all failure modes comprehensively (mostly
      done — 14 counters + 5 gauges).
- [ ] Add structured JSON logging mode (`LOG_FORMAT=json`) for log aggregation
      systems.
- [ ] Add a `/debug` HTTP endpoint that dumps internal state: current LSN,
      queue depth, relation cache contents, uptime, config (redacted passwords).
- [ ] Document common failure modes and their metric signatures.

---

## Priority Order

1. **TOASTed columns** (#2) — data correctness bug, small fix
2. **Large transaction memory** (#1) — OOM risk, moderate fix
3. **Keepalive during slow persist** (#4, simpler alternative) — prevents disconnects
4. **Replication error classification** (#5) — operational improvement
5. **Data type fidelity** (#3) — quality improvement, breaking change needs care
6. **Schema evolution E2E tests** (#6) — confidence building
7. **JSON logging** (#9) — operational improvement
8. Everything else — post-v1.0

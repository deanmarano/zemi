---
title: 'Benchmarks'
sidebar_label: 'Benchmarks'
---

# Benchmarks

All measurements were taken on a MacBook Pro (Apple M-series) comparing the original Bemi Docker image (`bemi-original`) against the Zemi Docker image and native binary.

## Docker Image Size

| Image | Size | Contents |
|-------|------|----------|
| **Original Bemi** | **3.23 GB** | Node.js 21 + Java JRE + Debezium 2.5 + NATS 2.10 + app code |
| **Zemi** | **1.04 MB** | Static Zig binary on `scratch` base |

**Reduction: 3,100x**

The original image is large because it bundles three complete runtimes: a JRE for Debezium, a Go binary for NATS, and Node.js for the worker. Zemi compiles everything into a single statically-linked binary with no runtime dependencies.

### How we measured

```bash
# Build both images
docker build --platform linux/amd64 -t bemi-original ./worker
docker build --platform linux/amd64 -t zemi .

# Compare
docker images bemi-original --format "{{.Size}}"  # 3.23GB
docker images zemi --format "{{.Size}}"            # 1.04MB
```

## Binary Size

| Target | Size |
|--------|------|
| x86_64-linux (static, musl) | 3.7 MB |
| aarch64-linux (static, musl) | 3.8 MB |
| x86_64-macos | 510 KB |
| aarch64-macos | 497 KB |

All Linux binaries are statically linked (`-target x86_64-linux-musl`) with `-Doptimize=ReleaseSafe`. No stripping applied — these are the sizes with full safety checks and debug info.

## Memory Usage

| Process | RSS (Resident Set Size) |
|---------|------------------------|
| **Zemi** (active WAL replication) | **2.8 MB** |
| **Original Bemi** — NATS server alone | 19 MB |
| **Original Bemi** — steady state (JVM + Node.js + NATS) | 300–500+ MB |

**Reduction: ~150x**

Zemi has no garbage collector and no hidden allocations. Memory usage is stable and predictable: a 64 KB read buffer for the WAL stream, allocations for decoded changes (freed after persistence), and the connection state.

### How we measured

```bash
# Start Zemi against a live PostgreSQL
DB_HOST=127.0.0.1 DB_PORT=5432 DB_NAME=bemi_test DB_USER=postgres DB_PASSWORD="" \
  ./zig-out/bin/zemi &

# After 2 seconds of active replication:
ps -o rss= -p $!  # 2896 KB = 2.8 MB
```

## Startup Time

| Process | Time to first log |
|---------|-------------------|
| **Zemi** | **&lt;1 ms** (`0.00 real` per `/usr/bin/time`) |
| **Original Bemi** | 30–60 seconds |

**Zemi starts instantly.** There is no JVM to warm up, no Node.js runtime to initialize, no NATS server to bootstrap. The binary loads, reads environment variables, and connects to PostgreSQL.

### How we measured

```bash
/usr/bin/time ./zig-out/bin/zemi  # 0.00 real, 0.00 user, 0.00 sys
```

The original Bemi needs ~30 seconds before all three processes (NATS, Debezium/JVM, Node.js worker) are ready and processing WAL changes.

## Process Count

| System | Processes |
|--------|-----------|
| **Zemi** | 1 |
| **Original Bemi** | 4+ (`sh`, `nats-server`, `java`, `node`) |

Fewer processes means fewer failure modes, simpler monitoring, and easier debugging.

## Runtime Dependencies

| System | Dependencies |
|--------|-------------|
| **Zemi** | None (statically linked) |
| **Original Bemi** | Node.js 21, Java JRE (OpenJDK), Debezium 2.5, NATS Server 2.10, pnpm, MikroORM, pg-protocol |

Zemi has zero runtime dependencies. Copy the binary to any Linux machine and run it. No package managers, no version conflicts, no supply chain to audit.

## Summary

| Metric | Original Bemi | Zemi | Factor |
|--------|--------------|------|--------|
| Docker image | 3.23 GB | 1.04 MB | 3,100x |
| Memory | 300–500 MB | 2.8 MB | ~150x |
| Startup | 30–60 s | &lt;1 ms | &gt;30,000x |
| Processes | 4+ | 1 | 4x |
| Dependencies | 7+ | 0 | -- |

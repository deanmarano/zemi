const std = @import("std");
const posix = std.posix;
const Connection = @import("connection.zig").Connection;
const Config = @import("config.zig").Config;
const ReplicationStream = @import("replication.zig").ReplicationStream;
const replication = @import("replication.zig");
const protocol = @import("protocol.zig");
const decoder = @import("decoder.zig");
const Storage = @import("storage.zig").Storage;
const health = @import("health.zig");
const Metrics = @import("metrics.zig").Metrics;
const MetricsServer = @import("metrics.zig").MetricsServer;

// Allow all messages at comptime; runtime filtering is in our custom logFn.
pub const std_options: std.Options = .{
    .log_level = .debug,
    .logFn = runtimeLogFn,
};

const log = std.log.scoped(.main);

/// Runtime log level, set from Config before the main loop starts.
var runtime_log_level: std.log.Level = .info;

/// Custom log function that filters based on runtime_log_level.
fn runtimeLogFn(
    comptime level: std.log.Level,
    comptime scope: anytype,
    comptime format: []const u8,
    args: anytype,
) void {
    // Check if this message's level is enabled by the runtime setting.
    // log.Level ordering: .err < .warn < .info < .debug
    // A level is enabled if its integer value <= the configured level.
    if (@intFromEnum(level) > @intFromEnum(runtime_log_level)) return;

    const scope_prefix = if (scope != .default)
        "(" ++ @tagName(scope) ++ ") "
    else
        "";

    const prefix = "[" ++ comptime level.asText() ++ "] " ++ scope_prefix;

    std.debug.lockStdErr();
    defer std.debug.unlockStdErr();

    const stderr = std.io.getStdErr().writer();
    nosuspend stderr.print(prefix ++ format ++ "\n", args) catch {};
}

/// Global flag set by signal handler to request shutdown.
var shutdown_requested = std.atomic.Value(bool).init(false);

/// Counter for number of signals received — second signal forces immediate exit.
var signal_count = std.atomic.Value(u32).init(0);

fn signalHandler(sig: c_int) callconv(.c) void {
    _ = sig;
    const count = signal_count.fetchAdd(1, .monotonic);
    if (count >= 1) {
        // Second signal — force immediate exit
        std.process.exit(1);
    }
    shutdown_requested.store(true, .release);
}

fn installSignalHandlers() void {
    const sa = posix.Sigaction{
        .handler = .{ .handler = signalHandler },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.INT, &sa, null);
    posix.sigaction(posix.SIG.TERM, &sa, null);
}

/// Classify whether a replication-loop error is permanent (non-retryable).
/// Permanent errors are caused by misconfiguration (bad credentials, missing
/// SSL certs, etc.) and retrying won't help without operator intervention.
/// Returns true for permanent errors, false for transient (network, server restart, etc.).
fn isPermanentError(err: anyerror) bool {
    return switch (err) {
        error.AuthenticationFailed,
        error.UnsupportedAuthMethod,
        error.SslNotSupported,
        error.SslCertificateError,
        error.ServerNonceMismatch,
        error.ServerSignatureMismatch,
        error.Base64DecodeFailed,
        error.WeakParameters,
        => true,
        else => false,
    };
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    installSignalHandlers();

    const config = Config.fromEnv();

    // Wire up runtime log level before anything else logs
    runtime_log_level = config.log_level;

    // Validate configuration
    if (config.validate()) |err_msg| {
        log.err("invalid configuration: {s}", .{err_msg});
        std.process.exit(1);
    }

    config.dump();

    // Initialize metrics
    var m = Metrics{ .start_time_secs = std.time.timestamp() };

    // Start health check server if configured
    var health_server: ?*health.HealthServer = null;
    if (config.health_port) |port| {
        if (health.HealthServer.start(port, allocator, &m)) |hs| {
            health_server = hs;
            log.info("health check server listening on port {d}", .{port});
        } else |err| {
            log.warn("failed to start health check server on port {d}: {}", .{ port, err });
        }
    }
    defer {
        if (health_server) |hs| {
            hs.stop();
            log.info("health check server stopped", .{});
        }
    }

    // Start metrics server if configured
    var metrics_server: ?*MetricsServer = null;
    if (config.metrics_port) |port| {
        if (MetricsServer.start(port, &m, allocator)) |ms| {
            metrics_server = ms;
            log.info("metrics server listening on port {d}", .{port});
        } else |err| {
            log.warn("failed to start metrics server on port {d}: {}", .{ port, err });
        }
    }
    defer {
        if (metrics_server) |ms| {
            ms.stop();
            log.info("metrics server stopped", .{});
        }
    }

    // Main loop with reconnection
    var attempt: u32 = 0;
    var permanent_attempt: u32 = 0;
    const max_permanent_retries: u32 = 3;
    const max_backoff_secs: u64 = 60;

    while (!shutdown_requested.load(.acquire)) {
        runReplicationLoop(allocator, config, &m) catch |err| {
            if (shutdown_requested.load(.acquire)) break;

            attempt += 1;
            Metrics.inc(&m.replication_reconnections_total);
            Metrics.set(&m.replication_connected, 0);

            if (isPermanentError(err)) {
                permanent_attempt += 1;
                Metrics.inc(&m.replication_permanent_errors_total);
                log.err("permanent replication error (attempt {d}/{d}): {}", .{ permanent_attempt, max_permanent_retries, err });

                if (permanent_attempt >= max_permanent_retries) {
                    log.err("max retries for permanent error reached, exiting. Fix configuration and restart.", .{});
                    std.process.exit(1);
                }

                // Short backoff for permanent errors (give operator time to see logs)
                const backoff: u64 = @min(max_backoff_secs, @as(u64, 5) * permanent_attempt);
                log.err("retrying in {d}s...", .{backoff});
                std.time.sleep(backoff * @as(u64, std.time.ns_per_s));
            } else {
                permanent_attempt = 0; // reset on transient error
                Metrics.inc(&m.replication_transient_errors_total);
                const backoff = @min(
                    max_backoff_secs,
                    @as(u64, 1) << @min(attempt, 6), // 1, 2, 4, 8, 16, 32, 60
                );
                log.err("transient replication error (attempt {d}): {}, retrying in {d}s...", .{ attempt, err, backoff });
                std.time.sleep(backoff * @as(u64, std.time.ns_per_s));
            }
            continue;
        };

        // If runReplicationLoop returned without error, it was a clean shutdown
        break;
    }

    // Cleanup: drop replication slot and publication if configured
    if (config.cleanup_on_shutdown) {
        log.info("CLEANUP_ON_SHUTDOWN enabled, removing slot and publication...", .{});
        replication.dropSlot(allocator, config);
        replication.dropPublication(allocator, config);
    }

    log.info("zemi shutdown complete", .{});
}

fn runReplicationLoop(allocator: std.mem.Allocator, config: Config, m: *Metrics) !void {
    // Step 1: Ensure the publication exists (normal connection)
    log.info("ensuring publication exists...", .{});
    replication.ensurePublication(allocator, config) catch |err| {
        log.err("failed to ensure publication: {}", .{err});
        return err;
    };

    // Step 2: Open storage connection for persisting changes
    log.info("opening storage connection...", .{});
    var storage = Storage.init(allocator, config, m) catch |err| {
        log.err("failed to open storage connection: {}", .{err});
        return err;
    };
    defer {
        storage.deinit();
        Metrics.set(&m.storage_connected, 0);
        log.info("storage connection closed", .{});
    }
    Metrics.set(&m.storage_connected, 1);

    // Step 3: Open replication connection
    log.info("opening replication connection...", .{});
    var stream = ReplicationStream.init(allocator, config, m) catch |err| {
        log.err("failed to open replication stream: {}", .{err});
        return err;
    };
    defer {
        // Send final status update before closing
        stream.sendStatusUpdate() catch {};
        stream.deinit();
        Metrics.set(&m.replication_connected, 0);
        log.info("replication connection closed", .{});
    }
    Metrics.set(&m.replication_connected, 1);

    // Step 4: Identify system
    const sys_info = try stream.identifySystem();
    log.info("system: id={s} timeline={s} xlogpos={s} db={s}", .{
        sys_info.systemid,
        sys_info.timeline,
        sys_info.xlogpos,
        sys_info.dbname,
    });

    // Step 5: Create replication slot
    try stream.createSlotIfNotExists();

    // Step 6: Start streaming
    try stream.startReplication(0);

    log.info("streaming WAL changes... (ctrl-c to stop)", .{});

    // Start background keepalive thread to prevent wal_sender_timeout
    // during long persistChanges() calls. Sends StandbyStatusUpdate
    // every 10 seconds, well within PostgreSQL's default 60s timeout.
    stream.startKeepaliveThread(10);

    // Step 7: Initialize the pgoutput decoder
    var dec = decoder.Decoder.init(allocator, config.db_name);
    dec.max_transaction_changes = config.max_transaction_changes;
    defer dec.deinit();

    // Step 8: Main loop - read, decode, persist WAL changes
    var changes_count: u64 = 0;
    var tx_count: u64 = 0;
    var last_summary_time: i64 = std.time.timestamp();

    while (!shutdown_requested.load(.acquire)) {
        const xlog = try stream.poll();

        if (xlog) |data| {
            // Decode the pgoutput message; returns changes on Commit or early flush
            const result = dec.decode(data.data, data.wal_start) catch |err| {
                log.warn("decode error: {}, skipping message", .{err});
                Metrics.inc(&m.decode_errors_total);
                continue;
            };

            const changes: ?[]decoder.Change = switch (result) {
                .commit => |c| c,
                .flush => |c| blk: {
                    Metrics.inc(&m.transaction_early_flushes_total);
                    break :blk c;
                },
                .none => null,
            };

            if (changes) |tx_changes| {
                defer {
                    for (tx_changes) |change| {
                        change.deinit(allocator);
                    }
                    allocator.free(tx_changes);
                }

                // Filter out changes to the 'changes' table itself to avoid
                // infinite feedback loops (our INSERTs generate WAL too).
                // Also apply TABLES filter if configured.
                var filtered = std.ArrayList(decoder.Change).init(allocator);
                defer filtered.deinit();
                for (tx_changes) |change| {
                    if (std.mem.eql(u8, change.table, "changes")) {
                        Metrics.inc(&m.changes_filtered_total);
                        continue;
                    }
                    if (!config.shouldTrackTable(change.table)) {
                        log.debug("skipping untracked table: {s}", .{change.table});
                        Metrics.inc(&m.changes_filtered_total);
                        continue;
                    }
                    filtered.append(change) catch {};
                }

                if (filtered.items.len == 0) {
                    // All changes were filtered out — skip
                    stream.confirmLsn(data.wal_end);
                    continue;
                }

                // Persist the transaction's changes to the changes table
                const inserted = storage.persistChanges(filtered.items) catch |err| {
                    log.err("failed to persist {d} changes: {}", .{ filtered.items.len, err });
                    Metrics.inc(&m.persist_errors_total);
                    // Don't confirm LSN — changes will be replayed on reconnect
                    continue;
                };

                tx_count += 1;
                changes_count += filtered.items.len;
                Metrics.inc(&m.transactions_processed_total);

                // Increment per-operation counters
                for (filtered.items) |change| {
                    switch (change.operation) {
                        .CREATE => Metrics.inc(&m.changes_created_total),
                        .UPDATE => Metrics.inc(&m.changes_updated_total),
                        .DELETE => Metrics.inc(&m.changes_deleted_total),
                        .TRUNCATE => Metrics.inc(&m.changes_truncated_total),
                    }
                }

                if (inserted > 0) {
                    for (filtered.items) |change| {
                        log.info("{s} {s}.{s}.{s} pk={s} (tx {d}, lsn={s})", .{
                            change.operation.toString(),
                            change.database,
                            change.schema,
                            change.table,
                            change.primary_key,
                            change.transaction_id,
                            change.position,
                        });
                    }
                    log.debug("persisted {d}/{d} changes", .{ inserted, filtered.items.len });
                } else {
                    log.debug("skipped {d} duplicate changes", .{filtered.items.len});
                    Metrics.add(&m.changes_duplicated_total, filtered.items.len);
                }

                // Only confirm LSN after successful persistence
                stream.confirmLsn(data.wal_end);
            } else {
                // Non-change messages (keepalives, etc) — confirm immediately
                stream.confirmLsn(data.wal_end);
            }

            // Periodic summary log (every 60 seconds)
            const now = std.time.timestamp();
            if (now - last_summary_time >= 60) {
                log.info("summary: {d} changes processed in {d} transactions, replication lag={d} bytes", .{
                    changes_count,
                    tx_count,
                    stream.last_received_lsn - stream.last_flushed_lsn,
                });
                last_summary_time = now;
            }
        }
    }

    log.info("shutdown requested, flushing {d} changes processed in {d} transactions...", .{ changes_count, tx_count });
}

// Pull in tests from all modules
test {
    _ = @import("protocol.zig");
    _ = @import("config.zig");
    _ = @import("decoder.zig");
    _ = @import("storage.zig");
    _ = @import("health.zig");
    _ = @import("scram.zig");
    _ = @import("metrics.zig");
}

test "isPermanentError classifies auth errors as permanent" {
    try std.testing.expect(isPermanentError(error.AuthenticationFailed));
    try std.testing.expect(isPermanentError(error.UnsupportedAuthMethod));
    try std.testing.expect(isPermanentError(error.SslNotSupported));
    try std.testing.expect(isPermanentError(error.SslCertificateError));
    try std.testing.expect(isPermanentError(error.ServerNonceMismatch));
    try std.testing.expect(isPermanentError(error.ServerSignatureMismatch));
    try std.testing.expect(isPermanentError(error.Base64DecodeFailed));
    try std.testing.expect(isPermanentError(error.WeakParameters));
}

test "isPermanentError classifies network errors as transient" {
    try std.testing.expect(!isPermanentError(error.ConnectionRefused));
    try std.testing.expect(!isPermanentError(error.ConnectionResetByPeer));
    try std.testing.expect(!isPermanentError(error.BrokenPipe));
    try std.testing.expect(!isPermanentError(error.UnexpectedEndOfData));
    try std.testing.expect(!isPermanentError(error.ServerError));
    try std.testing.expect(!isPermanentError(error.OutOfMemory));
}

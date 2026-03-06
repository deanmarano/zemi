const std = @import("std");
const posix = std.posix;
const net = std.net;

const log = std.log.scoped(.metrics);

// Standard Prometheus histogram bucket boundaries (in seconds).
// These are cumulative: each bucket counts observations <= boundary.
const histogram_boundaries = [_]f64{ 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0 };
const num_buckets = histogram_boundaries.len;

/// A thread-safe Prometheus histogram with fixed exponential buckets.
/// Uses atomic counters for lock-free concurrent access.
/// Bucket boundaries are defined at comptime (histogram_boundaries).
pub const Histogram = struct {
    /// Cumulative bucket counters: bucket[i] counts observations <= histogram_boundaries[i].
    buckets: [num_buckets]std.atomic.Value(u64),
    /// Total count of observations (+Inf bucket).
    count: std.atomic.Value(u64),
    /// Sum of all observed values, stored as microseconds (u64) to avoid float atomics.
    sum_usec: std.atomic.Value(u64),

    pub fn init() Histogram {
        var h: Histogram = undefined;
        for (&h.buckets) |*b| {
            b.* = std.atomic.Value(u64).init(0);
        }
        h.count = std.atomic.Value(u64).init(0);
        h.sum_usec = std.atomic.Value(u64).init(0);
        return h;
    }

    /// Observe a duration in nanoseconds (from std.time.Timer).
    pub fn observeNanos(self: *Histogram, nanos: u64) void {
        const usec = nanos / 1_000; // convert to microseconds
        const seconds = @as(f64, @floatFromInt(nanos)) / 1_000_000_000.0;
        // Increment all cumulative buckets where value <= boundary
        for (&self.buckets, 0..) |*bucket, i| {
            if (seconds <= histogram_boundaries[i]) {
                _ = bucket.fetchAdd(1, .monotonic);
            }
        }
        _ = self.count.fetchAdd(1, .monotonic);
        _ = self.sum_usec.fetchAdd(usec, .monotonic);
    }
};

/// Thread-safe Prometheus metrics for Zemi.
///
/// All counters use `std.atomic.Value(u64)` with `.monotonic` ordering,
/// which is sufficient for metrics (relaxed consistency is acceptable).
/// Gauges that represent state transitions use `.monotonic` as well.
pub const Metrics = struct {
    // --- Counters (monotonically increasing) ---

    /// Total changes processed by operation type.
    changes_created_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    changes_updated_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    changes_deleted_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    changes_truncated_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Changes skipped by table filter or feedback-loop filter.
    changes_filtered_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Changes skipped due to ON CONFLICT deduplication.
    changes_duplicated_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total committed transactions processed.
    transactions_processed_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total WAL messages received (XLogData).
    wal_messages_received_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total keepalive messages received.
    keepalives_received_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total decode errors encountered.
    decode_errors_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total persist errors (transient retries) encountered.
    persist_errors_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total storage reconnections performed.
    storage_reconnections_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total replication reconnections (main loop restarts).
    replication_reconnections_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Mid-transaction flushes due to MAX_TRANSACTION_CHANGES limit.
    transaction_early_flushes_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total replication errors classified as transient (retryable).
    replication_transient_errors_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Total replication errors classified as permanent (non-retryable).
    replication_permanent_errors_total: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    // --- Gauges (current value) ---

    /// Highest WAL position received.
    last_received_lsn: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Highest WAL position confirmed/flushed.
    last_flushed_lsn: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// 1 if replication is connected and streaming, 0 if reconnecting.
    replication_connected: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// 1 if storage connection is alive, 0 if reconnecting.
    storage_connected: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Process start time (Unix timestamp in seconds), set once at init.
    start_time_secs: i64 = 0,

    // --- Histograms (latency distributions) ---

    /// Duration of persistChanges() calls in seconds.
    persist_duration_seconds: Histogram = Histogram.init(),

    /// Duration of decode() calls in seconds (per WAL message).
    decode_duration_seconds: Histogram = Histogram.init(),

    // --- Helpers ---

    pub fn inc(counter: *std.atomic.Value(u64)) void {
        _ = counter.fetchAdd(1, .monotonic);
    }

    pub fn add(counter: *std.atomic.Value(u64), value: u64) void {
        _ = counter.fetchAdd(value, .monotonic);
    }

    pub fn set(gauge: *std.atomic.Value(u64), value: u64) void {
        gauge.store(value, .monotonic);
    }

    /// Render all metrics in Prometheus text exposition format.
    /// Emits each metric with both `zemi_` and `bemi_` prefixes for
    /// drop-in compatibility with existing dashboards and alerts.
    /// Caller must free the returned slice.
    pub fn render(self: *const Metrics, allocator: std.mem.Allocator) ![]u8 {
        var buf = std.ArrayList(u8).init(allocator);
        errdefer buf.deinit();

        // Snapshot all values once for consistency across both prefix passes.
        const created = self.changes_created_total.load(.monotonic);
        const updated = self.changes_updated_total.load(.monotonic);
        const deleted = self.changes_deleted_total.load(.monotonic);
        const truncated = self.changes_truncated_total.load(.monotonic);
        const filtered = self.changes_filtered_total.load(.monotonic);
        const duplicated = self.changes_duplicated_total.load(.monotonic);
        const transactions = self.transactions_processed_total.load(.monotonic);
        const wal_msgs = self.wal_messages_received_total.load(.monotonic);
        const keepalives = self.keepalives_received_total.load(.monotonic);
        const decode_errs = self.decode_errors_total.load(.monotonic);
        const persist_errs = self.persist_errors_total.load(.monotonic);
        const storage_reconns = self.storage_reconnections_total.load(.monotonic);
        const repl_reconns = self.replication_reconnections_total.load(.monotonic);
        const early_flushes = self.transaction_early_flushes_total.load(.monotonic);
        const repl_transient_errs = self.replication_transient_errors_total.load(.monotonic);
        const repl_permanent_errs = self.replication_permanent_errors_total.load(.monotonic);
        const received_lsn = self.last_received_lsn.load(.monotonic);
        const flushed_lsn = self.last_flushed_lsn.load(.monotonic);
        const lag = if (received_lsn >= flushed_lsn) received_lsn - flushed_lsn else 0;
        const repl_conn = self.replication_connected.load(.monotonic);
        const stor_conn = self.storage_connected.load(.monotonic);
        const now = std.time.timestamp();
        const uptime: u64 = if (now > self.start_time_secs) @intCast(now - self.start_time_secs) else 0;

        // Emit with both prefixes: zemi_ (canonical) and bemi_ (backward compat).
        const prefixes = [_][]const u8{ "zemi_", "bemi_" };
        for (prefixes) |prefix| {
            try appendCounterPrefixed(&buf, prefix, "changes_processed_total", "Total changes processed.", &[_]LabeledValue{
                .{ .labels = "operation=\"CREATE\"", .value = created },
                .{ .labels = "operation=\"UPDATE\"", .value = updated },
                .{ .labels = "operation=\"DELETE\"", .value = deleted },
                .{ .labels = "operation=\"TRUNCATE\"", .value = truncated },
            });

            try appendSimpleCounterPrefixed(&buf, prefix, "changes_filtered_total", "Changes skipped by table filter or feedback-loop filter.", filtered);
            try appendSimpleCounterPrefixed(&buf, prefix, "changes_duplicated_total", "Changes skipped due to ON CONFLICT deduplication.", duplicated);
            try appendSimpleCounterPrefixed(&buf, prefix, "transactions_processed_total", "Total committed transactions processed.", transactions);
            try appendSimpleCounterPrefixed(&buf, prefix, "wal_messages_received_total", "Total WAL messages received.", wal_msgs);
            try appendSimpleCounterPrefixed(&buf, prefix, "keepalives_received_total", "Total keepalive messages received.", keepalives);
            try appendSimpleCounterPrefixed(&buf, prefix, "decode_errors_total", "Total decode errors encountered.", decode_errs);
            try appendSimpleCounterPrefixed(&buf, prefix, "persist_errors_total", "Total persist errors encountered.", persist_errs);
            try appendSimpleCounterPrefixed(&buf, prefix, "storage_reconnections_total", "Total storage reconnections.", storage_reconns);
            try appendSimpleCounterPrefixed(&buf, prefix, "replication_reconnections_total", "Total replication reconnections.", repl_reconns);
            try appendSimpleCounterPrefixed(&buf, prefix, "transaction_early_flushes_total", "Mid-transaction flushes due to MAX_TRANSACTION_CHANGES limit.", early_flushes);
            try appendSimpleCounterPrefixed(&buf, prefix, "replication_transient_errors_total", "Replication errors classified as transient (retryable).", repl_transient_errs);
            try appendSimpleCounterPrefixed(&buf, prefix, "replication_permanent_errors_total", "Replication errors classified as permanent (non-retryable).", repl_permanent_errs);

            // Gauges
            try appendSimpleGaugePrefixed(&buf, prefix, "replication_lag_bytes", "Replication lag in bytes (received - flushed LSN).", lag);
            try appendSimpleGaugePrefixed(&buf, prefix, "last_received_lsn", "Highest WAL position received.", received_lsn);
            try appendSimpleGaugePrefixed(&buf, prefix, "last_flushed_lsn", "Highest WAL position confirmed.", flushed_lsn);
            try appendSimpleGaugePrefixed(&buf, prefix, "replication_connected", "1 if replication is streaming, 0 if reconnecting.", repl_conn);
            try appendSimpleGaugePrefixed(&buf, prefix, "storage_connected", "1 if storage connection is alive, 0 if reconnecting.", stor_conn);
            try appendSimpleGaugePrefixed(&buf, prefix, "uptime_seconds", "Seconds since process start.", uptime);

            // Histograms
            try appendHistogramPrefixed(&buf, prefix, "persist_duration_seconds", "Duration of persist operations in seconds.", &self.persist_duration_seconds);
            try appendHistogramPrefixed(&buf, prefix, "decode_duration_seconds", "Duration of decode operations in seconds.", &self.decode_duration_seconds);
        }

        return try buf.toOwnedSlice();
    }
};

const LabeledValue = struct {
    labels: []const u8,
    value: u64,
};

fn appendCounterPrefixed(buf: *std.ArrayList(u8), prefix: []const u8, name: []const u8, help: []const u8, labeled_values: []const LabeledValue) !void {
    try buf.appendSlice("# HELP ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" ");
    try buf.appendSlice(help);
    try buf.append('\n');
    try buf.appendSlice("# TYPE ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" counter");
    try buf.append('\n');
    for (labeled_values) |lv| {
        try buf.appendSlice(prefix);
        try buf.appendSlice(name);
        try buf.append('{');
        try buf.appendSlice(lv.labels);
        try buf.appendSlice("} ");
        try appendU64(buf, lv.value);
        try buf.append('\n');
    }
}

fn appendSimpleCounterPrefixed(buf: *std.ArrayList(u8), prefix: []const u8, name: []const u8, help: []const u8, value: u64) !void {
    try buf.appendSlice("# HELP ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" ");
    try buf.appendSlice(help);
    try buf.append('\n');
    try buf.appendSlice("# TYPE ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" counter");
    try buf.append('\n');
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.append(' ');
    try appendU64(buf, value);
    try buf.append('\n');
}

fn appendSimpleGaugePrefixed(buf: *std.ArrayList(u8), prefix: []const u8, name: []const u8, help: []const u8, value: u64) !void {
    try buf.appendSlice("# HELP ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" ");
    try buf.appendSlice(help);
    try buf.append('\n');
    try buf.appendSlice("# TYPE ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" gauge");
    try buf.append('\n');
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.append(' ');
    try appendU64(buf, value);
    try buf.append('\n');
}

fn appendCounter(buf: *std.ArrayList(u8), name: []const u8, help: []const u8, labeled_values: []const LabeledValue) !void {
    try appendLine(buf, "# HELP ", name, " ", help);
    try appendLine(buf, "# TYPE ", name, " counter", "");
    for (labeled_values) |lv| {
        try buf.appendSlice(name);
        try buf.append('{');
        try buf.appendSlice(lv.labels);
        try buf.appendSlice("} ");
        try appendU64(buf, lv.value);
        try buf.append('\n');
    }
}

fn appendSimpleCounter(buf: *std.ArrayList(u8), name: []const u8, help: []const u8, value: u64) !void {
    try appendLine(buf, "# HELP ", name, " ", help);
    try appendLine(buf, "# TYPE ", name, " counter", "");
    try buf.appendSlice(name);
    try buf.append(' ');
    try appendU64(buf, value);
    try buf.append('\n');
}

fn appendSimpleGauge(buf: *std.ArrayList(u8), name: []const u8, help: []const u8, value: u64) !void {
    try appendLine(buf, "# HELP ", name, " ", help);
    try appendLine(buf, "# TYPE ", name, " gauge", "");
    try buf.appendSlice(name);
    try buf.append(' ');
    try appendU64(buf, value);
    try buf.append('\n');
}

fn appendLine(buf: *std.ArrayList(u8), prefix: []const u8, name: []const u8, mid: []const u8, suffix: []const u8) !void {
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(mid);
    try buf.appendSlice(suffix);
    try buf.append('\n');
}

fn appendU64(buf: *std.ArrayList(u8), value: u64) !void {
    var num_buf: [20]u8 = undefined;
    const num_str = std.fmt.bufPrint(&num_buf, "{d}", .{value}) catch unreachable;
    try buf.appendSlice(num_str);
}

fn appendFloat(buf: *std.ArrayList(u8), value: f64) !void {
    var num_buf: [32]u8 = undefined;
    const num_str = std.fmt.bufPrint(&num_buf, "{d:.6}", .{value}) catch unreachable;
    try buf.appendSlice(num_str);
}

/// Render a Prometheus histogram with the given prefix.
/// Format:
///   # HELP {prefix}{name} {help}
///   # TYPE {prefix}{name} histogram
///   {prefix}{name}_bucket{le="0.001"} N
///   ...
///   {prefix}{name}_bucket{le="+Inf"} N
///   {prefix}{name}_sum N.NNNNNN
///   {prefix}{name}_count N
fn appendHistogramPrefixed(buf: *std.ArrayList(u8), prefix: []const u8, name: []const u8, help: []const u8, h: *const Histogram) !void {
    // HELP / TYPE
    try buf.appendSlice("# HELP ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" ");
    try buf.appendSlice(help);
    try buf.append('\n');
    try buf.appendSlice("# TYPE ");
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice(" histogram");
    try buf.append('\n');

    // Bucket lines
    const count = h.count.load(.monotonic);
    for (h.buckets, 0..) |bucket, i| {
        const bucket_count = bucket.load(.monotonic);
        try buf.appendSlice(prefix);
        try buf.appendSlice(name);
        try buf.appendSlice("_bucket{le=\"");
        var le_buf: [16]u8 = undefined;
        const le_str = std.fmt.bufPrint(&le_buf, "{d}", .{histogram_boundaries[i]}) catch unreachable;
        try buf.appendSlice(le_str);
        try buf.appendSlice("\"} ");
        try appendU64(buf, bucket_count);
        try buf.append('\n');
    }

    // +Inf bucket (always equals count)
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice("_bucket{le=\"+Inf\"} ");
    try appendU64(buf, count);
    try buf.append('\n');

    // Sum (convert from microseconds to seconds)
    const sum_usec = h.sum_usec.load(.monotonic);
    const sum_seconds = @as(f64, @floatFromInt(sum_usec)) / 1_000_000.0;
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice("_sum ");
    try appendFloat(buf, sum_seconds);
    try buf.append('\n');

    // Count
    try buf.appendSlice(prefix);
    try buf.appendSlice(name);
    try buf.appendSlice("_count ");
    try appendU64(buf, count);
    try buf.append('\n');
}

// ============================================================================
// Metrics HTTP Server
// ============================================================================

/// TCP server that serves Prometheus metrics on `/metrics` and a simple
/// health response on any other path. Follows the same pattern as HealthServer.
pub const MetricsServer = struct {
    thread: std.Thread,
    server_fd: posix.socket_t,
    should_stop: std.atomic.Value(bool),
    bound_port: u16,
    metrics: *const Metrics,
    allocator: std.mem.Allocator,

    /// Start the metrics HTTP server on the given port.
    /// The server is heap-allocated so that the background thread's
    /// pointer remains valid after this function returns.
    pub fn start(port: u16, m: *const Metrics, allocator: std.mem.Allocator) !*MetricsServer {
        const addr = net.Address.initIp4(.{ 0, 0, 0, 0 }, port);
        const server_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
        errdefer posix.close(server_fd);

        // SO_REUSEADDR
        const optval: c_int = 1;
        try posix.setsockopt(server_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, std.mem.asBytes(&optval));

        try posix.bind(server_fd, &addr.any, addr.getOsSockLen());
        try posix.listen(server_fd, 8);

        // Get actual bound port (needed for ephemeral port 0)
        var addr_storage: posix.sockaddr.storage = undefined;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
        try posix.getsockname(server_fd, @ptrCast(&addr_storage), &addr_len);
        const addr_in: *const posix.sockaddr.in = @ptrCast(@alignCast(&addr_storage));
        const bound_port = std.mem.bigToNative(u16, addr_in.port);

        const ms = try allocator.create(MetricsServer);
        ms.* = .{
            .thread = undefined,
            .server_fd = server_fd,
            .should_stop = std.atomic.Value(bool).init(false),
            .bound_port = bound_port,
            .metrics = m,
            .allocator = allocator,
        };

        ms.thread = try std.Thread.spawn(.{}, acceptLoop, .{ms});
        return ms;
    }

    pub fn stop(self: *MetricsServer) void {
        self.should_stop.store(true, .release);

        // Connect to ourselves to unblock accept()
        const wake_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, self.bound_port);
        if (posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0)) |wake_fd| {
            posix.connect(wake_fd, &wake_addr.any, wake_addr.getOsSockLen()) catch {};
            posix.close(wake_fd);
        } else |_| {}

        self.thread.join();
        posix.close(self.server_fd);
        self.allocator.destroy(self);
    }

    fn acceptLoop(self: *MetricsServer) void {
        while (!self.should_stop.load(.acquire)) {
            // Use raw C accept to avoid Zig's unreachable assertions on NOTSOCK/BADF
            const rc = std.c.accept(self.server_fd, null, null);
            if (rc < 0) continue;
            const client_fd: posix.socket_t = rc;

            if (self.should_stop.load(.acquire)) {
                posix.close(client_fd);
                break;
            }

            self.handleClient(client_fd);
            posix.close(client_fd);
        }
    }

    fn handleClient(self: *MetricsServer, client_fd: posix.socket_t) void {
        const stream = std.net.Stream{ .handle = client_fd };

        // Read enough of the HTTP request to determine the path
        var request_buf: [1024]u8 = undefined;
        const bytes_read = stream.read(&request_buf) catch {
            stream.writeAll(http_ok_response) catch {};
            return;
        };

        if (bytes_read == 0) return;

        const request = request_buf[0..bytes_read];

        // Check if the request is for /metrics
        if (isMetricsRequest(request)) {
            // Render metrics
            const body = self.metrics.render(self.allocator) catch {
                stream.writeAll("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 22\r\nConnection: close\r\n\r\nmetrics render failed\n") catch {};
                return;
            };
            defer self.allocator.free(body);

            // Build HTTP response with Content-Length
            var header_buf: [256]u8 = undefined;
            const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len}) catch {
                stream.writeAll(http_ok_response) catch {};
                return;
            };

            stream.writeAll(header) catch {};
            stream.writeAll(body) catch {};
        } else {
            stream.writeAll(http_ok_response) catch {};
        }
    }

    const http_ok_response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";
};

/// Check if an HTTP request is for the /metrics path.
fn isMetricsRequest(request: []const u8) bool {
    // Parse first line: "GET /metrics HTTP/1.1\r\n..."
    // Find the path by looking for "GET " or similar method prefix
    if (std.mem.startsWith(u8, request, "GET /metrics")) return true;
    if (std.mem.startsWith(u8, request, "get /metrics")) return true;
    return false;
}

// ============================================================================
// Tests
// ============================================================================

test "Metrics.inc increments counter" {
    var m = Metrics{ .start_time_secs = 0 };
    try std.testing.expectEqual(@as(u64, 0), m.changes_created_total.load(.monotonic));
    Metrics.inc(&m.changes_created_total);
    try std.testing.expectEqual(@as(u64, 1), m.changes_created_total.load(.monotonic));
    Metrics.inc(&m.changes_created_total);
    try std.testing.expectEqual(@as(u64, 2), m.changes_created_total.load(.monotonic));
}

test "Metrics.set updates gauge" {
    var m = Metrics{ .start_time_secs = 0 };
    Metrics.set(&m.last_received_lsn, 42);
    try std.testing.expectEqual(@as(u64, 42), m.last_received_lsn.load(.monotonic));
    Metrics.set(&m.last_received_lsn, 100);
    try std.testing.expectEqual(@as(u64, 100), m.last_received_lsn.load(.monotonic));
}

test "Metrics.render produces Prometheus text format" {
    var m = Metrics{ .start_time_secs = std.time.timestamp() };
    Metrics.inc(&m.changes_created_total);
    Metrics.inc(&m.changes_created_total);
    Metrics.inc(&m.changes_updated_total);
    Metrics.set(&m.last_received_lsn, 1000);
    Metrics.set(&m.last_flushed_lsn, 800);

    const output = try m.render(std.testing.allocator);
    defer std.testing.allocator.free(output);

    // Check for expected Prometheus format elements (zemi_ prefix)
    try std.testing.expect(std.mem.indexOf(u8, output, "# HELP zemi_changes_processed_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE zemi_changes_processed_total counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_changes_processed_total{operation=\"CREATE\"} 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_changes_processed_total{operation=\"UPDATE\"} 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_changes_processed_total{operation=\"DELETE\"} 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_replication_lag_bytes 200") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_last_received_lsn 1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_last_flushed_lsn 800") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE zemi_uptime_seconds gauge") != null);

    // Check for backward-compatible bemi_ prefix aliases
    try std.testing.expect(std.mem.indexOf(u8, output, "# HELP bemi_changes_processed_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bemi_changes_processed_total counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_changes_processed_total{operation=\"CREATE\"} 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_replication_lag_bytes 200") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_last_received_lsn 1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_last_flushed_lsn 800") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bemi_uptime_seconds gauge") != null);
}

test "isMetricsRequest detects /metrics path" {
    try std.testing.expect(isMetricsRequest("GET /metrics HTTP/1.1\r\n"));
    try std.testing.expect(isMetricsRequest("GET /metrics?foo=bar HTTP/1.1\r\n"));
    try std.testing.expect(!isMetricsRequest("GET / HTTP/1.1\r\n"));
    try std.testing.expect(!isMetricsRequest("GET /health HTTP/1.1\r\n"));
    try std.testing.expect(!isMetricsRequest("POST /metrics HTTP/1.1\r\n"));
}

test "MetricsServer http_ok_response is well-formed" {
    const resp = MetricsServer.http_ok_response;
    try std.testing.expect(std.mem.startsWith(u8, resp, "HTTP/1.1 200 OK"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "ok\n") != null);
}

test "Metrics.render includes transaction_early_flushes_total" {
    var m = Metrics{ .start_time_secs = std.time.timestamp() };
    Metrics.inc(&m.transaction_early_flushes_total);
    Metrics.inc(&m.transaction_early_flushes_total);
    Metrics.inc(&m.transaction_early_flushes_total);

    const output = try m.render(std.testing.allocator);
    defer std.testing.allocator.free(output);

    // Check zemi_ prefix
    try std.testing.expect(std.mem.indexOf(u8, output, "# HELP zemi_transaction_early_flushes_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE zemi_transaction_early_flushes_total counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_transaction_early_flushes_total 3") != null);

    // Check bemi_ backward-compat prefix
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_transaction_early_flushes_total 3") != null);
}

test "Metrics.render shows zero for transaction_early_flushes_total by default" {
    var m = Metrics{ .start_time_secs = std.time.timestamp() };

    const output = try m.render(std.testing.allocator);
    defer std.testing.allocator.free(output);

    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_transaction_early_flushes_total 0") != null);
}

test "Metrics.render includes replication error counters" {
    var m = Metrics{ .start_time_secs = std.time.timestamp() };
    Metrics.inc(&m.replication_transient_errors_total);
    Metrics.inc(&m.replication_transient_errors_total);
    Metrics.inc(&m.replication_permanent_errors_total);

    const output = try m.render(std.testing.allocator);
    defer std.testing.allocator.free(output);

    try std.testing.expect(std.mem.indexOf(u8, output, "# HELP zemi_replication_transient_errors_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_replication_transient_errors_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_replication_permanent_errors_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_replication_transient_errors_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_replication_permanent_errors_total 1") != null);
}

test "Histogram.observeNanos buckets and counts correctly" {
    var h = Histogram.init();

    // Observe 500 microseconds = 0.0005 seconds (should land in 0.001 bucket and above)
    h.observeNanos(500_000); // 500us

    try std.testing.expectEqual(@as(u64, 1), h.count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 500), h.sum_usec.load(.monotonic));
    // 0.0005s <= 0.001, so bucket[0] (le=0.001) should be 1
    try std.testing.expectEqual(@as(u64, 1), h.buckets[0].load(.monotonic));
    // All higher buckets should also be 1 (cumulative)
    try std.testing.expectEqual(@as(u64, 1), h.buckets[5].load(.monotonic)); // le=0.1
    try std.testing.expectEqual(@as(u64, 1), h.buckets[11].load(.monotonic)); // le=10.0

    // Observe 50 milliseconds = 0.05 seconds (bucket[4]=0.05 and above, but NOT bucket[3]=0.025)
    h.observeNanos(50_000_000); // 50ms

    try std.testing.expectEqual(@as(u64, 2), h.count.load(.monotonic));
    // bucket[3] (le=0.025): only the first observation fits
    try std.testing.expectEqual(@as(u64, 1), h.buckets[3].load(.monotonic));
    // bucket[4] (le=0.05): both observations fit
    try std.testing.expectEqual(@as(u64, 2), h.buckets[4].load(.monotonic));

    // Observe 15 seconds (exceeds all bucket boundaries)
    h.observeNanos(15_000_000_000); // 15s

    try std.testing.expectEqual(@as(u64, 3), h.count.load(.monotonic));
    // le=10.0 bucket should only have the first two observations
    try std.testing.expectEqual(@as(u64, 2), h.buckets[11].load(.monotonic));
}

test "Metrics.render includes histogram output" {
    var m = Metrics{ .start_time_secs = std.time.timestamp() };

    // Observe a 1ms decode (1_000_000 nanos)
    m.decode_duration_seconds.observeNanos(1_000_000);

    const output = try m.render(std.testing.allocator);
    defer std.testing.allocator.free(output);

    // Check histogram HELP/TYPE lines
    try std.testing.expect(std.mem.indexOf(u8, output, "# HELP zemi_decode_duration_seconds") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE zemi_decode_duration_seconds histogram") != null);

    // Check bucket lines — 1ms = 0.001s, so le="0.001" bucket should have 1
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_decode_duration_seconds_bucket{le=\"0.001\"} 1") != null);
    // Higher buckets should also have 1
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_decode_duration_seconds_bucket{le=\"10\"} 1") != null);
    // +Inf bucket
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_decode_duration_seconds_bucket{le=\"+Inf\"} 1") != null);
    // Count
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_decode_duration_seconds_count 1") != null);
    // Sum: 1ms = 1000 usec = 0.001000 seconds
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_decode_duration_seconds_sum 0.001000") != null);

    // Check persist histogram exists (empty)
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE zemi_persist_duration_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "zemi_persist_duration_seconds_count 0") != null);

    // Check bemi_ backward compat
    try std.testing.expect(std.mem.indexOf(u8, output, "# TYPE bemi_decode_duration_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "bemi_decode_duration_seconds_bucket{le=\"+Inf\"} 1") != null);
}

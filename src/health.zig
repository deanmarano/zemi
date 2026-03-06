const std = @import("std");
const posix = std.posix;
const net = std.net;
const Metrics = @import("metrics.zig").Metrics;

const log = std.log.scoped(.health);

/// Minimal TCP health check server for container orchestration.
/// When metrics are available, checks replication and storage connection
/// state and returns HTTP 503 if either is down.
/// Runs in a background thread. Heap-allocated so the thread's pointer
/// remains valid after start() returns.
pub const HealthServer = struct {
    thread: std.Thread,
    server_fd: posix.socket_t,
    should_stop: std.atomic.Value(bool),
    bound_port: u16,
    allocator: std.mem.Allocator,
    metrics: ?*const Metrics,

    const http_ok = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"ok\",\"replication\":true,\"storage\":true}\n";
    const http_degraded_repl = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"unhealthy\",\"replication\":false,\"storage\":true}\n";
    const http_degraded_stor = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"unhealthy\",\"replication\":true,\"storage\":false}\n";
    const http_degraded_both = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"unhealthy\",\"replication\":false,\"storage\":false}\n";
    const http_ok_simple = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";

    pub fn start(port: u16, allocator: std.mem.Allocator, metrics: ?*const Metrics) !*HealthServer {
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

        const hs = try allocator.create(HealthServer);
        hs.* = .{
            .thread = undefined,
            .server_fd = server_fd,
            .should_stop = std.atomic.Value(bool).init(false),
            .bound_port = bound_port,
            .allocator = allocator,
            .metrics = metrics,
        };

        hs.thread = try std.Thread.spawn(.{}, acceptLoop, .{hs});
        return hs;
    }

    pub fn stop(self: *HealthServer) void {
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

    fn getHealthResponse(self: *const HealthServer) []const u8 {
        if (self.metrics) |m| {
            const repl_ok = m.replication_connected.load(.monotonic) == 1;
            const stor_ok = m.storage_connected.load(.monotonic) == 1;
            if (repl_ok and stor_ok) return http_ok;
            if (!repl_ok and stor_ok) return http_degraded_repl;
            if (repl_ok and !stor_ok) return http_degraded_stor;
            return http_degraded_both;
        }
        // No metrics available — return simple OK (backward compatible)
        return http_ok_simple;
    }

    fn acceptLoop(self: *HealthServer) void {
        while (!self.should_stop.load(.acquire)) {
            // Use raw C accept to avoid Zig's unreachable assertions on NOTSOCK/BADF
            const rc = std.c.accept(self.server_fd, null, null);
            if (rc < 0) continue;
            const client_fd: posix.socket_t = rc;

            if (self.should_stop.load(.acquire)) {
                posix.close(client_fd);
                break;
            }

            const response = self.getHealthResponse();
            const stream = std.net.Stream{ .handle = client_fd };
            stream.writeAll(response) catch {};
            posix.close(client_fd);
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "http responses are well-formed" {
    // OK response
    try std.testing.expect(std.mem.startsWith(u8, HealthServer.http_ok, "HTTP/1.1 200 OK"));
    try std.testing.expect(std.mem.indexOf(u8, HealthServer.http_ok, "\"status\":\"ok\"") != null);

    // Degraded responses
    try std.testing.expect(std.mem.startsWith(u8, HealthServer.http_degraded_repl, "HTTP/1.1 503"));
    try std.testing.expect(std.mem.indexOf(u8, HealthServer.http_degraded_repl, "\"replication\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, HealthServer.http_degraded_repl, "\"storage\":true") != null);

    try std.testing.expect(std.mem.startsWith(u8, HealthServer.http_degraded_stor, "HTTP/1.1 503"));
    try std.testing.expect(std.mem.indexOf(u8, HealthServer.http_degraded_stor, "\"replication\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, HealthServer.http_degraded_stor, "\"storage\":false") != null);

    // Simple OK (no metrics)
    try std.testing.expect(std.mem.startsWith(u8, HealthServer.http_ok_simple, "HTTP/1.1 200 OK"));
    try std.testing.expect(std.mem.indexOf(u8, HealthServer.http_ok_simple, "ok\n") != null);
}

test "getHealthResponse reflects metrics state" {
    var m = Metrics{ .start_time_secs = 0 };

    // Both disconnected initially
    const hs_stack = HealthServer{
        .thread = undefined,
        .server_fd = 0,
        .should_stop = std.atomic.Value(bool).init(false),
        .bound_port = 0,
        .allocator = std.testing.allocator,
        .metrics = &m,
    };

    // Both disconnected (initial state = 0)
    try std.testing.expectEqualStrings(HealthServer.http_degraded_both, hs_stack.getHealthResponse());

    // Replication connected, storage still down
    Metrics.set(&m.replication_connected, 1);
    try std.testing.expectEqualStrings(HealthServer.http_degraded_stor, hs_stack.getHealthResponse());

    // Both connected
    Metrics.set(&m.storage_connected, 1);
    try std.testing.expectEqualStrings(HealthServer.http_ok, hs_stack.getHealthResponse());

    // Replication down, storage up
    Metrics.set(&m.replication_connected, 0);
    try std.testing.expectEqualStrings(HealthServer.http_degraded_repl, hs_stack.getHealthResponse());

    // No metrics = simple ok
    const hs_no_metrics = HealthServer{
        .thread = undefined,
        .server_fd = 0,
        .should_stop = std.atomic.Value(bool).init(false),
        .bound_port = 0,
        .allocator = std.testing.allocator,
        .metrics = null,
    };
    try std.testing.expectEqualStrings(HealthServer.http_ok_simple, hs_no_metrics.getHealthResponse());
}

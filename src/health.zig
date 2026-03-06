const std = @import("std");
const posix = std.posix;
const net = std.net;

const log = std.log.scoped(.health);

/// Minimal TCP health check server for container orchestration.
/// Accepts connections and responds with "HTTP/1.1 200 OK\r\n\r\nok\n".
/// Runs in a background thread. Heap-allocated so the thread's pointer
/// remains valid after start() returns.
pub const HealthServer = struct {
    thread: std.Thread,
    server_fd: posix.socket_t,
    should_stop: std.atomic.Value(bool),
    bound_port: u16,
    allocator: std.mem.Allocator,

    const http_response = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 3\r\nConnection: close\r\n\r\nok\n";

    pub fn start(port: u16, allocator: std.mem.Allocator) !*HealthServer {
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

            const stream = std.net.Stream{ .handle = client_fd };
            stream.writeAll(http_response) catch {};
            posix.close(client_fd);
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "http_response is well-formed" {
    const resp = HealthServer.http_response;
    try std.testing.expect(std.mem.startsWith(u8, resp, "HTTP/1.1 200 OK"));
    try std.testing.expect(std.mem.indexOf(u8, resp, "ok\n") != null);
}

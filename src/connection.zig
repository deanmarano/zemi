const std = @import("std");
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const tls = std.crypto.tls;
const Certificate = std.crypto.Certificate;
const protocol = @import("protocol.zig");
const scram = @import("scram.zig");
const config_mod = @import("config.zig");
const SslMode = config_mod.SslMode;

const log = std.log.scoped(.connection);

/// PostgreSQL connection wrapping a TCP socket with optional TLS.
/// Handles the wire protocol: startup, authentication, simple queries,
/// and reading raw backend messages.
pub const Connection = struct {
    stream: net.Stream,
    allocator: std.mem.Allocator,

    // TLS state (null when not using SSL)
    tls_client: ?tls.Client = null,
    ca_bundle: ?Certificate.Bundle = null,

    // Read buffer for receiving messages
    read_buf: [64 * 1024]u8 = undefined,
    read_pos: usize = 0,
    read_len: usize = 0,

    // Connection parameters
    host: []const u8,
    port: u16,
    user: []const u8,
    password: []const u8,
    database: []const u8,
    replication: ?[]const u8, // null for normal connection, "database" for replication

    // State
    backend_pid: i32 = 0,
    backend_key: []const u8 = "",
    server_params: std.StringHashMap([]const u8),
    is_ready: bool = false,
    scram_client: ?scram.ScramClient = null,

    pub const ConnectError = error{
        AuthenticationFailed,
        ServerError,
        UnsupportedAuthMethod,
        ConnectionRefused,
        InvalidServerResponse,
        ServerNonceMismatch,
        ServerSignatureMismatch,
        Base64DecodeFailed,
        SslNotSupported,
        SslCertificateError,
    } || net.Stream.ReadError || net.Stream.WriteError || std.mem.Allocator.Error || error{
        UnexpectedEndOfData,
        InvalidLsn,
        Overflow,
        InvalidCharacter,
        WeakParameters,
        OutputTooLong,
    };

    pub fn connect(
        allocator: std.mem.Allocator,
        host: []const u8,
        port: u16,
        user: []const u8,
        password: []const u8,
        database: []const u8,
        replication: ?[]const u8,
        ssl_mode: SslMode,
        ssl_root_cert: ?[]const u8,
    ) ConnectError!Connection {
        // Try numeric IP first (no allocation needed), then fall back to
        // DNS resolution for hostnames like "localhost".
        const stream = blk: {
            if (net.Address.resolveIp(host, port)) |address| {
                break :blk net.tcpConnectToAddress(address) catch |err| {
                    log.err("failed to connect to {s}:{d}: {}", .{ host, port, err });
                    return error.ConnectionRefused;
                };
            } else |_| {
                // resolveIp failed (e.g. hostname like "localhost") — use DNS resolution
                log.debug("resolving hostname {s} via DNS", .{host});
                break :blk net.tcpConnectToHost(allocator, host, port) catch |err| {
                    log.err("failed to connect to {s}:{d}: {}", .{ host, port, err });
                    return error.ConnectionRefused;
                };
            }
        };

        var conn = Connection{
            .stream = stream,
            .allocator = allocator,
            .host = host,
            .port = port,
            .user = user,
            .password = password,
            .database = database,
            .replication = replication,
            .server_params = std.StringHashMap([]const u8).init(allocator),
        };

        // Negotiate SSL if requested
        if (ssl_mode != .disable) {
            conn.negotiateSsl(ssl_mode, ssl_root_cert) catch |err| {
                log.err("SSL negotiation failed: {}", .{err});
                conn.server_params.deinit();
                conn.stream.close();
                return err;
            };
        }

        conn.performStartup() catch |err| {
            if (conn.ca_bundle) |*bundle| bundle.deinit(allocator);
            conn.server_params.deinit();
            conn.stream.close();
            return err;
        };

        return conn;
    }

    pub fn close(self: *Connection) void {
        // Send Terminate message
        const term_msg = protocol.buildTerminateMessage(self.allocator) catch {
            if (self.ca_bundle) |*bundle| bundle.deinit(self.allocator);
            self.server_params.deinit();
            self.stream.close();
            return;
        };
        defer self.allocator.free(term_msg);
        self.tlsWriteAll(term_msg) catch {};
        if (self.ca_bundle) |*bundle| bundle.deinit(self.allocator);
        self.server_params.deinit();
        self.stream.close();
    }

    // ========================================================================
    // SSL/TLS Negotiation
    // ========================================================================

    fn negotiateSsl(self: *Connection, ssl_mode: SslMode, ssl_root_cert: ?[]const u8) ConnectError!void {
        // Step 1: Send SSLRequest on raw TCP stream (before TLS handshake)
        const ssl_request = protocol.buildSslRequest();
        self.stream.writeAll(&ssl_request) catch |err| {
            log.err("failed to send SSLRequest: {}", .{err});
            return error.ConnectionRefused;
        };

        // Step 2: Read single-byte response
        var response: [1]u8 = undefined;
        const n = self.stream.read(&response) catch |err| {
            log.err("failed to read SSLRequest response: {}", .{err});
            return error.ConnectionRefused;
        };
        if (n == 0) {
            log.err("server closed connection during SSL negotiation", .{});
            return error.UnexpectedEndOfData;
        }

        if (response[0] == 'S') {
            // Server accepts SSL — perform TLS handshake
            log.info("server accepted SSL, starting TLS handshake", .{});
            try self.performTlsHandshake(ssl_mode, ssl_root_cert);
        } else if (response[0] == 'N') {
            // Server does not support SSL
            if (ssl_mode == .require or ssl_mode == .verify_ca or ssl_mode == .verify_full) {
                log.err("server does not support SSL but ssl_mode={s} requires it", .{ssl_mode.toString()});
                return error.SslNotSupported;
            }
            log.info("server does not support SSL, continuing with plain connection", .{});
        } else {
            log.err("unexpected SSL response byte: 0x{x}", .{response[0]});
            return error.InvalidServerResponse;
        }
    }

    fn performTlsHandshake(self: *Connection, ssl_mode: SslMode, ssl_root_cert: ?[]const u8) ConnectError!void {
        // Load CA certificates if needed for verify modes
        if (ssl_mode == .verify_ca or ssl_mode == .verify_full) {
            var bundle = Certificate.Bundle{};
            if (ssl_root_cert) |cert_path| {
                log.info("loading CA certificate from {s}", .{cert_path});
                bundle.addCertsFromFilePathAbsolute(self.allocator, cert_path) catch |err| {
                    log.err("failed to load CA certificate from {s}: {}", .{ cert_path, err });
                    return error.SslCertificateError;
                };
            } else {
                log.debug("loading system CA certificates", .{});
                bundle.rescan(self.allocator) catch |err| {
                    log.err("failed to load system CA certificates: {}", .{err});
                    return error.SslCertificateError;
                };
            }
            self.ca_bundle = bundle;
        }

        // Build TLS options based on SSL mode
        var options: tls.Client.Options = switch (ssl_mode) {
            .disable => unreachable,
            .require => .{
                .host = .no_verification,
                .ca = .no_verification,
            },
            .verify_ca => .{
                .host = .no_verification,
                .ca = .{ .bundle = self.ca_bundle.? },
            },
            .verify_full => .{
                .host = .{ .explicit = self.host },
                .ca = .{ .bundle = self.ca_bundle.? },
            },
        };
        _ = &options;

        self.tls_client = tls.Client.init(self.stream, options) catch |err| {
            log.err("TLS handshake failed: {}", .{err});
            // Clean up CA bundle on handshake failure to prevent memory leak
            if (self.ca_bundle) |*bundle| {
                bundle.deinit(self.allocator);
                self.ca_bundle = null;
            }
            return error.ConnectionRefused;
        };

        // Allow truncation attacks because PostgreSQL servers don't always
        // send TLS close_notify on disconnect.
        self.tls_client.?.allow_truncation_attacks = true;

        log.info("TLS handshake complete (version={s})", .{@tagName(self.tls_client.?.tls_version)});
    }

    // ========================================================================
    // TLS-aware I/O helpers
    // ========================================================================

    /// Read bytes through TLS if active, otherwise raw TCP.
    /// Returns number of bytes read, 0 means end of stream.
    /// TLS-specific errors are mapped to ConnectionRefused for simplicity.
    fn tlsRead(self: *Connection, buf: []u8) net.Stream.ReadError!usize {
        if (self.tls_client) |*tc| {
            return tc.read(self.stream, buf) catch |err| {
                // Map TLS-specific errors to stream-level errors that ConnectError understands
                const is_stream_err = @as(?net.Stream.ReadError, switch (err) {
                    error.ConnectionResetByPeer => error.ConnectionResetByPeer,
                    error.BrokenPipe => error.BrokenPipe,
                    error.ConnectionTimedOut => error.ConnectionTimedOut,
                    else => null,
                });
                if (is_stream_err) |stream_err| return stream_err;
                // TLS protocol errors (alerts, truncation, decode errors) — treat as EOF
                log.warn("TLS read error: {}, treating as connection close", .{err});
                return 0;
            };
        } else {
            return self.stream.read(buf);
        }
    }

    /// Write all bytes through TLS if active, otherwise raw TCP.
    fn tlsWriteAll(self: *Connection, data: []const u8) net.Stream.WriteError!void {
        if (self.tls_client) |*tc| {
            tc.writeAll(self.stream, data) catch |err| {
                // Map TLS-specific errors to stream-level errors
                const is_stream_err = @as(?net.Stream.WriteError, switch (err) {
                    error.ConnectionResetByPeer => error.ConnectionResetByPeer,
                    error.BrokenPipe => error.BrokenPipe,
                    error.AccessDenied => error.AccessDenied,
                    else => null,
                });
                if (is_stream_err) |stream_err| return stream_err;
                log.warn("TLS write error: {}, treating as broken pipe", .{err});
                return error.BrokenPipe;
            };
        } else {
            try self.stream.writeAll(data);
        }
    }

    // ========================================================================
    // Startup and Authentication
    // ========================================================================

    fn performStartup(self: *Connection) ConnectError!void {
        // Build startup parameters
        var params_list = std.ArrayList([2][]const u8).init(self.allocator);
        defer params_list.deinit();

        try params_list.append(.{ "user", self.user });
        try params_list.append(.{ "database", self.database });
        // Request protocol version 3.0 parameter encoding
        try params_list.append(.{ "client_encoding", "UTF8" });

        if (self.replication) |repl| {
            try params_list.append(.{ "replication", repl });
        }

        const startup_msg = try protocol.buildStartupMessage(self.allocator, params_list.items);
        defer self.allocator.free(startup_msg);

        try self.tlsWriteAll(startup_msg);

        // Read responses until ReadyForQuery
        try self.handleStartupResponses();
    }

    fn handleStartupResponses(self: *Connection) ConnectError!void {
        while (true) {
            const raw = try self.readMessage();

            switch (raw.msg_type) {
                .authentication => {
                    try self.handleAuthentication(raw.payload);
                },
                .parameter_status => {
                    // ParameterStatus: name\0 value\0
                    var reader = protocol.MessageReader.init(raw.payload);
                    const name = reader.readString() catch continue;
                    const value = reader.readString() catch continue;
                    self.server_params.put(name, value) catch {};
                },
                .backend_key_data => {
                    var reader = protocol.MessageReader.init(raw.payload);
                    self.backend_pid = reader.readInt32() catch 0;
                    self.backend_key = reader.readRemaining();
                },
                .ready_for_query => {
                    self.is_ready = true;
                    log.info("connected to {s}:{d} database={s} pid={d}", .{
                        self.host,
                        self.port,
                        self.database,
                        self.backend_pid,
                    });
                    return;
                },
                .error_response => {
                    const err = protocol.parseErrorFields(raw.payload);
                    log.err("server error during startup: {}", .{err});
                    return error.ServerError;
                },
                .notice_response => {
                    const notice = protocol.parseErrorFields(raw.payload);
                    log.warn("server notice: {}", .{notice});
                },
                else => {
                    log.debug("ignoring startup message type: {d}", .{@intFromEnum(raw.msg_type)});
                },
            }
        }
    }

    fn handleAuthentication(self: *Connection, payload: []const u8) ConnectError!void {
        var reader = protocol.MessageReader.init(payload);
        const auth_type_raw = reader.readUInt32() catch return error.UnexpectedEndOfData;
        const auth_type: protocol.AuthType = @enumFromInt(auth_type_raw);

        switch (auth_type) {
            .ok => {
                log.debug("authentication successful", .{});
                // Clean up SCRAM state if present
                if (self.scram_client) |*sc| {
                    sc.deinit();
                    self.scram_client = null;
                }
            },
            .cleartext_password => {
                log.debug("server requests cleartext password", .{});
                const msg = try protocol.buildPasswordMessage(self.allocator, self.password);
                defer self.allocator.free(msg);
                try self.tlsWriteAll(msg);
            },
            .md5_password => {
                log.debug("server requests MD5 password", .{});
                const salt = reader.readBytes(4) catch return error.UnexpectedEndOfData;
                const hashed = protocol.computeMd5Password(self.user, self.password, salt[0..4].*);
                const msg = try protocol.buildPasswordMessage(self.allocator, &hashed);
                defer self.allocator.free(msg);
                try self.tlsWriteAll(msg);
            },
            .sasl => {
                // Read available SASL mechanisms and check for SCRAM-SHA-256
                var found_scram256 = false;
                while (reader.remaining() > 0) {
                    const mech = reader.readString() catch break;
                    if (mech.len == 0) break;
                    log.debug("server offers SASL mechanism: {s}", .{mech});
                    if (mem.eql(u8, mech, "SCRAM-SHA-256")) {
                        found_scram256 = true;
                    }
                }

                if (!found_scram256) {
                    log.err("server does not offer SCRAM-SHA-256", .{});
                    return error.UnsupportedAuthMethod;
                }

                // Initialize SCRAM client and send client-first-message
                log.debug("starting SCRAM-SHA-256 authentication", .{});
                var sc = scram.ScramClient.init(self.allocator, self.password);

                const client_first = try sc.buildClientFirst();
                defer self.allocator.free(client_first);

                const msg = try scram.buildSaslInitialResponse(self.allocator, "SCRAM-SHA-256", client_first);
                defer self.allocator.free(msg);
                try self.tlsWriteAll(msg);

                self.scram_client = sc;
            },
            .sasl_continue => {
                // Server sent server-first-message
                const server_first = reader.readRemaining();
                log.debug("received SCRAM server-first-message ({d} bytes)", .{server_first.len});

                if (self.scram_client) |*sc| {
                    const client_final = try sc.buildClientFinal(server_first);
                    defer self.allocator.free(client_final);

                    const msg = try scram.buildSaslResponse(self.allocator, client_final);
                    defer self.allocator.free(msg);
                    try self.tlsWriteAll(msg);
                } else {
                    log.err("received SASL continue without prior SASL init", .{});
                    return error.AuthenticationFailed;
                }
            },
            .sasl_final => {
                // Server sent server-final-message with signature
                const server_final = reader.readRemaining();
                log.debug("received SCRAM server-final-message ({d} bytes)", .{server_final.len});

                if (self.scram_client) |*sc| {
                    sc.verifyServerFinal(server_final) catch |err| {
                        log.err("SCRAM server verification failed: {}", .{err});
                        return error.AuthenticationFailed;
                    };
                    log.info("SCRAM-SHA-256 authentication verified", .{});
                } else {
                    log.err("received SASL final without prior SASL state", .{});
                    return error.AuthenticationFailed;
                }
            },
            else => {
                log.err("unsupported authentication method: {d}", .{auth_type_raw});
                return error.UnsupportedAuthMethod;
            },
        }
    }

    // ========================================================================
    // Simple Query Protocol
    // ========================================================================

    /// Result from a simple query: rows and column descriptions.
    pub const QueryResult = struct {
        columns: []protocol.FieldDescription,
        rows: []protocol.DataRow,
        command_tag: []const u8,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *QueryResult) void {
            for (self.rows) |row| {
                self.allocator.free(row.columns);
            }
            self.allocator.free(self.rows);
            self.allocator.free(self.columns);
            self.command_tag = "";
        }
    };

    /// Execute a simple query and return results.
    pub fn query(self: *Connection, sql: []const u8) ConnectError!QueryResult {
        const msg = try protocol.buildQueryMessage(self.allocator, sql);
        defer self.allocator.free(msg);
        try self.tlsWriteAll(msg);

        var columns: []protocol.FieldDescription = &.{};
        var rows = std.ArrayList(protocol.DataRow).init(self.allocator);
        var command_tag: []const u8 = "";

        while (true) {
            const raw = try self.readMessage();

            switch (raw.msg_type) {
                .row_description => {
                    columns = try protocol.parseRowDescription(self.allocator, raw.payload);
                },
                .data_row => {
                    const row = try protocol.parseDataRow(self.allocator, raw.payload);
                    try rows.append(row);
                },
                .command_complete => {
                    var reader = protocol.MessageReader.init(raw.payload);
                    command_tag = reader.readString() catch "";
                },
                .empty_query_response => {},
                .ready_for_query => {
                    return .{
                        .columns = columns,
                        .rows = try rows.toOwnedSlice(),
                        .command_tag = command_tag,
                        .allocator = self.allocator,
                    };
                },
                .error_response => {
                    const err = protocol.parseErrorFields(raw.payload);
                    log.err("query error: {}", .{err});
                    // Continue reading until ReadyForQuery
                    while (true) {
                        const drain = try self.readMessage();
                        if (drain.msg_type == .ready_for_query) break;
                    }
                    return error.ServerError;
                },
                .notice_response => {
                    const notice = protocol.parseErrorFields(raw.payload);
                    log.warn("notice: {}", .{notice});
                },
                .copy_both_response => {
                    // We've entered replication streaming mode
                    return .{
                        .columns = columns,
                        .rows = try rows.toOwnedSlice(),
                        .command_tag = command_tag,
                        .allocator = self.allocator,
                    };
                },
                else => {
                    log.debug("ignoring message type during query: {d}", .{@intFromEnum(raw.msg_type)});
                },
            }
        }
    }

    /// Execute a simple query without caring about results (DDL, etc).
    pub fn exec(self: *Connection, sql: []const u8) ConnectError!void {
        var result = try self.query(sql);
        result.deinit();
    }

    // ========================================================================
    // Raw message reading
    // ========================================================================

    /// Read a single raw backend message from the connection.
    pub fn readMessage(self: *Connection) ConnectError!protocol.RawMessage {
        // Ensure we have at least 5 bytes (type + length)
        try self.ensureBuffered(5);

        const msg_type: protocol.BackendMessage = @enumFromInt(self.read_buf[self.read_pos]);
        const msg_len = mem.readInt(u32, self.read_buf[self.read_pos + 1 ..][0..4], .big);

        // msg_len includes itself (4 bytes) but not the type byte
        const total_len: usize = 1 + @as(usize, msg_len); // type + length + payload

        // Ensure full message is buffered
        try self.ensureBuffered(total_len);

        const payload = self.read_buf[self.read_pos + 5 .. self.read_pos + total_len];
        self.read_pos += total_len;

        return .{
            .msg_type = msg_type,
            .payload = payload,
        };
    }

    /// Read a raw CopyData message from the replication stream.
    /// Returns the inner payload (after the CopyData wrapper).
    pub fn readCopyData(self: *Connection) ConnectError!?[]const u8 {
        const raw = try self.readMessage();

        switch (raw.msg_type) {
            .copy_data => return raw.payload,
            .error_response => {
                const err = protocol.parseErrorFields(raw.payload);
                log.err("replication error: {}", .{err});
                return error.ServerError;
            },
            .notice_response => {
                const notice = protocol.parseErrorFields(raw.payload);
                log.warn("replication notice: {}", .{notice});
                return null; // caller should retry
            },
            else => {
                log.debug("unexpected message in replication stream: {d}", .{@intFromEnum(raw.msg_type)});
                return null;
            },
        }
    }

    /// Send raw bytes to the server.
    pub fn sendRaw(self: *Connection, data: []const u8) !void {
        try self.tlsWriteAll(data);
    }

    // ========================================================================
    // Buffer management
    // ========================================================================

    fn ensureBuffered(self: *Connection, needed: usize) !void {
        while (self.read_len - self.read_pos < needed) {
            // Compact buffer if needed
            if (self.read_pos > 0) {
                const remaining = self.read_len - self.read_pos;
                if (remaining > 0) {
                    std.mem.copyForwards(u8, self.read_buf[0..remaining], self.read_buf[self.read_pos..self.read_len]);
                }
                self.read_len = remaining;
                self.read_pos = 0;
            }

            // Read more data (through TLS if active)
            const n = try self.tlsRead(self.read_buf[self.read_len..]);
            if (n == 0) return error.UnexpectedEndOfData;
            self.read_len += n;
        }
    }

    /// Check if the underlying socket has data available for reading
    /// without blocking, using poll.
    pub fn hasDataAvailable(self: *Connection) bool {
        var poll_fds = [_]posix.pollfd{
            .{
                .fd = self.stream.handle,
                .events = posix.POLL.IN,
                .revents = 0,
            },
        };
        // Poll with 0 timeout (non-blocking check)
        const ready = posix.poll(&poll_fds, 0) catch return false;
        return ready > 0 or (self.read_len - self.read_pos > 0);
    }
};

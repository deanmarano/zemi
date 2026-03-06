const std = @import("std");
const protocol = @import("protocol.zig");
const Connection = @import("connection.zig").Connection;
const Config = @import("config.zig").Config;
const Metrics = @import("metrics.zig").Metrics;

const log = std.log.scoped(.replication);

/// Manages PostgreSQL logical replication: slot creation, WAL streaming,
/// and keepalive handling.
pub const ReplicationStream = struct {
    conn: Connection,
    allocator: std.mem.Allocator,
    config: Config,
    metrics: ?*Metrics = null,

    // Replication state
    last_received_lsn: u64 = 0,
    last_flushed_lsn: u64 = 0,

    // Thread safety for concurrent writes (keepalive thread + main thread)
    write_mutex: std.Thread.Mutex = .{},

    // Background keepalive thread
    keepalive_thread: ?std.Thread = null,
    keepalive_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    /// Callback type for processing XLogData messages.
    pub const XLogCallback = *const fn (data: protocol.XLogData, ctx: ?*anyopaque) void;

    pub fn init(allocator: std.mem.Allocator, config: Config, metrics: ?*Metrics) !ReplicationStream {
        // Open a replication connection
        const conn = Connection.connect(
            allocator,
            config.db_host,
            config.db_port,
            config.db_user,
            config.db_password,
            config.db_name,
            "database", // replication=database
            config.db_ssl_mode,
            config.db_ssl_root_cert,
            config.connect_timeout_secs,
            0, // no query timeout for replication connections
        ) catch |err| {
            log.err("failed to open replication connection: {}", .{err});
            return err;
        };

        return .{
            .conn = conn,
            .allocator = allocator,
            .config = config,
            .metrics = metrics,
        };
    }

    pub fn deinit(self: *ReplicationStream) void {
        self.stopKeepaliveThread();
        self.conn.close();
    }

    /// Start a background thread that sends StandbyStatusUpdate every
    /// `interval_secs` seconds. This prevents PostgreSQL from killing the
    /// replication connection via wal_sender_timeout when persistChanges()
    /// blocks the main loop for an extended period.
    pub fn startKeepaliveThread(self: *ReplicationStream, interval_secs: u64) void {
        if (self.keepalive_thread != null) return; // already running
        self.keepalive_stop.store(false, .release);

        self.keepalive_thread = std.Thread.spawn(.{}, keepaliveLoop, .{ self, interval_secs }) catch |err| {
            log.warn("failed to start keepalive thread: {}", .{err});
            return;
        };
        log.info("keepalive thread started (interval={d}s)", .{interval_secs});
    }

    /// Stop the background keepalive thread.
    pub fn stopKeepaliveThread(self: *ReplicationStream) void {
        if (self.keepalive_thread) |t| {
            self.keepalive_stop.store(true, .release);
            t.join();
            self.keepalive_thread = null;
            log.info("keepalive thread stopped", .{});
        }
    }

    fn keepaliveLoop(self: *ReplicationStream, interval_secs: u64) void {
        const interval_ns = interval_secs * std.time.ns_per_s;
        while (!self.keepalive_stop.load(.acquire)) {
            std.time.sleep(interval_ns);
            if (self.keepalive_stop.load(.acquire)) break;

            self.write_mutex.lock();
            defer self.write_mutex.unlock();

            self.sendStatusUpdateUnlocked() catch |err| {
                log.warn("keepalive thread: failed to send status update: {}", .{err});
                // Don't break — transient errors are OK, the main thread will
                // detect a dead connection on its next poll() call.
            };
        }
    }

    /// Run IDENTIFY_SYSTEM to get replication identity info.
    pub fn identifySystem(self: *ReplicationStream) !struct {
        systemid: []const u8,
        timeline: []const u8,
        xlogpos: []const u8,
        dbname: []const u8,
    } {
        var result = try self.conn.query("IDENTIFY_SYSTEM");
        defer result.deinit();

        if (result.rows.len == 0) {
            log.err("IDENTIFY_SYSTEM returned no rows", .{});
            return error.ServerError;
        }

        const row = result.rows[0];
        return .{
            .systemid = switch (row.columns[0]) {
                .text => |t| t,
                .null_value => "",
            },
            .timeline = switch (row.columns[1]) {
                .text => |t| t,
                .null_value => "",
            },
            .xlogpos = switch (row.columns[2]) {
                .text => |t| t,
                .null_value => "",
            },
            .dbname = switch (row.columns[3]) {
                .text => |t| t,
                .null_value => "",
            },
        };
    }

    /// Create a replication slot if it doesn't already exist.
    /// Uses pgoutput logical decoding plugin.
    pub fn createSlotIfNotExists(self: *ReplicationStream) !void {
        // Try to create the slot; ignore the "already exists" error
        var buf: [256]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, "CREATE_REPLICATION_SLOT {s} LOGICAL pgoutput NOEXPORT_SNAPSHOT", .{
            self.config.slot_name,
        }) catch return error.ServerError;

        self.conn.exec(sql) catch |err| {
            switch (err) {
                error.ServerError => {
                    // Slot may already exist, that's OK
                    log.info("replication slot '{s}' may already exist, continuing", .{self.config.slot_name});
                },
                else => return err,
            }
        };

        log.info("replication slot '{s}' ready", .{self.config.slot_name});
    }

    /// Start streaming WAL changes from the given LSN position.
    /// If start_lsn is 0, streams from the slot's confirmed_flush_lsn.
    /// Uses pgoutput proto_version 2 with streaming enabled for large
    /// transaction support (server sends StreamStart/StreamStop/StreamCommit/
    /// StreamAbort messages instead of buffering entire transactions).
    pub fn startReplication(self: *ReplicationStream, start_lsn: u64) !void {
        var lsn_buf: [32]u8 = undefined;
        const lsn_str = if (start_lsn == 0) "0/0" else protocol.formatLsn(&lsn_buf, start_lsn);

        var sql_buf: [512]u8 = undefined;
        const sql = std.fmt.bufPrint(&sql_buf, "START_REPLICATION SLOT {s} LOGICAL {s} (proto_version '2', publication_names '{s}', messages 'true', streaming 'true')", .{
            self.config.slot_name,
            lsn_str,
            self.config.publication_name,
        }) catch return error.ServerError;

        log.info("starting replication from LSN {s}", .{lsn_str});

        // This sends the query; the server will respond with CopyBothResponse
        // and then start sending CopyData messages
        _ = try self.conn.query(sql);
    }

    /// Read and process one message from the replication stream.
    /// Returns the XLogData if one was received, null otherwise (keepalive, etc).
    pub fn poll(self: *ReplicationStream) !?protocol.XLogData {
        const payload = try self.conn.readCopyData() orelse return null;

        const repl_msg = try protocol.parseReplicationMessage(payload);

        switch (repl_msg) {
            .xlog_data => |xlog| {
                if (xlog.wal_end > self.last_received_lsn) {
                    self.last_received_lsn = xlog.wal_end;
                }
                if (self.metrics) |m| {
                    Metrics.inc(&m.wal_messages_received_total);
                    Metrics.set(&m.last_received_lsn, self.last_received_lsn);
                }
                return xlog;
            },
            .keepalive => |ka| {
                if (ka.wal_end > self.last_received_lsn) {
                    self.last_received_lsn = ka.wal_end;
                }
                if (self.metrics) |m| {
                    Metrics.inc(&m.keepalives_received_total);
                    Metrics.set(&m.last_received_lsn, self.last_received_lsn);
                }
                if (ka.reply_requested) {
                    self.write_mutex.lock();
                    defer self.write_mutex.unlock();
                    try self.sendStatusUpdateUnlocked();
                }
                return null;
            },
            .unknown => |t| {
                log.warn("unknown replication message type: {d}", .{t});
                return null;
            },
        }
    }

    /// Send a StandbyStatusUpdate to the server.
    /// Thread-safe: acquires write_mutex.
    pub fn sendStatusUpdate(self: *ReplicationStream) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try self.sendStatusUpdateUnlocked();
    }

    /// Internal: send status update without acquiring the mutex.
    /// Caller must hold write_mutex.
    fn sendStatusUpdateUnlocked(self: *ReplicationStream) !void {
        const now = protocol.pgEpochMicroseconds();
        const msg = try protocol.buildStandbyStatusUpdate(
            self.allocator,
            self.last_received_lsn,
            self.last_flushed_lsn,
            0, // apply position (not used)
            now,
            false,
        );
        defer self.allocator.free(msg);
        try self.conn.sendRaw(msg);
    }

    /// Update the flushed LSN position (call after persisting changes).
    pub fn confirmLsn(self: *ReplicationStream, lsn: u64) void {
        if (lsn > self.last_flushed_lsn) {
            self.last_flushed_lsn = lsn;
            if (self.metrics) |m| {
                Metrics.set(&m.last_flushed_lsn, lsn);
            }
        }
    }
};

/// Connect to PostgreSQL as a normal (non-replication) client.
/// Used for setup queries like creating publications.
/// When config.tables is set, creates a publication for only those tables
/// (server-side filtering). Otherwise, creates a publication for all tables.
pub fn ensurePublication(allocator: std.mem.Allocator, config: Config) !void {
    var conn = try Connection.connect(
        allocator,
        config.db_host,
        config.db_port,
        config.db_user,
        config.db_password,
        config.db_name,
        null, // normal connection
        config.db_ssl_mode,
        config.db_ssl_root_cert,
        config.connect_timeout_secs,
        config.query_timeout_secs,
    );
    defer conn.close();

    if (config.tables) |tables_str| {
        // Build the table list for the publication (e.g. "users, orders, products")
        const table_list = try buildPublicationTableList(allocator, tables_str);
        defer allocator.free(table_list);

        // Try CREATE PUBLICATION ... FOR TABLE ...
        const create_sql = try std.fmt.allocPrint(allocator, "CREATE PUBLICATION {s} FOR TABLE {s}", .{
            config.publication_name, table_list,
        });
        defer allocator.free(create_sql);

        conn.exec(create_sql) catch {
            // Publication may already exist — update its table list
            log.info("publication '{s}' may already exist, updating table list", .{config.publication_name});
            const alter_sql = std.fmt.allocPrint(allocator, "ALTER PUBLICATION {s} SET TABLE {s}", .{
                config.publication_name, table_list,
            }) catch return error.ServerError;
            defer allocator.free(alter_sql);

            conn.exec(alter_sql) catch |alter_err| {
                log.err("failed to alter publication '{s}': {}", .{ config.publication_name, alter_err });
                return alter_err;
            };
        };

        log.info("publication '{s}' ready (tables: {s})", .{ config.publication_name, table_list });
    } else {
        // No table filter — publish all tables
        var buf: [256]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, "CREATE PUBLICATION {s} FOR ALL TABLES", .{
            config.publication_name,
        }) catch return error.ServerError;

        conn.exec(sql) catch {
            log.info("publication '{s}' may already exist, continuing", .{config.publication_name});
        };

        log.info("publication '{s}' ready (all tables)", .{config.publication_name});
    }
}

/// Build a comma-separated table list suitable for CREATE/ALTER PUBLICATION.
/// Input: "users, orders , products"
/// Output: "users, orders, products" (trimmed, validated non-empty)
fn buildPublicationTableList(allocator: std.mem.Allocator, tables_str: []const u8) ![]u8 {
    var result = std.ArrayList(u8).init(allocator);
    errdefer result.deinit();

    var first = true;
    var iter = std.mem.splitScalar(u8, tables_str, ',');
    while (iter.next()) |entry| {
        const trimmed = std.mem.trim(u8, entry, " \t");
        if (trimmed.len == 0) continue;

        if (!first) {
            try result.appendSlice(", ");
        }
        try result.appendSlice(trimmed);
        first = false;
    }

    return result.toOwnedSlice();
}

/// Drop the publication. Uses a normal (non-replication) connection.
/// Best-effort: logs a warning and returns on failure.
pub fn dropPublication(allocator: std.mem.Allocator, config: Config) void {
    var conn = Connection.connect(
        allocator,
        config.db_host,
        config.db_port,
        config.db_user,
        config.db_password,
        config.db_name,
        null, // normal connection
        config.db_ssl_mode,
        config.db_ssl_root_cert,
        config.connect_timeout_secs,
        config.query_timeout_secs,
    ) catch |err| {
        log.warn("failed to connect for publication cleanup: {}", .{err});
        return;
    };
    defer conn.close();

    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "DROP PUBLICATION IF EXISTS {s}", .{
        config.publication_name,
    }) catch {
        log.warn("failed to format DROP PUBLICATION query", .{});
        return;
    };

    conn.exec(sql) catch |err| {
        log.warn("failed to drop publication '{s}': {}", .{ config.publication_name, err });
        return;
    };

    log.info("dropped publication '{s}'", .{config.publication_name});
}

/// Drop the replication slot. Uses a normal (non-replication) connection.
/// Best-effort: logs a warning and returns on failure.
pub fn dropSlot(allocator: std.mem.Allocator, config: Config) void {
    var conn = Connection.connect(
        allocator,
        config.db_host,
        config.db_port,
        config.db_user,
        config.db_password,
        config.db_name,
        null, // normal connection
        config.db_ssl_mode,
        config.db_ssl_root_cert,
        config.connect_timeout_secs,
        config.query_timeout_secs,
    ) catch |err| {
        log.warn("failed to connect for slot cleanup: {}", .{err});
        return;
    };
    defer conn.close();

    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "SELECT pg_drop_replication_slot('{s}')", .{
        config.slot_name,
    }) catch {
        log.warn("failed to format slot drop query", .{});
        return;
    };

    conn.exec(sql) catch |err| {
        log.warn("failed to drop replication slot '{s}': {}", .{ config.slot_name, err });
        return;
    };

    log.info("dropped replication slot '{s}'", .{config.slot_name});
}

// ============================================================================
// Tests
// ============================================================================

test "buildPublicationTableList parses comma-separated tables" {
    const allocator = std.testing.allocator;

    {
        const result = try buildPublicationTableList(allocator, "users,orders,products");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("users, orders, products", result);
    }
}

test "buildPublicationTableList trims whitespace" {
    const allocator = std.testing.allocator;

    {
        const result = try buildPublicationTableList(allocator, " users , orders , products ");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("users, orders, products", result);
    }
}

test "buildPublicationTableList handles single table" {
    const allocator = std.testing.allocator;

    {
        const result = try buildPublicationTableList(allocator, "users");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("users", result);
    }
}

test "buildPublicationTableList skips empty entries" {
    const allocator = std.testing.allocator;

    {
        const result = try buildPublicationTableList(allocator, "users,,orders,");
        defer allocator.free(result);
        try std.testing.expectEqualStrings("users, orders", result);
    }
}

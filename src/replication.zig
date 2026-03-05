const std = @import("std");
const protocol = @import("protocol.zig");
const Connection = @import("connection.zig").Connection;
const Config = @import("config.zig").Config;

const log = std.log.scoped(.replication);

/// Manages PostgreSQL logical replication: slot creation, WAL streaming,
/// and keepalive handling.
pub const ReplicationStream = struct {
    conn: Connection,
    allocator: std.mem.Allocator,
    config: Config,

    // Replication state
    last_received_lsn: u64 = 0,
    last_flushed_lsn: u64 = 0,

    /// Callback type for processing XLogData messages.
    pub const XLogCallback = *const fn (data: protocol.XLogData, ctx: ?*anyopaque) void;

    pub fn init(allocator: std.mem.Allocator, config: Config) !ReplicationStream {
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
        ) catch |err| {
            log.err("failed to open replication connection: {}", .{err});
            return err;
        };

        return .{
            .conn = conn,
            .allocator = allocator,
            .config = config,
        };
    }

    pub fn deinit(self: *ReplicationStream) void {
        self.conn.close();
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

    /// Drop the replication slot.
    pub fn dropSlot(self: *ReplicationStream) !void {
        var buf: [128]u8 = undefined;
        const sql = std.fmt.bufPrint(&buf, "DROP_REPLICATION_SLOT {s} WAIT", .{
            self.config.slot_name,
        }) catch return error.ServerError;

        self.conn.exec(sql) catch |err| {
            log.warn("failed to drop replication slot: {}", .{err});
            return err;
        };
        log.info("dropped replication slot '{s}'", .{self.config.slot_name});
    }

    /// Start streaming WAL changes from the given LSN position.
    /// If start_lsn is 0, streams from the slot's confirmed_flush_lsn.
    pub fn startReplication(self: *ReplicationStream, start_lsn: u64) !void {
        var lsn_buf: [32]u8 = undefined;
        const lsn_str = if (start_lsn == 0) "0/0" else protocol.formatLsn(&lsn_buf, start_lsn);

        var sql_buf: [512]u8 = undefined;
        const sql = std.fmt.bufPrint(&sql_buf, "START_REPLICATION SLOT {s} LOGICAL {s} (proto_version '1', publication_names '{s}', messages 'true')", .{
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
                return xlog;
            },
            .keepalive => |ka| {
                if (ka.wal_end > self.last_received_lsn) {
                    self.last_received_lsn = ka.wal_end;
                }
                if (ka.reply_requested) {
                    try self.sendStatusUpdate();
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
    pub fn sendStatusUpdate(self: *ReplicationStream) !void {
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
        }
    }
};

/// Connect to PostgreSQL as a normal (non-replication) client.
/// Used for setup queries like creating publications.
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
    );
    defer conn.close();

    // Create publication if it doesn't exist
    var buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrint(&buf, "CREATE PUBLICATION {s} FOR ALL TABLES", .{
        config.publication_name,
    }) catch return error.ServerError;

    conn.exec(sql) catch {
        log.info("publication '{s}' may already exist, continuing", .{config.publication_name});
    };

    log.info("publication '{s}' ready", .{config.publication_name});
}

const std = @import("std");
const Connection = @import("connection.zig").Connection;
const config_mod = @import("config.zig");
const Config = config_mod.Config;
const decoder = @import("decoder.zig");
const protocol = @import("protocol.zig");
const Metrics = @import("metrics.zig").Metrics;

const log = std.log.scoped(.storage);

/// Manages a write connection to the destination PostgreSQL database
/// for persisting decoded Change records into the `changes` table.
///
/// The connection is resilient: if a transient error occurs during
/// `persistChanges()`, the dead connection is closed and a fresh one
/// is established before retrying the write. This avoids crashing the
/// replication loop on temporary network issues or PostgreSQL restarts.
pub const Storage = struct {
    conn: Connection,
    allocator: std.mem.Allocator,
    config: Config,
    metrics: ?*Metrics = null,

    pub const StorageError = Connection.ConnectError;

    /// Open a connection to the destination database and run migrations.
    pub fn init(allocator: std.mem.Allocator, config: Config, metrics: ?*Metrics) StorageError!Storage {
        var storage = Storage{
            .conn = try Connection.connect(
                allocator,
                config.getDestHost(),
                config.getDestPort(),
                config.getDestUser(),
                config.getDestPassword(),
                config.getDestName(),
                null, // normal connection, not replication
                config.getDestSslMode(),
                config.getDestSslRootCert(),
            ),
            .allocator = allocator,
            .config = config,
            .metrics = metrics,
        };

        storage.runMigrations() catch |err| {
            log.err("failed to run migrations: {}", .{err});
            storage.conn.close();
            return err;
        };

        return storage;
    }

    pub fn deinit(self: *Storage) void {
        self.conn.close();
    }

    /// Close the current connection and open a fresh one.
    /// Re-runs migrations (idempotent) to ensure the schema is ready.
    /// Used by persistChanges() after a transient connection failure.
    fn reconnect(self: *Storage) StorageError!void {
        log.info("reconnecting to destination database...", .{});
        if (self.metrics) |m| Metrics.set(&m.storage_connected, 0);
        self.conn.close();

        self.conn = try Connection.connect(
            self.allocator,
            self.config.getDestHost(),
            self.config.getDestPort(),
            self.config.getDestUser(),
            self.config.getDestPassword(),
            self.config.getDestName(),
            null,
            self.config.getDestSslMode(),
            self.config.getDestSslRootCert(),
        );

        self.runMigrations() catch |err| {
            log.err("failed to run migrations after reconnect: {}", .{err});
            self.conn.close();
            return err;
        };

        if (self.metrics) |m| {
            Metrics.inc(&m.storage_reconnections_total);
            Metrics.set(&m.storage_connected, 1);
        }
        log.info("reconnected to destination database", .{});
    }

    // ========================================================================
    // Schema Migrations
    // ========================================================================

    fn runMigrations(self: *Storage) StorageError!void {
        log.info("running schema migrations...", .{});

        // Create the changes table if it doesn't exist.
        // Schema matches the original Bemi TypeScript worker.
        try self.conn.exec(
            \\CREATE TABLE IF NOT EXISTS changes (
            \\    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            \\    primary_key VARCHAR(255),
            \\    before JSONB NOT NULL DEFAULT '{}',
            \\    after JSONB NOT NULL DEFAULT '{}',
            \\    context JSONB NOT NULL DEFAULT '{}',
            \\    database VARCHAR(255) NOT NULL,
            \\    schema VARCHAR(255) NOT NULL,
            \\    "table" VARCHAR(255) NOT NULL,
            \\    operation TEXT NOT NULL,
            \\    committed_at TIMESTAMPTZ NOT NULL,
            \\    queued_at TIMESTAMPTZ NOT NULL,
            \\    transaction_id BIGINT NOT NULL,
            \\    position BIGINT NOT NULL,
            \\    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            \\);
        );

        // Idempotent unique constraint for deduplication on replay.
        try self.conn.exec(
            \\DO $$ BEGIN
            \\    IF NOT EXISTS (
            \\        SELECT 1 FROM pg_constraint
            \\        WHERE conname = 'changes_position_table_schema_database_operation_unique'
            \\    ) THEN
            \\        ALTER TABLE changes
            \\            ADD CONSTRAINT changes_position_table_schema_database_operation_unique
            \\            UNIQUE (position, "table", schema, database, operation);
            \\    END IF;
            \\END $$;
        );

        // Create indexes idempotently.
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_committed_at_index ON changes (committed_at);");
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_table_index ON changes (\"table\");");
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_primary_key_index ON changes (primary_key);");
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_operation_index ON changes (operation);");
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_context_index ON changes USING GIN (context jsonb_path_ops);");
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_before_index ON changes USING GIN (before jsonb_path_ops);");
        try self.conn.exec("CREATE INDEX IF NOT EXISTS changes_after_index ON changes USING GIN (after jsonb_path_ops);");

        log.info("schema migrations complete", .{});
    }

    // ========================================================================
    // Change Persistence
    // ========================================================================

    /// Persist a batch of changes (typically one transaction's worth).
    /// Uses a multi-row INSERT with ON CONFLICT DO NOTHING for idempotency.
    /// Retries transient failures up to max_retries times with exponential backoff.
    /// On transient errors, the connection is closed and re-established before
    /// retrying, since the old connection is likely dead.
    /// Returns the number of rows actually inserted.
    pub fn persistChanges(self: *Storage, changes: []const decoder.Change) StorageError!usize {
        if (changes.len == 0) return 0;

        // Build the INSERT statement
        const sql = self.buildInsertSql(changes) catch |err| {
            log.err("failed to build INSERT SQL: {}", .{err});
            return err;
        };
        defer self.allocator.free(sql);

        log.debug("executing INSERT for {d} changes ({d} bytes)", .{ changes.len, sql.len });

        // Retry transient failures with reconnection
        const max_retries: u32 = 3;
        var attempt: u32 = 0;
        while (true) {
            var result = self.conn.query(sql) catch |err| {
                attempt += 1;
                if (attempt > max_retries or !isTransientError(err)) {
                    log.err("permanent write failure after {d} attempts: {}", .{ attempt, err });
                    return err;
                }
                const backoff_ms: u64 = @as(u64, 100) * (@as(u64, 1) << @min(attempt, 4));
                log.warn("transient write error (attempt {d}/{d}): {}, reconnecting in {d}ms...", .{
                    attempt, max_retries, err, backoff_ms,
                });
                std.time.sleep(backoff_ms * std.time.ns_per_ms);

                // Reconnect before retrying — the old connection is likely dead
                self.reconnect() catch |reconnect_err| {
                    log.err("reconnection failed (attempt {d}/{d}): {}", .{
                        attempt, max_retries, reconnect_err,
                    });
                    // Continue loop; next iteration will try reconnect again or give up
                };
                continue;
            };
            defer result.deinit();

            // Parse "INSERT 0 N" from command_tag to get rows inserted
            const inserted = parseInsertCount(result.command_tag);
            return inserted;
        }
    }

    /// Classify whether an error is transient (worth retrying) or permanent.
    fn isTransientError(err: StorageError) bool {
        return switch (err) {
            error.ConnectionRefused => true,
            error.ConnectionResetByPeer => true,
            error.BrokenPipe => true,
            error.UnexpectedEndOfData => true,
            error.ServerError => false, // SQL errors are not transient
            error.AuthenticationFailed => false,
            error.UnsupportedAuthMethod => false,
            else => false,
        };
    }

    fn buildInsertSql(self: *Storage, changes: []const decoder.Change) ![]u8 {
        var buf = std.ArrayList(u8).init(self.allocator);
        errdefer buf.deinit();

        try buf.appendSlice(
            \\INSERT INTO changes (primary_key, before, after, context, database, schema, "table", operation, committed_at, queued_at, transaction_id, position)
            \\VALUES
        );

        for (changes, 0..) |change, i| {
            if (i > 0) try buf.append(',');
            try buf.appendSlice("\n(");

            // primary_key
            try appendEscapedLiteral(&buf, change.primary_key);
            try buf.appendSlice(", ");

            // before (JSONB)
            try appendJsonbLiteral(&buf, change.before);
            try buf.appendSlice(", ");

            // after (JSONB)
            try appendJsonbLiteral(&buf, change.after);
            try buf.appendSlice(", ");

            // context (JSONB) — from _bemi logical message or empty
            if (change.context) |ctx| {
                try appendEscapedLiteral(&buf, ctx);
            } else {
                try buf.appendSlice("'{}'");
            }
            try buf.appendSlice(", ");

            // database
            try appendEscapedLiteral(&buf, change.database);
            try buf.appendSlice(", ");

            // schema
            try appendEscapedLiteral(&buf, change.schema);
            try buf.appendSlice(", ");

            // table
            try appendEscapedLiteral(&buf, change.table);
            try buf.appendSlice(", ");

            // operation
            try appendEscapedLiteral(&buf, change.operation.toString());
            try buf.appendSlice(", ");

            // committed_at — convert PG epoch microseconds to timestamptz
            try appendTimestampFromPgEpochUs(&buf, change.committed_at);
            try buf.appendSlice(", ");

            // queued_at — current time
            try buf.appendSlice("NOW(), ");

            // transaction_id
            var xid_buf: [20]u8 = undefined;
            const xid_str = std.fmt.bufPrint(&xid_buf, "{d}", .{change.transaction_id}) catch unreachable;
            try buf.appendSlice(xid_str);
            try buf.appendSlice(", ");

            // position — LSN as bigint
            const lsn_num = protocol.parseLsn(change.position) catch 0;
            var lsn_buf: [20]u8 = undefined;
            const lsn_str = std.fmt.bufPrint(&lsn_buf, "{d}", .{lsn_num}) catch unreachable;
            try buf.appendSlice(lsn_str);

            try buf.append(')');
        }

        try buf.appendSlice("\nON CONFLICT ON CONSTRAINT changes_position_table_schema_database_operation_unique DO NOTHING;");

        return try buf.toOwnedSlice();
    }

    /// Parse "INSERT 0 N" command tag to extract number of rows inserted.
    fn parseInsertCount(tag: []const u8) usize {
        // Format: "INSERT oid count" — for recent PG versions oid is always 0
        if (!std.mem.startsWith(u8, tag, "INSERT ")) return 0;
        const rest = tag["INSERT ".len..];
        // Find the last space
        var last_space: usize = 0;
        for (rest, 0..) |c, i| {
            if (c == ' ') last_space = i;
        }
        if (last_space == 0) return 0;
        const count_str = rest[last_space + 1 ..];
        return std.fmt.parseUnsigned(usize, count_str, 10) catch 0;
    }

    // ========================================================================
    // SQL Escaping Helpers
    // ========================================================================

    /// Append a PostgreSQL string literal with single-quote escaping.
    fn appendEscapedLiteral(buf: *std.ArrayList(u8), s: []const u8) !void {
        try buf.append('\'');
        for (s) |c| {
            if (c == '\'') {
                try buf.appendSlice("''");
            } else if (c == '\\') {
                try buf.appendSlice("\\\\");
            } else {
                try buf.append(c);
            }
        }
        try buf.append('\'');
    }

    /// Append a JSONB literal from a slice of NamedValues.
    /// Produces '{"col1": "val1", "col2": null}' etc.
    fn appendJsonbLiteral(buf: *std.ArrayList(u8), named_values: ?[]const decoder.NamedValue) !void {
        try buf.append('\'');
        try appendJsonObject(buf, named_values);
        try buf.append('\'');
    }

    /// Append a JSON object from NamedValues (without surrounding quotes).
    /// TOASTed (unchanged) columns are omitted entirely — they represent
    /// values that PostgreSQL did not send because they weren't modified.
    /// Including them as `null` would be incorrect: consumers couldn't
    /// distinguish "unchanged" from "actually NULL".
    fn appendJsonObject(buf: *std.ArrayList(u8), named_values: ?[]const decoder.NamedValue) !void {
        const nvs = named_values orelse {
            try buf.appendSlice("{}");
            return;
        };
        try buf.append('{');
        var first = true;
        for (nvs) |nv| {
            switch (nv.value) {
                .unchanged => continue, // omit TOASTed columns
                .text => |t| {
                    if (!first) try buf.appendSlice(", ");
                    first = false;
                    try appendJsonString(buf, nv.name);
                    try buf.appendSlice(": ");
                    try appendJsonString(buf, t);
                },
                .null_value => {
                    if (!first) try buf.appendSlice(", ");
                    first = false;
                    try appendJsonString(buf, nv.name);
                    try buf.appendSlice(": ");
                    try buf.appendSlice("null");
                },
            }
        }
        try buf.append('}');
    }

    /// Append a JSON-escaped string (with double quotes and escaping).
    fn appendJsonString(buf: *std.ArrayList(u8), s: []const u8) !void {
        try buf.append('"');
        for (s) |c| {
            switch (c) {
                '"' => try buf.appendSlice("\\\""),
                '\\' => try buf.appendSlice("\\\\"),
                '\n' => try buf.appendSlice("\\n"),
                '\r' => try buf.appendSlice("\\r"),
                '\t' => try buf.appendSlice("\\t"),
                else => {
                    if (c < 0x20) {
                        // Control character — escape as \u00XX
                        var hex_buf: [6]u8 = undefined;
                        _ = std.fmt.bufPrint(&hex_buf, "\\u{x:0>4}", .{c}) catch unreachable;
                        try buf.appendSlice(&hex_buf);
                    } else {
                        try buf.append(c);
                    }
                },
            }
        }
        try buf.append('"');
    }

    /// Append a timestamptz literal from PG epoch microseconds.
    /// PG epoch = 2000-01-01 00:00:00 UTC. We convert to Unix epoch seconds
    /// and use to_timestamp().
    fn appendTimestampFromPgEpochUs(buf: *std.ArrayList(u8), pg_epoch_us: i64) !void {
        const pg_epoch_offset_us: i64 = 946_684_800_000_000;
        const unix_us = pg_epoch_us + pg_epoch_offset_us;
        // to_timestamp takes seconds as a double
        const secs = @divTrunc(unix_us, 1_000_000);
        const frac_us = @mod(unix_us, 1_000_000);

        var ts_buf: [64]u8 = undefined;
        const ts_str = std.fmt.bufPrint(&ts_buf, "to_timestamp({d}.{d:0>6})", .{ secs, @as(u64, @intCast(if (frac_us < 0) -frac_us else frac_us)) }) catch unreachable;
        try buf.appendSlice(ts_str);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "parseInsertCount parses command tag" {
    try std.testing.expectEqual(@as(usize, 3), Storage.parseInsertCount("INSERT 0 3"));
    try std.testing.expectEqual(@as(usize, 0), Storage.parseInsertCount("INSERT 0 0"));
    try std.testing.expectEqual(@as(usize, 1), Storage.parseInsertCount("INSERT 0 1"));
    try std.testing.expectEqual(@as(usize, 0), Storage.parseInsertCount("UPDATE 3"));
    try std.testing.expectEqual(@as(usize, 0), Storage.parseInsertCount(""));
}

test "appendEscapedLiteral escapes quotes and backslashes" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try Storage.appendEscapedLiteral(&buf, "hello");
    try std.testing.expectEqualStrings("'hello'", buf.items);

    buf.clearRetainingCapacity();
    try Storage.appendEscapedLiteral(&buf, "it's a test");
    try std.testing.expectEqualStrings("'it''s a test'", buf.items);

    buf.clearRetainingCapacity();
    try Storage.appendEscapedLiteral(&buf, "back\\slash");
    try std.testing.expectEqualStrings("'back\\\\slash'", buf.items);
}

test "appendJsonString escapes correctly" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try Storage.appendJsonString(&buf, "hello");
    try std.testing.expectEqualStrings("\"hello\"", buf.items);

    buf.clearRetainingCapacity();
    try Storage.appendJsonString(&buf, "say \"hi\"");
    try std.testing.expectEqualStrings("\"say \\\"hi\\\"\"", buf.items);

    buf.clearRetainingCapacity();
    try Storage.appendJsonString(&buf, "line1\nline2");
    try std.testing.expectEqualStrings("\"line1\\nline2\"", buf.items);
}

test "appendJsonObject produces valid JSON" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    // null case
    try Storage.appendJsonObject(&buf, null);
    try std.testing.expectEqualStrings("{}", buf.items);

    buf.clearRetainingCapacity();
    const nvs = [_]decoder.NamedValue{
        .{ .name = "id", .value = .{ .text = "42" } },
        .{ .name = "name", .value = .{ .text = "Alice" } },
        .{ .name = "email", .value = .{ .null_value = {} } },
    };
    try Storage.appendJsonObject(&buf, &nvs);
    try std.testing.expectEqualStrings("{\"id\": \"42\", \"name\": \"Alice\", \"email\": null}", buf.items);
}

test "appendJsonObject omits unchanged (TOASTed) columns" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    const nvs = [_]decoder.NamedValue{
        .{ .name = "id", .value = .{ .text = "42" } },
        .{ .name = "bio", .value = .{ .unchanged = {} } }, // TOASTed — should be omitted
        .{ .name = "name", .value = .{ .text = "Alice" } },
    };
    try Storage.appendJsonObject(&buf, &nvs);
    try std.testing.expectEqualStrings("{\"id\": \"42\", \"name\": \"Alice\"}", buf.items);
}

test "appendJsonObject omits all unchanged columns" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    // All columns unchanged — should produce empty object
    const nvs = [_]decoder.NamedValue{
        .{ .name = "id", .value = .{ .unchanged = {} } },
        .{ .name = "name", .value = .{ .unchanged = {} } },
    };
    try Storage.appendJsonObject(&buf, &nvs);
    try std.testing.expectEqualStrings("{}", buf.items);
}

test "appendJsonbLiteral wraps in single quotes" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try Storage.appendJsonbLiteral(&buf, null);
    try std.testing.expectEqualStrings("'{}'", buf.items);
}

test "appendTimestampFromPgEpochUs produces to_timestamp" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    // 0 PG epoch = 2000-01-01 00:00:00 UTC = Unix 946684800
    try Storage.appendTimestampFromPgEpochUs(&buf, 0);
    try std.testing.expectEqualStrings("to_timestamp(946684800.000000)", buf.items);

    buf.clearRetainingCapacity();
    // 1.5 seconds after PG epoch
    try Storage.appendTimestampFromPgEpochUs(&buf, 1_500_000);
    try std.testing.expectEqualStrings("to_timestamp(946684801.500000)", buf.items);
}

test "buildInsertSql produces valid SQL for single change" {
    const allocator = std.testing.allocator;

    // Build a minimal Change
    const after_nvs = try allocator.alloc(decoder.NamedValue, 2);
    after_nvs[0] = .{ .name = "id", .value = .{ .text = "1" } };
    after_nvs[1] = .{ .name = "name", .value = .{ .text = "Alice" } };

    const changes = [_]decoder.Change{.{
        .primary_key = "1",
        .before = null,
        .after = after_nvs,
        .database = "testdb",
        .schema = "public",
        .table = "users",
        .operation = .CREATE,
        .committed_at = 0,
        .transaction_id = 100,
        .position = "0/1000",
    }};

    // We need a Storage with just an allocator for this test
    // Use a mock approach: call buildInsertSql directly
    var storage = Storage{
        .conn = undefined,
        .allocator = allocator,
        .config = Config{},
        .metrics = null,
    };

    const sql = try storage.buildInsertSql(&changes);
    defer allocator.free(sql);

    // Verify it contains key parts
    try std.testing.expect(std.mem.indexOf(u8, sql, "INSERT INTO changes") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "ON CONFLICT") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "'testdb'") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "'public'") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "'users'") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "'CREATE'") != null);
    try std.testing.expect(std.mem.indexOf(u8, sql, "4096") != null); // LSN 0/1000 = 4096

    // Clean up — don't call Change.deinit since we didn't dupe anything
    allocator.free(after_nvs);
}

test "buildInsertSql uses change.context when present" {
    const allocator = std.testing.allocator;

    const after_nvs = try allocator.alloc(decoder.NamedValue, 1);
    after_nvs[0] = .{ .name = "id", .value = .{ .text = "1" } };

    const changes = [_]decoder.Change{.{
        .primary_key = "1",
        .before = null,
        .after = after_nvs,
        .database = "testdb",
        .schema = "public",
        .table = "users",
        .operation = .CREATE,
        .committed_at = 0,
        .transaction_id = 100,
        .position = "0/1000",
        .context = "{\"user_id\": \"123\"}",
    }};

    var storage = Storage{
        .conn = undefined,
        .allocator = allocator,
        .config = Config{},
        .metrics = null,
    };

    const sql = try storage.buildInsertSql(&changes);
    defer allocator.free(sql);

    // Should contain the escaped context, not '{}'
    try std.testing.expect(std.mem.indexOf(u8, sql, "{\"user_id\": \"123\"}") != null);

    allocator.free(after_nvs);
}

test "isTransientError classifies errors correctly" {
    try std.testing.expect(Storage.isTransientError(error.ConnectionRefused));
    try std.testing.expect(Storage.isTransientError(error.ConnectionResetByPeer));
    try std.testing.expect(Storage.isTransientError(error.BrokenPipe));
    try std.testing.expect(Storage.isTransientError(error.UnexpectedEndOfData));
    try std.testing.expect(!Storage.isTransientError(error.ServerError));
    try std.testing.expect(!Storage.isTransientError(error.AuthenticationFailed));
    try std.testing.expect(!Storage.isTransientError(error.UnsupportedAuthMethod));
}

test "buildInsertSql uses empty context when null" {
    const allocator = std.testing.allocator;

    const after_nvs = try allocator.alloc(decoder.NamedValue, 1);
    after_nvs[0] = .{ .name = "id", .value = .{ .text = "1" } };

    const changes = [_]decoder.Change{.{
        .primary_key = "1",
        .before = null,
        .after = after_nvs,
        .database = "testdb",
        .schema = "public",
        .table = "users",
        .operation = .CREATE,
        .committed_at = 0,
        .transaction_id = 100,
        .position = "0/1000",
        // context defaults to null
    }};

    var storage = Storage{
        .conn = undefined,
        .allocator = allocator,
        .config = Config{},
        .metrics = null,
    };

    const sql = try storage.buildInsertSql(&changes);
    defer allocator.free(sql);

    // Should contain '{}' for empty context
    try std.testing.expect(std.mem.indexOf(u8, sql, "'{}'") != null);

    allocator.free(after_nvs);
}

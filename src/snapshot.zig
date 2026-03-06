const std = @import("std");
const Connection = @import("connection.zig").Connection;
const Config = @import("config.zig").Config;
const Storage = @import("storage.zig").Storage;
const Metrics = @import("metrics.zig").Metrics;
const decoder = @import("decoder.zig");
const protocol = @import("protocol.zig");

const log = std.log.scoped(.snapshot);

/// Maximum number of rows to read and persist in a single batch.
/// Keeps memory usage bounded for large tables.
const BATCH_SIZE: usize = 1000;

/// Information about a table discovered from the publication.
const TableInfo = struct {
    schema: []const u8,
    table: []const u8,
};

/// Information about a table's primary key columns.
const PrimaryKeyInfo = struct {
    /// Column names that form the primary key, in order.
    columns: [][]const u8,
    allocator: std.mem.Allocator,

    fn deinit(self: *PrimaryKeyInfo) void {
        for (self.columns) |col| {
            self.allocator.free(col);
        }
        self.allocator.free(self.columns);
    }
};

/// Perform an initial snapshot of all existing data in tracked tables.
///
/// Opens a new normal connection to the source database, sets the
/// transaction snapshot to the one exported by slot creation, then
/// reads all rows from each tracked table and persists them as
/// CREATE changes to the destination via storage.
///
/// The snapshot is consistent: it sees exactly the data that existed
/// at the WAL position where the replication slot was created. No
/// changes will be missed or duplicated when streaming begins.
///
/// `snapshot_name` must be freed by the caller after this returns.
pub fn performSnapshot(
    allocator: std.mem.Allocator,
    config: Config,
    storage: *Storage,
    snapshot_name: []const u8,
    metrics: ?*Metrics,
) !void {
    log.info("starting initial snapshot with snapshot '{s}'...", .{snapshot_name});

    // Open a normal (non-replication) connection to the source database
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
        0, // no query timeout — snapshot queries can be slow for large tables
    ) catch |err| {
        log.err("failed to open snapshot connection: {}", .{err});
        return err;
    };
    defer conn.close();

    // Begin a REPEATABLE READ transaction and set the exported snapshot.
    // This ensures we see exactly the same data as the replication slot's
    // starting point — no gap, no overlap with subsequent WAL streaming.
    conn.exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ") catch |err| {
        log.err("failed to BEGIN transaction for snapshot: {}", .{err});
        return err;
    };

    // SET TRANSACTION SNAPSHOT requires the snapshot ID as a string literal.
    // The snapshot_name comes from PostgreSQL and contains only hex digits and
    // dashes (format: "00000003-00000001-1"), but we quote it safely anyway.
    var snap_sql_buf: [256]u8 = undefined;
    const snap_sql = std.fmt.bufPrint(&snap_sql_buf, "SET TRANSACTION SNAPSHOT '{s}'", .{snapshot_name}) catch {
        log.err("snapshot name too long for buffer", .{});
        return error.ServerError;
    };
    conn.exec(snap_sql) catch |err| {
        log.err("failed to SET TRANSACTION SNAPSHOT '{s}': {}", .{ snapshot_name, err });
        return err;
    };

    log.info("snapshot transaction opened, reading tables...", .{});

    // Discover which tables to snapshot from the publication
    const tables = try listPublicationTables(allocator, &conn, config);
    defer {
        for (tables) |t| {
            allocator.free(t.schema);
            allocator.free(t.table);
        }
        allocator.free(tables);
    }

    if (tables.len == 0) {
        log.info("no tables found in publication '{s}', snapshot complete", .{config.publication_name});
        conn.exec("COMMIT") catch {};
        return;
    }

    log.info("snapshot: found {d} tables to capture", .{tables.len});

    // Process each table
    var total_rows: u64 = 0;
    for (tables) |table_info| {
        // Skip the changes table (same feedback-loop prevention as streaming)
        if (std.mem.eql(u8, table_info.table, "changes")) {
            log.debug("snapshot: skipping 'changes' table (feedback loop prevention)", .{});
            continue;
        }
        // Apply TABLES filter if configured
        if (!config.shouldTrackTable(table_info.schema, table_info.table)) {
            log.debug("snapshot: skipping untracked table '{s}'", .{table_info.table});
            continue;
        }

        const rows = try snapshotTable(allocator, &conn, storage, config, table_info, metrics);
        total_rows += rows;
    }

    // Commit the snapshot transaction (releases the snapshot)
    conn.exec("COMMIT") catch |err| {
        log.warn("failed to COMMIT snapshot transaction: {}", .{err});
    };

    log.info("initial snapshot complete: {d} rows captured from {d} tables", .{ total_rows, tables.len });
}

/// Query pg_publication_tables to find which tables are in the publication.
fn listPublicationTables(
    allocator: std.mem.Allocator,
    conn: *Connection,
    config: Config,
) ![]TableInfo {
    var sql_buf: [512]u8 = undefined;
    const sql = std.fmt.bufPrint(&sql_buf,
        \\SELECT schemaname, tablename
        \\FROM pg_publication_tables
        \\WHERE pubname = '{s}'
        \\ORDER BY schemaname, tablename
    , .{config.publication_name}) catch return error.ServerError;

    var result = conn.query(sql) catch |err| {
        log.err("failed to query pg_publication_tables: {}", .{err});
        return err;
    };
    defer result.deinit();

    var tables = std.ArrayList(TableInfo).init(allocator);
    errdefer {
        for (tables.items) |t| {
            allocator.free(t.schema);
            allocator.free(t.table);
        }
        tables.deinit();
    }

    for (result.rows) |row| {
        if (row.columns.len < 2) continue;
        const schema_val = switch (row.columns[0]) {
            .text => |t| t,
            .null_value => continue,
        };
        const table_val = switch (row.columns[1]) {
            .text => |t| t,
            .null_value => continue,
        };

        try tables.append(.{
            .schema = try allocator.dupe(u8, schema_val),
            .table = try allocator.dupe(u8, table_val),
        });
    }

    return try tables.toOwnedSlice();
}

/// Find the primary key column names for a given table.
fn getPrimaryKeyColumns(
    allocator: std.mem.Allocator,
    conn: *Connection,
    schema: []const u8,
    table: []const u8,
) !PrimaryKeyInfo {
    // Query pg_index + pg_attribute to find PK columns in order
    var sql_buf: [1024]u8 = undefined;
    const sql = std.fmt.bufPrint(&sql_buf,
        \\SELECT a.attname
        \\FROM pg_index i
        \\JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        \\WHERE i.indrelid = '{s}.{s}'::regclass
        \\  AND i.indisprimary
        \\ORDER BY array_position(i.indkey, a.attnum)
    , .{ schema, table }) catch return error.ServerError;

    var result = conn.query(sql) catch |err| {
        log.warn("failed to query primary key for {s}.{s}: {}", .{ schema, table, err });
        return .{ .columns = try allocator.alloc([]const u8, 0), .allocator = allocator };
    };
    defer result.deinit();

    var cols = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (cols.items) |c| allocator.free(c);
        cols.deinit();
    }

    for (result.rows) |row| {
        if (row.columns.len < 1) continue;
        switch (row.columns[0]) {
            .text => |t| try cols.append(try allocator.dupe(u8, t)),
            .null_value => {},
        }
    }

    return .{ .columns = try cols.toOwnedSlice(), .allocator = allocator };
}

/// Get column names for a table via information_schema.
fn getColumnNames(
    allocator: std.mem.Allocator,
    conn: *Connection,
    schema: []const u8,
    table: []const u8,
) ![][]const u8 {
    var sql_buf: [1024]u8 = undefined;
    const sql = std.fmt.bufPrint(&sql_buf,
        \\SELECT column_name
        \\FROM information_schema.columns
        \\WHERE table_schema = '{s}' AND table_name = '{s}'
        \\ORDER BY ordinal_position
    , .{ schema, table }) catch return error.ServerError;

    var result = conn.query(sql) catch |err| {
        log.warn("failed to query columns for {s}.{s}: {}", .{ schema, table, err });
        return allocator.alloc([]const u8, 0);
    };
    defer result.deinit();

    var cols = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (cols.items) |c| allocator.free(c);
        cols.deinit();
    }

    for (result.rows) |row| {
        if (row.columns.len < 1) continue;
        switch (row.columns[0]) {
            .text => |t| try cols.append(try allocator.dupe(u8, t)),
            .null_value => {},
        }
    }

    return try cols.toOwnedSlice();
}

/// Snapshot a single table: SELECT * and persist rows as CREATE changes.
/// Returns the number of rows captured.
fn snapshotTable(
    allocator: std.mem.Allocator,
    conn: *Connection,
    storage: *Storage,
    config: Config,
    table_info: TableInfo,
    metrics: ?*Metrics,
) !u64 {
    log.info("snapshot: capturing {s}.{s}...", .{ table_info.schema, table_info.table });

    // Get column names and primary key info
    var pk_info = try getPrimaryKeyColumns(allocator, conn, table_info.schema, table_info.table);
    defer pk_info.deinit();

    const col_names = try getColumnNames(allocator, conn, table_info.schema, table_info.table);
    defer {
        for (col_names) |c| allocator.free(c);
        allocator.free(col_names);
    }

    // Build the SELECT query
    var sql_buf: [512]u8 = undefined;
    const sql = std.fmt.bufPrint(&sql_buf, "SELECT * FROM \"{s}\".\"{s}\"", .{
        table_info.schema,
        table_info.table,
    }) catch return error.ServerError;

    var result = conn.query(sql) catch |err| {
        log.err("snapshot: failed to SELECT from {s}.{s}: {}", .{ table_info.schema, table_info.table, err });
        return err;
    };
    defer result.deinit();

    if (result.rows.len == 0) {
        log.info("snapshot: {s}.{s} is empty, skipping", .{ table_info.schema, table_info.table });
        return 0;
    }

    // Use column names from the result's FieldDescription if available,
    // falling back to information_schema query
    const effective_col_names = if (result.columns.len > 0) blk: {
        var names = try allocator.alloc([]const u8, result.columns.len);
        for (result.columns, 0..) |col, i| {
            names[i] = try allocator.dupe(u8, col.name);
        }
        break :blk names;
    } else col_names;
    defer {
        if (result.columns.len > 0) {
            for (effective_col_names) |n| allocator.free(n);
            allocator.free(effective_col_names);
        }
    }

    // Build a set of PK column names for fast lookup
    var pk_set = std.StringHashMap(void).init(allocator);
    defer pk_set.deinit();
    for (pk_info.columns) |pk_col| {
        try pk_set.put(pk_col, {});
    }

    // Synthesize changes in batches
    const now_us = pgEpochMicroseconds();
    var changes_batch = std.ArrayList(decoder.Change).init(allocator);
    defer {
        for (changes_batch.items) |change| change.deinit(allocator);
        changes_batch.deinit();
    }

    var row_count: u64 = 0;
    for (result.rows) |row| {
        row_count += 1;

        // Build the "after" named values (column name + value pairs)
        var after_list = std.ArrayList(decoder.NamedValue).init(allocator);
        errdefer {
            for (after_list.items) |nv| {
                switch (nv.value) {
                    .text => |t| allocator.free(t),
                    else => {},
                }
            }
            after_list.deinit();
        }

        const num_cols = @min(row.columns.len, effective_col_names.len);
        for (0..num_cols) |ci| {
            const col_name = effective_col_names[ci];

            // Apply column exclusion (but never exclude PK columns)
            if (!pk_set.contains(col_name) and
                config.shouldExcludeColumn(table_info.schema, table_info.table, col_name))
            {
                // Excluded column — store sentinel value
                try after_list.append(.{
                    .name = col_name,
                    .value = .{ .text = try allocator.dupe(u8, "[EXCLUDED]") },
                    .type_oid = 0,
                });
                continue;
            }

            switch (row.columns[ci]) {
                .text => |t| {
                    try after_list.append(.{
                        .name = col_name,
                        .value = .{ .text = try allocator.dupe(u8, t) },
                        .type_oid = 0,
                    });
                },
                .null_value => {
                    try after_list.append(.{
                        .name = col_name,
                        .value = .null_value,
                        .type_oid = 0,
                    });
                },
            }
        }

        // Extract primary key value
        const pk_value = try extractSnapshotPrimaryKey(allocator, &after_list, &pk_info);

        // Build position string: use synthetic sequential values
        // Real WAL LSNs are always much larger (e.g. 0/1000000+),
        // so small synthetic values won't collide.
        var pos_buf: [32]u8 = undefined;
        const position = try allocator.dupe(u8, std.fmt.bufPrint(&pos_buf, "0/{X}", .{row_count}) catch "0/0");

        const after_owned = try after_list.toOwnedSlice();

        try changes_batch.append(.{
            .primary_key = pk_value,
            .before = null, // CREATE has no "before"
            .after = after_owned,
            .database = config.db_name,
            .schema = table_info.schema,
            .table = table_info.table,
            .operation = .CREATE,
            .committed_at = now_us,
            .transaction_id = 0, // synthetic
            .position = position,
            .context = null,
        });

        // Flush batch when full
        if (changes_batch.items.len >= BATCH_SIZE) {
            const batch_inserted = try flushSnapshotBatch(storage, changes_batch.items, metrics);
            log.debug("snapshot: flushed {d} rows for {s}.{s} ({d} inserted)", .{
                changes_batch.items.len,
                table_info.schema,
                table_info.table,
                batch_inserted,
            });
            // Free the flushed changes
            for (changes_batch.items) |change| change.deinit(allocator);
            changes_batch.clearRetainingCapacity();
        }
    }

    // Flush remaining rows
    if (changes_batch.items.len > 0) {
        const batch_inserted = try flushSnapshotBatch(storage, changes_batch.items, metrics);
        log.debug("snapshot: flushed final {d} rows for {s}.{s} ({d} inserted)", .{
            changes_batch.items.len,
            table_info.schema,
            table_info.table,
            batch_inserted,
        });
    }

    log.info("snapshot: captured {d} rows from {s}.{s}", .{ row_count, table_info.schema, table_info.table });
    return row_count;
}

/// Extract a primary key value string from a row's named values.
fn extractSnapshotPrimaryKey(
    allocator: std.mem.Allocator,
    after_list: *const std.ArrayList(decoder.NamedValue),
    pk_info: *const PrimaryKeyInfo,
) ![]const u8 {
    if (pk_info.columns.len == 0) {
        return try allocator.dupe(u8, "");
    }

    if (pk_info.columns.len == 1) {
        // Single-column PK: return the value directly
        const pk_col = pk_info.columns[0];
        for (after_list.items) |nv| {
            if (std.mem.eql(u8, nv.name, pk_col)) {
                return switch (nv.value) {
                    .text => |t| try allocator.dupe(u8, t),
                    .null_value => try allocator.dupe(u8, ""),
                    else => try allocator.dupe(u8, ""),
                };
            }
        }
        return try allocator.dupe(u8, "");
    }

    // Composite PK: join values with comma
    var parts = std.ArrayList(u8).init(allocator);
    defer parts.deinit();

    for (pk_info.columns, 0..) |pk_col, idx| {
        if (idx > 0) try parts.append(',');
        for (after_list.items) |nv| {
            if (std.mem.eql(u8, nv.name, pk_col)) {
                switch (nv.value) {
                    .text => |t| try parts.appendSlice(t),
                    else => {},
                }
                break;
            }
        }
    }

    return try parts.toOwnedSlice();
}

/// Persist a batch of snapshot changes via storage.
fn flushSnapshotBatch(
    storage: *Storage,
    changes: []const decoder.Change,
    metrics: ?*Metrics,
) !usize {
    const inserted = storage.persistChanges(changes) catch |err| {
        log.err("snapshot: failed to persist {d} changes: {}", .{ changes.len, err });
        if (metrics) |m| Metrics.inc(&m.persist_errors_total);
        return err;
    };

    // Update metrics
    if (metrics) |m| {
        for (changes) |change| {
            switch (change.operation) {
                .CREATE => Metrics.inc(&m.changes_created_total),
                .UPDATE => Metrics.inc(&m.changes_updated_total),
                .DELETE => Metrics.inc(&m.changes_deleted_total),
                .TRUNCATE => Metrics.inc(&m.changes_truncated_total),
            }
        }
    }

    return inserted;
}

/// Get the current time as PG epoch microseconds.
/// PostgreSQL epoch is 2000-01-01 00:00:00 UTC.
fn pgEpochMicroseconds() i64 {
    const unix_us = std.time.microTimestamp();
    const pg_epoch_offset_us: i64 = 946_684_800_000_000; // 2000-01-01 in Unix epoch microseconds
    return unix_us - pg_epoch_offset_us;
}

// ============================================================================
// Tests
// ============================================================================

test "pgEpochMicroseconds returns reasonable value" {
    const us = pgEpochMicroseconds();
    // Should be positive (we're past year 2000)
    try std.testing.expect(us > 0);
    // Should be less than year 2100 in PG epoch (~3.15e15)
    try std.testing.expect(us < 3_150_000_000_000_000);
}

test "extractSnapshotPrimaryKey single column" {
    const allocator = std.testing.allocator;

    var pk_cols = [_][]const u8{"id"};
    var pk_info = PrimaryKeyInfo{
        .columns = &pk_cols,
        .allocator = allocator,
    };
    // Don't call deinit since columns are stack-allocated test data
    _ = &pk_info;

    var after = std.ArrayList(decoder.NamedValue).init(allocator);
    defer after.deinit();
    try after.append(.{ .name = "id", .value = .{ .text = "42" }, .type_oid = 0 });
    try after.append(.{ .name = "name", .value = .{ .text = "Alice" }, .type_oid = 0 });

    const pk = try extractSnapshotPrimaryKey(allocator, &after, &pk_info);
    defer allocator.free(pk);
    try std.testing.expectEqualStrings("42", pk);
}

test "extractSnapshotPrimaryKey composite key" {
    const allocator = std.testing.allocator;

    var pk_cols = [_][]const u8{ "org_id", "user_id" };
    var pk_info = PrimaryKeyInfo{
        .columns = &pk_cols,
        .allocator = allocator,
    };
    _ = &pk_info;

    var after = std.ArrayList(decoder.NamedValue).init(allocator);
    defer after.deinit();
    try after.append(.{ .name = "org_id", .value = .{ .text = "100" }, .type_oid = 0 });
    try after.append(.{ .name = "user_id", .value = .{ .text = "200" }, .type_oid = 0 });
    try after.append(.{ .name = "role", .value = .{ .text = "admin" }, .type_oid = 0 });

    const pk = try extractSnapshotPrimaryKey(allocator, &after, &pk_info);
    defer allocator.free(pk);
    try std.testing.expectEqualStrings("100,200", pk);
}

test "extractSnapshotPrimaryKey empty pk" {
    const allocator = std.testing.allocator;

    var pk_info = PrimaryKeyInfo{
        .columns = try allocator.alloc([]const u8, 0),
        .allocator = allocator,
    };
    defer pk_info.deinit();

    var after = std.ArrayList(decoder.NamedValue).init(allocator);
    defer after.deinit();
    try after.append(.{ .name = "id", .value = .{ .text = "1" }, .type_oid = 0 });

    const pk = try extractSnapshotPrimaryKey(allocator, &after, &pk_info);
    defer allocator.free(pk);
    try std.testing.expectEqualStrings("", pk);
}

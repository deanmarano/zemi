const std = @import("std");
const mem = std.mem;
const protocol = @import("protocol.zig");

const log = std.log.scoped(.decoder);

// ============================================================================
// pgoutput Logical Decoding Message Types
// ============================================================================

/// pgoutput message type identifiers (first byte of the message payload).
pub const PgOutputMessageType = enum(u8) {
    begin = 'B',
    commit = 'C',
    origin = 'O',
    relation = 'R',
    pg_type = 'Y',
    insert = 'I',
    update = 'U',
    delete = 'D',
    truncate = 'T',
    message = 'M',
    _,
};

/// Column type flag in tuple data.
pub const TupleDataType = enum(u8) {
    null_value = 'n',
    unchanged = 'u',
    text = 't',
    binary = 'b', // proto_version >= 2, but we support it
    _,
};

/// A single column value from a tuple.
pub const ColumnValue = union(enum) {
    null_value: void,
    unchanged: void,
    text: []const u8,
};

/// A decoded tuple (row) from pgoutput.
pub const TupleData = struct {
    columns: []ColumnValue,
};

/// Column definition from a Relation message.
pub const ColumnDef = struct {
    flags: u8, // 1 = part of replica identity key
    name: []const u8,
    type_oid: u32,
    type_modifier: i32,

    pub fn isKey(self: ColumnDef) bool {
        return (self.flags & 1) != 0;
    }
};

/// Relation (table metadata) from a Relation message.
pub const RelationInfo = struct {
    relation_id: u32,
    namespace: []const u8, // schema name
    name: []const u8, // table name
    replica_identity: u8, // 'd' = default, 'n' = nothing, 'f' = full, 'i' = index
    columns: []ColumnDef,
};

// ============================================================================
// Parsed pgoutput Messages
// ============================================================================

pub const BeginMessage = struct {
    final_lsn: u64,
    commit_timestamp: i64, // microseconds since PG epoch
    xid: u32,
};

pub const CommitMessage = struct {
    flags: u8,
    commit_lsn: u64,
    end_lsn: u64,
    commit_timestamp: i64,
};

pub const InsertMessage = struct {
    relation_id: u32,
    new_tuple: TupleData,
};

pub const UpdateMessage = struct {
    relation_id: u32,
    old_tuple: ?TupleData, // present when REPLICA IDENTITY FULL or key changed
    new_tuple: TupleData,
};

pub const DeleteMessage = struct {
    relation_id: u32,
    old_tuple: TupleData, // key columns or full depending on replica identity
};

pub const TruncateMessage = struct {
    option_bits: u8, // 1 = CASCADE, 2 = RESTART IDENTITY
    relation_ids: []u32,
};

pub const OriginMessage = struct {
    commit_lsn: u64,
    name: []const u8,
};

pub const TypeMessage = struct {
    type_id: u32,
    namespace: []const u8,
    name: []const u8,
};

pub const LogicalDecodingMessage = struct {
    flags: u8, // 1 = transactional
    lsn: u64,
    prefix: []const u8,
    content: []const u8,
};

/// Union of all pgoutput message types.
pub const PgOutputMessage = union(enum) {
    begin: BeginMessage,
    commit: CommitMessage,
    relation: RelationInfo,
    insert: InsertMessage,
    update: UpdateMessage,
    delete: DeleteMessage,
    truncate: TruncateMessage,
    origin: OriginMessage,
    pg_type: TypeMessage,
    message: LogicalDecodingMessage,
    unknown: u8,
};

// ============================================================================
// Change Record — the structured output
// ============================================================================

pub const Operation = enum {
    CREATE,
    UPDATE,
    DELETE,
    TRUNCATE,

    pub fn toString(self: Operation) []const u8 {
        return switch (self) {
            .CREATE => "CREATE",
            .UPDATE => "UPDATE",
            .DELETE => "DELETE",
            .TRUNCATE => "TRUNCATE",
        };
    }
};

/// A structured change record, the final output of decoding.
/// All string fields are owned by the allocator that created the Change.
pub const Change = struct {
    primary_key: []const u8,
    before: ?[]NamedValue, // null for INSERT
    after: ?[]NamedValue, // null for DELETE
    database: []const u8, // not owned (static/long-lived)
    schema: []const u8, // not owned (from relation cache, which owns it)
    table: []const u8, // not owned (from relation cache, which owns it)
    operation: Operation,
    committed_at: i64, // PG epoch microseconds
    transaction_id: u32,
    position: []const u8, // LSN as string — owned
    context: ?[]const u8 = null, // JSON context from _bemi logical message — owned

    /// Free all owned memory in this Change.
    pub fn deinit(self: *const Change, allocator: std.mem.Allocator) void {
        if (self.before) |before| {
            freeNamedValues(allocator, before);
        }
        if (self.after) |after| {
            freeNamedValues(allocator, after);
        }
        allocator.free(self.primary_key);
        allocator.free(self.position);
        if (self.context) |ctx| {
            allocator.free(ctx);
        }
    }
};

/// A named column value (column name + text representation).
pub const NamedValue = struct {
    name: []const u8, // not owned (from relation cache)
    value: ColumnValue, // .text is owned
};

fn freeNamedValues(allocator: std.mem.Allocator, nvs: []NamedValue) void {
    for (nvs) |nv| {
        switch (nv.value) {
            .text => |t| allocator.free(t),
            else => {},
        }
    }
    allocator.free(nvs);
}

// ============================================================================
// Tuple Data Parsing
// ============================================================================

/// Parse a tuple data section from a pgoutput message.
/// Format: Int16 num_columns, then per column: type_flag + optional data
/// NOTE: Returned slices point into the reader's data buffer — they are
/// NOT owned. The Decoder.decode method is responsible for duping them.
fn parseTupleData(reader: *protocol.MessageReader, allocator: std.mem.Allocator) !TupleData {
    const num_columns = try reader.readUInt16();
    var columns = try allocator.alloc(ColumnValue, num_columns);

    for (0..num_columns) |i| {
        const col_type_byte = try reader.readByte();
        const col_type: TupleDataType = @enumFromInt(col_type_byte);

        switch (col_type) {
            .null_value => {
                columns[i] = .null_value;
            },
            .unchanged => {
                columns[i] = .unchanged;
            },
            .text => {
                const value_len = try reader.readInt32();
                if (value_len < 0) {
                    columns[i] = .null_value;
                } else {
                    const value = try reader.readBytes(@intCast(value_len));
                    columns[i] = .{ .text = value };
                }
            },
            .binary => {
                // Binary format — read length + bytes, store as text for now
                const value_len = try reader.readInt32();
                if (value_len < 0) {
                    columns[i] = .null_value;
                } else {
                    const value = try reader.readBytes(@intCast(value_len));
                    columns[i] = .{ .text = value };
                }
            },
            _ => {
                log.warn("unknown tuple data type: 0x{x}", .{col_type_byte});
                columns[i] = .null_value;
            },
        }
    }

    return .{ .columns = columns };
}

// ============================================================================
// pgoutput Message Parsing
// ============================================================================

/// Parse a single pgoutput message from XLogData payload bytes.
/// NOTE: Returned string slices point into the input `data` buffer.
/// For long-lived use, the caller must dupe strings before the buffer is reused.
pub fn parsePgOutputMessage(allocator: std.mem.Allocator, data: []const u8) !PgOutputMessage {
    if (data.len < 1) return error.UnexpectedEndOfData;

    var reader = protocol.MessageReader.init(data);
    const msg_type_byte = try reader.readByte();
    const msg_type: PgOutputMessageType = @enumFromInt(msg_type_byte);

    switch (msg_type) {
        .begin => {
            const final_lsn = try reader.readUInt64();
            const commit_timestamp = try reader.readInt64();
            const xid = try reader.readUInt32();
            return .{ .begin = .{
                .final_lsn = final_lsn,
                .commit_timestamp = commit_timestamp,
                .xid = xid,
            } };
        },
        .commit => {
            const flags = try reader.readByte();
            const commit_lsn = try reader.readUInt64();
            const end_lsn = try reader.readUInt64();
            const commit_timestamp = try reader.readInt64();
            return .{ .commit = .{
                .flags = flags,
                .commit_lsn = commit_lsn,
                .end_lsn = end_lsn,
                .commit_timestamp = commit_timestamp,
            } };
        },
        .relation => {
            const relation_id = try reader.readUInt32();
            const namespace = try reader.readString();
            const name = try reader.readString();
            const replica_identity = try reader.readByte();
            const num_columns = try reader.readUInt16();

            var columns = try allocator.alloc(ColumnDef, num_columns);
            for (0..num_columns) |i| {
                const flags = try reader.readByte();
                const col_name = try reader.readString();
                const type_oid = try reader.readUInt32();
                const type_modifier = try reader.readInt32();
                columns[i] = .{
                    .flags = flags,
                    .name = col_name,
                    .type_oid = type_oid,
                    .type_modifier = type_modifier,
                };
            }

            return .{ .relation = .{
                .relation_id = relation_id,
                .namespace = namespace,
                .name = name,
                .replica_identity = replica_identity,
                .columns = columns,
            } };
        },
        .insert => {
            const relation_id = try reader.readUInt32();
            const tuple_marker = try reader.readByte();
            if (tuple_marker != 'N') {
                log.warn("expected 'N' tuple marker in INSERT, got: 0x{x}", .{tuple_marker});
            }
            const new_tuple = try parseTupleData(&reader, allocator);
            return .{ .insert = .{
                .relation_id = relation_id,
                .new_tuple = new_tuple,
            } };
        },
        .update => {
            const relation_id = try reader.readUInt32();
            // The next byte indicates what follows:
            // 'K' = old tuple (key), 'O' = old tuple (full), 'N' = new tuple only
            const marker = try reader.readByte();
            var old_tuple: ?TupleData = null;

            if (marker == 'K' or marker == 'O') {
                old_tuple = try parseTupleData(&reader, allocator);
                // After old tuple, read the 'N' marker for the new tuple
                const new_marker = try reader.readByte();
                if (new_marker != 'N') {
                    log.warn("expected 'N' tuple marker after old tuple in UPDATE, got: 0x{x}", .{new_marker});
                }
            } else if (marker != 'N') {
                log.warn("unexpected marker in UPDATE: 0x{x}", .{marker});
            }

            const new_tuple = try parseTupleData(&reader, allocator);
            return .{ .update = .{
                .relation_id = relation_id,
                .old_tuple = old_tuple,
                .new_tuple = new_tuple,
            } };
        },
        .delete => {
            const relation_id = try reader.readUInt32();
            // 'K' = key columns, 'O' = full old tuple
            const marker = try reader.readByte();
            if (marker != 'K' and marker != 'O') {
                log.warn("expected 'K' or 'O' marker in DELETE, got: 0x{x}", .{marker});
            }
            const old_tuple = try parseTupleData(&reader, allocator);
            return .{ .delete = .{
                .relation_id = relation_id,
                .old_tuple = old_tuple,
            } };
        },
        .truncate => {
            const num_relations = try reader.readUInt32();
            const option_bits = try reader.readByte();
            var relation_ids = try allocator.alloc(u32, num_relations);
            for (0..num_relations) |i| {
                relation_ids[i] = try reader.readUInt32();
            }
            return .{ .truncate = .{
                .option_bits = option_bits,
                .relation_ids = relation_ids,
            } };
        },
        .origin => {
            const commit_lsn = try reader.readUInt64();
            const name = try reader.readString();
            return .{ .origin = .{
                .commit_lsn = commit_lsn,
                .name = name,
            } };
        },
        .pg_type => {
            const type_id = try reader.readUInt32();
            const namespace = try reader.readString();
            const name = try reader.readString();
            return .{ .pg_type = .{
                .type_id = type_id,
                .namespace = namespace,
                .name = name,
            } };
        },
        .message => {
            const flags = try reader.readByte();
            const lsn = try reader.readUInt64();
            const prefix = try reader.readString();
            const content_len = try reader.readUInt32();
            const content = try reader.readBytes(content_len);
            return .{ .message = .{
                .flags = flags,
                .lsn = lsn,
                .prefix = prefix,
                .content = content,
            } };
        },
        _ => {
            return .{ .unknown = msg_type_byte };
        },
    }
}

// ============================================================================
// Relation Cache
// ============================================================================

/// Caches Relation messages by relation_id for decoding subsequent row data.
/// pgoutput sends a Relation message before the first change to each table
/// in a session, and again if the schema changes.
/// All string data is owned (duped) by the cache.
pub const RelationCache = struct {
    map: std.AutoHashMap(u32, RelationInfo),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) RelationCache {
        return .{
            .map = std.AutoHashMap(u32, RelationInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RelationCache) void {
        var iter = self.map.valueIterator();
        while (iter.next()) |rel| {
            self.freeRelation(rel.*);
        }
        self.map.deinit();
    }

    fn freeRelation(self: *RelationCache, rel: RelationInfo) void {
        self.allocator.free(rel.namespace);
        self.allocator.free(rel.name);
        for (rel.columns) |col| {
            self.allocator.free(col.name);
        }
        self.allocator.free(rel.columns);
    }

    /// Store a relation, duping all string data so it survives buffer reuse.
    /// The caller's ColumnDef slice (from parsePgOutputMessage) is freed here;
    /// the cache takes ownership.
    pub fn put(self: *RelationCache, rel: RelationInfo) !void {
        // If replacing an existing entry, free the old one
        if (self.map.fetchRemove(rel.relation_id)) |old| {
            self.freeRelation(old.value);
        }

        // Dupe all strings
        const namespace = try self.allocator.dupe(u8, rel.namespace);
        errdefer self.allocator.free(namespace);

        const name = try self.allocator.dupe(u8, rel.name);
        errdefer self.allocator.free(name);

        var columns = try self.allocator.alloc(ColumnDef, rel.columns.len);
        errdefer self.allocator.free(columns);

        for (rel.columns, 0..) |col, i| {
            columns[i] = .{
                .flags = col.flags,
                .name = try self.allocator.dupe(u8, col.name),
                .type_oid = col.type_oid,
                .type_modifier = col.type_modifier,
            };
        }

        // Free the original (non-owned) columns slice from parsing
        self.allocator.free(rel.columns);

        try self.map.put(rel.relation_id, .{
            .relation_id = rel.relation_id,
            .namespace = namespace,
            .name = name,
            .replica_identity = rel.replica_identity,
            .columns = columns,
        });
    }

    pub fn get(self: *const RelationCache, relation_id: u32) ?RelationInfo {
        return self.map.get(relation_id);
    }
};

// ============================================================================
// Decoder — stateful pgoutput stream decoder
// ============================================================================

/// Stateful decoder that processes a stream of pgoutput messages,
/// maintains a relation cache, and emits structured Change records.
/// All returned Change records own their string data.
pub const Decoder = struct {
    allocator: std.mem.Allocator,
    relation_cache: RelationCache,
    database: []const u8,

    // Current transaction state
    current_xid: u32 = 0,
    current_commit_timestamp: i64 = 0,
    in_transaction: bool = false,
    transaction_changes: std.ArrayList(Change),
    transaction_context: ?[]const u8 = null, // context from _bemi logical message

    // Large transaction support
    max_transaction_changes: ?u32 = null,

    pub const DecodeResult = union(enum) {
        /// No changes to return (e.g., Begin, Relation, mid-transaction DML below threshold).
        none,
        /// A complete committed transaction's changes.
        commit: []Change,
        /// A mid-transaction flush due to exceeding max_transaction_changes.
        /// The transaction is still in progress; more changes or a commit will follow.
        /// Context is NOT stamped on these changes (it may arrive later in the transaction).
        flush: []Change,
    };

    pub fn init(allocator: std.mem.Allocator, database: []const u8) Decoder {
        return .{
            .allocator = allocator,
            .relation_cache = RelationCache.init(allocator),
            .database = database,
            .transaction_changes = std.ArrayList(Change).init(allocator),
        };
    }

    pub fn deinit(self: *Decoder) void {
        self.relation_cache.deinit();
        self.freeChanges();
        self.transaction_changes.deinit();
        if (self.transaction_context) |ctx| {
            self.allocator.free(ctx);
            self.transaction_context = null;
        }
    }

    fn freeChanges(self: *Decoder) void {
        for (self.transaction_changes.items) |*change| {
            change.deinit(self.allocator);
        }
        self.transaction_changes.clearRetainingCapacity();
    }

    /// Process a single XLogData payload. Returns a DecodeResult:
    /// - `.none` — no changes to emit (Begin, Relation, mid-transaction DML below threshold)
    /// - `.commit` — completed transaction changes (on Commit message)
    /// - `.flush` — mid-transaction early flush (max_transaction_changes exceeded)
    /// All strings in returned Change records are owned (heap-allocated).
    pub fn decode(self: *Decoder, data: []const u8, lsn: u64) !DecodeResult {
        const msg = try parsePgOutputMessage(self.allocator, data);

        switch (msg) {
            .begin => |begin| {
                self.in_transaction = true;
                self.current_xid = begin.xid;
                self.current_commit_timestamp = begin.commit_timestamp;
                self.freeChanges();
                // Clear any leftover context from a previous transaction
                if (self.transaction_context) |ctx| {
                    self.allocator.free(ctx);
                    self.transaction_context = null;
                }
                log.debug("BEGIN xid={d}", .{begin.xid});
            },
            .commit => |commit| {
                self.in_transaction = false;
                self.current_commit_timestamp = commit.commit_timestamp;
                log.debug("COMMIT xid={d} changes={d}", .{ self.current_xid, self.transaction_changes.items.len });

                // Stamp context onto all changes in this transaction.
                // Note: changes that were already flushed early (via max_transaction_changes)
                // do NOT get context stamped — this is a known trade-off of streaming mode.
                if (self.transaction_context) |ctx| {
                    for (self.transaction_changes.items) |*change| {
                        change.context = try self.allocator.dupe(u8, ctx);
                    }
                    self.allocator.free(ctx);
                    self.transaction_context = null;
                }

                if (self.transaction_changes.items.len > 0) {
                    // Return owned slice of changes; caller must free each
                    // Change via change.deinit() then free the slice.
                    const changes = try self.transaction_changes.toOwnedSlice();
                    return .{ .commit = changes };
                }
                return .none;
            },
            .relation => |rel| {
                log.debug("RELATION {s}.{s} id={d} cols={d}", .{
                    rel.namespace,
                    rel.name,
                    rel.relation_id,
                    rel.columns.len,
                });
                // RelationCache.put dupes all strings and frees the parsed columns slice
                try self.relation_cache.put(rel);
            },
            .insert => |ins| {
                const rel = self.relation_cache.get(ins.relation_id) orelse {
                    log.warn("INSERT for unknown relation_id={d}", .{ins.relation_id});
                    self.allocator.free(ins.new_tuple.columns);
                    return .none;
                };

                const lsn_str = try self.dupeFormatLsn(lsn);
                errdefer self.allocator.free(lsn_str);

                const after = try self.buildOwnedNamedValues(rel.columns, ins.new_tuple.columns);
                const pk = try self.dupeExtractPrimaryKey(rel.columns, ins.new_tuple.columns);

                // Free the parsed (non-owned) tuple columns slice
                self.allocator.free(ins.new_tuple.columns);

                try self.transaction_changes.append(.{
                    .primary_key = pk,
                    .before = null,
                    .after = after,
                    .database = self.database,
                    .schema = rel.namespace, // owned by cache
                    .table = rel.name, // owned by cache
                    .operation = .CREATE,
                    .committed_at = self.current_commit_timestamp,
                    .transaction_id = self.current_xid,
                    .position = lsn_str,
                });
            },
            .update => |upd| {
                const rel = self.relation_cache.get(upd.relation_id) orelse {
                    log.warn("UPDATE for unknown relation_id={d}", .{upd.relation_id});
                    if (upd.old_tuple) |old| self.allocator.free(old.columns);
                    self.allocator.free(upd.new_tuple.columns);
                    return .none;
                };

                const lsn_str = try self.dupeFormatLsn(lsn);
                errdefer self.allocator.free(lsn_str);

                const before = if (upd.old_tuple) |old|
                    try self.buildOwnedNamedValues(rel.columns, old.columns)
                else
                    null;
                const after = try self.buildOwnedNamedValues(rel.columns, upd.new_tuple.columns);
                const pk = try self.dupeExtractPrimaryKey(rel.columns, upd.new_tuple.columns);

                // Free parsed tuple columns
                if (upd.old_tuple) |old| self.allocator.free(old.columns);
                self.allocator.free(upd.new_tuple.columns);

                try self.transaction_changes.append(.{
                    .primary_key = pk,
                    .before = before,
                    .after = after,
                    .database = self.database,
                    .schema = rel.namespace,
                    .table = rel.name,
                    .operation = .UPDATE,
                    .committed_at = self.current_commit_timestamp,
                    .transaction_id = self.current_xid,
                    .position = lsn_str,
                });
            },
            .delete => |del| {
                const rel = self.relation_cache.get(del.relation_id) orelse {
                    log.warn("DELETE for unknown relation_id={d}", .{del.relation_id});
                    self.allocator.free(del.old_tuple.columns);
                    return .none;
                };

                const lsn_str = try self.dupeFormatLsn(lsn);
                errdefer self.allocator.free(lsn_str);

                const before = try self.buildOwnedNamedValues(rel.columns, del.old_tuple.columns);
                const pk = try self.dupeExtractPrimaryKey(rel.columns, del.old_tuple.columns);

                self.allocator.free(del.old_tuple.columns);

                try self.transaction_changes.append(.{
                    .primary_key = pk,
                    .before = before,
                    .after = null,
                    .database = self.database,
                    .schema = rel.namespace,
                    .table = rel.name,
                    .operation = .DELETE,
                    .committed_at = self.current_commit_timestamp,
                    .transaction_id = self.current_xid,
                    .position = lsn_str,
                });
            },
            .truncate => |trunc| {
                for (trunc.relation_ids) |rel_id| {
                    const rel = self.relation_cache.get(rel_id) orelse {
                        log.warn("TRUNCATE for unknown relation_id={d}", .{rel_id});
                        continue;
                    };

                    const lsn_str = try self.dupeFormatLsn(lsn);

                    try self.transaction_changes.append(.{
                        .primary_key = try self.allocator.dupe(u8, ""),
                        .before = null,
                        .after = null,
                        .database = self.database,
                        .schema = rel.namespace,
                        .table = rel.name,
                        .operation = .TRUNCATE,
                        .committed_at = self.current_commit_timestamp,
                        .transaction_id = self.current_xid,
                        .position = lsn_str,
                    });
                }
                self.allocator.free(trunc.relation_ids);
            },
            .origin => |orig| {
                log.debug("ORIGIN: lsn={d} name={s}", .{ orig.commit_lsn, orig.name });
            },
            .pg_type => |typ| {
                log.debug("TYPE: id={d} {s}.{s}", .{ typ.type_id, typ.namespace, typ.name });
            },
            .message => |msg_data| {
                const is_transactional = (msg_data.flags & 1) != 0;
                log.debug("MESSAGE: prefix={s} len={d} transactional={}", .{
                    msg_data.prefix,
                    msg_data.content.len,
                    is_transactional,
                });

                // Detect _bemi-prefixed transactional messages for context stitching
                if (is_transactional and std.mem.startsWith(u8, msg_data.prefix, "_bemi")) {
                    // Free any previously stored context in this transaction
                    if (self.transaction_context) |old_ctx| {
                        self.allocator.free(old_ctx);
                    }
                    self.transaction_context = try self.allocator.dupe(u8, msg_data.content);
                    log.debug("captured _bemi context: {d} bytes", .{msg_data.content.len});
                }
            },
            .unknown => |t| {
                log.warn("unknown pgoutput message type: 0x{x}", .{t});
            },
        }

        // Check if we need to flush early due to max_transaction_changes
        if (self.in_transaction) {
            if (self.max_transaction_changes) |limit| {
                if (self.transaction_changes.items.len >= limit) {
                    log.info("flushing {d} changes mid-transaction (xid={d}, limit={d})", .{
                        self.transaction_changes.items.len,
                        self.current_xid,
                        limit,
                    });
                    const changes = try self.transaction_changes.toOwnedSlice();
                    return .{ .flush = changes };
                }
            }
        }
        return .none;
    }

    // ========================================================================
    // Helpers that produce owned (duped) strings
    // ========================================================================

    /// Format an LSN and return an owned copy.
    fn dupeFormatLsn(self: *Decoder, lsn: u64) ![]const u8 {
        var buf: [32]u8 = undefined;
        const formatted = protocol.formatLsn(&buf, lsn);
        return try self.allocator.dupe(u8, formatted);
    }

    /// Extract primary key text and return an owned copy.
    fn dupeExtractPrimaryKey(self: *Decoder, col_defs: []const ColumnDef, col_values: []const ColumnValue) ![]const u8 {
        const pk = extractPrimaryKey(col_defs, col_values);
        return try self.allocator.dupe(u8, pk);
    }

    /// Build named values with owned (duped) text values.
    fn buildOwnedNamedValues(self: *Decoder, col_defs: []const ColumnDef, col_values: []const ColumnValue) ![]NamedValue {
        const count = @min(col_defs.len, col_values.len);
        var result = try self.allocator.alloc(NamedValue, count);
        var initialized: usize = 0;
        errdefer {
            // On error, free any already-duped text values
            for (result[0..initialized]) |nv| {
                switch (nv.value) {
                    .text => |t| self.allocator.free(t),
                    else => {},
                }
            }
            self.allocator.free(result);
        }

        for (0..count) |i| {
            const duped_value: ColumnValue = switch (col_values[i]) {
                .text => |t| .{ .text = try self.allocator.dupe(u8, t) },
                .null_value => .null_value,
                .unchanged => .unchanged,
            };
            result[i] = .{
                .name = col_defs[i].name, // owned by RelationCache
                .value = duped_value,
            };
            initialized = i + 1;
        }
        return result;
    }
};

// ============================================================================
// Helper functions (non-owning, for parsing layer)
// ============================================================================

/// Extract the primary key value from tuple data (non-owning).
/// Uses the first column marked as part of the replica identity key.
/// Returns the text value or "" if not found.
fn extractPrimaryKey(col_defs: []const ColumnDef, col_values: []const ColumnValue) []const u8 {
    for (col_defs, 0..) |col, i| {
        if (col.isKey() and i < col_values.len) {
            switch (col_values[i]) {
                .text => |t| return t,
                else => {},
            }
        }
    }
    // Fallback: use first column if it has a text value
    if (col_values.len > 0) {
        switch (col_values[0]) {
            .text => |t| return t,
            else => {},
        }
    }
    return "";
}

// ============================================================================
// Tests
// ============================================================================

fn buildTestRelationMsg(allocator: std.mem.Allocator) ![]u8 {
    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('R'); // message type
    // relation_id = 16384
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x40, 0x00 });
    try w.writeString("public"); // namespace
    try w.writeString("users"); // table name
    try w.writeByte('d'); // replica identity = default
    // 3 columns
    try w.writeBytes(&[_]u8{ 0x00, 0x03 });

    // Column 1: id (key)
    try w.writeByte(1); // flags = key
    try w.writeString("id");
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x17 }); // type_oid = 23 (int4)
    try w.writeBytes(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF }); // type_modifier = -1

    // Column 2: name (not key)
    try w.writeByte(0); // flags
    try w.writeString("name");
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x19 }); // type_oid = 25 (text)
    try w.writeBytes(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF }); // type_modifier = -1

    // Column 3: email (not key)
    try w.writeByte(0); // flags
    try w.writeString("email");
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x19 }); // type_oid = 25 (text)
    try w.writeBytes(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF }); // type_modifier = -1

    return try w.toOwnedSlice();
}

fn buildTestInsertMsg(allocator: std.mem.Allocator, rel_id: u32) ![]u8 {
    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('I'); // message type
    var rid_buf: [4]u8 = undefined;
    mem.writeInt(u32, &rid_buf, rel_id, .big);
    try w.writeBytes(&rid_buf);
    try w.writeByte('N'); // new tuple marker

    // Tuple: 3 columns
    try w.writeBytes(&[_]u8{ 0x00, 0x03 });
    // col 1: text "42"
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x02 }); // len=2
    try w.writeBytes("42");
    // col 2: text "Alice"
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x05 }); // len=5
    try w.writeBytes("Alice");
    // col 3: null
    try w.writeByte('n');

    return try w.toOwnedSlice();
}

fn buildTestTruncateMsg(allocator: std.mem.Allocator, rel_ids: []const u32, option_bits: u8) ![]u8 {
    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('T'); // message type
    // num_relations
    var num_buf: [4]u8 = undefined;
    mem.writeInt(u32, &num_buf, @intCast(rel_ids.len), .big);
    try w.writeBytes(&num_buf);
    // option_bits
    try w.writeByte(option_bits);
    // relation IDs
    for (rel_ids) |rid| {
        var rid_buf: [4]u8 = undefined;
        mem.writeInt(u32, &rid_buf, rid, .big);
        try w.writeBytes(&rid_buf);
    }

    return try w.toOwnedSlice();
}

test "parse Begin message" {
    const data = blk: {
        var buf: [21]u8 = undefined;
        buf[0] = 'B';
        mem.writeInt(u64, buf[1..9], 0x0000000016B3748, .big); // final_lsn
        mem.writeInt(i64, buf[9..17], 123456789, .big); // commit_timestamp
        mem.writeInt(u32, buf[17..21], 42, .big); // xid
        break :blk buf;
    };
    const msg = try parsePgOutputMessage(std.testing.allocator, &data);
    switch (msg) {
        .begin => |begin| {
            try std.testing.expectEqual(@as(u64, 0x0000000016B3748), begin.final_lsn);
            try std.testing.expectEqual(@as(i64, 123456789), begin.commit_timestamp);
            try std.testing.expectEqual(@as(u32, 42), begin.xid);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Commit message" {
    const data = blk: {
        var buf: [26]u8 = undefined;
        buf[0] = 'C';
        buf[1] = 0; // flags
        mem.writeInt(u64, buf[2..10], 100, .big); // commit_lsn
        mem.writeInt(u64, buf[10..18], 200, .big); // end_lsn
        mem.writeInt(i64, buf[18..26], 300, .big); // commit_timestamp
        break :blk buf;
    };
    const msg = try parsePgOutputMessage(std.testing.allocator, &data);
    switch (msg) {
        .commit => |commit| {
            try std.testing.expectEqual(@as(u8, 0), commit.flags);
            try std.testing.expectEqual(@as(u64, 100), commit.commit_lsn);
            try std.testing.expectEqual(@as(u64, 200), commit.end_lsn);
            try std.testing.expectEqual(@as(i64, 300), commit.commit_timestamp);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Relation message" {
    const allocator = std.testing.allocator;
    const data = try buildTestRelationMsg(allocator);
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .relation => |rel| {
            try std.testing.expectEqual(@as(u32, 16384), rel.relation_id);
            try std.testing.expectEqualStrings("public", rel.namespace);
            try std.testing.expectEqualStrings("users", rel.name);
            try std.testing.expectEqual(@as(u8, 'd'), rel.replica_identity);
            try std.testing.expectEqual(@as(usize, 3), rel.columns.len);

            try std.testing.expectEqualStrings("id", rel.columns[0].name);
            try std.testing.expect(rel.columns[0].isKey());
            try std.testing.expectEqual(@as(u32, 23), rel.columns[0].type_oid);

            try std.testing.expectEqualStrings("name", rel.columns[1].name);
            try std.testing.expect(!rel.columns[1].isKey());

            try std.testing.expectEqualStrings("email", rel.columns[2].name);

            allocator.free(rel.columns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Insert message" {
    const allocator = std.testing.allocator;
    const data = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .insert => |ins| {
            try std.testing.expectEqual(@as(u32, 16384), ins.relation_id);
            try std.testing.expectEqual(@as(usize, 3), ins.new_tuple.columns.len);

            switch (ins.new_tuple.columns[0]) {
                .text => |t| try std.testing.expectEqualStrings("42", t),
                else => return error.TestUnexpectedResult,
            }
            switch (ins.new_tuple.columns[1]) {
                .text => |t| try std.testing.expectEqualStrings("Alice", t),
                else => return error.TestUnexpectedResult,
            }
            switch (ins.new_tuple.columns[2]) {
                .null_value => {},
                else => return error.TestUnexpectedResult,
            }

            allocator.free(ins.new_tuple.columns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Update message with old tuple" {
    const allocator = std.testing.allocator;

    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('U');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x40, 0x00 });
    try w.writeByte('O'); // old tuple marker (full)

    // Old tuple: 2 columns
    try w.writeBytes(&[_]u8{ 0x00, 0x02 });
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x01 });
    try w.writeBytes("1");
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x03 });
    try w.writeBytes("old");

    try w.writeByte('N'); // new tuple marker

    // New tuple: 2 columns
    try w.writeBytes(&[_]u8{ 0x00, 0x02 });
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x01 });
    try w.writeBytes("1");
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x03 });
    try w.writeBytes("new");

    const data = try w.toOwnedSlice();
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .update => |upd| {
            try std.testing.expectEqual(@as(u32, 16384), upd.relation_id);
            try std.testing.expect(upd.old_tuple != null);

            switch (upd.old_tuple.?.columns[1]) {
                .text => |t| try std.testing.expectEqualStrings("old", t),
                else => return error.TestUnexpectedResult,
            }
            switch (upd.new_tuple.columns[1]) {
                .text => |t| try std.testing.expectEqualStrings("new", t),
                else => return error.TestUnexpectedResult,
            }

            allocator.free(upd.old_tuple.?.columns);
            allocator.free(upd.new_tuple.columns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Delete message" {
    const allocator = std.testing.allocator;

    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('D');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x40, 0x00 });
    try w.writeByte('K'); // key columns

    // Key tuple: 1 column (the key)
    try w.writeBytes(&[_]u8{ 0x00, 0x01 });
    try w.writeByte('t');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x02 });
    try w.writeBytes("42");

    const data = try w.toOwnedSlice();
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .delete => |del| {
            try std.testing.expectEqual(@as(u32, 16384), del.relation_id);
            try std.testing.expectEqual(@as(usize, 1), del.old_tuple.columns.len);
            switch (del.old_tuple.columns[0]) {
                .text => |t| try std.testing.expectEqualStrings("42", t),
                else => return error.TestUnexpectedResult,
            }
            allocator.free(del.old_tuple.columns);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Truncate message" {
    const allocator = std.testing.allocator;

    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('T');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x02 });
    try w.writeByte(0x01); // CASCADE
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x40, 0x00 }); // 16384
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x40, 0x01 }); // 16385

    const data = try w.toOwnedSlice();
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .truncate => |trunc| {
            try std.testing.expectEqual(@as(u8, 0x01), trunc.option_bits);
            try std.testing.expectEqual(@as(usize, 2), trunc.relation_ids.len);
            try std.testing.expectEqual(@as(u32, 16384), trunc.relation_ids[0]);
            try std.testing.expectEqual(@as(u32, 16385), trunc.relation_ids[1]);
            allocator.free(trunc.relation_ids);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Origin message" {
    var w_buf: [64]u8 = undefined;
    var pos: usize = 0;
    w_buf[pos] = 'O';
    pos += 1;
    mem.writeInt(u64, w_buf[pos..][0..8], 999, .big);
    pos += 8;
    const origin_name = "my_origin";
    @memcpy(w_buf[pos .. pos + origin_name.len], origin_name);
    pos += origin_name.len;
    w_buf[pos] = 0;
    pos += 1;

    const msg = try parsePgOutputMessage(std.testing.allocator, w_buf[0..pos]);
    switch (msg) {
        .origin => |orig| {
            try std.testing.expectEqual(@as(u64, 999), orig.commit_lsn);
            try std.testing.expectEqualStrings("my_origin", orig.name);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Type message" {
    const allocator = std.testing.allocator;
    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('Y');
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x64 }); // type_id = 100
    try w.writeString("public");
    try w.writeString("my_enum");

    const data = try w.toOwnedSlice();
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .pg_type => |typ| {
            try std.testing.expectEqual(@as(u32, 100), typ.type_id);
            try std.testing.expectEqualStrings("public", typ.namespace);
            try std.testing.expectEqualStrings("my_enum", typ.name);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse Logical Decoding Message" {
    const allocator = std.testing.allocator;
    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('M');
    try w.writeByte(1); // flags = transactional
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00 }); // lsn = 256
    try w.writeString("bemi");
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x0B }); // content length = 11
    try w.writeBytes("hello world");

    const data = try w.toOwnedSlice();
    defer allocator.free(data);

    const msg = try parsePgOutputMessage(allocator, data);
    switch (msg) {
        .message => |m| {
            try std.testing.expectEqual(@as(u8, 1), m.flags);
            try std.testing.expectEqual(@as(u64, 256), m.lsn);
            try std.testing.expectEqualStrings("bemi", m.prefix);
            try std.testing.expectEqualStrings("hello world", m.content);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "RelationCache put and get with owned strings" {
    const allocator = std.testing.allocator;
    var cache = RelationCache.init(allocator);
    defer cache.deinit();

    // Simulate parsed columns (non-owned slices from a buffer)
    var columns = try allocator.alloc(ColumnDef, 1);
    columns[0] = .{ .flags = 1, .name = "id", .type_oid = 23, .type_modifier = -1 };

    // put() will dupe all strings and free the input columns slice
    try cache.put(.{
        .relation_id = 100,
        .namespace = "public",
        .name = "test",
        .replica_identity = 'd',
        .columns = columns,
    });

    const rel = cache.get(100);
    try std.testing.expect(rel != null);
    try std.testing.expectEqualStrings("test", rel.?.name);
    try std.testing.expectEqualStrings("public", rel.?.namespace);
    try std.testing.expectEqual(@as(usize, 1), rel.?.columns.len);
    try std.testing.expectEqualStrings("id", rel.?.columns[0].name);

    // Non-existent
    try std.testing.expect(cache.get(999) == null);
}

test "Decoder full transaction: Begin, Relation, Insert, Commit" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    // 1. Begin
    const begin_data = blk: {
        var buf: [21]u8 = undefined;
        buf[0] = 'B';
        mem.writeInt(u64, buf[1..9], 1000, .big);
        mem.writeInt(i64, buf[9..17], 500000, .big);
        mem.writeInt(u32, buf[17..21], 7, .big);
        break :blk buf;
    };
    const begin_result = try dec.decode(&begin_data, 900);
    try std.testing.expect(begin_result == .none);
    try std.testing.expect(dec.in_transaction);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    const rel_result = try dec.decode(rel_data, 950);
    try std.testing.expect(rel_result == .none);

    // 3. Insert
    const ins_data = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins_data);
    const ins_result = try dec.decode(ins_data, 960);
    try std.testing.expect(ins_result == .none);

    // 4. Commit
    const commit_data = blk: {
        var buf: [26]u8 = undefined;
        buf[0] = 'C';
        buf[1] = 0;
        mem.writeInt(u64, buf[2..10], 1000, .big);
        mem.writeInt(u64, buf[10..18], 1100, .big);
        mem.writeInt(i64, buf[18..26], 500000, .big);
        break :blk buf;
    };
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), changes.len);

    const change = changes[0];
    try std.testing.expectEqual(Operation.CREATE, change.operation);
    try std.testing.expectEqualStrings("testdb", change.database);
    try std.testing.expectEqualStrings("public", change.schema);
    try std.testing.expectEqualStrings("users", change.table);
    try std.testing.expectEqualStrings("42", change.primary_key);
    try std.testing.expect(change.before == null);
    try std.testing.expect(change.after != null);
    try std.testing.expectEqual(@as(usize, 3), change.after.?.len);
    try std.testing.expectEqualStrings("id", change.after.?[0].name);
    try std.testing.expectEqual(@as(u32, 7), change.transaction_id);
    try std.testing.expect(!dec.in_transaction);
    try std.testing.expect(change.context == null); // no _bemi message → null context

    // Free the returned changes (owned strings)
    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "extractPrimaryKey uses key column" {
    const col_defs = [_]ColumnDef{
        .{ .flags = 0, .name = "name", .type_oid = 25, .type_modifier = -1 },
        .{ .flags = 1, .name = "id", .type_oid = 23, .type_modifier = -1 },
    };
    const col_values = [_]ColumnValue{
        .{ .text = "Alice" },
        .{ .text = "99" },
    };
    const pk = extractPrimaryKey(&col_defs, &col_values);
    try std.testing.expectEqualStrings("99", pk);
}

test "extractPrimaryKey falls back to first column" {
    const col_defs = [_]ColumnDef{
        .{ .flags = 0, .name = "name", .type_oid = 25, .type_modifier = -1 },
        .{ .flags = 0, .name = "email", .type_oid = 25, .type_modifier = -1 },
    };
    const col_values = [_]ColumnValue{
        .{ .text = "Alice" },
        .{ .text = "alice@example.com" },
    };
    const pk = extractPrimaryKey(&col_defs, &col_values);
    try std.testing.expectEqualStrings("Alice", pk);
}

fn buildTestBemiMessage(allocator: std.mem.Allocator, prefix: []const u8, content: []const u8, transactional: bool) ![]u8 {
    var w = protocol.MessageWriter.init(allocator);
    defer w.deinit();

    try w.writeByte('M'); // Logical Decoding Message
    try w.writeByte(if (transactional) 1 else 0); // flags
    try w.writeBytes(&[_]u8{ 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00 }); // lsn = 256
    try w.writeString(prefix);
    var len_buf: [4]u8 = undefined;
    mem.writeInt(u32, &len_buf, @intCast(content.len), .big);
    try w.writeBytes(&len_buf);
    try w.writeBytes(content);

    return try w.toOwnedSlice();
}

fn buildTestBeginData(xid: u32) [21]u8 {
    var buf: [21]u8 = undefined;
    buf[0] = 'B';
    mem.writeInt(u64, buf[1..9], 1000, .big);
    mem.writeInt(i64, buf[9..17], 500000, .big);
    mem.writeInt(u32, buf[17..21], xid, .big);
    return buf;
}

fn buildTestCommitData() [26]u8 {
    var buf: [26]u8 = undefined;
    buf[0] = 'C';
    buf[1] = 0;
    mem.writeInt(u64, buf[2..10], 1000, .big);
    mem.writeInt(u64, buf[10..18], 1100, .big);
    mem.writeInt(i64, buf[18..26], 500000, .big);
    return buf;
}

test "context stitching: _bemi message stamps context on changes" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(10);
    _ = try dec.decode(&begin_data, 900);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // 3. _bemi context message (transactional)
    const context_json = "{\"user_id\": \"123\", \"endpoint\": \"/api/users\"}";
    const bemi_msg = try buildTestBemiMessage(allocator, "_bemi", context_json, true);
    defer allocator.free(bemi_msg);
    _ = try dec.decode(bemi_msg, 920);

    // 4. Insert
    const ins_data = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins_data);
    _ = try dec.decode(ins_data, 930);

    // 5. Commit
    const commit_data = buildTestCommitData();
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), changes.len);

    // Verify context was stamped
    const change = changes[0];
    try std.testing.expect(change.context != null);
    try std.testing.expectEqualStrings(context_json, change.context.?);
    try std.testing.expectEqual(Operation.CREATE, change.operation);

    // Free
    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "context stitching: non-transactional _bemi message is ignored" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(11);
    _ = try dec.decode(&begin_data, 900);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // 3. _bemi context message but NOT transactional
    const bemi_msg = try buildTestBemiMessage(allocator, "_bemi", "{\"user_id\": \"999\"}", false);
    defer allocator.free(bemi_msg);
    _ = try dec.decode(bemi_msg, 920);

    // 4. Insert
    const ins_data = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins_data);
    _ = try dec.decode(ins_data, 930);

    // 5. Commit
    const commit_data = buildTestCommitData();
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };

    // Context should be null — non-transactional message ignored
    try std.testing.expect(changes[0].context == null);

    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "context stitching: non-_bemi prefix is ignored" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    const begin_data = buildTestBeginData(12);
    _ = try dec.decode(&begin_data, 900);

    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // Transactional message but with different prefix
    const msg = try buildTestBemiMessage(allocator, "other_prefix", "{\"foo\": 1}", true);
    defer allocator.free(msg);
    _ = try dec.decode(msg, 920);

    const ins_data = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins_data);
    _ = try dec.decode(ins_data, 930);

    const commit_data = buildTestCommitData();
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(changes[0].context == null);

    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "context stitching: multiple changes in same transaction share context" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    const begin_data = buildTestBeginData(13);
    _ = try dec.decode(&begin_data, 900);

    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    const context_json = "{\"user_id\": \"42\"}";
    const bemi_msg = try buildTestBemiMessage(allocator, "_bemi", context_json, true);
    defer allocator.free(bemi_msg);
    _ = try dec.decode(bemi_msg, 920);

    // Two inserts in same transaction
    const ins1 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins1);
    _ = try dec.decode(ins1, 930);

    const ins2 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins2);
    _ = try dec.decode(ins2, 940);

    const commit_data = buildTestCommitData();
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), changes.len);

    // Both changes should have the same context
    try std.testing.expect(changes[0].context != null);
    try std.testing.expectEqualStrings(context_json, changes[0].context.?);
    try std.testing.expect(changes[1].context != null);
    try std.testing.expectEqualStrings(context_json, changes[1].context.?);

    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "Decoder full transaction: Begin, Relation, Truncate, Commit" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(20);
    const begin_result = try dec.decode(&begin_data, 900);
    try std.testing.expect(begin_result == .none);
    try std.testing.expect(dec.in_transaction);

    // 2. Relation (relation_id = 16384, "public.users")
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    const rel_result = try dec.decode(rel_data, 950);
    try std.testing.expect(rel_result == .none);

    // 3. Truncate (single table)
    const trunc_data = try buildTestTruncateMsg(allocator, &[_]u32{16384}, 0);
    defer allocator.free(trunc_data);
    const trunc_result = try dec.decode(trunc_data, 960);
    try std.testing.expect(trunc_result == .none);

    // 4. Commit
    const commit_data = buildTestCommitData();
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), changes.len);

    const change = changes[0];
    try std.testing.expectEqual(Operation.TRUNCATE, change.operation);
    try std.testing.expectEqualStrings("testdb", change.database);
    try std.testing.expectEqualStrings("public", change.schema);
    try std.testing.expectEqualStrings("users", change.table);
    try std.testing.expectEqualStrings("", change.primary_key);
    try std.testing.expect(change.before == null);
    try std.testing.expect(change.after == null);
    try std.testing.expectEqual(@as(u32, 20), change.transaction_id);
    try std.testing.expect(!dec.in_transaction);
    try std.testing.expect(change.context == null);

    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "Decoder TRUNCATE with context stitching" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(21);
    _ = try dec.decode(&begin_data, 900);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // 3. _bemi context message
    const context_json = "{\"user_id\": \"456\", \"reason\": \"cleanup\"}";
    const bemi_msg = try buildTestBemiMessage(allocator, "_bemi", context_json, true);
    defer allocator.free(bemi_msg);
    _ = try dec.decode(bemi_msg, 920);

    // 4. Truncate
    const trunc_data = try buildTestTruncateMsg(allocator, &[_]u32{16384}, 0);
    defer allocator.free(trunc_data);
    _ = try dec.decode(trunc_data, 930);

    // 5. Commit
    const commit_data = buildTestCommitData();
    const result = try dec.decode(&commit_data, 1000);
    const changes = switch (result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), changes.len);

    const change = changes[0];
    try std.testing.expectEqual(Operation.TRUNCATE, change.operation);
    try std.testing.expect(change.context != null);
    try std.testing.expectEqualStrings(context_json, change.context.?);

    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

test "Decoder mid-transaction flush when max_transaction_changes exceeded" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    dec.max_transaction_changes = 2; // flush after every 2 changes
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(30);
    const begin_result = try dec.decode(&begin_data, 900);
    try std.testing.expect(begin_result == .none);
    try std.testing.expect(dec.in_transaction);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // 3. First insert — should not flush yet (1 < 2)
    const ins1 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins1);
    const result1 = try dec.decode(ins1, 920);
    try std.testing.expect(result1 == .none);

    // 4. Second insert — should trigger flush (2 >= 2)
    const ins2 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins2);
    const result2 = try dec.decode(ins2, 930);
    const flushed = switch (result2) {
        .flush => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), flushed.len);
    // Still in transaction after flush
    try std.testing.expect(dec.in_transaction);

    for (flushed) |*c| {
        c.deinit(allocator);
    }
    allocator.free(flushed);

    // 5. Third insert — accumulates again (1 < 2)
    const ins3 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins3);
    const result3 = try dec.decode(ins3, 940);
    try std.testing.expect(result3 == .none);

    // 6. Commit — returns the remaining 1 change
    const commit_data = buildTestCommitData();
    const commit_result = try dec.decode(&commit_data, 1000);
    const remaining = switch (commit_result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), remaining.len);
    try std.testing.expect(!dec.in_transaction);

    for (remaining) |*c| {
        c.deinit(allocator);
    }
    allocator.free(remaining);
}

test "Decoder mid-transaction flush: context only stamped on unflushed changes" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    dec.max_transaction_changes = 1; // flush after every change
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(31);
    _ = try dec.decode(&begin_data, 900);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // 3. _bemi context message
    const context_json = "{\"user_id\": \"flush_test\"}";
    const bemi_msg = try buildTestBemiMessage(allocator, "_bemi", context_json, true);
    defer allocator.free(bemi_msg);
    _ = try dec.decode(bemi_msg, 920);

    // 4. First insert — triggers flush (1 >= 1). Flushed changes do NOT get context.
    const ins1 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins1);
    const result1 = try dec.decode(ins1, 930);
    const flushed = switch (result1) {
        .flush => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), flushed.len);
    // Flushed changes should NOT have context (known trade-off)
    try std.testing.expect(flushed[0].context == null);

    for (flushed) |*c| {
        c.deinit(allocator);
    }
    allocator.free(flushed);

    // 5. Second insert — triggers another flush
    const ins2 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins2);
    const result2 = try dec.decode(ins2, 940);
    const flushed2 = switch (result2) {
        .flush => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(flushed2[0].context == null);

    for (flushed2) |*c| {
        c.deinit(allocator);
    }
    allocator.free(flushed2);

    // 6. Third insert — will also be flushed
    const ins3 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins3);
    const result3 = try dec.decode(ins3, 950);
    const flushed3 = switch (result3) {
        .flush => |c| c,
        else => return error.TestUnexpectedResult,
    };
    // This one also gets flushed without context
    try std.testing.expect(flushed3[0].context == null);
    for (flushed3) |*c| {
        c.deinit(allocator);
    }
    allocator.free(flushed3);

    // 7. Commit with no remaining changes
    const commit_data = buildTestCommitData();
    const commit_result = try dec.decode(&commit_data, 1000);
    // No remaining changes — all were flushed
    try std.testing.expect(commit_result == .none);
}

test "Decoder flush with limit=0 flushes every DML immediately" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    dec.max_transaction_changes = 0; // flush after every DML
    defer dec.deinit();

    // 1. Begin
    const begin_data = buildTestBeginData(40);
    _ = try dec.decode(&begin_data, 900);

    // 2. Relation
    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // 3. Single insert — should trigger flush immediately (1 >= 0)
    const ins1 = try buildTestInsertMsg(allocator, 16384);
    defer allocator.free(ins1);
    const result1 = try dec.decode(ins1, 920);
    const flushed = switch (result1) {
        .flush => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 1), flushed.len);
    try std.testing.expect(dec.in_transaction);

    for (flushed) |*c| {
        c.deinit(allocator);
    }
    allocator.free(flushed);

    // 4. Commit — no remaining changes
    const commit_data = buildTestCommitData();
    const commit_result = try dec.decode(&commit_data, 1000);
    try std.testing.expect(commit_result == .none);
}

test "Decoder without max_transaction_changes never flushes mid-transaction" {
    const allocator = std.testing.allocator;
    var dec = Decoder.init(allocator, "testdb");
    // max_transaction_changes defaults to null — no flushing
    defer dec.deinit();

    const begin_data = buildTestBeginData(41);
    _ = try dec.decode(&begin_data, 900);

    const rel_data = try buildTestRelationMsg(allocator);
    defer allocator.free(rel_data);
    _ = try dec.decode(rel_data, 910);

    // Insert 5 changes — none should trigger flush
    var i: u64 = 0;
    var ins_bufs: [5][]u8 = undefined;
    while (i < 5) : (i += 1) {
        ins_bufs[i] = try buildTestInsertMsg(allocator, 16384);
        const result = try dec.decode(ins_bufs[i], 920 + i * 10);
        try std.testing.expect(result == .none);
    }
    defer for (&ins_bufs) |buf| {
        allocator.free(buf);
    };

    // Commit returns all 5 changes at once
    const commit_data = buildTestCommitData();
    const commit_result = try dec.decode(&commit_data, 1000);
    const changes = switch (commit_result) {
        .commit => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 5), changes.len);

    for (changes) |*c| {
        c.deinit(allocator);
    }
    allocator.free(changes);
}

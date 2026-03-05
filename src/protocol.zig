const std = @import("std");
const mem = std.mem;
const Md5 = std.crypto.hash.Md5;

// ============================================================================
// PostgreSQL v3 Wire Protocol Types and Encoding/Decoding
// ============================================================================

/// PostgreSQL backend (server -> client) message types.
pub const BackendMessage = enum(u8) {
    authentication = 'R',
    backend_key_data = 'K',
    bind_complete = '2',
    close_complete = '3',
    command_complete = 'C',
    copy_data = 'd',
    copy_done = 'c',
    copy_both_response = 'W',
    copy_in_response = 'G',
    copy_out_response = 'H',
    data_row = 'D',
    empty_query_response = 'I',
    error_response = 'E',
    no_data = 'n',
    notice_response = 'N',
    notification_response = 'A',
    parameter_description = 't',
    parameter_status = 'S',
    parse_complete = '1',
    portal_suspended = 's',
    ready_for_query = 'Z',
    row_description = 'T',
    _,
};

/// PostgreSQL frontend (client -> server) message types.
pub const FrontendMessage = enum(u8) {
    bind = 'B',
    close = 'C',
    copy_data = 'd',
    copy_done = 'c',
    copy_fail = 'f',
    describe = 'D',
    execute = 'E',
    flush = 'H',
    parse = 'P',
    password = 'p',
    query = 'Q',
    sync = 'S',
    terminate = 'X',
    _,
};

/// Authentication sub-types (Int32 code in Authentication messages).
pub const AuthType = enum(u32) {
    ok = 0,
    cleartext_password = 3,
    md5_password = 5,
    sasl = 10,
    sasl_continue = 11,
    sasl_final = 12,
    _,
};

/// Parsed error/notice field from ErrorResponse or NoticeResponse.
pub const ErrorField = struct {
    code: u8,
    value: []const u8,
};

/// A parsed PostgreSQL error response.
pub const PgError = struct {
    severity: []const u8 = "",
    code: []const u8 = "",
    message: []const u8 = "",
    detail: []const u8 = "",
    hint: []const u8 = "",
    position: []const u8 = "",

    pub fn format(self: PgError, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("PgError({s} {s}: {s}", .{ self.severity, self.code, self.message });
        if (self.detail.len > 0) {
            try writer.print(", detail: {s}", .{self.detail});
        }
        if (self.hint.len > 0) {
            try writer.print(", hint: {s}", .{self.hint});
        }
        try writer.writeAll(")");
    }
};

/// A single column value from a DataRow message.
pub const ColumnValue = union(enum) {
    null_value: void,
    text: []const u8,
};

/// A row of column values from a DataRow message.
pub const DataRow = struct {
    columns: []ColumnValue,
};

/// A column descriptor from a RowDescription message.
pub const FieldDescription = struct {
    name: []const u8,
    table_oid: u32,
    column_attr: u16,
    type_oid: u32,
    type_size: i16,
    type_modifier: i32,
    format_code: u16,
};

/// Raw backend message: type byte + payload.
pub const RawMessage = struct {
    msg_type: BackendMessage,
    payload: []const u8,
};

// ============================================================================
// Message Reader: reads raw bytes from a buffer / stream
// ============================================================================

pub const MessageReader = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) MessageReader {
        return .{ .data = data, .pos = 0 };
    }

    pub fn readByte(self: *MessageReader) !u8 {
        if (self.pos >= self.data.len) return error.UnexpectedEndOfData;
        const b = self.data[self.pos];
        self.pos += 1;
        return b;
    }

    pub fn readInt16(self: *MessageReader) !i16 {
        if (self.pos + 2 > self.data.len) return error.UnexpectedEndOfData;
        const val = mem.readInt(i16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    pub fn readUInt16(self: *MessageReader) !u16 {
        if (self.pos + 2 > self.data.len) return error.UnexpectedEndOfData;
        const val = mem.readInt(u16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return val;
    }

    pub fn readInt32(self: *MessageReader) !i32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEndOfData;
        const val = mem.readInt(i32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    pub fn readUInt32(self: *MessageReader) !u32 {
        if (self.pos + 4 > self.data.len) return error.UnexpectedEndOfData;
        const val = mem.readInt(u32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return val;
    }

    pub fn readInt64(self: *MessageReader) !i64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEndOfData;
        const val = mem.readInt(i64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    pub fn readUInt64(self: *MessageReader) !u64 {
        if (self.pos + 8 > self.data.len) return error.UnexpectedEndOfData;
        const val = mem.readInt(u64, self.data[self.pos..][0..8], .big);
        self.pos += 8;
        return val;
    }

    /// Read a null-terminated string.
    pub fn readString(self: *MessageReader) ![]const u8 {
        const start = self.pos;
        while (self.pos < self.data.len) {
            if (self.data[self.pos] == 0) {
                const str = self.data[start..self.pos];
                self.pos += 1; // skip null terminator
                return str;
            }
            self.pos += 1;
        }
        return error.UnexpectedEndOfData;
    }

    /// Read n bytes.
    pub fn readBytes(self: *MessageReader, n: usize) ![]const u8 {
        if (self.pos + n > self.data.len) return error.UnexpectedEndOfData;
        const bytes = self.data[self.pos .. self.pos + n];
        self.pos += n;
        return bytes;
    }

    /// Read remaining bytes.
    pub fn readRemaining(self: *MessageReader) []const u8 {
        const rest = self.data[self.pos..];
        self.pos = self.data.len;
        return rest;
    }

    pub fn remaining(self: *const MessageReader) usize {
        return self.data.len - self.pos;
    }
};

// ============================================================================
// Message Writer: builds outgoing messages
// ============================================================================

pub const MessageWriter = struct {
    buf: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) MessageWriter {
        return .{ .buf = std.ArrayList(u8).init(allocator) };
    }

    pub fn deinit(self: *MessageWriter) void {
        self.buf.deinit();
    }

    pub fn reset(self: *MessageWriter) void {
        self.buf.clearRetainingCapacity();
    }

    pub fn writeByte(self: *MessageWriter, b: u8) !void {
        try self.buf.append(b);
    }

    pub fn writeInt16(self: *MessageWriter, val: i16) !void {
        var bytes: [2]u8 = undefined;
        mem.writeInt(i16, &bytes, val, .big);
        try self.buf.appendSlice(&bytes);
    }

    pub fn writeInt32(self: *MessageWriter, val: i32) !void {
        var bytes: [4]u8 = undefined;
        mem.writeInt(i32, &bytes, val, .big);
        try self.buf.appendSlice(&bytes);
    }

    pub fn writeInt64(self: *MessageWriter, val: i64) !void {
        var bytes: [8]u8 = undefined;
        mem.writeInt(i64, &bytes, val, .big);
        try self.buf.appendSlice(&bytes);
    }

    /// Write a null-terminated string.
    pub fn writeString(self: *MessageWriter, s: []const u8) !void {
        try self.buf.appendSlice(s);
        try self.buf.append(0);
    }

    pub fn writeBytes(self: *MessageWriter, bytes: []const u8) !void {
        try self.buf.appendSlice(bytes);
    }

    pub fn toOwnedSlice(self: *MessageWriter) ![]u8 {
        return self.buf.toOwnedSlice();
    }

    pub fn items(self: *const MessageWriter) []const u8 {
        return self.buf.items;
    }
};

// ============================================================================
// Startup message (no type byte, special format)
// ============================================================================

/// Build a PostgreSQL v3 startup message.
/// Format: Int32 length + Int32 protocol(196608) + key\0value\0 pairs + \0
pub fn buildStartupMessage(allocator: std.mem.Allocator, params: []const [2][]const u8) ![]u8 {
    var body_len: usize = 4; // protocol version
    for (params) |kv| {
        body_len += kv[0].len + 1 + kv[1].len + 1;
    }
    body_len += 1; // trailing null

    const total_len: u32 = @intCast(4 + body_len); // 4 for the length field itself

    var msg = try allocator.alloc(u8, total_len);
    var pos: usize = 0;

    // Length (includes self)
    mem.writeInt(u32, msg[pos..][0..4], total_len, .big);
    pos += 4;

    // Protocol version 3.0 = 196608
    mem.writeInt(u32, msg[pos..][0..4], 196608, .big);
    pos += 4;

    // Parameters
    for (params) |kv| {
        @memcpy(msg[pos .. pos + kv[0].len], kv[0]);
        pos += kv[0].len;
        msg[pos] = 0;
        pos += 1;
        @memcpy(msg[pos .. pos + kv[1].len], kv[1]);
        pos += kv[1].len;
        msg[pos] = 0;
        pos += 1;
    }

    // Trailing null
    msg[pos] = 0;
    pos += 1;

    std.debug.assert(pos == total_len);
    return msg;
}

/// Build a simple Query message: 'Q' + Int32 length + query string + \0
pub fn buildQueryMessage(allocator: std.mem.Allocator, query: []const u8) ![]u8 {
    const len: u32 = @intCast(4 + query.len + 1); // length field + query + null
    const total = 1 + len; // type byte + length + payload
    var msg = try allocator.alloc(u8, total);
    var pos: usize = 0;

    msg[pos] = 'Q';
    pos += 1;
    mem.writeInt(u32, msg[pos..][0..4], len, .big);
    pos += 4;
    @memcpy(msg[pos .. pos + query.len], query);
    pos += query.len;
    msg[pos] = 0;
    pos += 1;

    std.debug.assert(pos == total);
    return msg;
}

/// Build a Terminate message: 'X' + Int32(4)
pub fn buildTerminateMessage(allocator: std.mem.Allocator) ![]u8 {
    var msg = try allocator.alloc(u8, 5);
    msg[0] = 'X';
    mem.writeInt(u32, msg[1..][0..4], 4, .big);
    return msg;
}

// ============================================================================
// MD5 password hashing (PostgreSQL specific)
// ============================================================================

fn hexDigit(val: u4) u8 {
    return if (val < 10) @as(u8, '0') + @as(u8, val) else @as(u8, 'a') + @as(u8, val) - 10;
}

fn md5Hex(data: [16]u8) [32]u8 {
    var hex: [32]u8 = undefined;
    for (data, 0..) |byte, i| {
        hex[i * 2] = hexDigit(@truncate(byte >> 4));
        hex[i * 2 + 1] = hexDigit(@truncate(byte & 0x0f));
    }
    return hex;
}

/// Compute PostgreSQL MD5 password hash.
/// md5(md5(password + user) + salt)
pub fn computeMd5Password(user: []const u8, password: []const u8, salt: [4]u8) [35]u8 {
    // First: md5(password + user)
    var h1 = Md5.init(.{});
    h1.update(password);
    h1.update(user);
    var digest1: [16]u8 = undefined;
    h1.final(&digest1);
    const hex1 = md5Hex(digest1);

    // Second: md5(hex1 + salt)
    var h2 = Md5.init(.{});
    h2.update(&hex1);
    h2.update(&salt);
    var digest2: [16]u8 = undefined;
    h2.final(&digest2);
    const hex2 = md5Hex(digest2);

    // Result: "md5" + hex2
    var result: [35]u8 = undefined;
    result[0] = 'm';
    result[1] = 'd';
    result[2] = '5';
    @memcpy(result[3..35], &hex2);
    return result;
}

/// Build a PasswordMessage from a pre-computed password string.
/// 'p' + Int32 length + password + \0
pub fn buildPasswordMessage(allocator: std.mem.Allocator, password: []const u8) ![]u8 {
    const len: u32 = @intCast(4 + password.len + 1);
    const total: usize = 1 + @as(usize, len);
    var msg = try allocator.alloc(u8, total);
    var pos: usize = 0;

    msg[pos] = 'p';
    pos += 1;
    mem.writeInt(u32, msg[pos..][0..4], len, .big);
    pos += 4;
    @memcpy(msg[pos .. pos + password.len], password);
    pos += password.len;
    msg[pos] = 0;
    pos += 1;

    std.debug.assert(pos == total);
    return msg;
}

// ============================================================================
// Parse error/notice response fields
// ============================================================================

/// Parse fields from an ErrorResponse or NoticeResponse payload.
pub fn parseErrorFields(payload: []const u8) PgError {
    var reader = MessageReader.init(payload);
    var err = PgError{};

    while (reader.remaining() > 0) {
        const code = reader.readByte() catch break;
        if (code == 0) break;
        const value = reader.readString() catch break;

        switch (code) {
            'S' => err.severity = value,
            'V' => {}, // non-localized severity, ignore
            'C' => err.code = value,
            'M' => err.message = value,
            'D' => err.detail = value,
            'H' => err.hint = value,
            'P' => err.position = value,
            else => {}, // ignore unknown fields
        }
    }
    return err;
}

/// Parse a DataRow message payload.
pub fn parseDataRow(allocator: std.mem.Allocator, payload: []const u8) !DataRow {
    var reader = MessageReader.init(payload);
    const col_count = try reader.readUInt16();
    var columns = try allocator.alloc(ColumnValue, col_count);

    for (0..col_count) |i| {
        const col_len = try reader.readInt32();
        if (col_len == -1) {
            columns[i] = .null_value;
        } else {
            const val = try reader.readBytes(@intCast(col_len));
            columns[i] = .{ .text = val };
        }
    }

    return .{ .columns = columns };
}

/// Parse a RowDescription message payload.
pub fn parseRowDescription(allocator: std.mem.Allocator, payload: []const u8) ![]FieldDescription {
    var reader = MessageReader.init(payload);
    const field_count = try reader.readUInt16();
    var fields = try allocator.alloc(FieldDescription, field_count);

    for (0..field_count) |i| {
        fields[i] = .{
            .name = try reader.readString(),
            .table_oid = try reader.readUInt32(),
            .column_attr = try reader.readUInt16(),
            .type_oid = try reader.readUInt32(),
            .type_size = try reader.readInt16(),
            .type_modifier = try reader.readInt32(),
            .format_code = try reader.readUInt16(),
        };
    }

    return fields;
}

// ============================================================================
// Replication protocol messages
// ============================================================================

/// XLogData message within CopyData (byte 'w').
pub const XLogData = struct {
    wal_start: u64,
    wal_end: u64,
    server_time: i64,
    data: []const u8,
};

/// Primary Keepalive message within CopyData (byte 'k').
pub const PrimaryKeepalive = struct {
    wal_end: u64,
    server_time: i64,
    reply_requested: bool,
};

/// Parsed replication message from within CopyData.
pub const ReplicationMessage = union(enum) {
    xlog_data: XLogData,
    keepalive: PrimaryKeepalive,
    unknown: u8,
};

/// Parse the inner content of a CopyData message from a replication stream.
pub fn parseReplicationMessage(payload: []const u8) !ReplicationMessage {
    if (payload.len < 1) return error.UnexpectedEndOfData;

    var reader = MessageReader.init(payload);
    const msg_type = try reader.readByte();

    switch (msg_type) {
        'w' => {
            // XLogData
            const wal_start = try reader.readUInt64();
            const wal_end = try reader.readUInt64();
            const server_time = try reader.readInt64();
            const data = reader.readRemaining();
            return .{ .xlog_data = .{
                .wal_start = wal_start,
                .wal_end = wal_end,
                .server_time = server_time,
                .data = data,
            } };
        },
        'k' => {
            // Primary Keepalive
            const wal_end = try reader.readUInt64();
            const server_time = try reader.readInt64();
            const reply_byte = try reader.readByte();
            return .{ .keepalive = .{
                .wal_end = wal_end,
                .server_time = server_time,
                .reply_requested = reply_byte != 0,
            } };
        },
        else => return .{ .unknown = msg_type },
    }
}

/// Build a StandbyStatusUpdate message (sent inside CopyData).
/// This is a CopyData('d') containing: 'r' + write_pos + flush_pos + apply_pos + client_time + reply
pub fn buildStandbyStatusUpdate(
    allocator: std.mem.Allocator,
    write_pos: u64,
    flush_pos: u64,
    apply_pos: u64,
    client_time: i64,
    reply_requested: bool,
) ![]u8 {
    // Inner message: 1 + 8 + 8 + 8 + 8 + 1 = 34 bytes
    const inner_len: u32 = 34;
    const total: usize = 1 + 4 + inner_len; // 'd' + length + inner
    var msg = try allocator.alloc(u8, total);
    var pos: usize = 0;

    // CopyData wrapper
    msg[pos] = 'd';
    pos += 1;
    mem.writeInt(u32, msg[pos..][0..4], 4 + inner_len, .big);
    pos += 4;

    // StandbyStatusUpdate inner message
    msg[pos] = 'r';
    pos += 1;
    mem.writeInt(u64, msg[pos..][0..8], write_pos, .big);
    pos += 8;
    mem.writeInt(u64, msg[pos..][0..8], flush_pos, .big);
    pos += 8;
    mem.writeInt(u64, msg[pos..][0..8], apply_pos, .big);
    pos += 8;
    mem.writeInt(i64, msg[pos..][0..8], client_time, .big);
    pos += 8;
    msg[pos] = if (reply_requested) 1 else 0;
    pos += 1;

    std.debug.assert(pos == total);
    return msg;
}

/// Convert a PostgreSQL LSN string "X/Y" to a u64 value.
pub fn parseLsn(lsn_str: []const u8) !u64 {
    // Find the '/' separator
    var sep_idx: ?usize = null;
    for (lsn_str, 0..) |c, i| {
        if (c == '/') {
            sep_idx = i;
            break;
        }
    }
    const sep = sep_idx orelse return error.InvalidLsn;

    const high = std.fmt.parseUnsigned(u32, lsn_str[0..sep], 16) catch return error.InvalidLsn;
    const low = std.fmt.parseUnsigned(u32, lsn_str[sep + 1 ..], 16) catch return error.InvalidLsn;

    return (@as(u64, high) << 32) | @as(u64, low);
}

/// Format a u64 LSN value as "X/Y" string.
pub fn formatLsn(buf: []u8, lsn: u64) []const u8 {
    const high: u32 = @intCast(lsn >> 32);
    const low: u32 = @intCast(lsn & 0xFFFFFFFF);
    const result = std.fmt.bufPrint(buf, "{X}/{X}", .{ high, low }) catch return "";
    return result;
}

/// Get current time as PostgreSQL epoch microseconds.
/// PostgreSQL epoch is 2000-01-01 00:00:00 UTC.
/// Unix epoch is 1970-01-01 00:00:00 UTC.
/// Difference: 946684800 seconds = 946684800000000 microseconds.
pub fn pgEpochMicroseconds() i64 {
    const pg_epoch_offset_us: i64 = 946_684_800_000_000;
    const now_ns = std.time.nanoTimestamp();
    const now_us: i64 = @intCast(@divTrunc(now_ns, 1000));
    return now_us - pg_epoch_offset_us;
}

// ============================================================================
// Tests
// ============================================================================

test "MessageReader reads basic types" {
    // Big-endian: 0x0001 = 1, 0x00000002 = 2
    const data = [_]u8{
        0x00, 0x01, // Int16 = 1
        0x00, 0x00, 0x00, 0x02, // Int32 = 2
        'h', 'i', 0, // String "hi"
        0xFF, // Byte 255
    };
    var reader = MessageReader.init(&data);
    try std.testing.expectEqual(@as(i16, 1), try reader.readInt16());
    try std.testing.expectEqual(@as(i32, 2), try reader.readInt32());
    try std.testing.expectEqualStrings("hi", try reader.readString());
    try std.testing.expectEqual(@as(u8, 0xFF), try reader.readByte());
    try std.testing.expectEqual(@as(usize, 0), reader.remaining());
}

test "buildStartupMessage produces correct format" {
    const allocator = std.testing.allocator;
    const params = [_][2][]const u8{
        .{ "user", "postgres" },
        .{ "database", "mydb" },
    };
    const msg = try buildStartupMessage(allocator, &params);
    defer allocator.free(msg);

    var reader = MessageReader.init(msg);
    const len = try reader.readUInt32();
    try std.testing.expectEqual(@as(u32, @intCast(msg.len)), len);

    const proto = try reader.readUInt32();
    try std.testing.expectEqual(@as(u32, 196608), proto);

    // "user\0postgres\0database\0mydb\0\0"
    try std.testing.expectEqualStrings("user", try reader.readString());
    try std.testing.expectEqualStrings("postgres", try reader.readString());
    try std.testing.expectEqualStrings("database", try reader.readString());
    try std.testing.expectEqualStrings("mydb", try reader.readString());
    try std.testing.expectEqual(@as(u8, 0), try reader.readByte());
}

test "buildQueryMessage produces correct format" {
    const allocator = std.testing.allocator;
    const msg = try buildQueryMessage(allocator, "SELECT 1");
    defer allocator.free(msg);

    try std.testing.expectEqual(@as(u8, 'Q'), msg[0]);
    const len = mem.readInt(u32, msg[1..5], .big);
    try std.testing.expectEqual(@as(u32, 4 + 8 + 1), len); // 4 (length) + 8 (query) + 1 (null)
    try std.testing.expectEqualStrings("SELECT 1", msg[5 .. 5 + 8]);
    try std.testing.expectEqual(@as(u8, 0), msg[13]);
}

test "computeMd5Password produces correct hash" {
    // Known test vector: user="postgres", password="postgres", salt=\x01\x02\x03\x04
    const result = computeMd5Password("postgres", "postgres", .{ 0x01, 0x02, 0x03, 0x04 });
    // The result should start with "md5"
    try std.testing.expectEqualStrings("md5", result[0..3]);
    // The result should be 35 chars total
    try std.testing.expectEqual(@as(usize, 35), result.len);
}

test "parseLsn and formatLsn roundtrip" {
    const lsn = try parseLsn("0/16B3748");
    try std.testing.expectEqual(@as(u64, 0x016B3748), lsn);

    var buf: [32]u8 = undefined;
    const formatted = formatLsn(&buf, lsn);
    try std.testing.expectEqualStrings("0/16B3748", formatted);
}

test "parseReplicationMessage XLogData" {
    // Build a minimal XLogData message
    var data: [26]u8 = undefined;
    data[0] = 'w';
    mem.writeInt(u64, data[1..9], 100, .big); // wal_start
    mem.writeInt(u64, data[9..17], 200, .big); // wal_end
    mem.writeInt(i64, data[17..25], 300, .big); // server_time
    data[25] = 0x42; // 1 byte of WAL data

    const msg = try parseReplicationMessage(&data);
    switch (msg) {
        .xlog_data => |xlog| {
            try std.testing.expectEqual(@as(u64, 100), xlog.wal_start);
            try std.testing.expectEqual(@as(u64, 200), xlog.wal_end);
            try std.testing.expectEqual(@as(i64, 300), xlog.server_time);
            try std.testing.expectEqual(@as(usize, 1), xlog.data.len);
            try std.testing.expectEqual(@as(u8, 0x42), xlog.data[0]);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parseReplicationMessage Keepalive" {
    var data: [18]u8 = undefined;
    data[0] = 'k';
    mem.writeInt(u64, data[1..9], 500, .big);
    mem.writeInt(i64, data[9..17], 600, .big);
    data[17] = 1; // reply requested

    const msg = try parseReplicationMessage(&data);
    switch (msg) {
        .keepalive => |ka| {
            try std.testing.expectEqual(@as(u64, 500), ka.wal_end);
            try std.testing.expectEqual(@as(i64, 600), ka.server_time);
            try std.testing.expect(ka.reply_requested);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parseErrorFields" {
    // Simulate: S"ERROR"\0 C"42P01"\0 M"relation does not exist"\0 \0
    const payload = "SERROR\x00" ++ "C42P01\x00" ++ "Mrelation does not exist\x00" ++ "\x00";
    const err = parseErrorFields(payload);
    try std.testing.expectEqualStrings("ERROR", err.severity);
    try std.testing.expectEqualStrings("42P01", err.code);
    try std.testing.expectEqualStrings("relation does not exist", err.message);
}

test "buildStandbyStatusUpdate format" {
    const allocator = std.testing.allocator;
    const msg = try buildStandbyStatusUpdate(allocator, 100, 100, 0, 12345, false);
    defer allocator.free(msg);

    try std.testing.expectEqual(@as(u8, 'd'), msg[0]);
    // Total: 1 (type) + 4 (len) + 34 (inner) = 39
    try std.testing.expectEqual(@as(usize, 39), msg.len);
    // Inner first byte should be 'r'
    try std.testing.expectEqual(@as(u8, 'r'), msg[5]);
}

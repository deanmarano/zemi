const std = @import("std");

const log = std.log.scoped(.config);

/// SSL/TLS connection mode, modeled after libpq sslmode values.
pub const SslMode = enum {
    /// No SSL (default for backward compatibility)
    disable,
    /// Encrypt the connection but do not verify the server certificate
    require,
    /// Encrypt and verify the server certificate against the CA bundle
    verify_ca,
    /// Encrypt, verify the CA, and verify that the server hostname matches
    verify_full,

    pub fn fromString(s: []const u8) ?SslMode {
        if (std.mem.eql(u8, s, "disable")) return .disable;
        if (std.mem.eql(u8, s, "require")) return .require;
        if (std.mem.eql(u8, s, "verify-ca")) return .verify_ca;
        if (std.mem.eql(u8, s, "verify-full")) return .verify_full;
        return null;
    }

    pub fn toString(self: SslMode) []const u8 {
        return switch (self) {
            .disable => "disable",
            .require => "require",
            .verify_ca => "verify-ca",
            .verify_full => "verify-full",
        };
    }
};

/// Configuration parsed from environment variables.
/// Compatible with the original Bemi environment variable interface.
pub const Config = struct {
    // Source database (the one being tracked)
    db_host: []const u8 = "127.0.0.1",
    db_port: u16 = 5432,
    db_name: []const u8 = "postgres",
    db_user: []const u8 = "postgres",
    db_password: []const u8 = "postgres",

    // Source database SSL settings
    db_ssl_mode: SslMode = .disable,
    db_ssl_root_cert: ?[]const u8 = null, // path to custom CA cert file

    // Replication settings (defaults match original Bemi for drop-in compatibility)
    slot_name: []const u8 = "bemi_local",
    publication_name: []const u8 = "bemi",

    // Destination database (where changes are written)
    // Defaults to same as source database if not specified.
    dest_db_host: ?[]const u8 = null,
    dest_db_port: ?u16 = null,
    dest_db_name: ?[]const u8 = null,
    dest_db_user: ?[]const u8 = null,
    dest_db_password: ?[]const u8 = null,

    // Destination database SSL settings (fall back to source if not set)
    dest_db_ssl_mode: ?SslMode = null,
    dest_db_ssl_root_cert: ?[]const u8 = null,

    // Logging
    log_level: std.log.Level = .info,

    // Table filtering: comma-separated list (e.g. "users,orders,products"
    // or "public.users,myschema.orders"). Schema-qualified entries match
    // only in that schema; bare names match any schema (backward compatible).
    // null = track all tables (default)
    tables: ?[]const u8 = null,

    // Operational settings
    shutdown_timeout_secs: u32 = 30,
    health_port: ?u16 = null, // null = health check disabled
    metrics_port: ?u16 = null, // null = metrics endpoint disabled
    cleanup_on_shutdown: bool = false, // drop slot + publication on graceful shutdown

    // Timeout settings (seconds). 0 = no timeout (default).
    connect_timeout_secs: u32 = 0,
    query_timeout_secs: u32 = 0,

    // Large transaction handling: flush accumulated changes to storage
    // mid-transaction when this limit is exceeded. null = unlimited (default).
    // Changes are idempotent via ON CONFLICT DO NOTHING, so early flushes
    // are safe even if the transaction is later rolled back (the changes
    // simply won't match any future commit and are harmless duplicates).
    max_transaction_changes: ?u32 = null,

    // JSON type coercion: when true, use PostgreSQL type OIDs to emit
    // typed JSON values (numbers, booleans, raw JSON) in before/after
    // JSONB columns instead of quoting everything as strings.
    // Default false for backward compatibility — existing consumers may
    // rely on all values being JSON strings.
    json_type_coercion: bool = false,

    // Column exclusion: comma-separated list of columns to exclude from
    // change tracking. Format: "table.column" or "schema.table.column".
    // Excluded columns have their values replaced with "[EXCLUDED]" in
    // before/after JSONB. Primary key columns are never excluded.
    // null = no exclusions (default).
    exclude_columns: ?[]const u8 = null,

    // Initial snapshot: when true and a NEW replication slot is created,
    // capture all existing rows in tracked tables as CREATE changes using
    // PostgreSQL's EXPORT_SNAPSHOT mechanism. This ensures no data is missed
    // when adding Zemi to an existing database. The snapshot is only taken
    // once (on first slot creation). Default false.
    initial_snapshot: bool = false,

    // Partitioned table support: when true, the publication is created with
    // publish_via_partition_root = true, so changes to partitioned tables
    // are reported under the parent (root) table name instead of the
    // individual partition name. This ensures TABLES filtering works
    // correctly for partitioned tables. Default true.
    publish_via_partition_root: bool = true,

    /// Returns the effective destination host (falls back to source).
    pub fn getDestHost(self: Config) []const u8 {
        return self.dest_db_host orelse self.db_host;
    }
    pub fn getDestPort(self: Config) u16 {
        return self.dest_db_port orelse self.db_port;
    }
    pub fn getDestName(self: Config) []const u8 {
        return self.dest_db_name orelse self.db_name;
    }
    pub fn getDestUser(self: Config) []const u8 {
        return self.dest_db_user orelse self.db_user;
    }
    pub fn getDestPassword(self: Config) []const u8 {
        return self.dest_db_password orelse self.db_password;
    }
    pub fn getDestSslMode(self: Config) SslMode {
        return self.dest_db_ssl_mode orelse self.db_ssl_mode;
    }
    pub fn getDestSslRootCert(self: Config) ?[]const u8 {
        return self.dest_db_ssl_root_cert orelse self.db_ssl_root_cert;
    }

    /// Check if a given table should be tracked.
    /// Returns true if no filter is configured (track all), or if the
    /// table appears in the TABLES comma-separated list.
    /// TABLES entries can be bare names ("users") which match any schema,
    /// or schema-qualified ("public.users") which match only that schema.
    pub fn shouldTrackTable(self: Config, schema: []const u8, table: []const u8) bool {
        const tables_str = self.tables orelse return true;
        var iter = std.mem.splitScalar(u8, tables_str, ',');
        while (iter.next()) |entry| {
            const trimmed = std.mem.trim(u8, entry, " \t");
            if (trimmed.len == 0) continue;

            // Check if entry is schema-qualified (contains a dot)
            if (std.mem.indexOfScalar(u8, trimmed, '.')) |dot_pos| {
                // Schema-qualified: match both schema and table
                const entry_schema = trimmed[0..dot_pos];
                const entry_table = trimmed[dot_pos + 1 ..];
                if (entry_schema.len > 0 and entry_table.len > 0 and
                    std.mem.eql(u8, entry_schema, schema) and
                    std.mem.eql(u8, entry_table, table))
                {
                    return true;
                }
            } else {
                // Bare name: match table in any schema (backward compatible)
                if (std.mem.eql(u8, trimmed, table)) {
                    return true;
                }
            }
        }
        return false;
    }

    /// Check if a column should be excluded from change tracking.
    /// Matches against EXCLUDE_COLUMNS entries in these formats:
    ///   - "table.column" — matches column in any schema of that table
    ///   - "schema.table.column" — matches only in the specific schema
    /// Returns true if the column should be excluded.
    pub fn shouldExcludeColumn(self: Config, schema: []const u8, table: []const u8, column: []const u8) bool {
        const exclude_str = self.exclude_columns orelse return false;
        var iter = std.mem.splitScalar(u8, exclude_str, ',');
        while (iter.next()) |entry| {
            const trimmed = std.mem.trim(u8, entry, " \t");
            if (trimmed.len == 0) continue;

            // Try schema.table.column (3 parts)
            if (parseExcludeEntry(trimmed)) |parsed| {
                if (parsed.schema) |s| {
                    // 3-part match: schema.table.column
                    if (std.mem.eql(u8, s, schema) and
                        std.mem.eql(u8, parsed.table, table) and
                        std.mem.eql(u8, parsed.column, column))
                    {
                        return true;
                    }
                } else {
                    // 2-part match: table.column
                    if (std.mem.eql(u8, parsed.table, table) and
                        std.mem.eql(u8, parsed.column, column))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    const ExcludeEntry = struct {
        schema: ?[]const u8,
        table: []const u8,
        column: []const u8,
    };

    /// Parse a single EXCLUDE_COLUMNS entry into its component parts.
    /// Supports "table.column" and "schema.table.column" formats.
    fn parseExcludeEntry(entry: []const u8) ?ExcludeEntry {
        // Count dots to determine format
        var dot_count: usize = 0;
        var first_dot: usize = 0;
        var second_dot: usize = 0;
        for (entry, 0..) |c, i| {
            if (c == '.') {
                dot_count += 1;
                if (dot_count == 1) first_dot = i;
                if (dot_count == 2) second_dot = i;
            }
        }

        if (dot_count == 1) {
            // table.column
            const tbl = entry[0..first_dot];
            const col = entry[first_dot + 1 ..];
            if (tbl.len == 0 or col.len == 0) return null;
            return .{ .schema = null, .table = tbl, .column = col };
        } else if (dot_count == 2) {
            // schema.table.column
            const sch = entry[0..first_dot];
            const tbl = entry[first_dot + 1 .. second_dot];
            const col = entry[second_dot + 1 ..];
            if (sch.len == 0 or tbl.len == 0 or col.len == 0) return null;
            return .{ .schema = sch, .table = tbl, .column = col };
        }
        return null; // invalid format
    }

    pub fn fromEnv() Config {
        var config = Config{};

        if (std.posix.getenv("DB_HOST")) |v| config.db_host = v;
        if (std.posix.getenv("DB_PORT")) |v| {
            config.db_port = std.fmt.parseUnsigned(u16, v, 10) catch blk: {
                log.warn("invalid DB_PORT '{s}', using default 5432", .{v});
                break :blk 5432;
            };
        }
        if (std.posix.getenv("DB_NAME")) |v| config.db_name = v;
        if (std.posix.getenv("DB_USER")) |v| config.db_user = v;
        if (std.posix.getenv("DB_PASSWORD")) |v| config.db_password = v;
        if (std.posix.getenv("DB_SSL_MODE")) |v| {
            if (SslMode.fromString(v)) |mode| {
                config.db_ssl_mode = mode;
            } else {
                log.warn("unknown DB_SSL_MODE '{s}', using default 'disable'", .{v});
            }
        }
        if (std.posix.getenv("DB_SSL_ROOT_CERT")) |v| {
            if (v.len > 0) config.db_ssl_root_cert = v;
        }
        if (std.posix.getenv("SLOT_NAME")) |v| config.slot_name = v;
        if (std.posix.getenv("PUBLICATION_NAME")) |v| config.publication_name = v;

        // Destination database overrides (optional).
        // Accept both DEST_DB_* (new) and DESTINATION_DB_* (original Bemi) names
        // for drop-in compatibility. The shorter DEST_DB_* form takes precedence.
        if (std.posix.getenv("DESTINATION_DB_HOST")) |v| config.dest_db_host = v;
        if (std.posix.getenv("DEST_DB_HOST")) |v| config.dest_db_host = v;
        if (std.posix.getenv("DESTINATION_DB_PORT") orelse std.posix.getenv("DEST_DB_PORT")) |v| {
            config.dest_db_port = std.fmt.parseUnsigned(u16, v, 10) catch blk: {
                log.warn("invalid DEST_DB_PORT '{s}', ignoring", .{v});
                break :blk null;
            };
        }
        if (std.posix.getenv("DESTINATION_DB_NAME")) |v| config.dest_db_name = v;
        if (std.posix.getenv("DEST_DB_NAME")) |v| config.dest_db_name = v;
        if (std.posix.getenv("DESTINATION_DB_USER")) |v| config.dest_db_user = v;
        if (std.posix.getenv("DEST_DB_USER")) |v| config.dest_db_user = v;
        if (std.posix.getenv("DESTINATION_DB_PASSWORD")) |v| config.dest_db_password = v;
        if (std.posix.getenv("DEST_DB_PASSWORD")) |v| config.dest_db_password = v;
        if (std.posix.getenv("DESTINATION_DB_SSL_MODE") orelse std.posix.getenv("DEST_DB_SSL_MODE")) |v| {
            if (SslMode.fromString(v)) |mode| {
                config.dest_db_ssl_mode = mode;
            } else {
                log.warn("unknown DEST_DB_SSL_MODE '{s}', ignoring", .{v});
            }
        }
        if (std.posix.getenv("DEST_DB_SSL_ROOT_CERT")) |v| {
            if (v.len > 0) config.dest_db_ssl_root_cert = v;
        }

        if (std.posix.getenv("LOG_LEVEL")) |v| {
            if (std.mem.eql(u8, v, "debug") or std.mem.eql(u8, v, "DEBUG")) {
                config.log_level = .debug;
            } else if (std.mem.eql(u8, v, "info") or std.mem.eql(u8, v, "INFO")) {
                config.log_level = .info;
            } else if (std.mem.eql(u8, v, "warn") or std.mem.eql(u8, v, "WARN")) {
                config.log_level = .warn;
            } else if (std.mem.eql(u8, v, "err") or std.mem.eql(u8, v, "ERROR") or std.mem.eql(u8, v, "error")) {
                config.log_level = .err;
            } else {
                log.warn("unknown LOG_LEVEL '{s}', using default 'info'", .{v});
            }
        }

        if (std.posix.getenv("TABLES")) |v| {
            if (v.len > 0) config.tables = v;
        }

        if (std.posix.getenv("SHUTDOWN_TIMEOUT")) |v| {
            config.shutdown_timeout_secs = std.fmt.parseUnsigned(u32, v, 10) catch blk: {
                log.warn("invalid SHUTDOWN_TIMEOUT '{s}', using default 30", .{v});
                break :blk 30;
            };
        }

        if (std.posix.getenv("HEALTH_PORT")) |v| {
            config.health_port = std.fmt.parseUnsigned(u16, v, 10) catch blk: {
                log.warn("invalid HEALTH_PORT '{s}', disabling health check", .{v});
                break :blk null;
            };
        }

        if (std.posix.getenv("METRICS_PORT")) |v| {
            config.metrics_port = std.fmt.parseUnsigned(u16, v, 10) catch blk: {
                log.warn("invalid METRICS_PORT '{s}', disabling metrics endpoint", .{v});
                break :blk null;
            };
        }

        if (std.posix.getenv("CLEANUP_ON_SHUTDOWN")) |v| {
            if (std.mem.eql(u8, v, "true") or std.mem.eql(u8, v, "1") or std.mem.eql(u8, v, "yes")) {
                config.cleanup_on_shutdown = true;
            }
        }

        if (std.posix.getenv("CONNECT_TIMEOUT")) |v| {
            config.connect_timeout_secs = std.fmt.parseUnsigned(u32, v, 10) catch blk: {
                log.warn("invalid CONNECT_TIMEOUT '{s}', using default 0 (no timeout)", .{v});
                break :blk 0;
            };
        }

        if (std.posix.getenv("QUERY_TIMEOUT")) |v| {
            config.query_timeout_secs = std.fmt.parseUnsigned(u32, v, 10) catch blk: {
                log.warn("invalid QUERY_TIMEOUT '{s}', using default 0 (no timeout)", .{v});
                break :blk 0;
            };
        }

        if (std.posix.getenv("MAX_TRANSACTION_CHANGES")) |v| {
            config.max_transaction_changes = std.fmt.parseUnsigned(u32, v, 10) catch blk: {
                log.warn("invalid MAX_TRANSACTION_CHANGES '{s}', disabling limit", .{v});
                break :blk null;
            };
        }

        if (std.posix.getenv("JSON_TYPE_COERCION")) |v| {
            if (std.mem.eql(u8, v, "true") or std.mem.eql(u8, v, "1") or std.mem.eql(u8, v, "yes")) {
                config.json_type_coercion = true;
            }
        }

        if (std.posix.getenv("EXCLUDE_COLUMNS")) |v| {
            if (v.len > 0) config.exclude_columns = v;
        }

        if (std.posix.getenv("INITIAL_SNAPSHOT")) |v| {
            if (std.mem.eql(u8, v, "true") or std.mem.eql(u8, v, "1") or std.mem.eql(u8, v, "yes")) {
                config.initial_snapshot = true;
            }
        }

        if (std.posix.getenv("PUBLISH_VIA_PARTITION_ROOT")) |v| {
            if (std.mem.eql(u8, v, "false") or std.mem.eql(u8, v, "0") or std.mem.eql(u8, v, "no")) {
                config.publish_via_partition_root = false;
            }
        }

        return config;
    }

    /// Validate the configuration and return an error message if invalid.
    /// Returns null if the configuration is valid.
    pub fn validate(self: Config) ?[]const u8 {
        if (self.db_host.len == 0) return "DB_HOST must not be empty";
        if (self.db_name.len == 0) return "DB_NAME must not be empty";
        if (self.db_user.len == 0) return "DB_USER must not be empty";
        if (self.slot_name.len == 0) return "SLOT_NAME must not be empty";
        if (!isValidIdentifier(self.slot_name)) return "SLOT_NAME must be a valid SQL identifier (letters, digits, underscores; must start with a letter or underscore; max 63 chars)";
        if (self.publication_name.len == 0) return "PUBLICATION_NAME must not be empty";
        if (!isValidIdentifier(self.publication_name)) return "PUBLICATION_NAME must be a valid SQL identifier (letters, digits, underscores; must start with a letter or underscore; max 63 chars)";
        if (self.shutdown_timeout_secs == 0) return "SHUTDOWN_TIMEOUT must be > 0";
        // Validate table names if TABLES is set (these go into SQL statements)
        if (self.tables) |tables_str| {
            if (validateTableNames(tables_str)) |err_msg| return err_msg;
        }
        // Validate EXCLUDE_COLUMNS entries
        if (self.exclude_columns) |exclude_str| {
            if (validateExcludeColumns(exclude_str)) |err_msg| return err_msg;
        }
        return null;
    }

    /// Check if a string is a valid PostgreSQL identifier: starts with a letter
    /// or underscore, followed by letters, digits, or underscores. Max 63 chars.
    /// This prevents SQL injection when interpolating names into DDL statements.
    pub fn isValidIdentifier(name: []const u8) bool {
        if (name.len == 0 or name.len > 63) return false;
        const first = name[0];
        if (!std.ascii.isAlphabetic(first) and first != '_') return false;
        for (name[1..]) |c| {
            if (!std.ascii.isAlphanumeric(c) and c != '_') return false;
        }
        return true;
    }

    /// Validate all table names in a comma-separated TABLES string.
    /// Each entry must be a valid identifier, or optionally schema-qualified
    /// (schema.table where both parts are valid identifiers).
    /// Returns an error message if invalid, null if all are valid.
    fn validateTableNames(tables_str: []const u8) ?[]const u8 {
        var iter = std.mem.splitScalar(u8, tables_str, ',');
        while (iter.next()) |entry| {
            const trimmed = std.mem.trim(u8, entry, " \t");
            if (trimmed.len == 0) continue;

            // Allow schema.table or just table
            if (std.mem.indexOfScalar(u8, trimmed, '.')) |dot_pos| {
                const schema_part = trimmed[0..dot_pos];
                const table_part = trimmed[dot_pos + 1 ..];
                if (!isValidIdentifier(schema_part) or !isValidIdentifier(table_part)) {
                    return "TABLES contains an invalid table name (must be valid SQL identifiers, e.g. 'users' or 'public.users')";
                }
            } else {
                if (!isValidIdentifier(trimmed)) {
                    return "TABLES contains an invalid table name (must be valid SQL identifiers, e.g. 'users' or 'public.users')";
                }
            }
        }
        return null;
    }

    /// Validate all entries in EXCLUDE_COLUMNS.
    /// Each entry must be "table.column" or "schema.table.column" where
    /// all parts are valid SQL identifiers.
    fn validateExcludeColumns(exclude_str: []const u8) ?[]const u8 {
        var iter = std.mem.splitScalar(u8, exclude_str, ',');
        while (iter.next()) |entry| {
            const trimmed = std.mem.trim(u8, entry, " \t");
            if (trimmed.len == 0) continue;

            const parsed = parseExcludeEntry(trimmed) orelse {
                return "EXCLUDE_COLUMNS entry must be 'table.column' or 'schema.table.column'";
            };

            if (parsed.schema) |s| {
                if (!isValidIdentifier(s)) {
                    return "EXCLUDE_COLUMNS contains an invalid schema name";
                }
            }
            if (!isValidIdentifier(parsed.table)) {
                return "EXCLUDE_COLUMNS contains an invalid table name";
            }
            if (!isValidIdentifier(parsed.column)) {
                return "EXCLUDE_COLUMNS contains an invalid column name";
            }
        }
        return null;
    }

    pub fn dump(self: Config) void {
        log.info("config: host={s} port={d} db={s} user={s} slot={s} publication={s}", .{
            self.db_host,
            self.db_port,
            self.db_name,
            self.db_user,
            self.slot_name,
            self.publication_name,
        });
        if (self.db_ssl_mode != .disable) {
            if (self.db_ssl_root_cert) |cert_path| {
                log.info("config: ssl_mode={s} ssl_root_cert={s}", .{ self.db_ssl_mode.toString(), cert_path });
            } else {
                log.info("config: ssl_mode={s}", .{self.db_ssl_mode.toString()});
            }
        }
        const dest_host = self.getDestHost();
        const dest_port = self.getDestPort();
        const dest_name = self.getDestName();
        const dest_user = self.getDestUser();
        if (self.dest_db_host != null or self.dest_db_port != null or self.dest_db_name != null) {
            log.info("config: dest_host={s} dest_port={d} dest_db={s} dest_user={s}", .{
                dest_host, dest_port, dest_name, dest_user,
            });
            const dest_ssl = self.getDestSslMode();
            if (dest_ssl != .disable) {
                if (self.getDestSslRootCert()) |cert_path| {
                    log.info("config: dest_ssl_mode={s} dest_ssl_root_cert={s}", .{ dest_ssl.toString(), cert_path });
                } else {
                    log.info("config: dest_ssl_mode={s}", .{dest_ssl.toString()});
                }
            }
        } else {
            log.info("config: changes written to source database", .{});
        }
        if (self.tables) |t| {
            log.info("config: tracking tables={s}", .{t});
        } else {
            log.info("config: tracking all tables", .{});
        }
        log.info("config: log_level={s} shutdown_timeout={d}s cleanup_on_shutdown={}", .{
            @tagName(self.log_level),
            self.shutdown_timeout_secs,
            self.cleanup_on_shutdown,
        });
        if (self.max_transaction_changes) |limit| {
            log.info("config: max_transaction_changes={d}", .{limit});
        }
        if (self.json_type_coercion) {
            log.info("config: json_type_coercion=true", .{});
        }
        if (self.exclude_columns) |ec| {
            log.info("config: exclude_columns={s}", .{ec});
        }
        if (self.initial_snapshot) {
            log.info("config: initial_snapshot=true", .{});
        }
        if (!self.publish_via_partition_root) {
            log.info("config: publish_via_partition_root=false", .{});
        }
        if (self.health_port) |port| {
            log.info("config: health_port={d}", .{port});
        }
        if (self.metrics_port) |port| {
            log.info("config: metrics_port={d}", .{port});
        }
        if (self.connect_timeout_secs > 0) {
            log.info("config: connect_timeout={d}s", .{self.connect_timeout_secs});
        }
        if (self.query_timeout_secs > 0) {
            log.info("config: query_timeout={d}s", .{self.query_timeout_secs});
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "shouldTrackTable with no filter tracks everything" {
    const config = Config{};
    try std.testing.expect(config.shouldTrackTable("public", "users"));
    try std.testing.expect(config.shouldTrackTable("public", "orders"));
    try std.testing.expect(config.shouldTrackTable("myschema", "anything"));
}

test "shouldTrackTable with bare names matches any schema" {
    const config = Config{ .tables = "users,orders,products" };
    try std.testing.expect(config.shouldTrackTable("public", "users"));
    try std.testing.expect(config.shouldTrackTable("myschema", "users"));
    try std.testing.expect(config.shouldTrackTable("public", "orders"));
    try std.testing.expect(config.shouldTrackTable("public", "products"));
    try std.testing.expect(!config.shouldTrackTable("public", "sessions"));
    try std.testing.expect(!config.shouldTrackTable("public", "changes"));
}

test "shouldTrackTable with schema-qualified names matches only that schema" {
    const config = Config{ .tables = "public.users, myschema.orders" };
    try std.testing.expect(config.shouldTrackTable("public", "users"));
    try std.testing.expect(!config.shouldTrackTable("other", "users"));
    try std.testing.expect(config.shouldTrackTable("myschema", "orders"));
    try std.testing.expect(!config.shouldTrackTable("public", "orders"));
}

test "shouldTrackTable with mixed bare and schema-qualified names" {
    const config = Config{ .tables = "users, public.orders" };
    // Bare "users" matches any schema
    try std.testing.expect(config.shouldTrackTable("public", "users"));
    try std.testing.expect(config.shouldTrackTable("other", "users"));
    // Schema-qualified "public.orders" matches only public
    try std.testing.expect(config.shouldTrackTable("public", "orders"));
    try std.testing.expect(!config.shouldTrackTable("other", "orders"));
}

test "shouldTrackTable trims whitespace" {
    const config = Config{ .tables = " users , orders " };
    try std.testing.expect(config.shouldTrackTable("public", "users"));
    try std.testing.expect(config.shouldTrackTable("public", "orders"));
    try std.testing.expect(!config.shouldTrackTable("public", "products"));
}

test "validate catches empty required fields" {
    {
        const config = Config{};
        try std.testing.expect(config.validate() == null);
    }
    {
        const config = Config{ .db_host = "" };
        try std.testing.expect(config.validate() != null);
    }
    {
        const config = Config{ .slot_name = "" };
        try std.testing.expect(config.validate() != null);
    }
    {
        const config = Config{ .shutdown_timeout_secs = 0 };
        try std.testing.expect(config.validate() != null);
    }
}

test "validate rejects invalid slot and publication names" {
    // SQL injection attempts
    {
        const config = Config{ .slot_name = "slot; DROP TABLE users" };
        try std.testing.expect(config.validate() != null);
    }
    {
        const config = Config{ .publication_name = "pub' OR '1'='1" };
        try std.testing.expect(config.validate() != null);
    }
    // Names starting with digits
    {
        const config = Config{ .slot_name = "123slot" };
        try std.testing.expect(config.validate() != null);
    }
    // Names with special characters
    {
        const config = Config{ .slot_name = "my-slot" };
        try std.testing.expect(config.validate() != null);
    }
    // Valid names pass
    {
        const config = Config{ .slot_name = "my_slot_123", .publication_name = "_pub" };
        try std.testing.expect(config.validate() == null);
    }
}

test "isValidIdentifier" {
    try std.testing.expect(Config.isValidIdentifier("bemi"));
    try std.testing.expect(Config.isValidIdentifier("_private"));
    try std.testing.expect(Config.isValidIdentifier("slot_123"));
    try std.testing.expect(Config.isValidIdentifier("A"));
    try std.testing.expect(!Config.isValidIdentifier(""));
    try std.testing.expect(!Config.isValidIdentifier("123abc"));
    try std.testing.expect(!Config.isValidIdentifier("my-name"));
    try std.testing.expect(!Config.isValidIdentifier("has space"));
    try std.testing.expect(!Config.isValidIdentifier("semi;colon"));
    try std.testing.expect(!Config.isValidIdentifier("quote'mark"));
    // 63 chars is ok, 64 is not
    try std.testing.expect(Config.isValidIdentifier("a" ** 63));
    try std.testing.expect(!Config.isValidIdentifier("a" ** 64));
}

test "validate rejects invalid table names in TABLES" {
    {
        const config = Config{ .tables = "users; DROP TABLE foo" };
        try std.testing.expect(config.validate() != null);
    }
    {
        const config = Config{ .tables = "users,orders" };
        try std.testing.expect(config.validate() == null);
    }
    {
        const config = Config{ .tables = "public.users, myschema.orders" };
        try std.testing.expect(config.validate() == null);
    }
    {
        const config = Config{ .tables = "public.users, bad;table" };
        try std.testing.expect(config.validate() != null);
    }
}

test "SslMode.fromString parses valid modes" {
    try std.testing.expectEqual(SslMode.disable, SslMode.fromString("disable").?);
    try std.testing.expectEqual(SslMode.require, SslMode.fromString("require").?);
    try std.testing.expectEqual(SslMode.verify_ca, SslMode.fromString("verify-ca").?);
    try std.testing.expectEqual(SslMode.verify_full, SslMode.fromString("verify-full").?);
    try std.testing.expect(SslMode.fromString("invalid") == null);
    try std.testing.expect(SslMode.fromString("") == null);
}

test "SslMode.toString roundtrips" {
    try std.testing.expectEqualStrings("disable", SslMode.disable.toString());
    try std.testing.expectEqualStrings("require", SslMode.require.toString());
    try std.testing.expectEqualStrings("verify-ca", SslMode.verify_ca.toString());
    try std.testing.expectEqualStrings("verify-full", SslMode.verify_full.toString());
}

test "dest SSL falls back to source SSL settings" {
    const config = Config{
        .db_ssl_mode = .verify_full,
        .db_ssl_root_cert = "/etc/ssl/certs/ca.crt",
    };
    try std.testing.expectEqual(SslMode.verify_full, config.getDestSslMode());
    try std.testing.expectEqualStrings("/etc/ssl/certs/ca.crt", config.getDestSslRootCert().?);

    // Explicit dest overrides source
    const config2 = Config{
        .db_ssl_mode = .verify_full,
        .db_ssl_root_cert = "/etc/ssl/certs/ca.crt",
        .dest_db_ssl_mode = .require,
        .dest_db_ssl_root_cert = "/other/ca.crt",
    };
    try std.testing.expectEqual(SslMode.require, config2.getDestSslMode());
    try std.testing.expectEqualStrings("/other/ca.crt", config2.getDestSslRootCert().?);
}

test "max_transaction_changes defaults to null (disabled)" {
    const config = Config{};
    try std.testing.expect(config.max_transaction_changes == null);
}

test "max_transaction_changes can be set explicitly" {
    const config = Config{ .max_transaction_changes = 5000 };
    try std.testing.expectEqual(@as(u32, 5000), config.max_transaction_changes.?);
}

test "validate accepts config with max_transaction_changes set" {
    const config = Config{ .max_transaction_changes = 100 };
    try std.testing.expect(config.validate() == null);
}

test "shouldExcludeColumn with no filter excludes nothing" {
    const config = Config{};
    try std.testing.expect(!config.shouldExcludeColumn("public", "users", "ssn"));
    try std.testing.expect(!config.shouldExcludeColumn("public", "users", "name"));
}

test "shouldExcludeColumn matches table.column format" {
    const config = Config{ .exclude_columns = "users.ssn, users.password" };
    try std.testing.expect(config.shouldExcludeColumn("public", "users", "ssn"));
    try std.testing.expect(config.shouldExcludeColumn("public", "users", "password"));
    try std.testing.expect(config.shouldExcludeColumn("other_schema", "users", "ssn"));
    try std.testing.expect(!config.shouldExcludeColumn("public", "users", "name"));
    try std.testing.expect(!config.shouldExcludeColumn("public", "orders", "ssn"));
}

test "shouldExcludeColumn matches schema.table.column format" {
    const config = Config{ .exclude_columns = "public.users.ssn, private.accounts.balance" };
    try std.testing.expect(config.shouldExcludeColumn("public", "users", "ssn"));
    try std.testing.expect(!config.shouldExcludeColumn("other", "users", "ssn"));
    try std.testing.expect(config.shouldExcludeColumn("private", "accounts", "balance"));
    try std.testing.expect(!config.shouldExcludeColumn("public", "accounts", "balance"));
}

test "shouldExcludeColumn handles mixed formats" {
    const config = Config{ .exclude_columns = "users.ssn, public.accounts.secret" };
    // table.column matches any schema
    try std.testing.expect(config.shouldExcludeColumn("public", "users", "ssn"));
    try std.testing.expect(config.shouldExcludeColumn("private", "users", "ssn"));
    // schema.table.column only matches specific schema
    try std.testing.expect(config.shouldExcludeColumn("public", "accounts", "secret"));
    try std.testing.expect(!config.shouldExcludeColumn("private", "accounts", "secret"));
}

test "validate rejects invalid EXCLUDE_COLUMNS entries" {
    {
        const config = Config{ .exclude_columns = "just_a_column" };
        try std.testing.expect(config.validate() != null);
    }
    {
        const config = Config{ .exclude_columns = "users.ssn; DROP TABLE foo" };
        try std.testing.expect(config.validate() != null);
    }
    {
        const config = Config{ .exclude_columns = "users.ssn,orders.total" };
        try std.testing.expect(config.validate() == null);
    }
    {
        const config = Config{ .exclude_columns = "public.users.ssn" };
        try std.testing.expect(config.validate() == null);
    }
}

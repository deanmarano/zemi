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

    // Replication settings
    slot_name: []const u8 = "zemi",
    publication_name: []const u8 = "zemi",

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

    // Table filtering: comma-separated list (e.g. "users,orders,products")
    // null = track all tables (default)
    tables: ?[]const u8 = null,

    // Operational settings
    shutdown_timeout_secs: u32 = 30,
    health_port: ?u16 = null, // null = health check disabled
    metrics_port: ?u16 = null, // null = metrics endpoint disabled
    cleanup_on_shutdown: bool = false, // drop slot + publication on graceful shutdown

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

    /// Check if a given table name should be tracked.
    /// Returns true if no filter is configured (track all), or if the
    /// table name appears in the TABLES comma-separated list.
    pub fn shouldTrackTable(self: Config, table: []const u8) bool {
        const tables_str = self.tables orelse return true;
        var iter = std.mem.splitScalar(u8, tables_str, ',');
        while (iter.next()) |entry| {
            const trimmed = std.mem.trim(u8, entry, " \t");
            if (trimmed.len > 0 and std.mem.eql(u8, trimmed, table)) {
                return true;
            }
        }
        return false;
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

        // Destination database overrides (optional)
        if (std.posix.getenv("DEST_DB_HOST")) |v| config.dest_db_host = v;
        if (std.posix.getenv("DEST_DB_PORT")) |v| {
            config.dest_db_port = std.fmt.parseUnsigned(u16, v, 10) catch blk: {
                log.warn("invalid DEST_DB_PORT '{s}', ignoring", .{v});
                break :blk null;
            };
        }
        if (std.posix.getenv("DEST_DB_NAME")) |v| config.dest_db_name = v;
        if (std.posix.getenv("DEST_DB_USER")) |v| config.dest_db_user = v;
        if (std.posix.getenv("DEST_DB_PASSWORD")) |v| config.dest_db_password = v;
        if (std.posix.getenv("DEST_DB_SSL_MODE")) |v| {
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

        return config;
    }

    /// Validate the configuration and return an error message if invalid.
    /// Returns null if the configuration is valid.
    pub fn validate(self: Config) ?[]const u8 {
        if (self.db_host.len == 0) return "DB_HOST must not be empty";
        if (self.db_name.len == 0) return "DB_NAME must not be empty";
        if (self.db_user.len == 0) return "DB_USER must not be empty";
        if (self.slot_name.len == 0) return "SLOT_NAME must not be empty";
        if (self.publication_name.len == 0) return "PUBLICATION_NAME must not be empty";
        if (self.shutdown_timeout_secs == 0) return "SHUTDOWN_TIMEOUT must be > 0";
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
        if (self.health_port) |port| {
            log.info("config: health_port={d}", .{port});
        }
        if (self.metrics_port) |port| {
            log.info("config: metrics_port={d}", .{port});
        }
    }
};

// ============================================================================
// Tests
// ============================================================================

test "shouldTrackTable with no filter tracks everything" {
    const config = Config{};
    try std.testing.expect(config.shouldTrackTable("users"));
    try std.testing.expect(config.shouldTrackTable("orders"));
    try std.testing.expect(config.shouldTrackTable("anything"));
}

test "shouldTrackTable with filter matches only listed tables" {
    const config = Config{ .tables = "users,orders,products" };
    try std.testing.expect(config.shouldTrackTable("users"));
    try std.testing.expect(config.shouldTrackTable("orders"));
    try std.testing.expect(config.shouldTrackTable("products"));
    try std.testing.expect(!config.shouldTrackTable("sessions"));
    try std.testing.expect(!config.shouldTrackTable("changes"));
}

test "shouldTrackTable trims whitespace" {
    const config = Config{ .tables = " users , orders " };
    try std.testing.expect(config.shouldTrackTable("users"));
    try std.testing.expect(config.shouldTrackTable("orders"));
    try std.testing.expect(!config.shouldTrackTable("products"));
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

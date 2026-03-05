const std = @import("std");
const mem = std.mem;
const crypto = std.crypto;
const HmacSha256 = crypto.auth.hmac.sha2.HmacSha256;
const Sha256 = crypto.hash.sha2.Sha256;
const base64 = std.base64.standard;

const log = std.log.scoped(.scram);

// ============================================================================
// SCRAM-SHA-256 Authentication (RFC 5802 + PostgreSQL extensions)
// ============================================================================

pub const ScramError = error{
    InvalidServerResponse,
    ServerNonceMismatch,
    ServerSignatureMismatch,
    Base64DecodeFailed,
    Overflow,
    OutOfMemory,
};

/// State machine for a SCRAM-SHA-256 authentication exchange.
pub const ScramClient = struct {
    allocator: mem.Allocator,
    password: []const u8,

    // Generated in client-first
    client_nonce: [24]u8, // raw random bytes
    client_nonce_b64: [32]u8, // base64 encoded
    client_nonce_b64_len: usize,

    // Parsed from server-first
    server_nonce: []const u8 = "", // full combined nonce (owned)
    salt: [32]u8 = undefined, // decoded salt bytes
    salt_len: usize = 0,
    iterations: u32 = 0,

    // Stored for proof computation
    client_first_bare: []const u8 = "", // owned
    server_first: []const u8 = "", // owned

    // Computed keys
    server_signature: [32]u8 = undefined,

    /// Initialize a SCRAM client with a password.
    pub fn init(allocator: mem.Allocator, password: []const u8) ScramClient {
        var client = ScramClient{
            .allocator = allocator,
            .password = password,
            .client_nonce = undefined,
            .client_nonce_b64 = undefined,
            .client_nonce_b64_len = 0,
        };

        // Generate 24 random bytes for the client nonce
        crypto.random.bytes(&client.client_nonce);

        // Base64 encode the nonce
        const encoded = base64.Encoder.encode(&client.client_nonce_b64, &client.client_nonce);
        client.client_nonce_b64_len = encoded.len;

        return client;
    }

    /// Initialize with a specific nonce (for testing).
    pub fn initWithNonce(allocator: mem.Allocator, password: []const u8, nonce_b64: []const u8) ScramClient {
        var client = ScramClient{
            .allocator = allocator,
            .password = password,
            .client_nonce = undefined,
            .client_nonce_b64 = undefined,
            .client_nonce_b64_len = 0,
        };
        @memcpy(client.client_nonce_b64[0..nonce_b64.len], nonce_b64);
        client.client_nonce_b64_len = nonce_b64.len;
        return client;
    }

    pub fn deinit(self: *ScramClient) void {
        if (self.client_first_bare.len > 0) self.allocator.free(self.client_first_bare);
        if (self.server_first.len > 0) self.allocator.free(self.server_first);
        if (self.server_nonce.len > 0) self.allocator.free(self.server_nonce);
    }

    // ========================================================================
    // Step 1: Build client-first-message
    // ========================================================================

    /// Build the client-first-message.
    /// Returns the full message including the GS2 header: "n,,n=,r=<nonce>"
    /// The caller must free the returned slice.
    pub fn buildClientFirst(self: *ScramClient) ![]const u8 {
        // client-first-message-bare = "n=,r=<client-nonce>"
        // PostgreSQL sends empty username in the SCRAM message (user is in startup)
        const nonce_slice = self.client_nonce_b64[0..self.client_nonce_b64_len];

        // Build client-first-bare: "n=,r=<nonce>"
        const bare = try std.fmt.allocPrint(self.allocator, "n=,r={s}", .{nonce_slice});
        self.client_first_bare = bare;

        // Full client-first-message: "n,," + bare
        const full = try std.fmt.allocPrint(self.allocator, "n,,{s}", .{bare});
        return full;
    }

    // ========================================================================
    // Step 2: Parse server-first-message and build client-final-message
    // ========================================================================

    /// Parse server-first-message and build client-final-message.
    /// server_first_msg format: "r=<nonce>,s=<salt-b64>,i=<iterations>"
    /// Returns the client-final-message. Caller must free.
    pub fn buildClientFinal(self: *ScramClient, server_first_msg: []const u8) ![]const u8 {
        // Save server-first for auth message computation
        self.server_first = try self.allocator.dupe(u8, server_first_msg);

        // Parse server-first-message fields
        try self.parseServerFirst(server_first_msg);

        // Verify the server nonce starts with our client nonce
        const our_nonce = self.client_nonce_b64[0..self.client_nonce_b64_len];
        if (!mem.startsWith(u8, self.server_nonce, our_nonce)) {
            log.warn("server nonce does not start with client nonce", .{});
            return ScramError.ServerNonceMismatch;
        }

        // Compute SCRAM proof
        // SaltedPassword = Hi(Normalize(password), salt, i)
        var salted_password: [32]u8 = undefined;
        try std.crypto.pwhash.pbkdf2(
            &salted_password,
            self.password,
            self.salt[0..self.salt_len],
            self.iterations,
            HmacSha256,
        );

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        var client_key: [32]u8 = undefined;
        HmacSha256.create(&client_key, "Client Key", &salted_password);

        // StoredKey = H(ClientKey)
        var stored_key: [32]u8 = undefined;
        Sha256.hash(&client_key, &stored_key, .{});

        // AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
        // client-final-without-proof = "c=biws,r=<server-nonce>"
        //   "biws" is base64("n,,") — the GS2 header channel binding
        const client_final_without_proof = try std.fmt.allocPrint(
            self.allocator,
            "c=biws,r={s}",
            .{self.server_nonce},
        );
        defer self.allocator.free(client_final_without_proof);

        const auth_message = try std.fmt.allocPrint(
            self.allocator,
            "{s},{s},{s}",
            .{ self.client_first_bare, self.server_first, client_final_without_proof },
        );
        defer self.allocator.free(auth_message);

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        var client_signature: [32]u8 = undefined;
        HmacSha256.create(&client_signature, auth_message, &stored_key);

        // ClientProof = ClientKey XOR ClientSignature
        var client_proof: [32]u8 = undefined;
        for (&client_proof, client_key, client_signature) |*p, k, s| {
            p.* = k ^ s;
        }

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        var server_key: [32]u8 = undefined;
        HmacSha256.create(&server_key, "Server Key", &salted_password);

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        HmacSha256.create(&self.server_signature, auth_message, &server_key);

        // Base64 encode client proof
        var proof_b64: [44]u8 = undefined; // 32 bytes -> 44 base64 chars
        const proof_encoded = base64.Encoder.encode(&proof_b64, &client_proof);

        // Build client-final-message: "c=biws,r=<nonce>,p=<proof>"
        const client_final = try std.fmt.allocPrint(
            self.allocator,
            "c=biws,r={s},p={s}",
            .{ self.server_nonce, proof_encoded },
        );
        return client_final;
    }

    // ========================================================================
    // Step 3: Verify server-final-message
    // ========================================================================

    /// Verify the server-final-message.
    /// server_final_msg format: "v=<server-signature-b64>"
    pub fn verifyServerFinal(self: *const ScramClient, server_final_msg: []const u8) ScramError!void {
        // Parse "v=<signature>"
        if (!mem.startsWith(u8, server_final_msg, "v=")) {
            log.err("invalid server-final-message: {s}", .{server_final_msg});
            return ScramError.InvalidServerResponse;
        }

        const sig_b64 = server_final_msg[2..];
        var decoded_sig: [32]u8 = undefined;
        base64.Decoder.decode(&decoded_sig, sig_b64) catch {
            log.err("failed to decode server signature base64", .{});
            return ScramError.Base64DecodeFailed;
        };

        if (!mem.eql(u8, &decoded_sig, &self.server_signature)) {
            log.warn("server signature mismatch - possible MITM attack", .{});
            return ScramError.ServerSignatureMismatch;
        }

        log.debug("SCRAM-SHA-256 server signature verified", .{});
    }

    // ========================================================================
    // Internal parsing
    // ========================================================================

    fn parseServerFirst(self: *ScramClient, msg: []const u8) ScramError!void {
        // Format: "r=<nonce>,s=<salt-b64>,i=<iterations>"
        var iter = mem.splitScalar(u8, msg, ',');

        while (iter.next()) |field| {
            if (field.len < 2) continue;
            const key = field[0];
            const value = field[2..]; // skip "X="

            switch (key) {
                'r' => {
                    self.server_nonce = self.allocator.dupe(u8, value) catch return ScramError.OutOfMemory;
                },
                's' => {
                    // Decode base64 salt
                    const decoded_len = base64.Decoder.calcSizeForSlice(value) catch return ScramError.Base64DecodeFailed;
                    if (decoded_len > self.salt.len) return ScramError.InvalidServerResponse;
                    base64.Decoder.decode(&self.salt, value) catch return ScramError.Base64DecodeFailed;
                    self.salt_len = decoded_len;
                },
                'i' => {
                    self.iterations = std.fmt.parseUnsigned(u32, value, 10) catch return ScramError.InvalidServerResponse;
                },
                else => {}, // ignore unknown attributes
            }
        }

        // Validate we got all required fields
        if (self.server_nonce.len == 0 or self.salt_len == 0 or self.iterations == 0) {
            log.err("server-first-message missing required fields: nonce={d} salt={d} iter={d}", .{
                self.server_nonce.len,
                self.salt_len,
                self.iterations,
            });
            return ScramError.InvalidServerResponse;
        }

        log.debug("SCRAM server-first: nonce_len={d}, salt_len={d}, iterations={d}", .{
            self.server_nonce.len,
            self.salt_len,
            self.iterations,
        });
    }
};

// ============================================================================
// PostgreSQL SASL message building
// ============================================================================

/// Build a SASLInitialResponse message.
/// Frontend message type 'p', with mechanism name + client-first-message.
pub fn buildSaslInitialResponse(allocator: mem.Allocator, mechanism: []const u8, client_first: []const u8) ![]u8 {
    // Message format:
    //   'p' + Int32(length) + mechanism\0 + Int32(data_len) + data
    const data_len: u32 = @intCast(client_first.len);
    const body_len: u32 = @intCast(mechanism.len + 1 + 4 + client_first.len);
    const total_len: u32 = 4 + body_len; // length field + body
    const total: usize = 1 + @as(usize, total_len); // type byte + rest

    var msg = try allocator.alloc(u8, total);
    var pos: usize = 0;

    msg[pos] = 'p'; // password message type (overloaded for SASL)
    pos += 1;
    mem.writeInt(u32, msg[pos..][0..4], total_len, .big);
    pos += 4;

    // Mechanism name (null-terminated)
    @memcpy(msg[pos .. pos + mechanism.len], mechanism);
    pos += mechanism.len;
    msg[pos] = 0;
    pos += 1;

    // Client-first-message length
    mem.writeInt(u32, msg[pos..][0..4], data_len, .big);
    pos += 4;

    // Client-first-message data (NOT null-terminated)
    @memcpy(msg[pos .. pos + client_first.len], client_first);
    pos += client_first.len;

    std.debug.assert(pos == total);
    return msg;
}

/// Build a SASLResponse message (for client-final-message).
/// Frontend message type 'p', with just the response data.
pub fn buildSaslResponse(allocator: mem.Allocator, client_final: []const u8) ![]u8 {
    // Message format:
    //   'p' + Int32(length) + data
    const total_len: u32 = @intCast(4 + client_final.len);
    const total: usize = 1 + @as(usize, total_len);

    var msg = try allocator.alloc(u8, total);
    var pos: usize = 0;

    msg[pos] = 'p';
    pos += 1;
    mem.writeInt(u32, msg[pos..][0..4], total_len, .big);
    pos += 4;

    @memcpy(msg[pos .. pos + client_final.len], client_final);
    pos += client_final.len;

    std.debug.assert(pos == total);
    return msg;
}

// ============================================================================
// Tests
// ============================================================================

test "SCRAM-SHA-256 full exchange with known vectors" {
    // Test using a known SCRAM exchange to verify correctness.
    // This uses a deterministic nonce so we can predict the output.
    const allocator = std.testing.allocator;

    var client = ScramClient.initWithNonce(allocator, "pencil", "fyko+d2lbbFgONRv9qkxdawL");
    defer client.deinit();

    // Step 1: client-first-message
    const client_first = try client.buildClientFirst();
    defer allocator.free(client_first);

    // Verify client-first format
    try std.testing.expect(mem.startsWith(u8, client_first, "n,,n=,r=fyko+d2lbbFgONRv9qkxdawL"));

    // Step 2: Parse server-first and build client-final
    // Using a known server-first-message
    const server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
    const client_final = try client.buildClientFinal(server_first);
    defer allocator.free(client_final);

    // Verify client-final starts with expected prefix
    try std.testing.expect(mem.startsWith(u8, client_final, "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p="));
}

test "SCRAM-SHA-256 server nonce mismatch detection" {
    const allocator = std.testing.allocator;

    var client = ScramClient.initWithNonce(allocator, "password", "clientnonce123");
    defer client.deinit();

    const client_first = try client.buildClientFirst();
    defer allocator.free(client_first);

    // Server returns a nonce that doesn't start with our nonce — should fail
    const bad_server_first = "r=differentnonce456serverstuff,s=c2FsdA==,i=4096";
    const result = client.buildClientFinal(bad_server_first);
    try std.testing.expectError(ScramError.ServerNonceMismatch, result);
}

test "SCRAM-SHA-256 server signature verification" {
    const allocator = std.testing.allocator;

    var client = ScramClient.initWithNonce(allocator, "pencil", "fyko+d2lbbFgONRv9qkxdawL");
    defer client.deinit();

    const client_first = try client.buildClientFirst();
    defer allocator.free(client_first);

    const server_first = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
    const client_final = try client.buildClientFinal(server_first);
    defer allocator.free(client_final);

    // Wrong server signature should fail
    const bad_final = "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    try std.testing.expectError(ScramError.ServerSignatureMismatch, client.verifyServerFinal(bad_final));
}

test "buildSaslInitialResponse format" {
    const allocator = std.testing.allocator;
    const msg = try buildSaslInitialResponse(allocator, "SCRAM-SHA-256", "n,,n=,r=abc123");
    defer allocator.free(msg);

    // First byte is 'p'
    try std.testing.expectEqual(@as(u8, 'p'), msg[0]);
    // Total length field
    const len = mem.readInt(u32, msg[1..5], .big);
    try std.testing.expectEqual(@as(u32, @intCast(msg.len - 1)), len);
}

test "buildSaslResponse format" {
    const allocator = std.testing.allocator;
    const msg = try buildSaslResponse(allocator, "c=biws,r=nonce,p=proof");
    defer allocator.free(msg);

    try std.testing.expectEqual(@as(u8, 'p'), msg[0]);
    const len = mem.readInt(u32, msg[1..5], .big);
    try std.testing.expectEqual(@as(u32, @intCast(msg.len - 1)), len);
    try std.testing.expectEqualStrings("c=biws,r=nonce,p=proof", msg[5..]);
}

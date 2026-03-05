#!/usr/bin/env bash
#
# Generate self-signed SSL certificates for PostgreSQL E2E testing.
# Creates server.key, server.crt, and root.crt in the specified directory.
#
set -euo pipefail

CERT_DIR="${1:-.}"
mkdir -p "$CERT_DIR"

# Generate CA key and certificate
openssl req -new -x509 -days 365 -nodes \
    -subj "/CN=Zemi Test CA" \
    -keyout "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    2>/dev/null

# Generate server key and CSR
openssl req -new -nodes \
    -subj "/CN=localhost" \
    -keyout "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.csr" \
    2>/dev/null

# Sign server certificate with CA
openssl x509 -req -days 365 \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/server.crt" \
    2>/dev/null

# PostgreSQL requires server.key to be owned by postgres (uid 70 in alpine)
# and have restrictive permissions
chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt"
chmod 644 "$CERT_DIR/ca.crt"

# Copy CA cert as root.crt (used by clients for verify-ca/verify-full)
cp "$CERT_DIR/ca.crt" "$CERT_DIR/root.crt"

# Clean up intermediate files
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/ca.srl"

echo "SSL certificates generated in $CERT_DIR"

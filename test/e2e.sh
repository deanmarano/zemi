#!/usr/bin/env bash
#
# E2E integration test for Zemi.
#
# Requires: PostgreSQL with wal_level=logical, a built Zemi binary.
# Usage: ./test/e2e.sh [--no-docker]
#
# With --no-docker, assumes PostgreSQL is already running on DB_HOST:DB_PORT.
# Without --no-docker, starts PostgreSQL via docker compose.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ZEMI_BIN="${ROOT_DIR}/zig-out/bin/zemi"
ZEMI_PID=""
PASS=0
FAIL=0
TOTAL=0

# Defaults (docker compose exposes 5433 -> 5432)
export DB_HOST="${DB_HOST:-127.0.0.1}"
export DB_PORT="${DB_PORT:-5433}"
export DB_NAME="${DB_NAME:-zemi_test}"
export DB_USER="${DB_USER:-postgres}"
export DB_PASSWORD="${DB_PASSWORD:-postgres}"
export SLOT_NAME="zemi_e2e_test"
export PUBLICATION_NAME="zemi_e2e_pub"
export LOG_LEVEL="${LOG_LEVEL:-debug}"

PSQL="psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -X -q"
USE_DOCKER=true

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

red()   { printf "\033[31m%s\033[0m" "$1"; }
green() { printf "\033[32m%s\033[0m" "$1"; }
bold()  { printf "\033[1m%s\033[0m" "$1"; }

log() { echo "--- $*"; }

cleanup() {
    log "Cleaning up..."
    if [ -n "$ZEMI_PID" ] && kill -0 "$ZEMI_PID" 2>/dev/null; then
        kill "$ZEMI_PID" 2>/dev/null || true
        wait "$ZEMI_PID" 2>/dev/null || true
    fi
    # Drop replication slot and publication (best effort)
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('$SLOT_NAME');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS $PUBLICATION_NAME;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS test_users CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS test_orders CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS changes CASCADE;" 2>/dev/null || true
    if [ "$USE_DOCKER" = true ]; then
        docker compose -f "$ROOT_DIR/docker-compose.test.yml" down -v 2>/dev/null || true
    fi
}
trap cleanup EXIT

assert_eq() {
    local label="$1" expected="$2" actual="$3"
    TOTAL=$((TOTAL + 1))
    if [ "$expected" = "$actual" ]; then
        PASS=$((PASS + 1))
        echo "  $(green "PASS") $label"
    else
        FAIL=$((FAIL + 1))
        echo "  $(red "FAIL") $label"
        echo "        expected: $expected"
        echo "        actual:   $actual"
    fi
}

assert_contains() {
    local label="$1" needle="$2" haystack="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$haystack" | grep -q "$needle"; then
        PASS=$((PASS + 1))
        echo "  $(green "PASS") $label"
    else
        FAIL=$((FAIL + 1))
        echo "  $(red "FAIL") $label"
        echo "        expected to contain: $needle"
        echo "        actual:              $haystack"
    fi
}

query() {
    PGPASSWORD="$DB_PASSWORD" $PSQL -t -A -c "$1" 2>/dev/null
}

wait_for_changes() {
    local expected_count="$1"
    local max_wait="${2:-15}"
    local elapsed=0
    while [ "$elapsed" -lt "$max_wait" ]; do
        local count
        count=$(query "SELECT COUNT(*) FROM changes;" 2>/dev/null || echo "0")
        if [ "$count" -ge "$expected_count" ]; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "  WARN: timed out waiting for $expected_count changes (got $(query "SELECT COUNT(*) FROM changes;"))"
    return 1
}

# ---------------------------------------------------------------------------
# Parse args
# ---------------------------------------------------------------------------
for arg in "$@"; do
    case "$arg" in
        --no-docker) USE_DOCKER=false ;;
    esac
done

# ---------------------------------------------------------------------------
# 1. Build Zemi
# ---------------------------------------------------------------------------
log "Building Zemi..."
(cd "$ROOT_DIR" && zig build)

if [ ! -f "$ZEMI_BIN" ]; then
    echo "ERROR: Zemi binary not found at $ZEMI_BIN"
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Start PostgreSQL (if using docker)
# ---------------------------------------------------------------------------
if [ "$USE_DOCKER" = true ]; then
    log "Starting PostgreSQL via docker compose..."
    docker compose -f "$ROOT_DIR/docker-compose.test.yml" up -d --wait
fi

# Wait for PostgreSQL to accept connections
log "Waiting for PostgreSQL..."
for i in $(seq 1 30); do
    if PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT 1;" >/dev/null 2>&1; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: PostgreSQL did not become ready in time"
        exit 1
    fi
    sleep 1
done
log "PostgreSQL is ready."

# ---------------------------------------------------------------------------
# 3. Set up test schema
# ---------------------------------------------------------------------------
log "Setting up test schema..."

query "DROP TABLE IF EXISTS changes CASCADE;"
query "DROP TABLE IF EXISTS test_users CASCADE;"
query "DROP TABLE IF EXISTS test_orders CASCADE;"
PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('$SLOT_NAME');" 2>/dev/null || true
PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS $PUBLICATION_NAME;" 2>/dev/null || true

query "CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);"

query "CREATE TABLE test_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES test_users(id),
    amount NUMERIC(10,2),
    status TEXT DEFAULT 'pending'
);"

# Create publication for test tables
query "CREATE PUBLICATION $PUBLICATION_NAME FOR TABLE test_users, test_orders;"

# ---------------------------------------------------------------------------
# 4. Start Zemi
# ---------------------------------------------------------------------------
log "Starting Zemi..."

"$ZEMI_BIN" > /tmp/zemi-e2e.log 2>&1 &
ZEMI_PID=$!

# Give Zemi time to connect and create slot
sleep 3

if ! kill -0 "$ZEMI_PID" 2>/dev/null; then
    echo "ERROR: Zemi exited early. Logs:"
    cat /tmp/zemi-e2e.log
    exit 1
fi
log "Zemi is running (PID=$ZEMI_PID)."

# ===========================================================================
# TEST SUITE
# ===========================================================================

echo ""
bold "=== E2E Test Suite ==="
echo ""

# ---------------------------------------------------------------------------
# Test 1: INSERT tracking
# ---------------------------------------------------------------------------
echo "$(bold "[Test 1] INSERT tracking")"

query "INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com');"
wait_for_changes 1

ROW=$(query "SELECT operation, \"table\", \"schema\", database, primary_key FROM changes WHERE operation = 'CREATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "INSERT creates a CREATE change" "CREATE|test_users|public|$DB_NAME|1" "$ROW"

AFTER=$(query "SELECT after->>'name' FROM changes WHERE operation = 'CREATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "after contains inserted data" "Alice" "$AFTER"

AFTER_EMAIL=$(query "SELECT after->>'email' FROM changes WHERE operation = 'CREATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "after contains email" "alice@example.com" "$AFTER_EMAIL"

BEFORE=$(query "SELECT before FROM changes WHERE operation = 'CREATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "before is empty for INSERT" "{}" "$BEFORE"

# ---------------------------------------------------------------------------
# Test 2: UPDATE tracking
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 2] UPDATE tracking")"

query "UPDATE test_users SET email = 'alice@newdomain.com' WHERE name = 'Alice';"
wait_for_changes 2

OP=$(query "SELECT operation FROM changes WHERE operation = 'UPDATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "UPDATE creates an UPDATE change" "UPDATE" "$OP"

AFTER_UPD=$(query "SELECT after->>'email' FROM changes WHERE operation = 'UPDATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "after contains updated email" "alice@newdomain.com" "$AFTER_UPD"

# ---------------------------------------------------------------------------
# Test 3: DELETE tracking
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 3] DELETE tracking")"

query "INSERT INTO test_users (name, email) VALUES ('Bob', 'bob@example.com');"
wait_for_changes 3
query "DELETE FROM test_users WHERE name = 'Bob';"
wait_for_changes 4

OP_DEL=$(query "SELECT operation FROM changes WHERE operation = 'DELETE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "DELETE creates a DELETE change" "DELETE" "$OP_DEL"

# ---------------------------------------------------------------------------
# Test 4: Multi-table tracking
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 4] Multi-table tracking")"

query "INSERT INTO test_orders (user_id, amount, status) VALUES (1, 99.99, 'confirmed');"
wait_for_changes 5

OP_ORD=$(query "SELECT operation, \"table\" FROM changes WHERE \"table\" = 'test_orders' LIMIT 1;")
assert_eq "order INSERT tracked" "CREATE|test_orders" "$OP_ORD"

AFTER_AMT=$(query "SELECT after->>'amount' FROM changes WHERE \"table\" = 'test_orders' LIMIT 1;")
assert_eq "order amount captured" "99.99" "$AFTER_AMT"

# ---------------------------------------------------------------------------
# Test 5: Transaction ID and position are populated
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 5] Metadata fields")"

TXN=$(query "SELECT transaction_id FROM changes WHERE \"table\" = 'test_users' AND operation = 'CREATE' LIMIT 1;")
assert_contains "transaction_id is populated" "^[0-9]" "$TXN"

POS=$(query "SELECT position FROM changes WHERE \"table\" = 'test_users' AND operation = 'CREATE' LIMIT 1;")
assert_contains "position is populated" "^[0-9]" "$POS"

COMMITTED=$(query "SELECT committed_at FROM changes WHERE \"table\" = 'test_users' AND operation = 'CREATE' LIMIT 1;")
assert_contains "committed_at is populated" "20" "$COMMITTED"

# ---------------------------------------------------------------------------
# Test 6: Context stitching via pg_logical_emit_message
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 6] Context stitching")"

CHANGES_BEFORE=$(query "SELECT COUNT(*) FROM changes;")

query "BEGIN;
SELECT pg_logical_emit_message(true, '_bemi', '{\"user_id\": 42, \"action\": \"api_call\"}');
INSERT INTO test_users (name, email) VALUES ('ContextUser', 'ctx@example.com');
COMMIT;" >/dev/null

wait_for_changes $((CHANGES_BEFORE + 1))

CTX=$(query "SELECT context->>'user_id' FROM changes WHERE after->>'name' = 'ContextUser' LIMIT 1;")
assert_eq "context.user_id stitched from _bemi message" "42" "$CTX"

CTX_ACTION=$(query "SELECT context->>'action' FROM changes WHERE after->>'name' = 'ContextUser' LIMIT 1;")
assert_eq "context.action stitched" "api_call" "$CTX_ACTION"

# ---------------------------------------------------------------------------
# Test 7: REPLICA IDENTITY FULL (before values on UPDATE)
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 7] REPLICA IDENTITY FULL")"

query "ALTER TABLE test_users REPLICA IDENTITY FULL;"

query "UPDATE test_users SET email = 'ctx-updated@example.com' WHERE name = 'ContextUser';"

CHANGES_AFTER=$(query "SELECT COUNT(*) FROM changes;")
wait_for_changes $((CHANGES_AFTER))

# With REPLICA IDENTITY FULL, the before image should contain the old values
BEFORE_FULL=$(query "SELECT before->>'email' FROM changes WHERE operation = 'UPDATE' AND after->>'name' = 'ContextUser' AND before != '{}' ORDER BY created_at DESC LIMIT 1;")
assert_eq "REPLICA IDENTITY FULL captures before values" "ctx@example.com" "$BEFORE_FULL"

# Reset
query "ALTER TABLE test_users REPLICA IDENTITY DEFAULT;"

# ---------------------------------------------------------------------------
# Test 8: changes table is NOT tracked (no infinite loop)
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 8] Self-tracking prevention")"

CHANGES_COUNT_1=$(query "SELECT COUNT(*) FROM changes;")
sleep 3
CHANGES_COUNT_2=$(query "SELECT COUNT(*) FROM changes;")
assert_eq "changes table doesn't trigger recursive tracking" "$CHANGES_COUNT_1" "$CHANGES_COUNT_2"

# ===========================================================================
# RESULTS
# ===========================================================================
echo ""
echo "==========================================="
if [ "$FAIL" -eq 0 ]; then
    echo "$(green "ALL $TOTAL TESTS PASSED") ($PASS/$TOTAL)"
else
    echo "$(red "$FAIL FAILED"), $PASS passed ($TOTAL total)"
fi
echo "==========================================="
echo ""

# Print Zemi logs on failure for debugging
if [ "$FAIL" -gt 0 ]; then
    echo "--- Zemi logs (last 50 lines):"
    tail -50 /tmp/zemi-e2e.log
    exit 1
fi

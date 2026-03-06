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
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('zemi_filter_test');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('zemi_metrics_test');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS $PUBLICATION_NAME;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS zemi_filter_pub;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS zemi_metrics_pub;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS test_users CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS test_orders CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS filter_tracked CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS filter_ignored CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS metrics_test CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS changes CASCADE;" 2>/dev/null || true
    # Clean up SSL test temp files
    rm -rf /tmp/zemi-ssl-certs 2>/dev/null || true
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

# ---------------------------------------------------------------------------
# Test 9: TRUNCATE tracking
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 9] TRUNCATE tracking")"

CHANGES_BEFORE_TRUNC=$(query "SELECT COUNT(*) FROM changes;")

query "TRUNCATE test_users CASCADE;"
wait_for_changes $((CHANGES_BEFORE_TRUNC + 1))

TRUNC_OP=$(query "SELECT operation FROM changes WHERE operation = 'TRUNCATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "TRUNCATE creates a TRUNCATE change" "TRUNCATE" "$TRUNC_OP"

TRUNC_BEFORE=$(query "SELECT before FROM changes WHERE operation = 'TRUNCATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "before is empty for TRUNCATE" "{}" "$TRUNC_BEFORE"

TRUNC_AFTER=$(query "SELECT after FROM changes WHERE operation = 'TRUNCATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "after is empty for TRUNCATE" "{}" "$TRUNC_AFTER"

TRUNC_PK=$(query "SELECT primary_key FROM changes WHERE operation = 'TRUNCATE' AND \"table\" = 'test_users' LIMIT 1;")
assert_eq "primary_key is empty for TRUNCATE" "" "$TRUNC_PK"

# ---------------------------------------------------------------------------
# Test 10: Storage reconnection on destination DB restart
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 10] Storage reconnection")"

# Reconnection test requires Docker to restart the PG container
DOCKER_AVAILABLE=false
if command -v docker >/dev/null 2>&1; then
    RECONNECT_CONTAINER=$(docker ps --format '{{.ID}} {{.Ports}}' 2>/dev/null | grep '5433->5432' | awk '{print $1}')
    if [ -n "$RECONNECT_CONTAINER" ]; then
        DOCKER_AVAILABLE=true
    fi
fi

if [ "$DOCKER_AVAILABLE" = true ]; then
    # Record how many changes exist before the restart test
    RECONNECT_BEFORE=$(query "SELECT COUNT(*) FROM changes;")

    # Insert a row before restart to confirm Zemi is healthy
    query "INSERT INTO test_users (name, email) VALUES ('PreRestart', 'pre@restart.com');"
    wait_for_changes $((RECONNECT_BEFORE + 1))

    RECONNECT_MID=$(query "SELECT COUNT(*) FROM changes;")
    assert_eq "reconnect: change tracked before restart" "$((RECONNECT_BEFORE + 1))" "$RECONNECT_MID"

    # Restart the PostgreSQL container (source + destination are the same DB)
    log "Restarting PostgreSQL container to test storage reconnection..."
    docker restart "$RECONNECT_CONTAINER" > /dev/null 2>&1

    # Wait for PostgreSQL to accept connections again
    RECONNECT_ELAPSED=0
    while [ "$RECONNECT_ELAPSED" -lt 30 ]; do
        if PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT 1;" >/dev/null 2>&1; then
            break
        fi
        sleep 1
        RECONNECT_ELAPSED=$((RECONNECT_ELAPSED + 1))
    done

    if [ "$RECONNECT_ELAPSED" -ge 30 ]; then
        echo "  $(red "FAIL") PostgreSQL did not come back after restart"
        FAIL=$((FAIL + 1))
        TOTAL=$((TOTAL + 1))
    else
        log "PostgreSQL is back after ${RECONNECT_ELAPSED}s, waiting for Zemi to recover..."

        # Give Zemi time to detect the broken connection and reconnect
        sleep 5

        # Verify Zemi is still running (didn't crash)
        if kill -0 "$ZEMI_PID" 2>/dev/null; then
            assert_eq "reconnect: Zemi still running after DB restart" "running" "running"

            # Insert a row after restart
            query "INSERT INTO test_users (name, email) VALUES ('PostRestart', 'post@restart.com');"

            # Wait for Zemi to reconnect and track the change (allow extra time)
            RECONNECT_ELAPSED=0
            RECONNECT_EXPECTED=$((RECONNECT_MID + 1))
            while [ "$RECONNECT_ELAPSED" -lt 30 ]; do
                RECONNECT_COUNT=$(query "SELECT COUNT(*) FROM changes;" 2>/dev/null || echo "0")
                if [ "$RECONNECT_COUNT" -ge "$RECONNECT_EXPECTED" ]; then break; fi
                sleep 1
                RECONNECT_ELAPSED=$((RECONNECT_ELAPSED + 1))
            done

            RECONNECT_OP=$(query "SELECT operation FROM changes WHERE after->>'name' = 'PostRestart' LIMIT 1;")
            assert_eq "reconnect: change tracked after DB restart" "CREATE" "$RECONNECT_OP"
        else
            echo "  $(red "FAIL") Zemi crashed after DB restart"
            echo "  --- Zemi logs (last 30 lines):"
            tail -30 /tmp/zemi-e2e.log
            FAIL=$((FAIL + 2))
            TOTAL=$((TOTAL + 2))
        fi
    fi
else
    echo "  $(red "FAIL") reconnection test requires Docker with PostgreSQL container on port 5433"
    FAIL=$((FAIL + 2))
    TOTAL=$((TOTAL + 2))
fi

# ---------------------------------------------------------------------------
# Test 11: Graceful shutdown cleanup (CLEANUP_ON_SHUTDOWN)
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 11] Graceful shutdown cleanup")"

CLEANUP_SLOT="zemi_cleanup_test"
CLEANUP_PUB="zemi_cleanup_pub"
CLEANUP_ZEMI_PID=""

cleanup_test_teardown() {
    if [ -n "$CLEANUP_ZEMI_PID" ] && kill -0 "$CLEANUP_ZEMI_PID" 2>/dev/null; then
        kill "$CLEANUP_ZEMI_PID" 2>/dev/null || true
        wait "$CLEANUP_ZEMI_PID" 2>/dev/null || true
    fi
    # Best-effort cleanup in case the test fails
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('$CLEANUP_SLOT');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS $CLEANUP_PUB;" 2>/dev/null || true
}

# Start Zemi with CLEANUP_ON_SHUTDOWN=true and a dedicated slot/publication
CLEANUP_ON_SHUTDOWN=true \
SLOT_NAME="$CLEANUP_SLOT" \
PUBLICATION_NAME="$CLEANUP_PUB" \
"$ZEMI_BIN" > /tmp/zemi-e2e-cleanup.log 2>&1 &
CLEANUP_ZEMI_PID=$!

sleep 3

if kill -0 "$CLEANUP_ZEMI_PID" 2>/dev/null; then
    # Verify the slot was created
    SLOT_EXISTS=$(query "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '$CLEANUP_SLOT';")
    assert_eq "cleanup: slot created" "1" "$SLOT_EXISTS"

    # Verify the publication was created
    PUB_EXISTS=$(query "SELECT COUNT(*) FROM pg_publication WHERE pubname = '$CLEANUP_PUB';")
    assert_eq "cleanup: publication created" "1" "$PUB_EXISTS"

    # Send SIGTERM for graceful shutdown
    kill "$CLEANUP_ZEMI_PID" 2>/dev/null || true
    wait "$CLEANUP_ZEMI_PID" 2>/dev/null || true
    CLEANUP_ZEMI_PID=""

    # Give PostgreSQL a moment to release resources
    sleep 1

    # Verify the slot was dropped
    SLOT_AFTER=$(query "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '$CLEANUP_SLOT';")
    assert_eq "cleanup: slot dropped on shutdown" "0" "$SLOT_AFTER"

    # Verify the publication was dropped
    PUB_AFTER=$(query "SELECT COUNT(*) FROM pg_publication WHERE pubname = '$CLEANUP_PUB';")
    assert_eq "cleanup: publication dropped on shutdown" "0" "$PUB_AFTER"
else
    echo "  $(red "FAIL") Zemi failed to start for cleanup test"
    echo "  --- Cleanup test Zemi logs:"
    cat /tmp/zemi-e2e-cleanup.log
    FAIL=$((FAIL + 4))
    TOTAL=$((TOTAL + 4))
    cleanup_test_teardown
fi

# ---------------------------------------------------------------------------
# Test 12: SCRAM-SHA-256 authentication
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 12] SCRAM-SHA-256 authentication")"

SCRAM_PORT="${SCRAM_PORT:-5434}"
SCRAM_PSQL="psql -h $DB_HOST -p $SCRAM_PORT -U $DB_USER -d $DB_NAME -X -q"
SCRAM_ZEMI_PID=""

scram_query() {
    PGPASSWORD="$DB_PASSWORD" $SCRAM_PSQL -t -A -c "$1" 2>/dev/null
}

scram_cleanup() {
    if [ -n "$SCRAM_ZEMI_PID" ] && kill -0 "$SCRAM_ZEMI_PID" 2>/dev/null; then
        kill "$SCRAM_ZEMI_PID" 2>/dev/null || true
        wait "$SCRAM_ZEMI_PID" 2>/dev/null || true
    fi
    PGPASSWORD="$DB_PASSWORD" $SCRAM_PSQL -c "SELECT pg_drop_replication_slot('zemi_scram_test');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SCRAM_PSQL -c "DROP PUBLICATION IF EXISTS zemi_scram_pub;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SCRAM_PSQL -c "DROP TABLE IF EXISTS scram_test_items CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SCRAM_PSQL -c "DROP TABLE IF EXISTS changes CASCADE;" 2>/dev/null || true
}

# Check if SCRAM PostgreSQL is available (skip if --no-docker without SCRAM PG)
SCRAM_AVAILABLE=false
for i in $(seq 1 15); do
    if PGPASSWORD="$DB_PASSWORD" $SCRAM_PSQL -c "SELECT 1;" >/dev/null 2>&1; then
        SCRAM_AVAILABLE=true
        break
    fi
    sleep 1
done

if [ "$SCRAM_AVAILABLE" = true ]; then
    # Clean up any previous state
    scram_cleanup

    # Create test table and publication on SCRAM PostgreSQL
    scram_query "CREATE TABLE scram_test_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
    scram_query "CREATE PUBLICATION zemi_scram_pub FOR TABLE scram_test_items;"

    # Start a second Zemi instance against SCRAM PostgreSQL
    DB_PORT="$SCRAM_PORT" \
    SLOT_NAME="zemi_scram_test" \
    PUBLICATION_NAME="zemi_scram_pub" \
    "$ZEMI_BIN" > /tmp/zemi-e2e-scram.log 2>&1 &
    SCRAM_ZEMI_PID=$!

    sleep 3

    if kill -0 "$SCRAM_ZEMI_PID" 2>/dev/null; then
        assert_eq "Zemi connects with SCRAM-SHA-256" "running" "running"

        # Insert a row and verify it's tracked
        scram_query "INSERT INTO scram_test_items (name) VALUES ('scram-test-item');"

        # Wait for the change to be captured
        SCRAM_ELAPSED=0
        while [ "$SCRAM_ELAPSED" -lt 15 ]; do
            SCRAM_COUNT=$(scram_query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'scram_test_items';" 2>/dev/null || echo "0")
            if [ "$SCRAM_COUNT" -ge 1 ]; then break; fi
            sleep 1
            SCRAM_ELAPSED=$((SCRAM_ELAPSED + 1))
        done

        SCRAM_OP=$(scram_query "SELECT operation FROM changes WHERE \"table\" = 'scram_test_items' LIMIT 1;" || echo "")
        assert_eq "SCRAM: INSERT tracked as CREATE" "CREATE" "$SCRAM_OP"

        SCRAM_AFTER=$(scram_query "SELECT after->>'name' FROM changes WHERE \"table\" = 'scram_test_items' LIMIT 1;" || echo "")
        assert_eq "SCRAM: after contains inserted data" "scram-test-item" "$SCRAM_AFTER"
    else
        echo "  $(red "FAIL") Zemi failed to start with SCRAM-SHA-256"
        echo "  --- SCRAM Zemi logs:"
        cat /tmp/zemi-e2e-scram.log
        FAIL=$((FAIL + 3))
        TOTAL=$((TOTAL + 3))
    fi

    scram_cleanup
else
    echo "  $(red "FAIL") SCRAM PostgreSQL not available on port $SCRAM_PORT"
    FAIL=$((FAIL + 3))
    TOTAL=$((TOTAL + 3))
fi

# ---------------------------------------------------------------------------
# Test 13: SSL/TLS connection (sslmode=require)
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 13] SSL/TLS connection")"

SSL_PORT="${SSL_PORT:-5435}"
SSL_PSQL="psql -h $DB_HOST -p $SSL_PORT -U $DB_USER -d $DB_NAME -X -q"
SSL_ZEMI_PID=""

ssl_query() {
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -t -A -c "$1" 2>/dev/null
}

ssl_cleanup() {
    if [ -n "$SSL_ZEMI_PID" ] && kill -0 "$SSL_ZEMI_PID" 2>/dev/null; then
        kill "$SSL_ZEMI_PID" 2>/dev/null || true
        wait "$SSL_ZEMI_PID" 2>/dev/null || true
    fi
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "SELECT pg_drop_replication_slot('zemi_ssl_test');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "SELECT pg_drop_replication_slot('zemi_ssl_verify');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "SELECT pg_drop_replication_slot('zemi_ssl_full');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "DROP PUBLICATION IF EXISTS zemi_ssl_pub;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "DROP TABLE IF EXISTS ssl_test_items CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "DROP TABLE IF EXISTS changes CASCADE;" 2>/dev/null || true
}

# Check if SSL PostgreSQL is available
SSL_AVAILABLE=false
for i in $(seq 1 15); do
    if PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "SELECT 1;" >/dev/null 2>&1; then
        SSL_AVAILABLE=true
        break
    fi
    sleep 1
done

if [ "$SSL_AVAILABLE" = true ]; then
    # Verify SSL is enabled on the server
    SSL_ENABLED=$(ssl_query "SHOW ssl;")
    if [ "$SSL_ENABLED" != "on" ]; then
        echo "  $(red "FAIL") SSL not enabled on PostgreSQL at port $SSL_PORT (ssl=$SSL_ENABLED)"
        FAIL=$((FAIL + 7))
        TOTAL=$((TOTAL + 7))
    else
        # Clean up any previous state
        ssl_cleanup

        # Create test table and publication
        ssl_query "CREATE TABLE ssl_test_items (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
        ssl_query "CREATE PUBLICATION zemi_ssl_pub FOR TABLE ssl_test_items;"

        # --- Test 13a: sslmode=require (no cert verification) ---
        echo "  $(bold "13a: sslmode=require")"

        DB_PORT="$SSL_PORT" \
        DB_SSL_MODE="require" \
        SLOT_NAME="zemi_ssl_test" \
        PUBLICATION_NAME="zemi_ssl_pub" \
        "$ZEMI_BIN" > /tmp/zemi-e2e-ssl.log 2>&1 &
        SSL_ZEMI_PID=$!

        sleep 3

        if kill -0 "$SSL_ZEMI_PID" 2>/dev/null; then
            assert_eq "Zemi connects with SSL (require)" "running" "running"

            # Insert and verify tracking works over SSL
            ssl_query "INSERT INTO ssl_test_items (name) VALUES ('ssl-test-item');"

            SSL_ELAPSED=0
            while [ "$SSL_ELAPSED" -lt 15 ]; do
                SSL_COUNT=$(ssl_query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'ssl_test_items';" 2>/dev/null || echo "0")
                if [ "$SSL_COUNT" -ge 1 ]; then break; fi
                sleep 1
                SSL_ELAPSED=$((SSL_ELAPSED + 1))
            done

            SSL_OP=$(ssl_query "SELECT operation FROM changes WHERE \"table\" = 'ssl_test_items' LIMIT 1;" || echo "")
            assert_eq "SSL: INSERT tracked as CREATE" "CREATE" "$SSL_OP"

            SSL_AFTER=$(ssl_query "SELECT after->>'name' FROM changes WHERE \"table\" = 'ssl_test_items' LIMIT 1;" || echo "")
            assert_eq "SSL: after contains inserted data" "ssl-test-item" "$SSL_AFTER"
        else
            echo "  $(red "FAIL") Zemi failed to start with SSL (require)"
            echo "  --- SSL Zemi logs:"
            cat /tmp/zemi-e2e-ssl.log
            FAIL=$((FAIL + 3))
            TOTAL=$((TOTAL + 3))
        fi

        # Stop SSL Zemi instance
        if [ -n "$SSL_ZEMI_PID" ] && kill -0 "$SSL_ZEMI_PID" 2>/dev/null; then
            kill "$SSL_ZEMI_PID" 2>/dev/null || true
            wait "$SSL_ZEMI_PID" 2>/dev/null || true
        fi
        SSL_ZEMI_PID=""

        # Drop the replication slot between 13a and 13b so 13b doesn't
        # pick up stale WAL events (DELETEs from cleanup) via the old slot position.
        # Wait briefly for PostgreSQL to fully release the slot after Zemi exits.
        sleep 1
        PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "SELECT pg_drop_replication_slot('zemi_ssl_test');" 2>/dev/null || true

        # --- Test 13b: sslmode=verify-ca with root cert ---
        echo "  $(bold "13b: sslmode=verify-ca")"

        # Extract the CA certificate from the SSL container
        SSL_CERT_DIR="/tmp/zemi-ssl-certs"
        mkdir -p "$SSL_CERT_DIR"

        # Try to extract cert via docker cp (works in CI with --no-docker too,
        # since the SSL container is started manually in the workflow)
        if command -v docker >/dev/null 2>&1; then
            SSL_CONTAINER=$(docker ps --format '{{.ID}} {{.Ports}}' 2>/dev/null | grep '5435->5432' | awk '{print $1}')
            if [ -n "$SSL_CONTAINER" ]; then
                docker cp "$SSL_CONTAINER:/var/lib/postgresql/ssl/root.crt" "$SSL_CERT_DIR/root.crt" 2>/dev/null || true
            fi
        fi

        if [ -f "$SSL_CERT_DIR/root.crt" ]; then
            # Clean up changes from 13a (may not exist if Zemi couldn't connect)
            ssl_query "DELETE FROM changes;" 2>/dev/null || true
            ssl_query "DELETE FROM ssl_test_items;" 2>/dev/null || true

            DB_PORT="$SSL_PORT" \
            DB_SSL_MODE="verify-ca" \
            DB_SSL_ROOT_CERT="$SSL_CERT_DIR/root.crt" \
            SLOT_NAME="zemi_ssl_verify" \
            PUBLICATION_NAME="zemi_ssl_pub" \
            "$ZEMI_BIN" > /tmp/zemi-e2e-ssl-verify.log 2>&1 &
            SSL_ZEMI_PID=$!

            sleep 3

            if kill -0 "$SSL_ZEMI_PID" 2>/dev/null; then
                assert_eq "Zemi connects with SSL (verify-ca)" "running" "running"

                ssl_query "INSERT INTO ssl_test_items (name) VALUES ('ssl-verify-ca-item');"

                SSL_ELAPSED=0
                while [ "$SSL_ELAPSED" -lt 15 ]; do
                    SSL_COUNT=$(ssl_query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'ssl_test_items' AND operation = 'CREATE' AND after->>'name' = 'ssl-verify-ca-item';" 2>/dev/null || echo "0")
                    if [ "$SSL_COUNT" -ge 1 ]; then break; fi
                    sleep 1
                    SSL_ELAPSED=$((SSL_ELAPSED + 1))
                done

                SSL_VERIFY_OP=$(ssl_query "SELECT operation FROM changes WHERE \"table\" = 'ssl_test_items' AND after->>'name' = 'ssl-verify-ca-item' LIMIT 1;" || echo "")
                assert_eq "SSL verify-ca: INSERT tracked" "CREATE" "$SSL_VERIFY_OP"
            else
                echo "  $(red "FAIL") Zemi failed to start with SSL (verify-ca)"
                echo "  --- SSL verify-ca Zemi logs:"
                cat /tmp/zemi-e2e-ssl-verify.log
                FAIL=$((FAIL + 2))
                TOTAL=$((TOTAL + 2))
            fi

            # --- Test 13c: sslmode=verify-full with hostname verification ---
            echo "  $(bold "13c: sslmode=verify-full")"

            # Stop previous Zemi instance if still running
            if [ -n "$SSL_ZEMI_PID" ] && kill -0 "$SSL_ZEMI_PID" 2>/dev/null; then
                kill "$SSL_ZEMI_PID" 2>/dev/null || true
                wait "$SSL_ZEMI_PID" 2>/dev/null || true
            fi
            SSL_ZEMI_PID=""

            # Drop the verify-ca slot before starting verify-full
            sleep 1
            PGPASSWORD="$DB_PASSWORD" $SSL_PSQL -c "SELECT pg_drop_replication_slot('zemi_ssl_verify');" 2>/dev/null || true

            # Clean up changes from 13b
            ssl_query "DELETE FROM changes;" 2>/dev/null || true
            ssl_query "DELETE FROM ssl_test_items;" 2>/dev/null || true

            # verify-full checks that the server certificate hostname matches DB_HOST.
            # Our test cert has SAN DNS:localhost — Zig's TLS verifies DNS SANs
            # but not IP SANs, so we connect via localhost instead of 127.0.0.1.
            DB_HOST="localhost" \
            DB_PORT="$SSL_PORT" \
            DB_SSL_MODE="verify-full" \
            DB_SSL_ROOT_CERT="$SSL_CERT_DIR/root.crt" \
            SLOT_NAME="zemi_ssl_full" \
            PUBLICATION_NAME="zemi_ssl_pub" \
            "$ZEMI_BIN" > /tmp/zemi-e2e-ssl-full.log 2>&1 &
            SSL_ZEMI_PID=$!

            sleep 3

            if kill -0 "$SSL_ZEMI_PID" 2>/dev/null; then
                assert_eq "Zemi connects with SSL (verify-full)" "running" "running"

                ssl_query "INSERT INTO ssl_test_items (name) VALUES ('ssl-verify-full-item');"

                SSL_ELAPSED=0
                while [ "$SSL_ELAPSED" -lt 15 ]; do
                    SSL_COUNT=$(ssl_query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'ssl_test_items' AND operation = 'CREATE' AND after->>'name' = 'ssl-verify-full-item';" 2>/dev/null || echo "0")
                    if [ "$SSL_COUNT" -ge 1 ]; then break; fi
                    sleep 1
                    SSL_ELAPSED=$((SSL_ELAPSED + 1))
                done

                SSL_FULL_OP=$(ssl_query "SELECT operation FROM changes WHERE \"table\" = 'ssl_test_items' AND after->>'name' = 'ssl-verify-full-item' LIMIT 1;" || echo "")
                assert_eq "SSL verify-full: INSERT tracked" "CREATE" "$SSL_FULL_OP"
            else
                echo "  $(red "FAIL") Zemi failed to start with SSL (verify-full)"
                echo "  --- SSL verify-full Zemi logs:"
                cat /tmp/zemi-e2e-ssl-full.log
                FAIL=$((FAIL + 2))
                TOTAL=$((TOTAL + 2))
            fi

            rm -rf "$SSL_CERT_DIR"
        else
            echo "  $(red "FAIL") Could not extract root certificate from SSL container"
            echo "  Ensure the postgres-ssl container is running with port 5435->5432"
            FAIL=$((FAIL + 4))
            TOTAL=$((TOTAL + 4))
        fi

        ssl_cleanup
    fi
else
    echo "  $(red "FAIL") SSL PostgreSQL not available on port $SSL_PORT"
    FAIL=$((FAIL + 7))
    TOTAL=$((TOTAL + 7))
fi

# ---------------------------------------------------------------------------
# Test 14: Table filtering (TABLES env var)
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 14] Table filtering (TABLES env var)")"

FILTER_SLOT="zemi_filter_test"
FILTER_PUB="zemi_filter_pub"
FILTER_ZEMI_PID=""

filter_cleanup() {
    if [ -n "$FILTER_ZEMI_PID" ] && kill -0 "$FILTER_ZEMI_PID" 2>/dev/null; then
        kill "$FILTER_ZEMI_PID" 2>/dev/null || true
        wait "$FILTER_ZEMI_PID" 2>/dev/null || true
    fi
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('$FILTER_SLOT');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS $FILTER_PUB;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS filter_tracked CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS filter_ignored CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS changes CASCADE;" 2>/dev/null || true
}

# Clean up any previous state
filter_cleanup

# Create test tables — both are in the publication, but TABLES will restrict tracking
query "CREATE TABLE filter_tracked (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
query "CREATE TABLE filter_ignored (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
query "CREATE PUBLICATION $FILTER_PUB FOR TABLE filter_tracked, filter_ignored;"

# Start Zemi with TABLES=filter_tracked (only track one table)
TABLES="filter_tracked" \
SLOT_NAME="$FILTER_SLOT" \
PUBLICATION_NAME="$FILTER_PUB" \
"$ZEMI_BIN" > /tmp/zemi-e2e-filter.log 2>&1 &
FILTER_ZEMI_PID=$!

sleep 3

if kill -0 "$FILTER_ZEMI_PID" 2>/dev/null; then
    # Insert into both tables
    query "INSERT INTO filter_tracked (name) VALUES ('should-appear');"
    query "INSERT INTO filter_ignored (name) VALUES ('should-not-appear');"

    # Wait for the tracked change to appear
    FILTER_ELAPSED=0
    while [ "$FILTER_ELAPSED" -lt 15 ]; do
        FILTER_COUNT=$(query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'filter_tracked';" 2>/dev/null || echo "0")
        if [ "$FILTER_COUNT" -ge 1 ]; then break; fi
        sleep 1
        FILTER_ELAPSED=$((FILTER_ELAPSED + 1))
    done

    # Give a bit more time to ensure the ignored table's change would have appeared if not filtered
    sleep 2

    TRACKED_COUNT=$(query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'filter_tracked';")
    assert_eq "filter: tracked table changes recorded" "1" "$TRACKED_COUNT"

    IGNORED_COUNT=$(query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'filter_ignored';")
    assert_eq "filter: ignored table changes excluded" "0" "$IGNORED_COUNT"

    TRACKED_NAME=$(query "SELECT after->>'name' FROM changes WHERE \"table\" = 'filter_tracked' LIMIT 1;")
    assert_eq "filter: tracked data correct" "should-appear" "$TRACKED_NAME"
else
    echo "  $(red "FAIL") Zemi failed to start for table filtering test"
    echo "  --- Filter Zemi logs:"
    cat /tmp/zemi-e2e-filter.log
    FAIL=$((FAIL + 3))
    TOTAL=$((TOTAL + 3))
fi

filter_cleanup

# ---------------------------------------------------------------------------
# Test 15: Prometheus metrics endpoint (METRICS_PORT)
# ---------------------------------------------------------------------------
echo ""
echo "$(bold "[Test 15] Prometheus metrics endpoint (METRICS_PORT)")"

METRICS_SLOT="zemi_metrics_test"
METRICS_PUB="zemi_metrics_pub"
METRICS_PORT_NUM=9191
METRICS_ZEMI_PID=""

metrics_cleanup() {
    if [ -n "$METRICS_ZEMI_PID" ] && kill -0 "$METRICS_ZEMI_PID" 2>/dev/null; then
        kill "$METRICS_ZEMI_PID" 2>/dev/null || true
        wait "$METRICS_ZEMI_PID" 2>/dev/null || true
    fi
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "SELECT pg_drop_replication_slot('$METRICS_SLOT');" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP PUBLICATION IF EXISTS $METRICS_PUB;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS metrics_test CASCADE;" 2>/dev/null || true
    PGPASSWORD="$DB_PASSWORD" $PSQL -c "DROP TABLE IF EXISTS changes CASCADE;" 2>/dev/null || true
}

# Clean up any previous state
metrics_cleanup

# Create test table
query "CREATE TABLE metrics_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"

# Start Zemi with METRICS_PORT
SLOT_NAME="$METRICS_SLOT" \
PUBLICATION_NAME="$METRICS_PUB" \
METRICS_PORT="$METRICS_PORT_NUM" \
"$ZEMI_BIN" > /tmp/zemi-e2e-metrics.log 2>&1 &
METRICS_ZEMI_PID=$!

sleep 3

if kill -0 "$METRICS_ZEMI_PID" 2>/dev/null; then
    # Insert a row to generate a tracked change
    query "INSERT INTO metrics_test (name) VALUES ('metrics-item');"

    # Wait for the change to be processed
    METRICS_ELAPSED=0
    while [ "$METRICS_ELAPSED" -lt 15 ]; do
        METRICS_COUNT=$(query "SELECT COUNT(*) FROM changes WHERE \"table\" = 'metrics_test';" 2>/dev/null || echo "0")
        if [ "$METRICS_COUNT" -ge 1 ]; then break; fi
        sleep 1
        METRICS_ELAPSED=$((METRICS_ELAPSED + 1))
    done

    # Fetch metrics endpoint
    METRICS_OUTPUT=$(curl -s --max-time 5 "http://127.0.0.1:${METRICS_PORT_NUM}/metrics" 2>/dev/null || echo "")

    assert_contains "metrics: endpoint returns zemi_changes_processed_total" "zemi_changes_processed_total" "$METRICS_OUTPUT"
    assert_contains "metrics: endpoint returns zemi_uptime_seconds" "zemi_uptime_seconds" "$METRICS_OUTPUT"
    assert_contains "metrics: endpoint returns TYPE annotations" "# TYPE zemi_changes_processed_total counter" "$METRICS_OUTPUT"

    # Verify non-metrics path returns 'ok'
    HEALTH_OUTPUT=$(curl -s --max-time 5 "http://127.0.0.1:${METRICS_PORT_NUM}/" 2>/dev/null || echo "")
    assert_eq "metrics: non-metrics path returns ok" "ok" "$HEALTH_OUTPUT"
else
    echo "  $(red "FAIL") Zemi failed to start for metrics endpoint test"
    echo "  --- Metrics Zemi logs:"
    cat /tmp/zemi-e2e-metrics.log
    FAIL=$((FAIL + 4))
    TOTAL=$((TOTAL + 4))
fi

metrics_cleanup

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

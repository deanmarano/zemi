#!/usr/bin/env bash
# Zemi vs Bemi Throughput Benchmark
#
# Runs 4 benchmark scenarios against both Zemi and original Bemi, measuring:
#   1. Sustained INSERT throughput (changes/sec)
#   2. Mixed operations throughput (INSERT + UPDATE + DELETE)
#   3. Large transaction throughput (bulk inserts per transaction)
#   4. End-to-end latency (WAL event to changes table)
#
# Usage:
#   ./test/benchmark.sh                    # Full benchmark (requires Docker)
#   ./test/benchmark.sh --zemi-only        # Skip Bemi comparison
#   ./test/benchmark.sh --bemi-only        # Skip Zemi comparison
#   ./test/benchmark.sh --summary-only     # Only print results from previous runs
#   ./test/benchmark.sh --scenario=inserts # Run specific scenario
#
# Prerequisites:
#   - Docker and docker compose
#   - Zig 0.14.1 (for building Zemi from source)
#   - psql (PostgreSQL client)

set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────

SOURCE_HOST="${SOURCE_HOST:-127.0.0.1}"
SOURCE_PORT="${SOURCE_PORT:-5440}"
SOURCE_DB="${SOURCE_DB:-bench_source}"
DEST_HOST="${DEST_HOST:-127.0.0.1}"
DEST_PORT="${DEST_PORT:-5441}"
DEST_DB="${DEST_DB:-bench_dest}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-postgres}"

# Benchmark parameters
SUSTAINED_INSERT_COUNT="${SUSTAINED_INSERT_COUNT:-10000}"
MIXED_OPS_COUNT="${MIXED_OPS_COUNT:-5000}"           # per operation type
LARGE_TXN_ROWS="${LARGE_TXN_ROWS:-5000}"
LARGE_TXN_COUNT="${LARGE_TXN_COUNT:-5}"
LATENCY_SAMPLE_COUNT="${LATENCY_SAMPLE_COUNT:-20}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300}"                   # seconds to wait for changes

ZEMI_ONLY=false
BEMI_ONLY=false
SUMMARY_ONLY=false
SCENARIO_FILTER=""
SKIP_DOCKER=false
NO_BUILD=false

# Bemi Docker image
BEMI_IMAGE="public.ecr.aws/bemi/dev:latest"

# Results storage
RESULTS_DIR="/tmp/benchmark-results"
ZEMI_RESULTS="$RESULTS_DIR/zemi"
BEMI_RESULTS="$RESULTS_DIR/bemi"

# ─── Parse arguments ──────────────────────────────────────────────────────────

for arg in "$@"; do
  case $arg in
    --zemi-only)     ZEMI_ONLY=true ;;
    --bemi-only)     BEMI_ONLY=true ;;
    --summary-only)  SUMMARY_ONLY=true ;;
    --scenario=*)    SCENARIO_FILTER="${arg#*=}" ;;
    --skip-docker)   SKIP_DOCKER=true ;;
    --no-build)      NO_BUILD=true ;;
    *)               echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# ─── Helpers ──────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

log()  { echo -e "${BLUE}[bench]${NC} $*" >&2; }
ok()   { echo -e "${GREEN}[bench]${NC} $*" >&2; }
warn() { echo -e "${YELLOW}[bench]${NC} $*" >&2; }
err()  { echo -e "${RED}[bench]${NC} $*" >&2; }

psql_source() {
  PGPASSWORD="$DB_PASSWORD" psql -h "$SOURCE_HOST" -p "$SOURCE_PORT" -U "$DB_USER" -d "$SOURCE_DB" -t -A "$@"
}

psql_dest() {
  PGPASSWORD="$DB_PASSWORD" psql -h "$DEST_HOST" -p "$DEST_PORT" -U "$DB_USER" -d "$DEST_DB" -t -A "$@"
}

now_ms() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    python3 -c 'import time; print(int(time.time() * 1000))'
  else
    date +%s%3N
  fi
}

cleanup_tracker() {
  local tracker_name="$1"
  if [[ "$tracker_name" == "zemi" ]]; then
    if [[ -n "${ZEMI_PID:-}" ]] && kill -0 "$ZEMI_PID" 2>/dev/null; then
      kill "$ZEMI_PID" 2>/dev/null || true
      # Wait up to 5 seconds for graceful shutdown, then force kill
      local waited=0
      while kill -0 "$ZEMI_PID" 2>/dev/null && [[ $waited -lt 5 ]]; do
        sleep 1
        waited=$((waited + 1))
      done
      if kill -0 "$ZEMI_PID" 2>/dev/null; then
        kill -9 "$ZEMI_PID" 2>/dev/null || true
      fi
      wait "$ZEMI_PID" 2>/dev/null || true
    fi
    ZEMI_PID=""
  elif [[ "$tracker_name" == "bemi" ]]; then
    docker rm -f bemi-benchmark 2>/dev/null || true
  fi
}

cleanup_all() {
  # Skip cleanup in summary-only mode (no infrastructure to clean up)
  if [[ "${SUMMARY_ONLY:-false}" == "true" ]]; then return 0; fi
  log "Cleaning up..."
  cleanup_tracker "zemi"
  cleanup_tracker "bemi"
  # Wait briefly for PG to detect disconnected replication clients
  sleep 1
  # Drop only inactive replication slots (active slots would block forever)
  psql_source -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name IN ('bench_zemi', 'bemi_local') AND NOT active;" > /dev/null 2>&1 || true
  psql_source -c "DROP PUBLICATION IF EXISTS bench_zemi;" > /dev/null 2>&1 || true
  psql_source -c "DROP PUBLICATION IF EXISTS bemi_local;" > /dev/null 2>&1 || true
  psql_source -c "DROP PUBLICATION IF EXISTS dbz_publication;" > /dev/null 2>&1 || true
}

trap cleanup_all EXIT

# Create the bench_items table if it doesn't exist
create_bench_table() {
  psql_source -c "CREATE TABLE IF NOT EXISTS bench_items (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
  );" > /dev/null
  psql_source -c "ALTER TABLE bench_items REPLICA IDENTITY FULL;" > /dev/null
}

# Reset the test tables and changes table between scenarios.
# Uses TRUNCATE instead of DROP/CREATE to avoid breaking Debezium's table tracking.
reset_tables() {
  log "Resetting tables..."
  psql_source -c "TRUNCATE bench_items RESTART IDENTITY;" > /dev/null 2>&1 || true
  # Wait for the TRUNCATE WAL event to propagate, then clean dest twice
  # to handle the race where the TRUNCATE change arrives after the first DELETE.
  sleep 2
  psql_dest -c "DELETE FROM changes WHERE \"table\" = 'bench_items';" > /dev/null 2>&1 || true
  sleep 1
  psql_dest -c "DELETE FROM changes WHERE \"table\" = 'bench_items';" > /dev/null 2>&1 || true
}

# Verify a tracker is actually streaming by inserting a canary row and waiting for it
verify_tracker_streaming() {
  local tracker="$1"
  local timeout_secs="${2:-30}"
  log "[$tracker] Verifying tracker is streaming (canary insert)..."

  # Insert a canary row
  psql_source -c "INSERT INTO bench_items (name, value) VALUES ('__canary__', 0);" > /dev/null

  # Wait for the canary to appear in the changes table
  local waited=0
  while [[ $waited -lt $timeout_secs ]]; do
    local found
    found=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation != 'TRUNCATE' AND after::text LIKE '%__canary__%';" 2>/dev/null || echo "0")
    found="${found//[[:space:]]/}"
    if [[ "$found" -ge 1 ]]; then
      ok "[$tracker] Tracker is streaming (canary detected in ${waited}s)"
      # Clean up canary row and reset for benchmarks
      psql_source -c "TRUNCATE bench_items RESTART IDENTITY;" > /dev/null
      # Wait for TRUNCATE WAL event to propagate, then wipe dest changes
      sleep 2
      psql_dest -c "DELETE FROM changes WHERE \"table\" = 'bench_items';" > /dev/null 2>&1 || true
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done

  err "[$tracker] Tracker is NOT streaming after ${timeout_secs}s — canary not detected"
  return 1
}

# Wait until the expected number of non-TRUNCATE changes appear in the destination.
# TRUNCATE operations from reset_tables() are excluded to avoid off-by-one races.
# Returns the elapsed time in milliseconds.
wait_for_changes() {
  local expected_count="$1"
  local start_ms
  start_ms=$(now_ms)
  local deadline=$((start_ms + WAIT_TIMEOUT * 1000))

  while true; do
    local current
    current=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation != 'TRUNCATE';" 2>/dev/null || echo "0")
    current="${current//[[:space:]]/}"

    if [[ "$current" -ge "$expected_count" ]]; then
      local end_ms
      end_ms=$(now_ms)
      echo $((end_ms - start_ms))
      return 0
    fi

    local now
    now=$(now_ms)
    if [[ "$now" -ge "$deadline" ]]; then
      err "Timeout waiting for $expected_count changes (got $current after ${WAIT_TIMEOUT}s)"
      local end_ms
      end_ms=$(now_ms)
      echo $((end_ms - start_ms))
      return 1
    fi

    sleep 0.5
  done
}

# ─── Tracker management ──────────────────────────────────────────────────────

start_zemi() {
  local slot_name="${1:-bench_zemi}"
  local pub_name="${2:-bench_zemi}"
  log "Starting Zemi (slot=$slot_name, pub=$pub_name)..."
  DB_HOST="$SOURCE_HOST" \
  DB_PORT="$SOURCE_PORT" \
  DB_NAME="$SOURCE_DB" \
  DB_USER="$DB_USER" \
  DB_PASSWORD="$DB_PASSWORD" \
  DEST_DB_HOST="$DEST_HOST" \
  DEST_DB_PORT="$DEST_PORT" \
  DEST_DB_NAME="$DEST_DB" \
  DEST_DB_USER="$DB_USER" \
  DEST_DB_PASSWORD="$DB_PASSWORD" \
  SLOT_NAME="$slot_name" \
  PUBLICATION_NAME="$pub_name" \
  LOG_LEVEL=warn \
  HEALTH_PORT=0 \
  ./zig-out/bin/zemi > /tmp/zemi-bench.log 2>&1 &
  ZEMI_PID=$!
  # Wait for Zemi to be ready (it connects quickly)
  sleep 2
  if ! kill -0 "$ZEMI_PID" 2>/dev/null; then
    err "Zemi failed to start. Logs:"
    tail -20 /tmp/zemi-bench.log
    return 1
  fi
  ok "Zemi started (PID=$ZEMI_PID)"
}

start_bemi() {
  local network_mode=""
  # In CI (Linux), use host networking so Bemi can reach localhost PG
  if [[ "$OSTYPE" != "darwin"* ]]; then
    network_mode="--network=host"
  fi

  log "Starting Bemi..."
  local bemi_source_host="$SOURCE_HOST"
  local bemi_dest_host="$DEST_HOST"
  if [[ "$OSTYPE" == "darwin"* ]]; then
    bemi_source_host="host.docker.internal"
    bemi_dest_host="host.docker.internal"
  fi

  docker run -d --name bemi-benchmark \
    $network_mode \
    -e DB_HOST="$bemi_source_host" \
    -e DB_PORT="$SOURCE_PORT" \
    -e DB_NAME="$SOURCE_DB" \
    -e DB_USER="$DB_USER" \
    -e DB_PASSWORD="$DB_PASSWORD" \
    -e DESTINATION_DB_HOST="$bemi_dest_host" \
    -e DESTINATION_DB_PORT="$DEST_PORT" \
    -e DESTINATION_DB_NAME="$DEST_DB" \
    -e DESTINATION_DB_USER="$DB_USER" \
    -e DESTINATION_DB_PASSWORD="$DB_PASSWORD" \
    "$BEMI_IMAGE" > /dev/null 2>&1

  # Bemi has a ~20-30 second startup (NATS + Debezium + Worker)
  log "Waiting for Bemi to start (this takes ~30 seconds)..."
  local waited=0
  while [[ $waited -lt 60 ]]; do
    if docker logs bemi-benchmark 2>&1 | grep -q "Listening for changes" 2>/dev/null; then
      break
    fi
    # Also check if the worker started via replication slot
    if psql_source -c "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'bemi_local' AND active = true;" 2>/dev/null | grep -q "1"; then
      break
    fi
    sleep 2
    waited=$((waited + 2))
  done

  # Give the worker a few more seconds to fully initialize
  sleep 5

  if ! docker ps -q -f name=bemi-benchmark | grep -q .; then
    err "Bemi container is not running. Logs:"
    docker logs bemi-benchmark 2>&1 | tail -30
    return 1
  fi
  ok "Bemi started (waited ${waited}s)"
}

# ─── Benchmark scenarios ──────────────────────────────────────────────────────

# Scenario 1: Sustained INSERT throughput
run_sustained_inserts() {
  local tracker="$1"
  local result_file="$2"
  local count="$SUSTAINED_INSERT_COUNT"

  log "[$tracker] Scenario: Sustained INSERTs ($count rows)..."
  reset_tables

  # Generate load: rapid-fire inserts
  local gen_start
  gen_start=$(now_ms)

  psql_source -c "
    INSERT INTO bench_items (name, value, data)
    SELECT
      'item-' || i,
      i,
      repeat('x', 100)
    FROM generate_series(1, $count) AS i;
  " > /dev/null

  local gen_end
  gen_end=$(now_ms)
  local gen_time=$((gen_end - gen_start))

  log "[$tracker] Generated $count inserts in ${gen_time}ms. Waiting for changes..."

  local elapsed
  elapsed=$(wait_for_changes "$count") || true

  local actual_count
  actual_count=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation != 'TRUNCATE';" 2>/dev/null)
  actual_count="${actual_count//[[:space:]]/}"

  local throughput=0
  if [[ "$elapsed" -gt 0 ]]; then
    throughput=$(python3 -c "print(round($actual_count / ($elapsed / 1000.0), 1))")
  fi

  echo "sustained_inserts_count=$actual_count" >> "$result_file"
  echo "sustained_inserts_expected=$count" >> "$result_file"
  echo "sustained_inserts_elapsed_ms=$elapsed" >> "$result_file"
  echo "sustained_inserts_gen_ms=$gen_time" >> "$result_file"
  echo "sustained_inserts_throughput=$throughput" >> "$result_file"

  ok "[$tracker] Sustained INSERTs: $actual_count/$count changes in ${elapsed}ms ($throughput changes/sec)"
}

# Scenario 2: Mixed operations (INSERT + UPDATE + DELETE)
run_mixed_ops() {
  local tracker="$1"
  local result_file="$2"
  local count="$MIXED_OPS_COUNT"

  log "[$tracker] Scenario: Mixed operations ($count each of INSERT/UPDATE/DELETE)..."
  reset_tables

  local gen_start
  gen_start=$(now_ms)

  # Phase 1: Insert rows
  psql_source -c "
    INSERT INTO bench_items (name, value, data)
    SELECT 'item-' || i, i, repeat('x', 50)
    FROM generate_series(1, $count) AS i;
  " > /dev/null

  # Phase 2: Update half of them
  psql_source -c "
    UPDATE bench_items SET value = value + 1000, data = repeat('y', 50)
    WHERE id <= $count / 2;
  " > /dev/null

  # Phase 3: Delete the other half
  psql_source -c "
    DELETE FROM bench_items WHERE id > $count / 2;
  " > /dev/null

  local gen_end
  gen_end=$(now_ms)
  local gen_time=$((gen_end - gen_start))

  # Expected: count inserts + count/2 updates + count/2 deletes = 2 * count
  local expected=$((count * 2))
  log "[$tracker] Generated mixed ops in ${gen_time}ms. Waiting for $expected changes..."

  local elapsed
  elapsed=$(wait_for_changes "$expected") || true

  local actual_count
  actual_count=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation != 'TRUNCATE';" 2>/dev/null)
  actual_count="${actual_count//[[:space:]]/}"

  local insert_count update_count delete_count
  insert_count=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation = 'CREATE';" 2>/dev/null)
  update_count=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation = 'UPDATE';" 2>/dev/null)
  delete_count=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation = 'DELETE';" 2>/dev/null)
  insert_count="${insert_count//[[:space:]]/}"
  update_count="${update_count//[[:space:]]/}"
  delete_count="${delete_count//[[:space:]]/}"

  local throughput=0
  if [[ "$elapsed" -gt 0 ]]; then
    throughput=$(python3 -c "print(round($actual_count / ($elapsed / 1000.0), 1))")
  fi

  echo "mixed_ops_count=$actual_count" >> "$result_file"
  echo "mixed_ops_expected=$expected" >> "$result_file"
  echo "mixed_ops_inserts=$insert_count" >> "$result_file"
  echo "mixed_ops_updates=$update_count" >> "$result_file"
  echo "mixed_ops_deletes=$delete_count" >> "$result_file"
  echo "mixed_ops_elapsed_ms=$elapsed" >> "$result_file"
  echo "mixed_ops_gen_ms=$gen_time" >> "$result_file"
  echo "mixed_ops_throughput=$throughput" >> "$result_file"

  ok "[$tracker] Mixed ops: $actual_count/$expected changes in ${elapsed}ms ($throughput changes/sec)"
}

# Scenario 3: Large transactions
run_large_txns() {
  local tracker="$1"
  local result_file="$2"
  local rows="$LARGE_TXN_ROWS"
  local txns="$LARGE_TXN_COUNT"
  local total=$((rows * txns))

  log "[$tracker] Scenario: Large transactions ($txns txns x $rows rows = $total total)..."
  reset_tables

  local gen_start
  gen_start=$(now_ms)

  for i in $(seq 1 "$txns"); do
    psql_source -c "
      BEGIN;
      INSERT INTO bench_items (name, value, data)
      SELECT 'txn${i}-item-' || j, j, repeat('z', 200)
      FROM generate_series(1, $rows) AS j;
      COMMIT;
    " > /dev/null
  done

  local gen_end
  gen_end=$(now_ms)
  local gen_time=$((gen_end - gen_start))

  log "[$tracker] Generated $txns large transactions in ${gen_time}ms. Waiting for $total changes..."

  local elapsed
  elapsed=$(wait_for_changes "$total") || true

  local actual_count
  actual_count=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND operation != 'TRUNCATE';" 2>/dev/null)
  actual_count="${actual_count//[[:space:]]/}"

  local throughput=0
  if [[ "$elapsed" -gt 0 ]]; then
    throughput=$(python3 -c "print(round($actual_count / ($elapsed / 1000.0), 1))")
  fi

  echo "large_txn_count=$actual_count" >> "$result_file"
  echo "large_txn_expected=$total" >> "$result_file"
  echo "large_txn_elapsed_ms=$elapsed" >> "$result_file"
  echo "large_txn_gen_ms=$gen_time" >> "$result_file"
  echo "large_txn_throughput=$throughput" >> "$result_file"

  ok "[$tracker] Large txns: $actual_count/$total changes in ${elapsed}ms ($throughput changes/sec)"
}

# Scenario 4: End-to-end latency
run_latency() {
  local tracker="$1"
  local result_file="$2"
  local count="$LATENCY_SAMPLE_COUNT"

  log "[$tracker] Scenario: End-to-end latency ($count samples)..."
  reset_tables

  # Insert rows one at a time, recording when we inserted and when the change appeared
  local latencies=""
  local success_count=0

  for i in $(seq 1 "$count"); do
    local insert_time
    insert_time=$(now_ms)

    psql_source -c "INSERT INTO bench_items (name, value) VALUES ('latency-$i', $i);" > /dev/null

    # Poll for this specific change with a short timeout
    local found=false
    local deadline=$((insert_time + 30000))  # 30 second timeout per row (Bemi polls every ~10s)
    while true; do
      local change_exists
      change_exists=$(psql_dest -c "SELECT COUNT(*) FROM changes WHERE \"table\" = 'bench_items' AND after::text LIKE '%latency-$i%';" 2>/dev/null)
      change_exists="${change_exists//[[:space:]]/}"
      if [[ "$change_exists" -ge 1 ]]; then
        found=true
        break
      fi
      local now
      now=$(now_ms)
      if [[ "$now" -ge "$deadline" ]]; then
        break
      fi
      sleep 0.05
    done

    if $found; then
      local detect_time
      detect_time=$(now_ms)
      local latency=$((detect_time - insert_time))
      latencies="${latencies}${latency}\n"
      success_count=$((success_count + 1))
    fi
  done

  # Calculate percentiles using python
  if [[ "$success_count" -gt 0 ]]; then
    local stats
    stats=$(echo -e "$latencies" | python3 -c "
import sys
vals = sorted([int(l.strip()) for l in sys.stdin if l.strip()])
n = len(vals)
print(f'p50={vals[n//2]}')
print(f'p95={vals[int(n*0.95)]}')
print(f'p99={vals[int(n*0.99)]}')
print(f'min={vals[0]}')
print(f'max={vals[-1]}')
print(f'avg={sum(vals)//n}')
")
    local p50 p95 p99 lat_min lat_max lat_avg
    p50=$(echo "$stats" | grep "p50=" | cut -d= -f2)
    p95=$(echo "$stats" | grep "p95=" | cut -d= -f2)
    p99=$(echo "$stats" | grep "p99=" | cut -d= -f2)
    lat_min=$(echo "$stats" | grep "min=" | cut -d= -f2)
    lat_max=$(echo "$stats" | grep "max=" | cut -d= -f2)
    lat_avg=$(echo "$stats" | grep "avg=" | cut -d= -f2)

    echo "latency_samples=$success_count" >> "$result_file"
    echo "latency_p50_ms=$p50" >> "$result_file"
    echo "latency_p95_ms=$p95" >> "$result_file"
    echo "latency_p99_ms=$p99" >> "$result_file"
    echo "latency_min_ms=$lat_min" >> "$result_file"
    echo "latency_max_ms=$lat_max" >> "$result_file"
    echo "latency_avg_ms=$lat_avg" >> "$result_file"

    ok "[$tracker] Latency ($success_count samples): p50=${p50}ms p95=${p95}ms p99=${p99}ms min=${lat_min}ms max=${lat_max}ms"
  else
    err "[$tracker] No latency samples collected"
    echo "latency_samples=0" >> "$result_file"
  fi
}

# ─── Run all scenarios for a tracker ──────────────────────────────────────────

run_benchmarks() {
  local tracker="$1"
  local result_file="$2"

  > "$result_file"  # truncate

  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "inserts" ]]; then
    run_sustained_inserts "$tracker" "$result_file"
    # Clean up between scenarios
    psql_dest -c "DELETE FROM changes WHERE \"table\" = 'bench_items';" > /dev/null 2>&1 || true
  fi

  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "mixed" ]]; then
    run_mixed_ops "$tracker" "$result_file"
    psql_dest -c "DELETE FROM changes WHERE \"table\" = 'bench_items';" > /dev/null 2>&1 || true
  fi

  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "large-txn" ]]; then
    run_large_txns "$tracker" "$result_file"
    psql_dest -c "DELETE FROM changes WHERE \"table\" = 'bench_items';" > /dev/null 2>&1 || true
  fi

  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "latency" ]]; then
    run_latency "$tracker" "$result_file"
  fi
}

# ─── Results formatting ──────────────────────────────────────────────────────

get_result() {
  local file="$1"
  local key="$2"
  local default="${3:-N/A}"
  if [[ -f "$file" ]]; then
    local val
    val=$(grep "^${key}=" "$file" 2>/dev/null | tail -1 | cut -d= -f2)
    echo "${val:-$default}"
  else
    echo "$default"
  fi
}

print_results() {
  local zemi_file="$ZEMI_RESULTS"
  local bemi_file="$BEMI_RESULTS"

  echo ""
  echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
  echo -e "${BOLD}  Benchmark Results: Zemi vs Bemi${NC}"
  echo -e "${BOLD}═══════════════════════════════════════════════════════════════${NC}"
  echo ""

  # Sustained INSERTs
  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "inserts" ]]; then
    echo -e "${BOLD}Sustained INSERTs ($SUSTAINED_INSERT_COUNT rows)${NC}"
    printf "  %-20s %15s %15s\n" "" "Zemi" "Bemi"
    printf "  %-20s %12s ms %12s ms\n" "Total time" \
      "$(get_result "$zemi_file" sustained_inserts_elapsed_ms)" \
      "$(get_result "$bemi_file" sustained_inserts_elapsed_ms)"
    printf "  %-20s %12s/s %12s/s\n" "Throughput" \
      "$(get_result "$zemi_file" sustained_inserts_throughput)" \
      "$(get_result "$bemi_file" sustained_inserts_throughput)"
    printf "  %-20s %12s %15s\n" "Changes captured" \
      "$(get_result "$zemi_file" sustained_inserts_count)" \
      "$(get_result "$bemi_file" sustained_inserts_count)"
    echo ""
  fi

  # Mixed operations
  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "mixed" ]]; then
    local expected=$((MIXED_OPS_COUNT * 2))
    echo -e "${BOLD}Mixed Operations ($expected total: INSERT + UPDATE + DELETE)${NC}"
    printf "  %-20s %15s %15s\n" "" "Zemi" "Bemi"
    printf "  %-20s %12s ms %12s ms\n" "Total time" \
      "$(get_result "$zemi_file" mixed_ops_elapsed_ms)" \
      "$(get_result "$bemi_file" mixed_ops_elapsed_ms)"
    printf "  %-20s %12s/s %12s/s\n" "Throughput" \
      "$(get_result "$zemi_file" mixed_ops_throughput)" \
      "$(get_result "$bemi_file" mixed_ops_throughput)"
    printf "  %-20s %12s %15s\n" "Changes captured" \
      "$(get_result "$zemi_file" mixed_ops_count)" \
      "$(get_result "$bemi_file" mixed_ops_count)"
    echo ""
  fi

  # Large transactions
  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "large-txn" ]]; then
    local total=$((LARGE_TXN_ROWS * LARGE_TXN_COUNT))
    echo -e "${BOLD}Large Transactions ($LARGE_TXN_COUNT txns x $LARGE_TXN_ROWS rows)${NC}"
    printf "  %-20s %15s %15s\n" "" "Zemi" "Bemi"
    printf "  %-20s %12s ms %12s ms\n" "Total time" \
      "$(get_result "$zemi_file" large_txn_elapsed_ms)" \
      "$(get_result "$bemi_file" large_txn_elapsed_ms)"
    printf "  %-20s %12s/s %12s/s\n" "Throughput" \
      "$(get_result "$zemi_file" large_txn_throughput)" \
      "$(get_result "$bemi_file" large_txn_throughput)"
    printf "  %-20s %12s %15s\n" "Changes captured" \
      "$(get_result "$zemi_file" large_txn_count)" \
      "$(get_result "$bemi_file" large_txn_count)"
    echo ""
  fi

  # Latency
  if [[ -z "$SCENARIO_FILTER" || "$SCENARIO_FILTER" == "latency" ]]; then
    echo -e "${BOLD}End-to-End Latency ($LATENCY_SAMPLE_COUNT samples)${NC}"
    printf "  %-20s %15s %15s\n" "" "Zemi" "Bemi"
    printf "  %-20s %12s ms %12s ms\n" "p50" \
      "$(get_result "$zemi_file" latency_p50_ms)" \
      "$(get_result "$bemi_file" latency_p50_ms)"
    printf "  %-20s %12s ms %12s ms\n" "p95" \
      "$(get_result "$zemi_file" latency_p95_ms)" \
      "$(get_result "$bemi_file" latency_p95_ms)"
    printf "  %-20s %12s ms %12s ms\n" "p99" \
      "$(get_result "$zemi_file" latency_p99_ms)" \
      "$(get_result "$bemi_file" latency_p99_ms)"
    printf "  %-20s %12s ms %12s ms\n" "min" \
      "$(get_result "$zemi_file" latency_min_ms)" \
      "$(get_result "$bemi_file" latency_min_ms)"
    printf "  %-20s %12s ms %12s ms\n" "max" \
      "$(get_result "$zemi_file" latency_max_ms)" \
      "$(get_result "$bemi_file" latency_max_ms)"
    echo ""
  fi
}

# Generate GitHub Actions job summary (markdown)
generate_summary() {
  local zemi_file="$ZEMI_RESULTS"
  local bemi_file="$BEMI_RESULTS"
  local summary_file="${GITHUB_STEP_SUMMARY:-/tmp/benchmark-summary.md}"

  cat > "$summary_file" <<EOF
## Benchmark Results: Zemi vs Bemi

### Sustained INSERTs ($SUSTAINED_INSERT_COUNT rows)
| Metric | Zemi | Bemi |
|--------|------|------|
| Total time | $(get_result "$zemi_file" sustained_inserts_elapsed_ms) ms | $(get_result "$bemi_file" sustained_inserts_elapsed_ms) ms |
| Throughput | $(get_result "$zemi_file" sustained_inserts_throughput) /s | $(get_result "$bemi_file" sustained_inserts_throughput) /s |
| Changes captured | $(get_result "$zemi_file" sustained_inserts_count) | $(get_result "$bemi_file" sustained_inserts_count) |

### Mixed Operations ($((MIXED_OPS_COUNT * 2)) total)
| Metric | Zemi | Bemi |
|--------|------|------|
| Total time | $(get_result "$zemi_file" mixed_ops_elapsed_ms) ms | $(get_result "$bemi_file" mixed_ops_elapsed_ms) ms |
| Throughput | $(get_result "$zemi_file" mixed_ops_throughput) /s | $(get_result "$bemi_file" mixed_ops_throughput) /s |
| Changes captured | $(get_result "$zemi_file" mixed_ops_count) | $(get_result "$bemi_file" mixed_ops_count) |

### Large Transactions ($LARGE_TXN_COUNT txns x $LARGE_TXN_ROWS rows)
| Metric | Zemi | Bemi |
|--------|------|------|
| Total time | $(get_result "$zemi_file" large_txn_elapsed_ms) ms | $(get_result "$bemi_file" large_txn_elapsed_ms) ms |
| Throughput | $(get_result "$zemi_file" large_txn_throughput) /s | $(get_result "$bemi_file" large_txn_throughput) /s |
| Changes captured | $(get_result "$zemi_file" large_txn_count) | $(get_result "$bemi_file" large_txn_count) |

### End-to-End Latency ($LATENCY_SAMPLE_COUNT samples)
| Metric | Zemi | Bemi |
|--------|------|------|
| p50 | $(get_result "$zemi_file" latency_p50_ms) ms | $(get_result "$bemi_file" latency_p50_ms) ms |
| p95 | $(get_result "$zemi_file" latency_p95_ms) ms | $(get_result "$bemi_file" latency_p95_ms) ms |
| p99 | $(get_result "$zemi_file" latency_p99_ms) ms | $(get_result "$bemi_file" latency_p99_ms) ms |
| min | $(get_result "$zemi_file" latency_min_ms) ms | $(get_result "$bemi_file" latency_min_ms) ms |
| max | $(get_result "$zemi_file" latency_max_ms) ms | $(get_result "$bemi_file" latency_max_ms) ms |
EOF

  log "Summary written to $summary_file"
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
  mkdir -p "$RESULTS_DIR"

  # ── Summary-only mode: skip all benchmarks, just format results ──
  if [[ "$SUMMARY_ONLY" == "true" ]]; then
    log "Summary-only mode: formatting results from previous runs..."
    print_results
    generate_summary
    ok "Summary complete!"
    return 0
  fi

  # Start infrastructure
  if [[ "$SKIP_DOCKER" != "true" ]]; then
    log "Starting benchmark PostgreSQL instances..."
    docker compose -f docker-compose.benchmark.yml up -d --wait
  fi

  # Wait for PostgreSQL to be ready
  log "Waiting for source PostgreSQL..."
  for i in $(seq 1 30); do
    if psql_source -c "SELECT 1;" > /dev/null 2>&1; then break; fi
    sleep 1
  done
  log "Waiting for destination PostgreSQL..."
  for i in $(seq 1 30); do
    if psql_dest -c "SELECT 1;" > /dev/null 2>&1; then break; fi
    sleep 1
  done

  # Build Zemi (skip if --no-build or --bemi-only)
  if [[ "$NO_BUILD" != "true" && "$BEMI_ONLY" != "true" ]]; then
    log "Building Zemi..."
    zig build 2>&1
  fi

  # Create the bench table once (used by both trackers)
  create_bench_table

  # ── Run Zemi benchmarks ──
  if [[ "$BEMI_ONLY" != "true" ]]; then
    # Ensure Zemi starts with a clean changes table (Bemi may have left its own schema)
    psql_dest -c "DROP TABLE IF EXISTS _bemi_migrations CASCADE;" > /dev/null 2>&1 || true
    psql_dest -c "DROP TABLE IF EXISTS changes CASCADE;" > /dev/null 2>&1 || true

    echo ""
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  Running benchmarks: ZEMI${NC}"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    start_zemi "bench_zemi" "bench_zemi"
    verify_tracker_streaming "zemi" 15
    run_benchmarks "zemi" "$ZEMI_RESULTS"
    cleanup_tracker "zemi"

    # Clean up Zemi's replication slot and publication
    sleep 1
    psql_source -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'bench_zemi' AND NOT active;" > /dev/null 2>&1 || true
    psql_source -c "DROP PUBLICATION IF EXISTS bench_zemi;" > /dev/null 2>&1 || true
  fi

  # ── Run Bemi benchmarks ──
  if [[ "$ZEMI_ONLY" != "true" ]]; then
    echo ""
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  Running benchmarks: BEMI${NC}"
    echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    # Drop Zemi's changes table AND Bemi's migration table for a clean Bemi start.
    # Bemi uses MikroORM migrations that only run if _bemi_migrations is missing/incomplete.
    psql_dest -c "DROP TABLE IF EXISTS _bemi_migrations CASCADE;" > /dev/null 2>&1 || true
    psql_dest -c "DROP TABLE IF EXISTS changes CASCADE;" > /dev/null 2>&1 || true

    # Ensure bench_items exists (Bemi needs it in the publication from the start)
    create_bench_table

    # Pull Bemi image if needed
    log "Pulling Bemi image..."
    docker pull "$BEMI_IMAGE" 2>/dev/null || warn "Failed to pull Bemi image (may use cached)"

    start_bemi
    verify_tracker_streaming "bemi" 60
    run_benchmarks "bemi" "$BEMI_RESULTS"
    cleanup_tracker "bemi"

    # Clean up Bemi's replication slot and publication
    sleep 1
    psql_source -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'bemi_local' AND NOT active;" > /dev/null 2>&1 || true
    psql_source -c "DROP PUBLICATION IF EXISTS dbz_publication;" > /dev/null 2>&1 || true
  fi

  # ── Print results ──
  print_results
  generate_summary

  # Tear down
  if [[ "$SKIP_DOCKER" != "true" ]]; then
    log "Stopping benchmark infrastructure..."
    docker compose -f docker-compose.benchmark.yml down -v 2>/dev/null || true
  fi

  ok "Benchmark complete!"
}

main

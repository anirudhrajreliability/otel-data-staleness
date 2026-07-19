#!/usr/bin/env bash
# Runs the five real-workload validations against an ALREADY-RUNNING integration
# stack (deploy/integration). Asserts NUMERIC CORRECTNESS, not just presence.
# Assumes Prometheus is reachable at $PROM and the Collector is scraping.
#   Bring the stack up first (see scripts/ec2-integration.sh), then run this.
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"   # repo root

COMPOSE="deploy/integration/docker-compose.yaml"
PROM="${PROM:-http://localhost:9090}"
V="python3 deploy/integration/validate.py --prom $PROM"
DC="sudo docker compose -f $COMPOSE"

PASS=0; FAIL=0
run() { # run "<label>" <validate.py args...>
  local label="$1"; shift
  echo "------------------------------------------------------------"
  echo "CHECK: $label"
  if "$@"; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); echo ">> FAILED: $label"; fi
}

echo "############################################################"
echo "# Real-workload validation suite"
echo "############################################################"

# ---------------------------------------------------------------------------
# GAP #2 — ACCURACY: inject a KNOWN epoch (120s ago) into demo.accuracy and
# assert last_update.timestamp equals it exactly and age is ~120s.
# ---------------------------------------------------------------------------
echo "Waiting for Postgres seed (demo.accuracy) to be created..."
for i in $(seq 1 45); do
  if $DC exec -T postgres psql -U postgres -tAc "SELECT to_regclass('demo.accuracy')" 2>/dev/null | grep -q accuracy; then
    echo "  demo.accuracy is ready"; break
  fi
  sleep 2
done
EPOCH=$(date -d '120 seconds ago' +%s)
echo "Injecting demo.accuracy row with updated_at = epoch $EPOCH (120s ago)"
for attempt in 1 2 3; do
  if $DC exec -T postgres psql -U postgres -q -c \
      "INSERT INTO demo.accuracy (updated_at) VALUES (to_timestamp($EPOCH));" 2>/dev/null; then
    echo "  injected"; break
  fi
  echo "  insert attempt $attempt failed; retrying"; sleep 3
done
run "accuracy: age == known injected offset (±)" \
  $V --wait 120 age --source orders_accuracy --expect-epoch "$EPOCH" --ts-tol 3 --age-tol 30

# ---------------------------------------------------------------------------
# GAP #1 — SCALE / KNOWN LAG: orders_stream has a pinned backlog of 100;
# events_scale is a sustained multi-partition topic.
# ---------------------------------------------------------------------------
run "scale: consumer lag == known backlog (100)" \
  $V --wait 150 records-behind --source orders_stream --expect 100 --tol 10
run "scale: multi-partition topic emits age (events_scale)" \
  $V --wait 120 present --source events_scale

# ---------------------------------------------------------------------------
# GAP #3 — AWS-NATIVE LIVE: Kinesis (LocalStack), Schema Registry, DB migration.
# ---------------------------------------------------------------------------
run "aws: Kinesis stream freshness (LocalStack)" \
  $V --wait 150 present --source clickstream
run "aws: Schema Registry version drift detected" \
  $V --wait 150 series --source orders_schema --metric data_staleness_records_behind --min 1
run "aws: DB migration version drift detected" \
  $V --wait 120 series --source db_schema --metric data_staleness_records_behind --min 1

# ---------------------------------------------------------------------------
# GAP #4 — SDK IN THE LIVE PATH: SDK service probes real Postgres + Redis,
# exports via OTLP -> Collector -> Prometheus.
# ---------------------------------------------------------------------------
run "sdk: SQL probe over real Postgres (sdk_orders)" \
  $V --wait 150 present --source sdk_orders
run "sdk: cache probe over real Redis (sdk_cache)" \
  $V --wait 150 present --source sdk_cache

# ---------------------------------------------------------------------------
# GAP #5 — CHAOS / FAILURE:
#   (a) clock skew -> age clamps to >= 0
#   (b) stop Postgres -> probe.errors becomes VISIBLE (not a fabricated 0)
#   (c) restart Postgres -> the source recovers
# ---------------------------------------------------------------------------
run "chaos: future timestamp clamps age to >= 0 (orders_skew)" \
  $V --wait 120 nonnegative --source orders_skew

echo "------------------------------------------------------------"
echo "CHAOS: stopping Postgres to force a visible probe failure..."
$DC stop postgres >/dev/null 2>&1
sleep 25
run "chaos: broken source surfaces probe.errors (not silent)" \
  $V --wait 90 probe-errors --source orders_fresh --min 1
echo "CHAOS: restarting Postgres..."
$DC start postgres >/dev/null 2>&1
run "chaos: source recovers after Postgres returns (orders_fresh)" \
  $V --wait 120 present --source orders_fresh

echo "############################################################"
echo "# RESULT: $PASS passed, $FAIL failed"
echo "############################################################"
[ "$FAIL" -eq 0 ] || exit 1
echo "ALL REAL-WORKLOAD VALIDATIONS PASSED ✅"

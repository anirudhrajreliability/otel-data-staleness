#!/usr/bin/env bash
# Asserts the demo actually produces data.staleness.* metrics and that a
# deliberately-stale source is breaching its SLA. Exits non-zero on failure.
set -uo pipefail
PROM="${PROM:-http://localhost:9090}"
count() { curl -s "$PROM/api/v1/query" --data-urlencode "query=$1" \
  | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["data"]["result"]))' 2>/dev/null || echo 0; }

echo "Waiting for the stack and for staleness to develop (up to ~2.5 min)..."
for i in $(seq 1 30); do
  sleep 5
  ages=$(count 'data_staleness_age')
  breaching=$(count 'data_staleness_sla_breached == 1')
  echo "  [$i] age series=$ages  breaching sources=$breaching"
  if [ "${ages:-0}" -ge 3 ] && [ "${breaching:-0}" -ge 1 ]; then
    echo
    echo "PASS ✅  data.staleness.* metrics are flowing and a stale source is breaching its SLA."
    echo "  Prometheus: $PROM  (try: data_staleness_age, data_staleness_sla_breached)"
    echo "  Grafana:    http://localhost:3000  -> dashboard 'Data Staleness' (admin/admin)"
    exit 0
  fi
done
echo
echo "FAIL ❌  expected metrics did not appear in time."
echo "  Debug: sudo docker compose -f deploy/ec2-demo/docker-compose.yaml logs otel-collector | tail -50"
exit 1

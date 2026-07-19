#!/usr/bin/env bash
# Keeps the "fresh" sources fresh: demo.fresh, demo.sdk_orders, and /data/fresh.txt.
# Seeds /data/stale.txt once (never touched again -> age climbs).
set -e
export PGPASSWORD=demopw
echo "freshener: waiting for postgres..."
until psql -h postgres -U postgres -c 'select 1' >/dev/null 2>&1; do sleep 2; done
[ -f /data/stale.txt ] || echo "stale" > /data/stale.txt
echo "freshener: updating demo.fresh + demo.sdk_orders + /data/fresh.txt every 5s"
while true; do
  psql -h postgres -U postgres -c "INSERT INTO demo.fresh (updated_at) VALUES (now());" >/dev/null 2>&1 || true
  psql -h postgres -U postgres -c "INSERT INTO demo.sdk_orders (updated_at) VALUES (now());" >/dev/null 2>&1 || true
  date +%s > /data/fresh.txt
  sleep 5
done

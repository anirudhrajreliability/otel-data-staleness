#!/usr/bin/env bash
set -e
export PGPASSWORD=demopw
echo "waiting for postgres..."
until psql -h postgres -U postgres -c 'select 1' >/dev/null 2>&1; do sleep 2; done
# seed the stale file once (never touched again -> age climbs)
[ -f /data/stale.txt ] || echo "stale" > /data/stale.txt
echo "freshener running: updating demo.fresh + /data/fresh.txt every 10s"
while true; do
  psql -h postgres -U postgres -c "INSERT INTO demo.fresh (updated_at) VALUES (now());" >/dev/null 2>&1 || true
  date +%s > /data/fresh.txt
  sleep 10
done

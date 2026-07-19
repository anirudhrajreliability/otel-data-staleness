#!/usr/bin/env bash
# Kafka load generator that creates two deterministic conditions:
#   1. orders_stream (1 partition): a KNOWN consumer-group backlog of 100 records,
#      so data.staleness.records.behind can be asserted exactly.
#   2. events_scale (4 partitions): a SUSTAINED producer, so the multi-partition
#      topic stays under load (per-partition freshness + scale).
set -e
B=/opt/kafka/bin
BS=kafka:9092
echo "loadgen: waiting for kafka..."
until $B/kafka-topics.sh --bootstrap-server $BS --list >/dev/null 2>&1; do sleep 3; done

# --- 1. deterministic consumer lag on a single-partition topic ---------------
$B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic orders_stream --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
echo "loadgen: producing 1000 records to orders_stream"
for i in $(seq 1 1000); do echo "order-$i"; done \
  | $B/kafka-console-producer.sh --bootstrap-server $BS --topic orders_stream >/dev/null 2>&1

# Create the consumer group by reading one record (group must exist to reset).
$B/kafka-console-consumer.sh --bootstrap-server $BS --topic orders_stream \
  --group analytics --max-messages 1 --timeout-ms 20000 >/dev/null 2>&1 || true
# Pin the committed offset to 900 -> end(1000) - committed(900) = 100 lag exactly.
$B/kafka-consumer-groups.sh --bootstrap-server $BS --group analytics \
  --topic orders_stream:0 --reset-offsets --to-offset 900 --execute >/dev/null 2>&1 || true
echo "loadgen: orders_stream committed offset pinned to 900 (expected records.behind=100)"

# --- 2. sustained multi-partition producer (scale) ---------------------------
$B/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic events_scale --partitions 4 --replication-factor 1 >/dev/null 2>&1 || true
echo "loadgen: sustained producer on events_scale (4 partitions) running"
n=0
while true; do
  for i in $(seq 1 200); do echo "evt-$n-$i"; done \
    | $B/kafka-console-producer.sh --bootstrap-server $BS --topic events_scale >/dev/null 2>&1 || true
  n=$((n+1))
  sleep 2
done

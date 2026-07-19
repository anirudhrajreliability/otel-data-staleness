#!/usr/bin/env bash
set -e
B=/opt/kafka/bin
echo "waiting for kafka..."
until $B/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do sleep 3; done
$B/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic events --partitions 2 >/dev/null 2>&1 || true
echo "producing a burst of 20 records to 'events', then stopping (topic age will climb)"
for i in $(seq 1 20); do echo "event-$i"; done | $B/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic events
echo "burst done; producer exiting."

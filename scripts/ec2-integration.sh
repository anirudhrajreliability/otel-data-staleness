#!/usr/bin/env bash
# One-command DEEP validation: brings up the comprehensive integration stack
# (real Postgres + Kafka + Redis + LocalStack Kinesis + Confluent Schema Registry
# + SDK service) and runs the five real-workload validations.
#
# Prereqs are installed by scripts/ec2-bootstrap.sh; run that first (or on a box
# that already has Docker + Python). Recommended instance: t3.xlarge (16 GB) —
# the Schema Registry JVM + LocalStack + Kafka + two image builds need headroom.
#   Run from the repo root:  bash scripts/ec2-integration.sh
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"   # repo root
step() { echo -e "\n============================================================\n== $*\n============================================================"; }

INT="deploy/integration/docker-compose.yaml"
DEMO="deploy/ec2-demo/docker-compose.yaml"

step "Freeing ports (tear down the simple demo if it is running)"
sudo docker compose -f "$DEMO" down -v >/dev/null 2>&1 || true

step "Building + starting the integration stack (this pulls SR/LocalStack + builds 2 images)"
sudo docker compose -f "$INT" up -d --build

step "Waiting for Prometheus to come up"
for i in $(seq 1 30); do
  curl -sf http://localhost:9090/-/ready >/dev/null 2>&1 && break
  sleep 3
done

step "Running the real-workload validation suite"
set +e
bash scripts/integration-suite.sh
rc=$?
set -e

step "Integration stack is still running for inspection"
echo "  Prometheus: http://localhost:9090"
echo "  Tear down:  sudo docker compose -f $INT down -v"
exit $rc

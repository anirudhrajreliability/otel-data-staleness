#!/usr/bin/env bash
# One-command validation on a fresh Ubuntu 24.04 EC2 instance.
# Installs prerequisites, runs every test suite, builds & validates the custom
# Collector, brings up the real-backend demo, and smoke-tests the metrics.
#   Run from the repo root:  bash scripts/ec2-bootstrap.sh
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"   # repo root
step() { echo -e "\n============================================================\n== $*\n============================================================"; }

step "Installing prerequisites (Go 1.25, Docker, Python)"
sudo apt-get update -y
sudo apt-get install -y git build-essential python3 python3-pip python3-venv curl
if ! command -v go >/dev/null 2>&1 || ! go version 2>/dev/null | grep -qE 'go1\.(2[5-9]|[3-9][0-9])'; then
  curl -sSL https://go.dev/dl/go1.25.4.linux-amd64.tar.gz -o /tmp/go.tgz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
fi
export PATH="$PATH:/usr/local/go/bin:$HOME/go/bin"
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER" || true
fi

step "Python SDK + conformance"
python3 -m venv .venv && source .venv/bin/activate
pip install -q -e "python/.[dev,otlp,version]"
( cd python && python -m pytest -q )
PYTHONPATH=python/src python conformance/runner.py

step "Go components (processor + receiver)"
for d in collector/datastalenessprocessor collector/datastalenessreceiver; do
  ( cd "$d" && go build ./... && go vet ./... && go test ./... )
done

step "Build + validate the custom Collector"
go install go.opentelemetry.io/collector/cmd/builder@v0.110.0
builder --config=collector/builder-config.yaml
./_build/otelcol-datastaleness validate --config=deploy/ec2-demo/collector-config.yaml
echo "collector config OK"

step "Start the real-backend demo (Postgres + Kafka + Collector + Prometheus + Grafana)"
sudo docker compose -f deploy/ec2-demo/docker-compose.yaml up -d --build

step "Smoke test"
bash scripts/smoke-test.sh

# Deep real-workload validation (accuracy, scale, AWS-native, SDK-live, chaos).
# Opt-in because it stands up a heavier stack (Schema Registry + LocalStack +
# Redis + SDK build) — recommended on t3.xlarge. Set RUN_INTEGRATION=1 to run it
# automatically after the smoke test; otherwise run it yourself when ready.
if [ "${RUN_INTEGRATION:-0}" = "1" ]; then
  step "Deep real-workload validation (RUN_INTEGRATION=1)"
  bash scripts/ec2-integration.sh
else
  echo
  echo "Smoke test done. For the FULL real-workload validation (numeric accuracy,"
  echo "scale/known-lag, LocalStack Kinesis + Schema Registry + DB-migration drift,"
  echo "the SDK in the live path, and chaos/failure), run:"
  echo "    bash scripts/ec2-integration.sh        # recommended on t3.xlarge"
  echo "(or re-run this script with RUN_INTEGRATION=1)."
fi

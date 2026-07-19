# EC2 Deploy & Test Runbook

End-to-end guide to provision an EC2 instance and validate **every** part of the
`otel-data-staleness` project: the Python SDK, conformance suite, both Go
Collector components, the custom Collector build (OCB), the Docker image, the
docker-compose demo (Collector + Prometheus + Grafana), the zero-code receiver,
and the Helm chart.

Everything here was dry-run in a Linux sandbox; commands assume **Ubuntu 24.04
LTS**. Where a step needed a workaround in the sandbox (flaky module proxy),
it's called out as *not needed on EC2*.

> Time: ~30–45 min. Cost: a `t3.large` is ~$0.08/hr; remember to terminate
> (Step 11). Nothing here needs production data.

---

## TL;DR — one command

On a fresh Ubuntu 24.04 EC2 instance, from the repo root:

```bash
bash scripts/ec2-bootstrap.sh
```

This installs prerequisites, runs **every** test suite, builds + validates the
custom Collector, brings up a demo with **real Postgres + Kafka** behind the
zero-code receiver, and smoke-tests that `data.staleness.*` metrics flow and a
stale source breaches its SLA (exit 0 = success). See
[`../deploy/ec2-demo/README.md`](../deploy/ec2-demo/README.md). The sections
below are the manual, step-by-step version.

## 0. What you'll validate

| Aspect | Step | Success signal |
|--------|------|----------------|
| Python SDK | 4 | `21 passed` |
| Conformance suite | 4 | `PASS — 0 mismatch(es)` |
| Go processor | 5 | `ok ...datastalenessprocessor` |
| Go receiver | 5 | `ok ...datastalenessreceiver` |
| Custom Collector (OCB) | 6 | `Compiled` + `validate rc=0` |
| Docker image | 7 | image builds; `validate` rc=0 |
| Demo stack + Grafana | 8 | dashboard shows a source breaching SLA |
| Zero-code receiver | 9 | `data_staleness_age` climbing, breach flips to 1 |
| Helm chart | 10 | `helm template` renders 3 resources |

---

## 1. Provision the instance

Recommended: **t3.large** (2 vCPU / 8 GB — Go + Docker builds need the RAM),
**30 GB gp3**, **Ubuntu Server 24.04 LTS**.

Console: EC2 → Launch instance → Ubuntu 24.04 → t3.large → 30 GB → create/select
a key pair → Security group: **inbound SSH (22) from your IP only**. Do *not*
open 3000/9090 to the world — we'll use SSH tunnels instead.

Or via CLI (adjust IDs):

```bash
aws ec2 run-instances \
  --image-id resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --instance-type t3.large \
  --key-name YOUR_KEY \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=otel-staleness-test}]'
```

SSH in:

```bash
ssh -i YOUR_KEY.pem ubuntu@<EC2_PUBLIC_IP>
```

---

## 2. Install prerequisites

```bash
sudo apt-get update
sudo apt-get install -y git build-essential python3 python3-pip python3-venv curl

# Go 1.25+ (required to build the receiver, which pulls modern collector deps)
curl -sSL https://go.dev/dl/go1.25.4.linux-amd64.tar.gz -o /tmp/go.tgz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
echo 'export PATH=$PATH:/usr/local/go/bin:$(go env GOPATH)/bin' >> ~/.bashrc
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin
go version   # go1.25.x

# Docker + compose plugin
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update && sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER && newgrp docker   # use docker without sudo
docker version

# Helm (for Step 10)
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

---

## 3. Get the code

Option A — push the project to your own GitHub repo, then:

```bash
git clone https://github.com/<you>/otel-data-staleness.git
cd otel-data-staleness
```

Option B — copy the folder from your laptop:

```bash
# run on your laptop:
scp -i YOUR_KEY.pem -r ./otel-data-staleness ubuntu@<EC2_PUBLIC_IP>:~/
# then on EC2:
cd ~/otel-data-staleness
```

---

## 4. Python SDK + conformance

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e "python/.[dev,otlp]"

cd python && python -m pytest -q && cd ..             # expect: 21 passed
PYTHONPATH=python/src python conformance/runner.py    # expect: PASS — 0 mismatch(es)
```

---

## 5. Go Collector components

```bash
for d in collector/datastalenessprocessor collector/datastalenessreceiver; do
  ( cd "$d" && go build ./... && go vet ./... && test -z "$(gofmt -l .)" && go test ./... )
done
# expect: ok ...datastalenessprocessor  and  ok ...datastalenessreceiver
```

---

## 6. Build the custom Collector (OCB)

```bash
go install go.opentelemetry.io/collector/cmd/builder@v0.110.0

# IMPORTANT: run from the repo root — builder-config.yaml's replace paths
# (../collector/...) are resolved relative to the generated ./_build/go.mod.
builder --config=collector/builder-config.yaml          # ends with: Compiled

./_build/otelcol-datastaleness components | grep -A2 datastaleness   # both registered
./_build/otelcol-datastaleness validate --config=collector/example-config.yaml
echo "validate rc=$?"                                    # expect: 0
```

Notes:
- If `go install`/`builder` hits a transient proxy error, just retry — it's the
  module proxy, not the config. (`GOSUMDB=off` was only needed in the locked
  sandbox; you don't need it on EC2.)
- The build pulls the full Collector toolchain on first run (a few minutes).
- The manifest ships only the `env` + `file` confmap providers (they are the
  versions published on the 0.110.0 line); that's all a file-based config needs.

---

## 7. Build the Docker image

```bash
docker build -f collector/Dockerfile -t otelcol-datastaleness:local .
docker run --rm -v "$PWD/collector/example-config.yaml":/etc/otelcol/config.yaml \
  otelcol-datastaleness:local validate --config=/etc/otelcol/config.yaml
echo "rc=$?"   # expect: 0
```

---

## 8. Run the demo stack (Collector + Prometheus + Grafana)

This uses the **stock** Collector image (the SDK emits over OTLP), so no custom
build is required for the demo.

```bash
cd deploy
docker compose up -d
docker compose ps          # otel-collector, prometheus, grafana all "Up"
cd ..

# feed it (in the venv from Step 4)
source .venv/bin/activate
OTLP_ENDPOINT=http://localhost:4318 python deploy/demo/generator.py &
```

View the UIs from your **laptop** via SSH tunnel (keeps ports closed publicly):

```bash
# run on your laptop:
ssh -i YOUR_KEY.pem -N \
  -L 3000:localhost:3000 -L 9090:localhost:9090 ubuntu@<EC2_PUBLIC_IP>
```

Then open:
- Grafana → http://localhost:3000 → dashboard **"Data Staleness"**. Within ~30s
  the `clickstream` source crosses its 30s SLA: **"Sources breaching SLA"** flips
  to 1 and its age line climbs.
- Prometheus → http://localhost:9090 → query `data_staleness_age`; check
  Status → Rules for `DataStalenessSLABreached`.

Stop the generator with `kill %1`; `cd deploy && docker compose down` when done.

---

## 9. Zero-code receiver (no application code)

Run the custom Collector binary from Step 6 with a file source and watch it
detect staleness purely from a file's age.

```bash
cat > /tmp/recv.yaml <<'YAML'
receivers:
  datastaleness:
    collection_interval: 5s
    sources:
      - type: file
        name: nightly_dump
        system: s3
        path: /tmp/nightly.parquet
        sla_threshold_seconds: 30
exporters:
  debug: { verbosity: detailed }
service:
  pipelines:
    metrics:
      receivers: [datastaleness]
      exporters: [debug]
YAML

touch /tmp/nightly.parquet            # "fresh" now
./_build/otelcol-datastaleness --config=/tmp/recv.yaml &
sleep 7                                # age small, breached=0
sleep 30                              # now older than the 30s SLA
# In the debug output you'll see data.staleness.age climbing and
# data.staleness.sla.breached flip 0 -> 1. Stop with: kill %1
```

> Tip: don't enable the processor's `compute_age_from_last_update` *and* the
> receiver together — the receiver already emits `data.staleness.age`, so you'd
> get duplicate series. Use the processor only for centralized SLA policy.

### 9a. Validate the SQL scraper against a real database

The `sql` scraper is compiled with the Postgres and MySQL drivers. Spin up a
throwaway Postgres, seed a row, and point a source at it:

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16
sleep 5
docker exec -i pg psql -U postgres -c \
  "CREATE TABLE orders(id int, updated_at timestamptz); \
   INSERT INTO orders VALUES (1, now() - interval '120 seconds');"

cat > /tmp/sql.yaml <<'YAML'
receivers:
  datastaleness:
    collection_interval: 5s
    sources:
      - type: sql
        name: orders
        system: postgresql
        driver: postgres
        dsn: "postgres://postgres:pw@localhost:5432/postgres?sslmode=disable"
        table: orders
        timestamp_column: updated_at
        sla_threshold_seconds: 60
exporters:
  debug: { verbosity: detailed }
service:
  pipelines:
    metrics: { receivers: [datastaleness], exporters: [debug] }
YAML

./_build/otelcol-datastaleness --config=/tmp/sql.yaml &
# Expect data.staleness.age ~120 and data.staleness.sla.breached=1 (120 > 60).
# Then DROP the row and watch it emit a `null_timestamp` probe error instead of
# a fake age:  docker exec -i pg psql -U postgres -c "DELETE FROM orders;"
# Stop with: kill %1 ; docker rm -f pg
```

---

### 9b. Validate the Kafka scraper against a real broker

Spin up a single-node Kafka, create a topic, produce a couple of records, then
point a source at it (PLAINTEXT — no auth):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.8.0
sleep 10
KT="docker exec kafka /opt/kafka/bin"
$KT/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic clickstream --partitions 2
printf 'a\nb\nc\n' | $KT/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic clickstream

cat > /tmp/kafka.yaml <<'YAML'
receivers:
  datastaleness:
    collection_interval: 5s
    sources:
      - type: kafka
        name: clickstream
        system: kafka
        brokers: [localhost:9092]
        topic: clickstream
        consumer_group: analytics     # created lazily; lag shows once a consumer commits
        sla_threshold_seconds: 30
exporters:
  debug: { verbosity: detailed }
service:
  pipelines:
    metrics: { receivers: [datastaleness], exporters: [debug] }
YAML

./_build/otelcol-datastaleness --config=/tmp/kafka.yaml &
# Expect data.staleness.age per partition (small at first, climbing as no new
# records arrive), and data.staleness.records.behind once the group commits.
# Stop with: kill %1 ; docker rm -f kafka
```

For SASL/MSK, set `sasl_mechanism` (`scram-sha-512`, `aws-msk-iam`, …) and the
credentials/environment described in the receiver README. For **Kafka consumer
time-lag**, add `measure_time_lag: true` to the source — it emits
`data.staleness.lag` (now − timestamp at the committed offset).

### 9c. Validate the Kinesis scraper

Against a real stream (needs AWS creds via the instance role or env):

```bash
aws kinesis create-stream --stream-name clickstream --shard-count 1 --region us-east-1
aws kinesis put-record --stream-name clickstream --partition-key k --data "$(date)" --region us-east-1

cat > /tmp/kin.yaml <<'YAML'
receivers:
  datastaleness:
    collection_interval: 10s
    sources:
      - type: kinesis
        name: clickstream
        system: kinesis
        stream_name: clickstream
        aws_region: us-east-1
        lookback: 30m
        sla_threshold_seconds: 60
exporters:
  debug: { verbosity: detailed }
service:
  pipelines:
    metrics: { receivers: [datastaleness], exporters: [debug] }
YAML

./_build/otelcol-datastaleness --config=/tmp/kin.yaml &
# Expect data.staleness.age per shard from the record's arrival time; it climbs
# until you put another record. Stop: kill %1 ; aws kinesis delete-stream ...
```

No AWS handy? `localstack` provides a Kinesis endpoint; point the SDK at it via
`AWS_ENDPOINT_URL` and dummy creds for a local smoke test.

## 10. Helm chart (optional, needs a cluster)

Render without a cluster (always works):

```bash
helm lint deploy/helm/datastaleness-collector
helm template ds deploy/helm/datastaleness-collector   # ConfigMap, Service, Deployment
```

Deploy to a throwaway in-VM cluster:

```bash
# kind + kubectl
go install sigs.k8s.io/kind@latest
curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install kubectl /usr/local/bin/kubectl
kind create cluster
kind load docker-image otelcol-datastaleness:local
helm install ds deploy/helm/datastaleness-collector \
  --set image.repository=otelcol-datastaleness --set image.tag=local
kubectl get pods                    # ds-... Running
kubectl port-forward svc/ds 8889:8889 &
curl -s localhost:8889/metrics | grep data_staleness   # metrics exposed
```

---

## 11. Teardown

```bash
cd ~/otel-data-staleness/deploy && docker compose down
kind delete cluster 2>/dev/null || true
# then terminate the instance:
aws ec2 terminate-instances --instance-ids <INSTANCE_ID>
```

---

## Appendix: optional checks

- **Paper**: `sudo apt-get install -y texlive-latex-recommended texlive-latex-extra && cd paper && pdflatex main.tex` → `main.pdf` (7 pages).
- **Weaver model**: install `weaver` and run `weaver registry check -r model/registry` to lint the semantic-convention model the way the OTEL SIG would.
- **Run the CI locally**: every command above mirrors `.github/workflows/ci.yml`; pushing to GitHub runs all 7 jobs automatically.

> Part of **otel-data-staleness** — see the [root README](../README.md) for the project overview and the SDK-vs-Collector comparison.

# Deployment & demos

Everything here runs the system against **real** backends — nothing emits
simulated staleness values.

| Path | What it is |
|------|------------|
| [`ec2-demo/`](ec2-demo/) | One-command smoke demo: the custom Collector scrapes **real Postgres + Kafka + files**, into Prometheus + a **Grafana** dashboard. Fresh sources stay ~0s; "stale" sources climb and breach their SLA. |
| [`integration/`](integration/) | Full **real-workload validation** stack: Postgres, Kafka, Redis, LocalStack Kinesis, Confluent Schema Registry, and the SDK in the live path. Asserts numeric accuracy, scale, AWS-native paths, and failure behavior. See [`../docs/INTEGRATION-VALIDATION.md`](../docs/INTEGRATION-VALIDATION.md). |
| [`helm/`](helm/) | Helm chart to deploy the custom Collector on Kubernetes. |
| `grafana/`, `prometheus-alerts.yml` | Shared dashboard, provisioning, and freshness alert rules used by the demos. |

## Quick start (smoke demo)

On a machine with Docker (or a fresh EC2 box — see
[`../docs/EC2-BEGINNER-GUIDE.md`](../docs/EC2-BEGINNER-GUIDE.md)):

```bash
sudo docker compose -f ec2-demo/docker-compose.yaml up -d --build
bash ../scripts/smoke-test.sh
```

- **Grafana**: http://localhost:3000 (anonymous admin) → dashboard **"Data Staleness"**
- **Prometheus**: http://localhost:9090 → try `data_staleness_age`,
  `data_staleness_sla_breached == 1`

The first `up --build` compiles the custom Collector (a few minutes); reach the
UIs over an SSH tunnel rather than opening ports publicly.

## Full real-workload validation

```bash
bash ../scripts/ec2-integration.sh    # recommended on t3.xlarge
```

## Useful PromQL

```promql
data_staleness_sla_breached == 1                       # breaching sources
max by (data_source_system) (data_staleness_age)       # worst per system
increase(data_staleness_probe_errors[10m]) > 0         # failing probes
```

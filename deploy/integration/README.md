> Part of **otel-data-staleness** — see the [root README](../../README.md) for the project overview.

# Integration stack — real-workload validation

A comprehensive stack that validates **numeric accuracy**, **scale**, the
**AWS-native** paths, the **SDK in the live path**, and **failure behavior** —
the five gaps beyond the quick smoke test. Full explanation:
[`../../docs/INTEGRATION-VALIDATION.md`](../../docs/INTEGRATION-VALIDATION.md).

## Run it

On a box prepared by `scripts/ec2-bootstrap.sh` (**t3.xlarge recommended**):

```bash
bash scripts/ec2-integration.sh          # build + up + run all checks
# or, if the stack is already up:
bash scripts/integration-suite.sh
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yaml` | Postgres, Kafka, Redis, LocalStack, Schema Registry, Collector, SDK service, Prometheus. |
| `collector-config.yaml` | Scrapes every source type + accepts OTLP from the SDK service. |
| `seed.sql` | Tables for the fresh/stale/accuracy/skew/migration/SDK sources. |
| `freshener.sh` / (redis-freshener) | Keep the "fresh" sources fresh. |
| `loadgen.sh` | Deterministic Kafka consumer lag + sustained multi-partition load. |
| `localstack-init.sh` | Creates the Kinesis stream + puts records (real Kinesis API). |
| `sr-seed.sh` | Registers two schema versions (drift). |
| `sdk-service/` | The Python SDK probing real Postgres + Redis, exporting via OTLP. |
| `validate.py` | The assertion engine (queries Prometheus, asserts correctness). |
| `test_validate.py` | Unit tests for the assertion math (run in CI, no containers). |

## Teardown

```bash
sudo docker compose -f deploy/integration/docker-compose.yaml down -v
```

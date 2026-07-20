# Real-workload validation

The quick EC2 demo proves `data.staleness.*` metrics *flow* from real Postgres +
Kafka + files. This suite goes further: it asserts the metrics are **numerically
correct** and that every producer path — including the AWS-native ones and the
Python SDK — works against real (or faithfully emulated) backends, plus how the
system behaves under **failure**. It exists to close the five honest gaps in the
smoke test.

## What it stands up

`deploy/integration/docker-compose.yaml` brings up:

- **Postgres 16** — `fresh`, `stale`, `accuracy`, `skew`, `schema_migrations`, and
  `sdk_orders` tables.
- **Kafka 3.8** + a **load generator** — a single-partition topic with a *pinned*
  consumer backlog and a sustained 4-partition topic.
- **Redis 7** + a freshener — the cache the SDK probes.
- **LocalStack** — a real Kinesis API (stream created + records put).
- **Confluent Schema Registry** — two registered schema versions.
- The **custom Collector** — scraping all of the above (SQL, Kafka, file, Kinesis
  via an endpoint override, Schema Registry, DB migration) and accepting **OTLP**.
- The **Python SDK service** — probing real Postgres + Redis and exporting via
  OTLP to the Collector, so SDK and receiver metrics land in one Prometheus with
  identical names.

## How to run it

On a prepared box (run `scripts/ec2-bootstrap.sh` first for Docker + Python;
**t3.xlarge / 16 GB recommended** — Schema Registry's JVM plus LocalStack plus two
image builds need headroom):

```bash
bash scripts/ec2-integration.sh
```

That builds + starts the stack and runs `scripts/integration-suite.sh`, which
prints a per-check `PASS/FAIL` and a final tally (**11 checks** — the SDK row
below is two: `sdk_orders` and `sdk_cache`). Exit code `0` = all passed.

## What each check proves

| # | Gap it closes | Check | Assertion |
|---|---------------|-------|-----------|
| 2 | Numeric accuracy | `accuracy` | Inject a row with a **known epoch (120 s ago)**; assert `last_update.timestamp` equals it (±3 s, non-drifting) **and** `age` ≈ 120 s (±30 s). Proves the number is *right*, not just present. |
| 1 | Scale / real lag | `scale: consumer lag` | Pin the `analytics` group's committed offset to 900 of 1000 records; assert `records.behind == 100` (±10). Deterministic lag against a real broker. |
| 1 | Scale | `scale: multi-partition` | A sustained 4-partition producer; assert per-partition `age` is emitted. |
| 3 | AWS-native | `Kinesis (LocalStack)` | Real Kinesis `create-stream` + `put-record` via an **endpoint override**; assert stream freshness is emitted. |
| 3 | AWS-native | `Schema Registry drift` | Register v1 + v2, pin documented=1; assert `records.behind ≥ 1` (version drift detected live). |
| 3 | AWS-native | `DB migration drift` | Applied `0005` vs expected `0007`; assert drift is emitted. |
| 4 | SDK in the live path | `sdk_orders`, `sdk_cache` | The **SDK** (not the receiver) probes real Postgres + Redis and its series appear in Prometheus via OTLP. |
| 5 | Failure — clock skew | `nonnegative` | A **future** timestamp; assert `age` clamps to ≥ 0 (never a negative/fabricated value). |
| 5 | Failure — visibility | `probe.errors` | **Stop Postgres**; assert `data.staleness.probe.errors` increments — a broken check is *visible*, not silently zero. |
| 5 | Failure — recovery | `recovers` | **Restart Postgres**; assert the source resumes emitting a fresh age. |

## The assertion engine is itself tested

`deploy/integration/validate.py` holds the accuracy math (tolerance comparison,
per-partition summing, Prometheus parsing). Its pure helpers are unit-tested in
`deploy/integration/test_validate.py`, and the SDK service wiring in
`deploy/integration/sdk-service/test_sdk_probe_service.py` — both run in normal CI
without any containers, so the correctness logic is verified even off-box.

## Honest limitations

- **LocalStack ≠ AWS.** The Kinesis path is exercised against LocalStack's Kinesis
  API. It validates the scraper's request/response handling and the new endpoint
  override end to end, but it is an emulator; a final pass against a real stream is
  still worthwhile.
- **MSK IAM is not covered here.** SASL/SCRAM auth plumbing is unit-tested, but
  `aws-msk-iam` SigV4 auth requires a real Amazon MSK cluster and IAM role — there
  is no local emulator. Validate it directly against MSK using the config in the
  receiver README (inject `AWS_*` from the instance role). This is the one path
  that remains real-AWS-only.
- **Scale is "sustained," not "stress."** The load generator keeps topics under
  continuous load and produces a deterministic lag; it is not a throughput
  benchmark. Freshness is a low-frequency, low-cardinality signal, so this
  validates correctness under load rather than raw ceiling.

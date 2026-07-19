# otel-data-staleness

A vendor-neutral **OpenTelemetry semantic convention for data staleness (data
freshness)**, plus a reference implementation: a Python SDK plus
OpenTelemetry Collector receiver and processor components. Released under Apache-2.0.

> **What problem this solves.** Freshness is a well-studied quantity — it is the
> *Age of Information* from networked-systems research, and it is what every
> data-observability vendor already computes internally. But there is no
> portable way to *emit* it. OpenTelemetry standardizes latency, errors, and
> throughput across vendors; it has **no** convention for "how old is my data."
> This project proposes one and ships the code to produce it. The contribution
> is **standardization and consolidation**, not a new metric.

## Versions

Everything is currently at **`0.4.0`** — the *convention version* is the number
that unifies the whole project; each implementation tracks it.

| Component | Latest version |
|-----------|----------------|
| **Convention / spec** (`spec/semantic-conventions.md`) | **0.4.0** |
| Python SDK — `otel-staleness` | 0.4.0 |
| Collector receiver — `datastalenessreceiver` | 0.4.0 |
| Collector processor — `datastalenessprocessor` | 0.4.0 |
| Weaver model / conformance suite | tracks 0.4.0 |
| Built & tested against | OpenTelemetry Collector `v0.110.0`, pdata `v1.16.0` |

Stability: **Development** — names and versions may change before they
stabilize. See [`spec/semantic-conventions.md`](spec/semantic-conventions.md)
for the per-release changelog.

## Highlights

- **Measures freshness across everything**: SQL/warehouses, Kafka, Kinesis,
  files/HTTP, caches, search/vector indexes, replicas, dbt pipelines, and even
  RAG/documentation *version currency*.
- **Two ways to adopt**: a Python SDK for your apps, or a **zero-code** Collector
  receiver that scrapes your sources from YAML alone.
- **Honest by design**: an empty table, timeout, or unparseable value becomes a
  *visible error* — never a fabricated "0 seconds old."
- **Standard, not just a library**: a spec, a machine-readable Weaver model, and
  a language-agnostic conformance suite.
- **Provable end-to-end**: a one-command demo with real Postgres + Kafka + a
  Grafana dashboard, and a turnkey EC2 test script.

See **[`FEATURES.md`](FEATURES.md)** for the full plain-English feature list.

## Python SDK vs. Collector receiver — which to use

You need **only one** producer to start. Both emit the same `data.staleness.*`
metrics into the same backend, so they interoperate — and many teams use both
(the SDK inside their services, the receiver for infrastructure they don't own
the code for). They are **complementary, not redundant**, and their feature sets
are not identical:

| | **Python SDK** | **Collector receiver** |
|---|---|---|
| How you adopt it | `pip install` into your app | run a Collector container; sources in YAML |
| Runtime requirement | Python in your service | a prebuilt Collector binary (**no Go at runtime**) |
| Best for | code you own; app-level signals | infra you don't own the code for; **zero code changes** |
| SQL · Kafka · file · HTTP | ✓ | ✓ |
| Kinesis | — | ✓ |
| Confluent Schema Registry drift | — | ✓ |
| DB migration drift | — | ✓ |
| AWS MSK IAM auth | — | ✓ |
| RAG / package version-currency (PyPI, npm, GitHub, Docker) | ✓ | — |
| Differential (index-vs-corpus, replica-vs-primary) | ✓ | — |
| End-to-end age across hops (baggage) | ✓ | — |
| Peak age · update-interval · partition-skew | ✓ | — |
| dbt artifact integration | ✓ | — |

The optional **[Collector processor](collector/datastalenessprocessor/)** applies
central age-derivation and SLA policy to metrics from *either* producer.

```bash
# Python SDK — add to your app:
pip install -e python/
# Collector (zero-code) — build a Collector with both components:
docker build -f collector/Dockerfile -t otelcol-datastaleness .
```

> Feature asymmetry is expected for a young convention (reference
> implementations lead in different languages); today the SDK is broader on
> signals, the receiver is lower-friction and owns the AWS/streaming/registry
> integrations.

## Repository layout

| Path | What it is |
|------|------------|
| [`FEATURES.md`](FEATURES.md) | The full plain-English feature list. |
| [`spec/semantic-conventions.md`](spec/semantic-conventions.md) | The proposed `data.staleness.*` metrics and attributes (OTEP-ready). |
| [`model/registry/data-staleness.yaml`](model/registry/data-staleness.yaml) | Machine-readable OTEL **Weaver** model (validation + multi-language codegen). |
| [`python/`](python/) | `otel-staleness` — pip-installable SDK with probes for SQL, Kafka, pipelines, caches, plus differential (index/replica) probes and a **dbt integration**. |
| [`collector/datastalenessprocessor/`](collector/datastalenessprocessor/) | Go Collector metrics processor: derives age, evaluates freshness SLAs. |
| [`collector/datastalenessreceiver/`](collector/datastalenessreceiver/) | Go Collector **receiver**: zero-code scraping of SQL, Kafka, Kinesis, file, HTTP sources, plus Schema Registry & DB-migration version drift. |
| [`collector/builder-config.yaml`](collector/builder-config.yaml) + [`Dockerfile`](collector/Dockerfile) | OCB manifest + Dockerfile to build a custom Collector with both components. |
| [`conformance/`](conformance/) | Language-agnostic conformance vectors + runner (makes it a *standard*, not one library). |
| [`deploy/`](deploy/) | `docker-compose` demo (Collector + Prometheus + **Grafana** dashboard + alerts) and a **Helm chart**. |
| [`paper/`](paper/) | arXiv-style preprint (`data-staleness-otel-preprint.pdf`) and LaTeX source. |
| [`docs/STALENESS-TAXONOMY.md`](docs/STALENESS-TAXONOMY.md) | Every staleness *type* mapped to the metric/mechanism that captures it. |
| [`.github/workflows/`](.github/workflows/) | CI (Python + both Go modules + conformance + paper) and PyPI publish. |

> **This root README is the single overview.** Each folder also has a short,
> *scoped* README covering just that component (the one in `python/` doubles as
> the PyPI package page); deeper guides live in [`docs/`](docs/) and
> [`otep/`](otep/). They are not duplicates — start here, then drill down.

## The convention in one screen

Metrics (durations in seconds; timestamps in Unix epoch seconds):

- `data.staleness.age` — `now − event_time` of the freshest record (the primary, AoI-style metric).
- `data.staleness.lag` — `processing_time − event_time` of the most recent record.
- `data.staleness.last_update.timestamp` — Unix time of the last update.
- `data.staleness.records.behind` — positional backlog (e.g. Kafka lag).
- `data.staleness.sla.threshold` / `.sla.breached` / `.sla.breaches` — SLA evaluation.

Key attributes: `data.source.system`, `data.source.name`,
`data.source.namespace`, `data.staleness.method`, `data.staleness.partition`,
`data.pipeline.stage`.

`age` vs `lag`: if a source stops producing, the last record's `lag` stays
constant while `age` keeps rising — which is exactly the failure freshness
monitoring exists to catch.

## Quick start

**SDK** (emit freshness from your app):

```bash
cd python && pip install -e .
python examples/quickstart.py        # prints data.staleness.* to the console
```

**Collector** (derive age + evaluate SLAs centrally):

```bash
cd collector/datastalenessprocessor && go test ./...
```

**Demo stack**:

```bash
cd deploy && docker compose up -d    # Collector + Prometheus
```

## Status of the components

| Component | Tests | Notes |
|-----------|-------|-------|
| Python SDK | 50 passing | probes for SQL/Kafka/pipeline/cache/index/replica/version + dbt & RAG version-currency |
| Conformance suite | 6 vectors | language-agnostic spec tests |
| Go processor | 9 passing + benchmark | derives age, evaluates SLAs |
| Go receiver | 40 passing | SQL/Kafka/Kinesis/file/HTTP + schema-registry & db-migration version-drift; logic tested against hermetic fakes |
| Custom Collector | builds + validates | OCB build with both components + Postgres/MySQL + Kafka + Kinesis clients linked |
| Paper | builds | 7-page preprint, `pdflatex paper/main.tex` |

Building the custom Collector requires **Go 1.25+** (Kafka/collector deps).

## Measured overhead

- SDK: ~22–39 µs per monitored source per collection cycle.
- Processor: ~0.93 µs per data point (>1M points/s/core).

Freshness is a low-frequency, low-cardinality signal (one set of points per
source per export interval), so the overhead is negligible in practice.

## Path to standardization

The formal **OTEP** and a ready-to-post SIG discussion issue live in
[`otep/`](otep/). `spec/semantic-conventions.md` is the full draft convention.
Recommended path: publish → open the discussion issue → find a SIG sponsor →
open the OTEP PR (see [`otep/README.md`](otep/README.md)). Names are at
**Development** stability and may change before they stabilize.

## License

Apache-2.0. See [`LICENSE`](LICENSE).

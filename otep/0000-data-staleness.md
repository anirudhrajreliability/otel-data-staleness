# Data Staleness Semantic Conventions

Propose a vendor-neutral set of semantic conventions for **data staleness**
(a.k.a. data freshness): a small, comparable set of metrics and attributes that
express *how old data is* across heterogeneous data systems — relational
databases and warehouses, streaming platforms, batch/ELT pipelines, caches and
object stores, search/vector indexes, and RAG/documentation content.

<!-- toc -->

- [Motivation](#motivation)
- [Explanation](#explanation)
- [Internal details](#internal-details)
- [Trade-offs and mitigations](#trade-offs-and-mitigations)
- [Prior art and alternatives](#prior-art-and-alternatives)
- [Open questions](#open-questions)
- [Future possibilities](#future-possibilities)

<!-- tocstop -->

## Motivation

Data can be *stale* — no longer reflecting the real world — without any error or
latency signal firing. A pipeline can have zero failed operations and low
per-operation latency, yet serve hours-old data because an upstream producer
stopped. This failure mode is distinct from what traces, logs, and existing
metrics capture, and it is a first-class reliability concern for analytics, ML
feature stores, and RAG systems.

OpenTelemetry already defines semantic conventions for database client metrics
and messaging/Kafka metrics, but these describe operation **duration** and
message/row **counts** — not how *old* the data itself is. As of semantic
conventions v1.42 and the published 2026 roadmap, there is:

- no standardized metric for data freshness/staleness, and
- not even a standardized consumer-lag metric.

Meanwhile the quantity is well understood on two independent fronts. In
networked systems, the **Age of Information (AoI)** literature formalizes
freshness as the time since the most recent update was generated at its source.
In data engineering, every data-observability vendor (Monte Carlo, Metaplane,
Sifflet, …), `dbt source freshness`, and Kafka-lag tools (Burrow) compute
freshness — but each in a proprietary, siloed way. The result: freshness cannot
be compared or alerted on portably across systems, and it cannot ride the same
observability pipeline as latency and errors.

This proposal standardizes that one, well-studied quantity in OpenTelemetry. It
is explicitly a **consolidation and standardization** effort, not a new metric.

## Explanation

For a logical *source* (a table, topic, stream, model, cache namespace, index,
or document set) whose freshest record has event time `t_e`, observed/processed
at `t_p`, with current wall-clock time `t_now`:

```
lag = t_p  - t_e        # one-shot delay a record incurred moving through the pipeline
age = t_now - t_e       # >= lag; grows without bound while no newer record arrives
```

`age` is the primary AoI-style signal: if a source stops producing, `age` climbs
while the last record's `lag` stays constant — exactly the condition freshness
monitoring exists to catch.

### Core metrics

All durations use UCUM `s`; timestamps are Unix epoch seconds. Point-in-time
values are asynchronous (observable) gauges; cumulative counts are monotonic
counters.

| Metric | Instrument | Unit | Meaning |
|--------|-----------|------|---------|
| `data.staleness.age` | Gauge | `s` | `now − event_time` of the freshest data (primary). |
| `data.staleness.lag` | Gauge | `s` | `processing_time − event_time` of the most recent record. |
| `data.staleness.last_update.timestamp` | Gauge | `s` | Unix time of the last successful update. |
| `data.staleness.records.behind` | Gauge | `{record}` | Positional backlog (e.g. Kafka consumer lag). |
| `data.staleness.sla.threshold` | Gauge | `s` | Configured max acceptable age. |
| `data.staleness.sla.breached` | Gauge | `1` | `1` if `age > threshold`, else `0`. |
| `data.staleness.sla.breaches` | Counter | `{breach}` | Transitions into a breached state. |

### Key attributes

`data.source.system` (reusing `db.system.name` / `messaging.system` values where
they exist), `data.source.name`, `data.source.namespace`,
`data.staleness.method` (how the value was derived — `max_timestamp`,
`watermark`, `consumer_lag`, `run_completion`, `object_mtime`, `ttl_age`,
`heartbeat`, …), `data.staleness.partition`, and `data.pipeline.stage`.

### Per-source mapping (illustrative)

- **SQL/warehouse:** `age = now − MAX(updated_at)`, or (preferred for
  completeness) a load-audit/watermark table.
- **Kafka/Kinesis:** newest record event time for `age`; broker offset lag for
  `records.behind`; per partition/shard.
- **Batch/dbt/Airflow:** `age = now − last successful run end`.
- **Caches / object storage:** last write / last-modified time.

A full specification with every metric, attribute, method value, and per-source
recipe — plus a reference implementation (a Python SDK and two OpenTelemetry
Collector components) and a conformance test suite — accompanies this OTEP.

### Reference implementation and validation

The design is backed by a complete Apache-2.0 reference implementation, and it is
**validated end-to-end against real backends on a clean cloud instance** — not
just unit-tested. Alongside 54 SDK, 42 receiver, and 9 processor unit tests and a
language-agnostic conformance suite, a one-command suite stands up real Postgres,
Kafka, Redis, LocalStack Kinesis, and Confluent Schema Registry and asserts the
metrics are *numerically correct*, not merely present (11/11 checks green on a
fresh EC2 instance):

- **Accuracy** — inject a record with a known event-time; `last_update.timestamp`
  matches it exactly and `age` is correct to the second.
- **Scale / lag** — a pinned Kafka consumer backlog yields `records.behind == 100`
  exactly; a multi-partition topic reports per-partition freshness.
- **AWS-native paths** — live Kinesis freshness, Schema Registry version drift,
  and DB-migration drift.
- **SDK in the live path** — the SDK probes real Postgres + Redis and exports via OTLP.
- **Honest failure** — a future timestamp clamps `age` to ≥ 0; a stopped source
  surfaces `data.staleness.probe.errors` (never a fabricated `0`) and recovers on
  restart.

This exercise validated the "derive, don't emit" and "honest measurement"
principles above under real failure conditions, and surfaced a real correctness
bug (a `Decimal` returned by PostgreSQL's `EXTRACT(EPOCH …)` that the SDK
initially rejected) that presence-only testing would have missed. (The one path
still requiring real AWS is MSK IAM authentication, documented as such.)

## Internal details

- **Instrument choice.** Freshness is a current-value signal → observable
  gauges. SLA breach *events* are a monotonic counter. `update.interval`
  (an optional extension) is a histogram.
- **Composition with existing conventions.** `data.staleness.*` is orthogonal to
  `db.*` and `messaging.*`: those describe *operations*, this describes the
  *data's age*. `data.source.system` reuses `db.system.name` /
  `messaging.system` values so the two compose on the same resource.
- **Cardinality.** Freshness is low-frequency (one point set per source per
  export interval) and low-cardinality (bounded by distinct
  `(system, name, partition)` tuples). A companion reference measured
  ~22–39 µs/source of emission overhead.
- **"Derive, don't emit."** SLA budget, burn rate, SLO compliance ratio, and
  mean age are all derivable by a backend from the above and are intentionally
  NOT metrics. Only quantities a backend *cannot* reconstruct (e.g. Peak AoI,
  the inter-update-interval distribution) are proposed as optional extensions.
- **Honest measurement.** Implementations MUST surface a failed or
  indeterminable measurement (empty table, NULL `MAX()`, timeout, unparseable
  value) via `data.staleness.probe.errors` rather than emitting a fabricated
  age.

## Trade-offs and mitigations

- **Clock skew.** `age` depends on producer/consumer clock agreement. Mitigation:
  clamp to non-negative; prefer broker-authoritative timestamps (Kafka
  `LogAppendTime`) or watermark tables where available; document the trust
  boundary per source.
- **`MAX(timestamp)` optimism.** A single late row can make a table look fresh.
  Mitigation: recommend watermark/load-audit tables when completeness matters,
  and expose `records.behind`.
- **Producer-set timestamps.** Kafka `CreateTime` is only as trustworthy as the
  producer's clock. Mitigation: state it explicitly; recommend `LogAppendTime`.
- **Namespace churn.** Introducing a `data.*` root is a decision (see Open
  questions). Mitigation: start at Development stability; align naming with SIG.

## Prior art and alternatives

- **Age of Information** (Kaul, Yates, Gruteser 2012; Yates et al. survey 2021):
  the formal grounding; `data.staleness.age` is AoI applied to data systems.
- **Data-management freshness** (Peralta 2006) and **data observability**
  vendors (Monte Carlo's "five pillars", Metaplane, Sifflet): the operational
  practice this consolidates.
- **`dbt source freshness`**, **Burrow** (Kafka lag): per-tool, siloed
  precedents.
- **Existing OTEL semconv** (`db.*`, `messaging.*`): adjacent, composable, but
  cover operations, not data age.
- **Alternative: leave it to vendors.** Rejected — that is the status quo, and
  it is precisely what prevents portable, cross-system freshness alerting.
- **Alternative: a Prometheus-style staleness marker.** OTEL has a low-level
  "staleness marker" for missing timeseries; it is unrelated to data freshness.

## Open questions

1. **Naming:** `data.staleness.*` vs `data.freshness.*`. "Staleness" names the
   failure; "freshness" names the property. Which does the SIG prefer?
2. **Namespace:** a new top-level `data.*` root, or nest under an existing area?
   Does this warrant a new semantic-conventions area/sub-SIG?
3. **SLA in metrics:** should `sla.threshold`/`breached` be part of the
   convention at all, or left entirely to the backend? (Current stance: optional.)
4. **`version_drift` scope:** version-currency (RAG docs, schema, migration
   drift) reuses the same shape but needs a registry oracle. In or out of the
   first convention?
5. **Federation:** should this land first as a third-party/federated extension
   (per the 2026 roadmap) and migrate into core once adopted?

## Future possibilities

- **Optional extension metrics** already specified in the reference: `probe.errors`,
  `update.interval` (cadence), `age.peak` (Peak AoI), `partition.skew`.
- **End-to-end freshness via baggage:** propagate the origin event-time in OTEL
  baggage so a consumer computes true cross-hop age (`method=end_to_end`).
- **Differential freshness:** index-vs-corpus and replica-vs-primary lag
  (`relative_to`, `index_lag`, `replication_lag`).
- **Broader sources:** BigQuery/Snowflake metadata freshness, S3/GCS object
  stores, lakehouse (Iceberg/Delta) snapshot age, feature stores.
- **Auto-baselined SLAs** derived from the observed `update.interval`
  distribution, reducing hand-tuned thresholds.

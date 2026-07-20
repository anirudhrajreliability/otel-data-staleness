# Semantic Conventions for Data Staleness

**Status:** Development (proposed)
**Convention version:** 0.4.0
**Applies to:** Metrics

This document proposes a vendor-neutral OpenTelemetry semantic convention for
*data staleness* (a.k.a. data freshness). It standardizes how producers across
heterogeneous data systems — relational databases and warehouses, streaming
platforms, batch/ELT pipelines, and caches/object stores — emit a common,
comparable set of freshness signals.

The convention is grounded in the **Age of Information (AoI)** literature, which
defines freshness as the time elapsed since the most recent received update was
generated at its source, and it consolidates the proprietary, source-specific
freshness signals offered by data-observability tooling into a single OTEL
vocabulary.

## 1. Motivation

OpenTelemetry already defines semantic conventions for database client metrics
and messaging/Kafka metrics, but these capture *operation duration* and
*message/row counts* — not how *old* the data itself is. There is no
standardized metric for data freshness/staleness, and not even a standardized
consumer-lag metric. Each data-observability vendor models freshness
differently, which prevents cross-system comparison and portable alerting.

This convention closes that gap.

## 2. Metrics

All durations use UCUM unit `s` (seconds). Timestamps use `s` as **Unix epoch
seconds**. Point-in-time values SHOULD be reported as asynchronous
(observable) gauges; cumulative event counts use monotonic counters.

| Metric name                           | Instrument          | Unit       | Description |
|---------------------------------------|---------------------|------------|-------------|
| `data.staleness.age`                  | ObservableGauge     | `s`        | Current staleness: `now - event_time` of the most recent data available at the consumer. The primary AoI-style metric. Grows monotonically while no new data arrives. |
| `data.staleness.lag`                  | ObservableGauge     | `s`        | Processing lag measured *at update time*: `processing_time - event_time` of the most recently processed record. Distinct from `age`: lag is the one-shot delay incurred moving a record through the pipeline; age keeps increasing during a stall. |
| `data.staleness.last_update.timestamp`| ObservableGauge     | `s`        | Wall-clock Unix timestamp of the most recent successful update/record for the source. |
| `data.staleness.records.behind`       | ObservableGauge     | `{record}` | Backlog size between produced and consumed positions (e.g. Kafka consumer lag in messages, rows pending). Optional; emit when a positional backlog is meaningful. |
| `data.staleness.sla.threshold`        | ObservableGauge     | `s`        | Configured maximum acceptable `age` (the freshness SLA / SLO target). Optional. |
| `data.staleness.sla.breached`         | ObservableGauge     | `1`        | `1` if `age > threshold`, else `0`. Optional; requires a threshold. |
| `data.staleness.sla.breaches`         | Counter (monotonic) | `{breach}` | Cumulative count of transitions into a breached state. Optional. |

### 2.1 Relationship between `age` and `lag`

For the most recent record with event time `t_e` observed/processed at `t_p`:

```
lag  = t_p - t_e                  (delay through the pipeline, fixed per record)
age  = now - t_e                  (>= lag, grows until a newer record arrives)
```

If a source stops producing, `lag` of the last record stays constant while
`age` rises without bound — which is precisely the failure mode freshness
monitoring exists to catch.

### 2.2 Optional extension metrics

The metrics in §2 are the core. The following are **optional** and SHOULD be
emitted only when meaningful for a source. They are included because each
captures information that **cannot be reconstructed by a backend** from the core
gauges (see §7 for the metrics we deliberately leave out for the opposite
reason).

| Metric name                       | Instrument          | Unit        | Description |
|-----------------------------------|---------------------|-------------|-------------|
| `data.staleness.probe.errors`     | Counter (monotonic) | `{error}`   | Count of failed freshness measurement attempts for a source. Producers that isolate failing probes (so one broken check does not suppress the others) MUST still emit this counter, otherwise a broken freshness check is invisible. Carries `error.type` when known. |
| `data.staleness.update.interval`  | Histogram           | `s`         | Distribution of elapsed time between successive updates of a source (inter-arrival time). Enables baselining the expected refresh cadence, detecting "updates still arriving but slower," and deriving SLA thresholds from observed behavior rather than hand-tuning. |
| `data.staleness.age.peak`         | ObservableGauge     | `s`         | The maximum `age` reached within the reporting interval, i.e. the value just before fresh data reset it. This is *Peak AoI*. A sampled `age` gauge misses the spike between collections, so the peak must be tracked at the source. |
| `data.staleness.partition.skew`   | ObservableGauge     | `s`         | `max(age) − min(age)` across the partitions/shards of a single source. Surfaces a straggler partition that an aggregate `age` would hide. Reported at the source level **without** `data.staleness.partition` (it is a cross-partition aggregate). |

`update.interval` histogram buckets SHOULD be chosen to straddle the source's
expected cadence (e.g. exponential buckets around the schedule interval).

## 3. Attributes

| Attribute                 | Type   | Req. | Example values | Notes |
|---------------------------|--------|------|----------------|-------|
| `data.source.system`      | string | Required | `postgresql`, `mysql`, `snowflake`, `redshift`, `bigquery`, `kafka`, `kinesis`, `dbt`, `airflow`, `redis`, `s3`, `gcs`, `mongodb`, `dynamodb`, `cassandra`, `couchbase`, `neo4j`, `influxdb`, `timescaledb`, `iceberg`, `delta`, `hudi`, `elasticsearch`, `opensearch`, `pinecone`, `weaviate`, `milvus`, `qdrant`, `pgvector`, `feast`, `debezium`, `http` | Reuses `db.system.name` / `messaging.system` values where one exists. Open-ended; the list is illustrative, not exhaustive. |
| `data.source.name`        | string | Required | `orders`, `events.clickstream`, `mart.daily_revenue`, `s3://bucket/raw/` | Logical dataset: table, topic, model, object prefix, or cache namespace. |
| `data.source.namespace`   | string | Recommended | `public`, `analytics`, `consumer-group-a`, `my-bucket` | Schema/database/consumer group/bucket. |
| `data.staleness.method`   | string | Recommended | `max_timestamp`, `watermark`, `consumer_lag`, `run_completion`, `object_mtime`, `ttl_age`, `heartbeat`, `writetime`, `snapshot`, `replication_lag`, `index_lag`, `version_drift`, `end_to_end` | How the value was derived (see §4). |
| `data.staleness.relative_to` | string | Opt | `public.orders`, `source-region:us-east-1` | The upstream the measurement is compared against, for *differential* freshness (§4.1). Absent for absolute (vs `now`) measurements. |
| `data.staleness.partition`| string | Opt | `7`, `shard-0001` | Partition/shard identity for partitioned sources. |
| `data.pipeline.stage`     | string | Opt | `ingest`, `transform`, `serve` | Pipeline position the measurement was taken at. |
| `error.type`              | string | Opt | `timeout`, `query_failed`, `no_value` | On `data.staleness.probe.errors` only. Reuses the OTEL general `error.type` convention; producers SHOULD use low-cardinality snake_case values so failures are comparable across producers/languages. |
| `data.staleness.version.documented` | string | Opt | `1.28.0` | Version the (RAG/doc) content describes. |
| `data.staleness.version.current` | string | Opt | `1.31.0` | Current release from the registry oracle. |

### 3.1 `data.staleness.method` values

- `max_timestamp` — `now - MAX(updated_at)` from a SQL column.
- `watermark` — explicit low-watermark exported by a stream processor.
- `consumer_lag` — derived from broker offset lag and/or last record timestamp.
- `run_completion` — `now - last_successful_run_end` for a batch job/model.
- `object_mtime` — `now - object.last_modified` for object storage.
- `ttl_age` — `now - key_write_time` for a cache entry.
- `heartbeat` — `now - last_heartbeat` for a liveness-style producer.
- `writetime` — `now - per-cell write timestamp` (e.g. Cassandra `WRITETIME()`).
- `snapshot` — `now - latest snapshot/commit time` of a lakehouse table
  (Iceberg/Delta/Hudi transaction log).
- `replication_lag` — differential: target apply time vs source commit time
  (CDC, read replicas, multi-region). Compared against `data.staleness.relative_to`.
- `index_lag` — differential: a derived index/embedding store vs its source
  corpus (search/vector stores). Compared against `data.staleness.relative_to`.
- `end_to_end` — `age` propagated across pipeline hops via OTEL baggage (origin
  event-time carried in context); the consumer measures true cross-hop age.
- `version_drift` — content describes an older release than the current one:
  `records.behind` = releases behind, `age` = time since the first newer release.
  Requires a registry oracle (PyPI/npm/GitHub/Docker Hub). Carries
  `data.staleness.version.documented` / `.version.current`.

## 4. Mapping guidance per source type

**SQL databases / warehouses.** Query `MAX(<event_or_updated_column>)`; set
`age = now - max`, `method=max_timestamp`. Note that `MAX(timestamp)` answers
"has new data arrived?" but is *optimistic about completeness*: a single late
row makes a table look fresh, and a full-refresh job resets `updated_at` even
when source data is unchanged. Where completeness matters, prefer a
**load-audit / watermark table** the pipeline writes on successful completion
(`method=watermark`), optionally emitting `data.staleness.records.behind` as the
expected-minus-loaded backlog. A NULL/empty result MUST be reported as an error
(via `data.staleness.probe.errors`), not as a fabricated age.

**Streaming / Kafka / Kinesis.** Report per `data.staleness.partition`:
`age = now - timestamp_of_newest_record` (topic freshness, which catches a
stalled producer), and `data.staleness.records.behind = end_offset -
committed_offset` for a consumer group (consumer lag). `method=consumer_lag`.
The topic-level rollup (max age, total backlog) is derived by the backend, not
emitted. Be explicit about the timestamp source: Kafka `CreateTime` is
producer-set and only as trustworthy as the producer's clock, whereas
`LogAppendTime` is broker-authoritative. An empty partition MUST be reported as
an error (`data.staleness.probe.errors`), not a fabricated age. Optionally emit
`data.staleness.lag = now - timestamp_at_committed_offset` (consumer time-lag —
how stale the data the consumer has processed is).

**Amazon Kinesis.** Report per-shard `age = now - newest_record_arrival_time`
(`data.staleness.partition` = shard id). Kinesis does not expose consumer
position without the KCL checkpoint store, so a generic probe reports stream
freshness rather than consumer lag. If no record exists within the read/lookback
window, the exact age is unknown and MUST be reported as an error, not
fabricated.

**Pipelines / dbt / Airflow.** `age = now - last_successful_run_end`,
`method=run_completion`. `data.source.name` is the model/task; SLA threshold is
the schedule interval plus tolerance.

**Caches / object storage.** Redis: `age = now - key_write_time`,
`method=ttl_age`. S3/GCS: `age = now - object.last_modified`,
`method=object_mtime`, `data.source.name` is the prefix being tracked.

**NoSQL (document / wide-column / graph).** MongoDB, DynamoDB, Couchbase,
Neo4j: `age = now - MAX(updated_at)` (`method=max_timestamp`). Cassandra can use
`WRITETIME()` for a precise per-cell write time (`method=writetime`).

**Lakehouse table formats.** Iceberg, Delta, Hudi: `age = now - latest
snapshot/commit time` from the table's transaction log (`method=snapshot`).

**Time-series DBs.** Influx, Timescale, Victoria Metrics: `age = now - last
sample timestamp` of the series (`method=max_timestamp` or `heartbeat`).

**External feeds / APIs.** Price, weather, or third-party feeds:
`age = now - last successful fetch` (`method=heartbeat`),
`data.source.system=http`.

**RAG / documentation content (version-currency).** For version-specific docs,
time-freshness is insufficient: content can be freshly crawled yet describe an
old release. Compare the version the content documents against the current
release from a package registry: `data.staleness.records.behind` = number of
releases newer than documented, `age = now - release_time_of_the_oldest_newer_
release` (how long newer information has existed unreflected), `method=
version_drift`, with `data.staleness.version.documented` / `.version.current`.
Exclude pre-releases from "current" by default; an unparseable or undeterminable
version MUST be reported as an error, not a fabricated `0`. The documented
version is typically extracted from the content itself (frontmatter, a version
meta tag, or the docs URL path) rather than hard-coded.

**Schema / migration version drift.** The same `version_drift` shape applies to
data systems whose *oracle* is a registry rather than a package index: a **Kafka
Schema Registry** subject on version N while the latest registered is M
(`records.behind = M - N`), or a **database** whose applied migration version
lags the version the application code expects (`records.behind` = 1 if behind).
These oracles usually expose no per-version timestamp, so emit `records.behind`
(and the version attributes) but **not** `age` — alert on `records.behind > 0`.

### 4.1 Differential (source-relative) freshness

Some systems' freshness is best expressed not against `now` but against an
*upstream*: "the index is N seconds behind its source table," "the replica is N
seconds behind the primary." This is **differential** freshness.

Express it with the existing `data.staleness.lag` metric, setting the record's
`event_time` to the upstream write/commit time and recording the upstream in the
`data.staleness.relative_to` attribute, with `data.pipeline.stage` marking where
the measurement sits:

- **Search / vector indexes** (Elasticsearch, OpenSearch, Pinecone, Weaviate,
  Milvus, Qdrant, pgvector): `lag = index_upsert_time - source_write_time`,
  `method=index_lag`, `relative_to=<source dataset>`, `pipeline.stage=transform`.
  This is the dominant freshness failure in RAG systems: the index silently
  trails the corpus.
- **CDC / replication / read replicas / multi-region** (Debezium, DMS, Postgres
  replicas, DynamoDB global tables): `lag = target_apply_time -
  source_commit_time`, `method=replication_lag`, `relative_to=<source>`.
- **Feature stores** (Feast, Tecton): online-vs-offline store skew, same pattern.

Absolute (`age` vs `now`) and differential (`lag` vs upstream) freshness are
complementary: emit both when both matter (e.g. a vector index that is both
stale vs wall clock *and* behind its corpus).

## 5. Example (OTLP, abbreviated)

```
data.staleness.age{
  data.source.system="postgresql",
  data.source.name="orders",
  data.source.namespace="public",
  data.staleness.method="max_timestamp"
} = 42.0   # seconds

data.staleness.sla.threshold{...same attrs...} = 300.0
data.staleness.sla.breached{...same attrs...}  = 0
```

## 6. Prior art and positioning

This convention does **not** introduce a new freshness metric. It standardizes
an existing, well-studied one. The notion of `age` is the Age of Information
metric from networked-systems research (Kaul, Yates, Gruteser; and the AoI
survey literature, with variants such as Peak AoI and Age of Synchronization).
The per-source measurement recipes mirror what data-observability platforms
(Monte Carlo, Metaplane, Sifflet, Elementary, dbt `source freshness`) and
Kafka-lag tools (Burrow) already compute internally — but here they are
expressed once, in OpenTelemetry, so they are comparable and portable across
backends.

## 7. Non-goals (derive, don't emit)

To keep the convention small and bounded, the following are intentionally
**not** metrics. Each is trivially computable by a backend from the metrics
above, so emitting it would only add cardinality:

- **SLA budget remaining** (`threshold − age`) and **burn rate** — compute from
  `data.staleness.sla.threshold` and `data.staleness.age` at query time.
- **SLO compliance ratio** (fraction of time within SLA over a window) — a
  windowed aggregation over `data.staleness.sla.breached`.
- **Mean / time-averaged age** — derivable from the `age` series; only
  `age.peak` (§2.2) needs source-side tracking.

The following are **out of scope** because they belong to a different
observability pillar or require an oracle this convention does not assume:

- **Volume / row-count drift** — this is the *volume* pillar, not freshness.
- **Age of Incorrect Information** and value-of-information weighting — these
  require a correctness oracle (knowing the data is *wrong*, not just *old*).
  Note the exception: *software/tool version drift* (§4, `version_drift`) is in
  scope precisely because a package registry / schema registry / migrations
  table provides an objective "what's current" oracle.

Guiding rule: **emit what cannot be reconstructed; derive the rest.**

## 8. Stability

This is a Development-stage proposal intended as the basis for an OpenTelemetry
Enhancement Proposal (OTEP) / semantic-conventions contribution. Names MAY
change before stabilization.

### Changelog

- **0.4.0** — Added the `version_drift` (RAG/package registry, Schema Registry,
  DB migration) and `end_to_end` methods, the `data.staleness.version.documented`
  / `.version.current` attributes, and §4.1 differential + schema/migration
  mapping guidance.
- **0.3.0** — Expanded the `data.source.system` and `data.staleness.method`
  enums (NoSQL, lakehouse, time-series, search/vector, CDC, feeds); added
  `data.staleness.relative_to` and §4.1 differential (source-relative) freshness.
- **0.2.0** — Added optional extension metrics (§2.2): `probe.errors`,
  `update.interval`, `age.peak`, `partition.skew`; added the non-goals section.
- **0.1.0** — Initial core metrics and attributes.

> Part of **otel-data-staleness** — see the [root README](../../README.md) for the project overview and the SDK-vs-Collector comparison.

# datastaleness receiver

An OpenTelemetry Collector **metrics receiver** that actively scrapes data
sources on a schedule and emits `data.staleness.*` metrics — **with zero
application code**. Configure sources in YAML; the Collector does the rest.

This is the lowest-friction way to adopt the
[data-staleness convention](../../spec/semantic-conventions.md): no SDK, no code
in your services, just Collector config.

## Scraper types

| `type` | Source | How freshness is derived | `method` emitted |
|--------|--------|--------------------------|------------------|
| `sql` | database / warehouse | `MAX(column)` or a custom watermark query | `max_timestamp` / `watermark` |
| `kafka` | Kafka / MSK topics | newest record ts + consumer offset lag (+ optional time-lag), per partition | `consumer_lag` |
| `kinesis` | Amazon Kinesis Data Streams | newest record arrival time per shard | `max_timestamp` |
| `schema_registry` | Confluent Schema Registry | subject version vs pinned/contract version | `version_drift` |
| `db_migration` | any SQL DB | applied migration version vs expected | `version_drift` |
| `file` | local/mounted file | file modification time | `object_mtime` |
| `http` | URL / feed | `Last-Modified` header, or an epoch-seconds body | `heartbeat` |
| `static` | fixed value | `last_update_epoch` / `age_seconds` from config (demo/testing) | `max_timestamp` |

Failed scrapes emit `data.staleness.probe.errors` (cumulative, with
`error.type`) so a broken check is **visible** rather than silent.

## SQL scraper — the reliability contract

Because the credibility of the whole signal rests on the measurement, the SQL
scraper is explicit about what it does and does not guarantee.

**Two measurement modes:**

- **Convenience** — set `table` + `timestamp_column`; the scraper runs
  `SELECT MAX(timestamp_column) FROM [namespace.]table` (`method=max_timestamp`).
- **Trustworthy** — set `query` to a custom SQL that returns one row of
  `(freshness_timestamp)` or `(freshness_timestamp, records_behind)`
  (`method=watermark`). Point this at a **load-audit / watermark table** your
  ETL writes on successful completion, rather than scanning the data table.

```yaml
receivers:
  datastaleness:
    collection_interval: 30s
    sources:
      - type: sql
        name: orders
        system: postgresql
        namespace: public
        driver: postgres                 # postgres & mysql registered out of the box
        dsn: "postgres://user:pass@db:5432/app?sslmode=disable"
        query: >
          SELECT last_loaded_at, expected_rows - loaded_rows
          FROM etl.load_audit WHERE table_name = 'orders'
        sla_threshold_seconds: 300
        query_timeout: 5s
```

**What it validates, honestly:**

- `MAX(timestamp)` answers *"has new data arrived?"* — a good freshness proxy,
  but it is **optimistic about completeness**: one late row makes a table look
  fresh even if the bulk of an expected load is missing, and a full-refresh ETL
  that rewrites every row resets `updated_at` even when nothing changed. Use the
  watermark-query mode when completeness matters.
- **NULL/empty result** → `null_timestamp` probe error (never a fabricated age).
- **Timeouts** are bounded by `query_timeout` (default 5s) → `timeout` error.
- **Timestamps** parse from `time.Time`, strings, or epochs; naive values are
  treated as UTC (`assume_local_time: true` to override).
- **Connections** are pooled per driver+DSN.

**Drivers**: `postgres` (`lib/pq`) and `mysql` (`go-sql-driver/mysql`) are
compiled in by default. Add other drivers to your Collector build as needed.

## Kafka scraper — the reliability contract

The Kafka scraper is **admin-only**: it reads cluster metadata and does not join
a consumer group or read message payloads. Per partition it reports two signals:

- **Topic freshness** — `data.staleness.age` = `now − timestamp of the newest
  record` (broker MAX_TIMESTAMP query, KIP-734). This catches a **stalled
  producer**: age climbs even while the consumer is caught up.
- **Consumer lag** — `data.staleness.records.behind` = `end_offset −
  committed_offset` for the configured `consumer_group` (omit the group for
  freshness only).
- **Consumer time-lag** (opt-in, `measure_time_lag: true`) —
  `data.staleness.lag` = `now − timestamp of the record at the committed
  offset`, i.e. how stale the data the consumer has actually processed is. This
  does one bounded fetch per partition (so it is no longer strictly admin-only);
  it degrades gracefully to no-lag on fetch failure.

Each reading carries `data.staleness.partition`; the topic-level rollup
(`max by (topic)`, `sum by (topic)`) is left to the backend, per the
convention's "derive, don't emit" rule.

```yaml
      - type: kafka
        name: clickstream
        system: kafka
        brokers: [broker-1:9092, broker-2:9092]
        topic: clickstream
        consumer_group: analytics        # omit for topic-freshness only
        sasl_mechanism: scram-sha-512     # "", plain, scram-sha-256/512, aws-msk-iam
        username: ${env:KAFKA_USER}
        password: ${env:KAFKA_PASSWORD}
        sla_threshold_seconds: 60
        query_timeout: 10s
```

**Auth**: PLAINTEXT (no `sasl_mechanism`); SASL `plain` / `scram-sha-256` /
`scram-sha-512` over TLS (TLS auto-enables with SASL; override with `tls`); and
`aws-msk-iam` for Amazon MSK. MSK IAM reads AWS credentials from the standard
environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`)
plus `AWS_REGION` for SigV4 — inject them from the instance/pod role.

**What it validates, honestly:**

- The record timestamp is whatever producers set (`CreateTime`) unless the topic
  uses broker `LogAppendTime`. With `CreateTime`, a misbehaving producer can
  skew timestamps — age is only as trustworthy as the producer's clock;
  `LogAppendTime` topics give broker-authoritative freshness.
- An **empty partition** is reported as an `empty_partition` probe error, never a
  fabricated age.
- Broker/metadata failures and timeouts surface as `describe_failed` / `timeout`
  probe errors.

## Kinesis scraper — the reliability contract

Measures **stream freshness** per shard: `data.staleness.age` =
`now − ApproximateArrivalTimestamp of the newest record`. It reads forward from
`now − lookback` toward the tip (bounded by `query_timeout` and an internal
iteration cap), tracking the newest arrival.

```yaml
      - type: kinesis
        name: clickstream
        system: kinesis
        stream_name: clickstream
        aws_region: us-east-1
        lookback: 1h                 # read window; set comfortably above the SLA
        sla_threshold_seconds: 300
        query_timeout: 10s
```

**Auth**: the standard AWS credential chain — environment variables, shared
config, or the **EC2/EKS role** (IMDS/IRSA). Set `aws_region`.

**What it validates, honestly:**

- Consumer *position* (offset/records-behind) is **not** exposed by the Kinesis
  API without the KCL checkpoint store (DynamoDB), so this scraper reports
  stream freshness, not consumer lag. (Kafka covers consumer lag.)
- If a shard has **no records within `lookback`**, the exact age is unknown (the
  newest record is older than the window), so it is reported as a
  `no_recent_records` probe error — which itself indicates staleness ≥ lookback,
  not a fabricated value. Set `lookback` above your freshness SLA.
- On very high-volume shards the scraper may not reach the exact tip within its
  bounds; it then reports the newest arrival it *did* see, which errs toward
  *older* (safe for alerting).

## Schema & migration version drift

Two scrapers report **version-currency** rather than time-freshness (the same
`version_drift` signal the SDK uses for RAG docs): they emit
`data.staleness.records.behind` (versions behind) with
`data.staleness.version.documented` / `.version.current` attributes — and **no
`age`** (these oracles carry no per-version timestamps, so alert on
`records.behind > 0`).

**`schema_registry`** — how many schema versions a subject is behind the
registry's latest, relative to a pinned/contract version:

```yaml
      - type: schema_registry
        name: orders-value
        system: kafka
        registry_url: http://schema-registry:8081
        subject: orders-value
        documented_version: "3"          # the version your contract/consumer pins
        username: ${env:SR_USER}          # optional basic auth
        password: ${env:SR_PASSWORD}
```

**`db_migration`** — is the database's applied schema-migration version behind
the version your deploy expects? (`records.behind` = 1 if behind, else 0 —
migration numbering is not necessarily contiguous, so it is a boolean, not a
count):

```yaml
      - type: db_migration
        name: app-db
        system: postgresql
        driver: postgres
        dsn: "postgres://user:pass@db:5432/app?sslmode=disable"
        version_query: "SELECT MAX(version) FROM schema_migrations"
        current_version: "20240115"       # the latest migration your code ships
```

**What they validate, honestly:** a missing subject / unreachable registry, a
bad pinned version, or a failing/NULL migration query all surface as probe
errors (`registry_failed`, `bad_documented_version`, `query_failed`,
`null_version`) — never a fabricated "0 behind". Content ahead of the oracle
(pinned/applied ≥ latest) reports 0, not a negative.

## Other sources

```yaml
      - type: file
        name: nightly_dump
        system: s3
        path: /mnt/exports/nightly.parquet
        sla_threshold_seconds: 86400
      - type: http
        name: fx_rates
        system: http
        url: https://example.com/rates.json
        sla_threshold_seconds: 120
```

## Pairing with the processor

Use the receiver to *produce* freshness, and the companion
[`datastaleness` processor](../datastalenessprocessor/) for org-wide SLA policy.
Keep the processor's `compute_age_from_last_update` **off** when using the
receiver to avoid duplicate `age` series.

## Development

```bash
go test ./...        # 42 tests; SQL/Kafka/Kinesis logic run against hermetic fakes
go vet ./... && gofmt -l .
```

Requires **Go 1.25+** (pulled in by the Kafka client and modern collector deps).
Tested against Collector API `v0.110.0` / pdata `v1.16.0`. Stability:
**development**.

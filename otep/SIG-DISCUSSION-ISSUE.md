# Proposal — Semantic Conventions SIG: data staleness / freshness

The discussion proposal for a `data.staleness.*` semantic convention, intended
for the OpenTelemetry Semantic Conventions SIG. It is kept in the repo as a
record; the live discussion is tracked in the SIG issue linked below once opened.

- Discussion issue: _(add link once posted)_
- Full design: [`0000-data-staleness.md`](0000-data-staleness.md) · Path to adoption: [`README.md`](README.md)

---

## What

There is currently no OpenTelemetry semantic convention for **data staleness**
(a.k.a. data freshness) — *how old the data itself is*. `db.*` and `messaging.*`
cover operation duration and message/row counts, but nothing captures "this
table / topic / index / document set is N seconds out of date," and there isn't
even a standardized consumer-lag metric.

I'd like to gauge interest in standardizing this, and find a sponsor.

## Why it's worth standardizing

Staleness is a distinct failure mode: a pipeline can have zero errors and low
latency yet serve hours-old data because a producer stalled. The quantity is
well understood but not portable:

- Formally it's **Age of Information** (`age = now − event_time` of the freshest
  record) from networked-systems research.
- Operationally every data-observability vendor (Monte Carlo, Metaplane, …),
  `dbt source freshness`, and Kafka-lag tools compute it — each in a proprietary,
  siloed way, so it can't be compared or alerted on across systems or backends.

Putting it in OTEL makes "how old is my data" as portable as latency and errors.

## Proposed shape (Development stability)

A small metric set — `data.staleness.age` / `.lag` / `.last_update.timestamp` /
`.records.behind` / `.sla.*` — with attributes `data.source.system` (reusing
`db.system.name` / `messaging.system`), `data.source.name`, `.namespace`,
`data.staleness.method`, `.partition`, `data.pipeline.stage`. Orthogonal to and
composable with `db.*` / `messaging.*`. Full spec, Weaver model, and reference
implementation: https://github.com/anirudhrajreliability/otel-data-staleness

## Prior art / grounding

Age of Information (Kaul/Yates/Gruteser); Peralta's data-freshness survey; the
data-observability "five pillars"; `dbt source freshness`; Burrow. This is
explicitly a **consolidation** of a known quantity, not a new metric.

## There's already a working, end-to-end-validated reference

To de-risk the design, there's a complete reference implementation (Apache-2.0):
a Python SDK, two OpenTelemetry Collector components (a zero-config receiver and
an SLA processor) covering SQL/warehouses, Kafka, Kinesis, files/HTTP, Schema
Registry and DB-migration drift, plus a Weaver model and a language-agnostic
conformance suite. https://github.com/anirudhrajreliability/otel-data-staleness

It's not just unit-tested — it's **validated end-to-end against real backends on
a clean cloud instance**. A one-command suite stands up real Postgres + Kafka +
Redis + LocalStack Kinesis + Confluent Schema Registry and asserts the metrics
are *numerically correct*, not merely present:

- **54** Python SDK + **42** receiver + **9** processor unit tests, and a
  language-agnostic **conformance** suite — all green.
- The machine-readable **Weaver model** passes `weaver registry check` (v0.24.2)
  with zero violations — the same tool the Semantic Conventions SIG uses to
  validate and generate conventions.
- **11/11 real-workload checks** on a fresh EC2 box:
  - **Accuracy** — inject a row with a known event-time; `data.staleness.last_update.timestamp`
    matches it exactly and `age` is correct to the second (not just "a number appeared").
  - **Scale / lag** — a pinned Kafka consumer backlog yields `records.behind == 100`
    exactly; a multi-partition topic reports per-partition freshness.
  - **AWS-native** — live Kinesis freshness (LocalStack), Schema Registry version
    drift, and DB-migration drift.
  - **SDK in the live path** — the SDK probes real Postgres + Redis and exports via OTLP.
  - **Failure behavior** — a future timestamp clamps `age` to ≥ 0; stopping Postgres
    surfaces `data.staleness.probe.errors` (a broken check is *visible*, never a
    fabricated `0`); the source recovers on restart.

This exercise also caught and fixed a real correctness bug (PostgreSQL
`EXTRACT(EPOCH …)` returns a `Decimal` the SDK initially rejected) — the kind of
thing presence-only testing misses. (LocalStack is an emulator; the one path that
still needs real AWS is **MSK IAM** auth, which is documented as such.)

## Relationship to existing proposals

This complements rather than competes with #3762 (`pipeline.*` for data-pipeline
runs). That proposal is *execution-centric* — freshness as a quality attribute of
a pipeline *run* (Databricks/dbt/Glue); this one is *source-centric* — the Age of
Information of *any* data source (table, topic, cache, index, replica, RAG
corpus), independent of whether a pipeline produced it. They compose:
`pipeline.quality.freshness_lag_seconds` in #3762 could reuse
`data.staleness.age` / `.lag`, so the ecosystem gets **one** freshness primitive
rather than two incompatible ones. I'm also mindful of the `messaging.*`
partition / consumer-group work (#797): `data.source.*` and
`data.staleness.partition` should align with those attributes, not duplicate them.

## Questions for the SIG

1. Is there appetite for this in Semantic Conventions? Would anyone involved in
   instrumentation be interested in **co-owning** a new `data.*`/freshness area?
2. `data.staleness.*` vs `data.freshness.*`, and is `data.*` an acceptable new
   root?
3. Should this start as a decentralized/third-party registry (built on Weaver)
   and migrate into core once adopted?
4. Should SLA threshold/breach be in the convention, or left to the backend?

Happy to bring this to a Semantic Conventions SIG meeting.

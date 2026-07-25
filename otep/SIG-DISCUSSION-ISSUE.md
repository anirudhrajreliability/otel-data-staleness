# Proposal — Semantic Conventions SIG: data staleness / freshness

The discussion proposal for a `data.staleness.*` semantic convention, intended
for the OpenTelemetry Semantic Conventions SIG. It is kept in the repo as a
record; the live discussion is tracked in the SIG issue linked below.

- Discussion issue: https://github.com/open-telemetry/semantic-conventions/issues/3909
- Full design: [`0000-data-staleness.md`](0000-data-staleness.md) · Path to adoption: [`README.md`](README.md)

---

## TL;DR

OpenTelemetry has no portable way to emit *how old data is*. Most single-system
freshness is already solved locally with a `MAX(updated_at)` query, so "freshness
for everything" is a weak pitch. But **two specific signals have no OTEL standard
at all**, and both are instances of one small primitive (Age of Information):

1. **Streaming freshness / consumer lag** — `messaging.*` has no standardized
   consumer-lag or freshness-lag metric.
2. **Differential / relative freshness** — no convention for how far one source
   is behind another (index-vs-corpus, replica-vs-primary, cache-vs-source).

This proposal defines that primitive, `data.staleness.*`, at **Development**
stability, with a validated reference implementation. Seeking interest and a
sponsor — and specifically feedback on whether to start narrow (the two gaps) or
with the full set.

## The two gaps that have no standard today

Rather than lead with "freshness for everything" — much of which teams already
solve locally, and don't want to standardize (a single internal table with a
working `MAX(updated_at)` panel has no reason to adopt a convention) — the
sharpest, genuinely **unmet** needs are:

1. **Streaming freshness / consumer lag.** OTEL `messaging.*` has *no*
   standardized consumer-lag or freshness-lag metric. Every shop reinvents it
   (Burrow, bespoke exporters, vendor-specific panels), so a lag alert or SLO
   isn't portable across brokers or backends. This is felt today in plain
   Kafka/Kinesis enterprise systems — no AI required.

2. **Differential / relative freshness.** "How far is this vector index behind
   the corpus it was built from?" "How far is this read replica or cache behind
   its primary?" There is no convention for the *relative* age of one source
   vs. another — and unlike single-table freshness, there is no clean local
   one-liner for it. This is the silent, expensive failure mode behind stale RAG
   retrieval and stale agent/shared state: the query succeeds, latency is normal,
   no error fires, and the answer is confidently wrong.

Both are the **same quantity** — Age of Information — applied to different
sources. So rather than bolt on two narrow one-off metrics, define the primitive
once.

## The primitive (breadth is a side effect, not the pitch)

`data.staleness.age` = `now − event_time(freshest record)`, plus `.lag`,
`.last_update.timestamp`, `.records.behind`, `.sla.*`. Once the primitive exists
it applies uniformly to SQL/warehouse, streaming, caches, replicas, and indexes —
but that breadth is **opt-in portability** for teams that want one freshness
vocabulary across a pipeline, not a claim that every single-DB owner needs it.

## Honest scope — what this is *not*

- It is **not** trying to replace a team's local `MAX(updated_at)` for one
  system where that is already sufficient. For a single source with no
  cross-system or portability need, the local metric is fine and a convention is
  overkill — we are explicit about that.
- The addressable value is (a) the two genuinely-missing signals above, and
  (b) cross-system / cross-backend **portability** for platform and observability
  teams and vendors.
- The *metric* is not novel — it is AoI, and data-observability vendors already
  compute it. The contribution is **standardization and consolidation**, so a
  freshness alert, a Grafana panel, or an SLO travels across Postgres, Kafka, a
  replica, and a vector index — and across Prometheus, an OTLP store, or any
  vendor backend — unchanged.

## Proposed shape (Development stability)

A small metric set — `data.staleness.age` / `.lag` / `.last_update.timestamp` /
`.records.behind` / `.sla.*` — with attributes `data.source.system` (reusing
`db.system.name` / `messaging.system`), `data.source.name`, `.namespace`,
`data.staleness.method`, `.partition`, `data.pipeline.stage`. Orthogonal to and
composable with `db.*` / `messaging.*`. Full spec, Weaver model, and reference
implementation: https://github.com/anirudhrajreliability/otel-data-staleness

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

## Prior art / grounding

Age of Information (Kaul/Yates/Gruteser); Peralta's data-freshness survey; the
data-observability "five pillars"; `dbt source freshness`; Burrow. This is
explicitly a **consolidation** of a known quantity, not a new metric.

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
`data.staleness.partition` should align with those attributes, not duplicate them —
and a standardized consumer-lag metric is exactly gap (1) above.

## Questions for the SIG

1. **Scope:** is the right *minimal* starting point the two unmet gaps — a
   standardized **streaming freshness / consumer-lag** metric and a
   **differential freshness** metric — with the broader source coverage
   following later? Or is the full `data.staleness.*` set preferable up front?
2. Is there appetite for this in Semantic Conventions, and would anyone involved
   in instrumentation be interested in **co-owning** a `data.*` / freshness area?
3. `data.staleness.*` vs `data.freshness.*`, and is `data.*` an acceptable new
   root?
4. Should this start as a decentralized/third-party registry (built on Weaver)
   and migrate into core once adopted?
5. Should SLA threshold/breach be in the convention, or left to the backend?

Happy to bring this to a Semantic Conventions SIG meeting.

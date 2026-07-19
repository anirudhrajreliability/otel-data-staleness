# A Taxonomy of Data Staleness

Data can be "old" in more than one way. This document enumerates every *kind* of
staleness the convention aims to capture, the metric/mechanism that captures it,
and — importantly — the kinds we deliberately leave out and why. It doubles as
the completeness argument for the OTEP: the convention is not one metric, it is a
coordinated set that covers the space.

The organizing idea is **Age of Information (AoI)**: freshness is `now − t_e`,
the time since the freshest available data was generated at its source. Each row
below is a different *lens* on that quantity.

## Dimensions captured

| # | Staleness dimension | Question it answers | Metric / mechanism | `method` | Status |
|---|---------------------|---------------------|--------------------|----------|--------|
| 1 | **Absolute freshness** | How old is the newest data vs *now*? | `data.staleness.age` | `max_timestamp`, `object_mtime`, `ttl_age`, `snapshot`, `writetime`, `heartbeat` | ✅ core |
| 2 | **Pipeline lag** | What one-shot delay did the last record incur moving through the pipeline? | `data.staleness.lag` | `consumer_lag` | ✅ core |
| 3 | **Positional backlog** | How far behind is the consumer in *records*? | `data.staleness.records.behind` | `consumer_lag` | ✅ core |
| 4 | **SLA breach** | Is age past its agreed threshold? | `data.staleness.sla.threshold` / `.breached` / `.breaches` | any | ✅ core |
| 5 | **Differential (source-relative)** | How far does a derived store trail its *upstream* (index vs corpus, replica vs primary)? | `data.staleness.lag` + `relative_to` | `index_lag`, `replication_lag` | ✅ |
| 6 | **Version / content currency** | Does the content describe the *current* release (RAG docs, schema, migration)? | `records.behind` + `version.documented`/`.current` | `version_drift` | ✅ |
| 7 | **End-to-end (cross-hop)** | What is the *cumulative* age across every pipeline hop, not just the last? | `data.staleness.age` via **OTEL baggage** propagation | `end_to_end` | ✅ |
| 8 | **Cadence / interval** | Are updates arriving, but *slower* than expected? | `data.staleness.update.interval` (histogram) | — | ✅ ext |
| 9 | **Peak / worst-case** | What was the *maximum* age reached before fresh data reset it? (Peak AoI) | `data.staleness.age.peak` | — | ✅ ext |
| 10 | **Partition skew** | Is one partition/shard a straggler while the aggregate looks healthy? | `data.staleness.partition.skew` | — | ✅ ext |
| 11 | **Liveness / stall** | Did the source stop producing entirely? | `age` rising without bound + `heartbeat` method | `heartbeat` | ✅ (via 1) |
| 12 | **Measurement integrity** | Is the freshness *check itself* broken (so a gap is visible, not silent)? | `data.staleness.probe.errors` + `error.type` | — | ✅ ext |
| 13 | **Completeness** | Is the data fresh but *incomplete* (partial load)? | `records.behind` from a watermark/audit query | `watermark` | ◑ partial |

Legend: ✅ core = §2; ✅ ext = §2.2 extension metrics; ◑ partial = expressible
today via the watermark pattern but not a first-class signal.

## Two mechanisms worth calling out

**End-to-end via baggage (#7).** Point measurements answer "how old is the data
*here*." The end-to-end mechanism stamps the origin event-time into OTEL
**baggage** at ingest; because baggage rides the standard OTEL context
propagators, any downstream stage — across service and messaging boundaries —
reads it back and computes true cross-hop age. This is the one dimension that is
*impossible* to reconstruct from per-hop snapshots, and it is OTEL-native (no
bespoke transport). See `python/src/otel_staleness/freshness_context.py`.

**Cadence, peak, and skew (#8–#10)** need cross-collection state, so they are
computed *at the source* (the SDK monitor) rather than derived downstream — a
sampled gauge cannot recover a peak between scrapes, and a histogram of
inter-update intervals cannot be rebuilt from point samples. This is the same
"emit what cannot be reconstructed" principle that governs the whole convention.

## Deliberately out of scope (and why)

These are **non-goals** — either derivable by a backend (so emitting them only
adds cardinality) or requiring an oracle the convention does not assume:

- **SLA budget / burn rate / SLO compliance ratio** — trivially derived from
  `age`, `threshold`, and `breached` at query time.
- **Time-averaged (mean) age** — derivable from the `age` series (only Peak AoI
  needs source-side tracking).
- **Volume / row-count drift** — a different observability pillar (*volume*),
  not freshness.
- **Age of Incorrect Information (semantic correctness)** — needs a correctness
  oracle (knowing data is *wrong*, not just *old*). The one exception is
  *software/tool version drift* (#6), which is in scope precisely because a
  package registry / schema registry / migrations table provides an objective
  "what's current" oracle.

## Guiding rule

**Emit what cannot be reconstructed; derive the rest.** Every dimension above is
included only because a backend cannot recompute it from cheaper signals; every
non-goal is excluded because it can, or because it needs information the
convention refuses to assume.

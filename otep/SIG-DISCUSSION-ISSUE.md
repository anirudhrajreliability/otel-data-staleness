# [Ready to post] Semantic Conventions discussion issue

Post this in **open-telemetry/semantic-conventions** as a new issue
(type: *New convention* / discussion). Keep it short — the goal is to gauge
appetite and find a sponsor, not to merge a spec on day one. Fill in the two
`<...>` links after you push the repo public.

---

**Title:** Proposal: semantic conventions for data staleness / freshness

**Body:**

### What

There is currently no OpenTelemetry semantic convention for **data staleness**
(a.k.a. data freshness) — *how old the data itself is*. `db.*` and `messaging.*`
cover operation duration and message/row counts, but nothing captures "this
table / topic / index / document set is N seconds out of date," and there isn't
even a standardized consumer-lag metric.

I'd like to gauge interest in standardizing this, and find a sponsor.

### Why it's worth standardizing

Staleness is a distinct failure mode: a pipeline can have zero errors and low
latency yet serve hours-old data because a producer stalled. The quantity is
well understood but not portable:

- Formally it's **Age of Information** (`age = now − event_time` of the freshest
  record) from networked-systems research.
- Operationally every data-observability vendor (Monte Carlo, Metaplane, …),
  `dbt source freshness`, and Kafka-lag tools compute it — each in a proprietary,
  siloed way, so it can't be compared or alerted on across systems or backends.

Putting it in OTEL makes "how old is my data" as portable as latency and errors.

### Proposed shape (Development stability)

A small metric set — `data.staleness.age` / `.lag` / `.last_update.timestamp` /
`.records.behind` / `.sla.*` — with attributes `data.source.system` (reusing
`db.system.name` / `messaging.system`), `data.source.name`, `.namespace`,
`data.staleness.method`, `.partition`, `data.pipeline.stage`. Orthogonal to and
composable with `db.*` / `messaging.*`. Full draft + OTEP: `<repo link>`.

### Prior art / grounding

Age of Information (Kaul/Yates/Gruteser); Peralta's data-freshness survey; the
data-observability "five pillars"; `dbt source freshness`; Burrow. This is
explicitly a **consolidation** of a known quantity, not a new metric.

### There's already a working reference

To de-risk the design, there's a complete reference implementation (Apache-2.0):
a Python SDK, two OpenTelemetry Collector components (a zero-config receiver and
an SLA processor) covering SQL/warehouses, Kafka, Kinesis, files/HTTP, plus a
Weaver model and a language-agnostic conformance suite. `<repo link>`

### Questions for the SIG

1. Is there appetite for this in Semantic Conventions? Is a sponsor available?
2. `data.staleness.*` vs `data.freshness.*`, and is `data.*` an acceptable new
   root?
3. Should this start as a federated/third-party extension (per the 2026 roadmap)
   and migrate into core once adopted?
4. Should SLA threshold/breach be in the convention, or left to the backend?

Happy to bring this to a Semantic Conventions SIG meeting.

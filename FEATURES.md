# Features

Everything `otel-data-staleness` does, in plain English. It answers one question
— **"how old is my data?"** — as portable OpenTelemetry metrics, across every
kind of data system, honestly.

## What it measures (the signals)

- **Age** — the headline: how old your freshest data is (`now − when the newest
  record was created`). If it keeps climbing, your data is going stale.
- **Lag** — the one-time delay a record incurred moving through the pipeline.
- **Records behind** — how far a consumer is behind (Kafka backlog in messages,
  or how many software/schema versions behind).
- **Last-update timestamp** — the raw time of the most recent update.
- **SLA threshold, breach flag, breach counter** — your freshness deadline, a
  live "is it past due?" 0/1 flag, and a running count of breaches.
- **Peak age** — the worst spike before fresh data reset it (a sampled gauge
  misses this).
- **Update interval** — how often updates actually arrive, as a distribution
  (catches "still arriving, but slower").
- **Partition skew** — the gap between the freshest and stalest partition, so one
  straggler isn't hidden by an average.
- **Probe errors** — a visible counter for when a freshness *check itself* breaks.

## The Python library (for apps you write code in)

- **Ready-made probes**, one per source — point and go: SQL databases/warehouses,
  Kafka, dbt/Airflow pipelines, Redis caches, S3/object stores.
- **Differential probes** — how far a *derived copy* trails its source: a
  search/vector index behind its documents, or a database replica behind primary.
- **RAG / version currency** — is your documentation describing the *current*
  release? Compares the documented version against a package registry (PyPI, npm,
  GitHub Releases, Docker Hub).
- **Version extractors** — pull the "documented version" straight from your
  content (YAML frontmatter, a semver token, a JSON field, an HTML `<meta>`, or a
  docs URL) so you don't hard-code it.
- **dbt integration** — turns dbt's own `sources.json` / `run_results.json` into
  the standard metrics automatically.
- **End-to-end freshness** — stamps the origin event-time into OpenTelemetry
  *baggage* so a downstream stage computes the *true* age across every hop, not
  just the last one.
- **The monitor** — register probes and it emits everything on your normal
  metrics export; a failing probe is isolated (doesn't break the others) but is
  still counted, never silently swallowed.

## The zero-code Collector receiver (no app changes)

List your sources in YAML; the Collector checks them on a schedule.

- **SQL / warehouse** — a quick `MAX(timestamp)` check, or a *trustworthy* custom
  query against a load-audit/watermark table (honest about completeness, not just
  "did one row arrive"). Postgres + MySQL drivers built in.
- **Kafka / MSK** — topic freshness, consumer lag, and optional consumer
  time-lag, per partition. Auth: plaintext, SASL `plain`/`scram-256`/`scram-512`
  over TLS, and **AWS MSK IAM**.
- **Kinesis** — per-shard stream freshness via the standard AWS credential chain
  (including the EC2/EKS role); paginated across all shards.
- **Schema Registry** — how many schema versions a Confluent subject is behind
  its latest, relative to your pinned/contract version.
- **DB migration** — is the database's applied migration version behind what your
  deploy expects?
- **File / HTTP / static** — a file's modification time, an HTTP `Last-Modified`
  header, or fixed values for demos/testing.

## The central Collector processor

- **Derive age from a bare timestamp** — lightweight apps emit only a timestamp
  and the Collector computes freshness centrally.
- **Central SLA policy** — apply freshness deadlines and breach flags in one
  place with per-source rules; it de-duplicates so a source that already reported
  its own SLA isn't evaluated twice.

## The reliability philosophy (in every feature)

- **Never fabricate a number** — an empty table, NULL result, timeout, unreadable
  file, or unparseable version becomes a *visible error*, never a fake "0 seconds
  old."
- **Honest about limits** — e.g. it flags when a timestamp comes from a
  producer's (less-trustworthy) clock vs the broker's, and when `MAX()` can be
  fooled by a partial load.
- **Emit what can't be reconstructed, derive the rest** — only ships metrics a
  backend genuinely can't recompute, keeping cost and cardinality low.

## The standardization pieces (what a *standard* needs)

- **Specification** — the agreed metric names, units, attributes, and per-source
  recipes, written to become an OpenTelemetry semantic convention.
- **Weaver model** — machine-readable, so the definitions validate and can
  generate code constants in many languages.
- **Conformance suite** — language-agnostic test cases so anyone can build a
  compatible implementation and prove it matches.
- **Staleness taxonomy** — a documented map of every *kind* of staleness to the
  feature that captures it, plus what's deliberately out of scope and why.

## Operations, packaging, and proof

- **Install / build** — `pip install` the Python library; build a custom
  Collector via Docker or the OpenTelemetry Collector Builder; a **Helm chart**
  for Kubernetes.
- **Live demo** — one command spins up real Postgres + Kafka behind the
  Collector, deliberately lets some sources go stale, and shows it on a pre-built
  **Grafana** dashboard with alerts.
- **Turnkey testing** — a one-command **EC2 bootstrap** that installs everything,
  runs all tests, and smoke-tests the live demo; plus a beginner-friendly AWS
  walkthrough.
- **CI** — automated tests across the Python library, both Go components, the
  conformance suite, and the paper.
- **Paper + OTEP** — a research preprint and a ready-to-submit OpenTelemetry
  Enhancement Proposal, with a ready-to-post community discussion issue.

## Quality bar

All of the above is tested: **54** Python tests, **42** Go receiver + **9** Go
processor tests, and **6** conformance cases — all passing (vet + gofmt clean) —
with the custom Collector building and validating. A dedicated hardening pass
fixed the known correctness bugs and enforces the honest-measurement rule
everywhere.

Stability: **Development** — names may change before they stabilize. Building the
custom Collector requires **Go 1.25+**.

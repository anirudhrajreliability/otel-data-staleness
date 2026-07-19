> Part of **otel-data-staleness** — see the [root README](../README.md) for the project overview and the SDK-vs-Collector comparison.

# otel-staleness (Python SDK)

Vendor-neutral OpenTelemetry instrumentation for **data staleness / data
freshness**, implementing the conventions in
[`../spec/semantic-conventions.md`](../spec/semantic-conventions.md).

You give the SDK *probes* that report the event time of the freshest record for
a logical source; it emits the standardized `data.staleness.*` metrics
(age, lag, last-update timestamp, records-behind, SLA threshold/breach) via the
normal OpenTelemetry metrics pipeline (OTLP, Prometheus, console, etc.).

## Install

```bash
pip install -e .              # core
pip install -e ".[sql,kafka,cache,dev]"   # with optional integrations
```

## Quick start

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter, PeriodicExportingMetricReader)

from otel_staleness import StalenessMonitor, conventions as sc
from otel_staleness.probes import SQLFreshnessProbe

reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

monitor = StalenessMonitor()
monitor.add_probe(SQLFreshnessProbe(
    fetch_max_epoch=lambda: get_latest_updated_at(),   # returns Unix seconds or datetime
    source_name="orders", system=sc.System.POSTGRESQL,
    namespace="public", sla_threshold_seconds=300))
monitor.start()
```

Run the bundled demo (emits all four source types to the console):

```bash
python examples/quickstart.py
```

## Probes

| Probe | Source | Method emitted |
|-------|--------|----------------|
| `SQLFreshnessProbe` | Postgres/MySQL/Snowflake/Redshift/BigQuery | `max_timestamp` |
| `KafkaFreshnessProbe` | Kafka/Kinesis (per partition) | `consumer_lag` |
| `PipelineFreshnessProbe` | dbt/Airflow batch jobs | `run_completion` |
| `CacheFreshnessProbe` | Redis | `ttl_age` |
| `ObjectStoreFreshnessProbe` | S3/GCS | `object_mtime` |
| `IndexFreshnessProbe` | Search/vector indexes (Elasticsearch, OpenSearch, Pinecone, Weaviate, Milvus, Qdrant, pgvector) | `index_lag` |
| `ReplicationFreshnessProbe` | Replicas / CDC targets / multi-region (Postgres replicas, Debezium, DynamoDB global tables) | `replication_lag` |
| `VersionFreshnessProbe` | RAG / docs version-currency vs a registry (PyPI, npm, GitHub, Docker Hub) | `version_drift` |

The first five emit **absolute** freshness (`age` vs `now`).
`IndexFreshnessProbe` / `ReplicationFreshnessProbe` emit **differential**
(source-relative) freshness — how far a derived/replicated store trails its
upstream — via `data.staleness.lag` plus a `data.staleness.relative_to`
attribute. `VersionFreshnessProbe` emits **version-currency** (how many releases
behind). Example of the differential form:

```python
from otel_staleness.probes import IndexFreshnessProbe
# how far the vector index trails its source corpus
monitor.add_probe(IndexFreshnessProbe(
    fetch_source_epoch=lambda: newest_source_write_time(),
    fetch_index_epoch=lambda: newest_source_time_present_in_index(),
    index_name="docs-v1", source="public.documents",
    system=sc.System.PINECONE, sla_threshold_seconds=60))
```

**Version-currency for RAG / documentation** — is your indexed doc describing
the *current* release? Compares the documented version against a registry:

```python
from otel_staleness.probes import VersionFreshnessProbe
# your RAG corpus documents Kubernetes 1.28; how far behind is it?
monitor.add_probe(VersionFreshnessProbe.from_github_releases(
    "kubernetes/kubernetes", documented_version="1.28.0",
    source_name="k8s-docs", sla_threshold_seconds=60*60*24*14))  # 14 days
```

Emits `records.behind` (releases behind), `age` (how long newer releases have
existed unreflected), and `version.documented` / `version.current` attributes.
Helpers: `from_pypi`, `from_npm`, `from_github_releases`, `from_dockerhub`
(needs the `version` extra: `pip install -e ".[version]"`). The comparison logic
is pure and unit-tested; unparseable versions raise rather than fabricate a `0`.

The **documented version** rarely needs hard-coding — extract it from the
content with `otel_staleness.version_extract` (frontmatter, a semver token, a
JSON field, an HTML `<meta>`, or a docs URL path), composed with `first_of`:

```python
from otel_staleness.version_extract import first_of, extract_frontmatter_version, extract_url_path_version
documented = first_of(
    lambda: extract_frontmatter_version(open("docs/networking.md").read()),
    lambda: extract_url_path_version("https://kubernetes.io/docs/v1.28/"),
)  # pass as documented_version=...
```

See `examples/rag_freshness.py` for the combined RAG check: index time-lag
(`IndexFreshnessProbe`) **and** version currency (`VersionFreshnessProbe`) on the
same corpus — the two failure modes that make RAG answers wrong.

Every probe accepts plain callables (returning Unix seconds or a `datetime`),
so it is fully unit-testable without a live backend. `SQLFreshnessProbe` also
ships a `from_sqlalchemy(...)` convenience constructor.

A failing probe is isolated — it cannot break collection of the others.

## End-to-end freshness across hops

Point probes measure freshness *at one place*. To get **true cross-hop age**,
stamp the origin event-time into OTEL baggage at ingest; any downstream stage
reads it back (it rides the standard OTEL context propagators across
service/messaging boundaries):

```python
from otel_staleness.freshness_context import stamp_origin, EndToEndFreshness
from opentelemetry import context as otel_context

# at ingest:
otel_context.attach(stamp_origin(event_epoch))
# ...work flows downstream carrying baggage...
# at a later stage:
e2e = EndToEndFreshness()              # emits data.staleness.age, method=end_to_end
e2e.record("orders-pipeline", stage="serve")
```

## Every staleness dimension

`StalenessMonitor.start()` also emits the extension metrics that capture the
dimensions a backend cannot reconstruct: `data.staleness.age.peak` (Peak AoI),
`data.staleness.update.interval` (cadence histogram), `data.staleness.partition.skew`
(straggler partitions), and `data.staleness.probe.errors` (a failing probe is now
**visible**, not silently swallowed — pass `add_probe(p, name="...")` to label it).
See [`../docs/STALENESS-TAXONOMY.md`](../docs/STALENESS-TAXONOMY.md) for the full
map of staleness types to mechanisms.

## Custom probes

Subclass `StalenessProbe` and return `FreshnessReading` objects:

```python
from otel_staleness import StalenessProbe, FreshnessReading, conventions as sc

class MyProbe(StalenessProbe):
    def read(self):
        return [FreshnessReading(
            source_system="custom", source_name="widget",
            last_update_epoch=get_event_time(),       # or age_seconds=...
            sla_threshold_seconds=60)]
```

## Tests

```bash
python -m pytest -q                 # 50 tests
PYTHONPATH=src python ../conformance/runner.py   # conformance vectors
```

Requires **Python 3.8+**. Optional extras: `sql`, `kafka`, `cache`, `otlp`,
`version`, `dev` (e.g. `pip install -e ".[dev,version]"`).

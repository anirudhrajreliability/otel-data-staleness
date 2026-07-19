"""Combined RAG freshness: the two failure modes that make RAG answers wrong.

1. TIME  — the vector index trails its source corpus (IndexFreshnessProbe).
2. CURRENCY — the corpus itself documents an outdated software version
   (VersionFreshnessProbe), with the documented version extracted from the docs.

    python examples/rag_freshness.py
"""
import time

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

from otel_staleness import StalenessMonitor, conventions as sc
from otel_staleness.probes import IndexFreshnessProbe, VersionFreshnessProbe
from otel_staleness.version_extract import (
    first_of, extract_frontmatter_version, extract_url_path_version,
)

reader = PeriodicExportingMetricReader(ConsoleMetricExporter(), export_interval_millis=5000)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
monitor = StalenessMonitor()

# (1) TIME: how far the Pinecone index trails the source corpus.
monitor.add_probe(IndexFreshnessProbe(
    fetch_source_epoch=lambda: newest_doc_edit_time(),        # your CMS/git
    fetch_index_epoch=lambda: newest_doc_time_in_index(),     # your vector DB
    index_name="k8s-docs-v1", source="docs-corpus",
    system=sc.System.PINECONE, sla_threshold_seconds=3600))

# (2) CURRENCY: does the corpus document the current Kubernetes release?
# The documented version is pulled from the doc's frontmatter, falling back to
# the docs-site URL path (e.g. /docs/v1.28/...).
sample_doc = "---\ntitle: Networking\nversion: 1.28.0\n---\n"
doc_url = "https://kubernetes.io/docs/v1.28/concepts/"
monitor.add_probe(VersionFreshnessProbe.from_github_releases(
    "kubernetes/kubernetes",
    documented_version=first_of(
        lambda: extract_frontmatter_version(sample_doc),
        lambda: extract_url_path_version(doc_url),
    ),
    source_name="k8s-docs", sla_threshold_seconds=60 * 60 * 24 * 14))  # 14 days

monitor.start()
print("Emitting RAG freshness (index time-lag + version currency). Ctrl-C to stop.")


def newest_doc_edit_time():
    return time.time() - 120


def newest_doc_time_in_index():
    return time.time() - 900   # index ~13 min behind the corpus


try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

"""Demo: emit data-staleness metrics to a Collector over OTLP.

Simulates four sources; the 'clickstream' Kafka topic progressively falls
behind and breaches its SLA so the Grafana dashboard visibly lights up.

    pip install otel-staleness opentelemetry-exporter-otlp
    OTLP_ENDPOINT=http://localhost:4318 python generator.py
"""
import os
import time

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

from otel_staleness import StalenessMonitor, conventions as sc
from otel_staleness.probes import (
    SQLFreshnessProbe, KafkaFreshnessProbe, PipelineFreshnessProbe, CacheFreshnessProbe,
)
from otel_staleness.probes.kafka import PartitionState

endpoint = os.environ.get("OTLP_ENDPOINT", "http://localhost:4318")
reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=f"{endpoint}/v1/metrics"), export_interval_millis=5000)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

START = time.time()
monitor = StalenessMonitor()
monitor.add_probe(SQLFreshnessProbe(lambda: time.time() - 5, source_name="orders",
                  system=sc.System.POSTGRESQL, namespace="public", sla_threshold_seconds=60))
# clickstream falls behind ~1s per second elapsed -> breaches 30s SLA after ~30s
monitor.add_probe(KafkaFreshnessProbe(
    lambda: [PartitionState("0", last_event_epoch=START,
                            last_consume_epoch=time.time(),
                            records_behind=int(time.time() - START) * 10)],
    topic="clickstream", consumer_group="analytics", sla_threshold_seconds=30))
monitor.add_probe(PipelineFreshnessProbe(lambda: time.time() - 600, model="mart.daily_revenue",
                  system=sc.System.DBT, sla_threshold_seconds=300))
monitor.add_probe(CacheFreshnessProbe(lambda: time.time() - 2, key_namespace="sessions",
                  system=sc.System.REDIS, sla_threshold_seconds=120))
monitor.start()

print(f"Emitting data-staleness metrics to {endpoint}. Ctrl-C to stop.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

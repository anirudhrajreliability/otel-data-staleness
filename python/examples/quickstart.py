"""Runnable demo: emit data-staleness metrics to the console.

    python examples/quickstart.py

Simulates four sources (SQL, Kafka, dbt, Redis) with synthetic ages and prints
the standardized metrics every few seconds via the OTLP console exporter.
"""
import time

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

from otel_staleness import StalenessMonitor, conventions as sc
from otel_staleness.probes import (
    SQLFreshnessProbe, KafkaFreshnessProbe, PipelineFreshnessProbe, CacheFreshnessProbe,
)
from otel_staleness.probes.kafka import PartitionState

START = time.time()

reader = PeriodicExportingMetricReader(ConsoleMetricExporter(), export_interval_millis=3000)
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

monitor = StalenessMonitor()
# SQL: fresh, updates every loop
monitor.add_probe(SQLFreshnessProbe(lambda: time.time() - 5, source_name="orders",
                  system=sc.System.POSTGRESQL, namespace="public", sla_threshold_seconds=60))
# Kafka: one partition slowly falling behind
monitor.add_probe(KafkaFreshnessProbe(
    lambda: [PartitionState("0", last_event_epoch=time.time() - (time.time() - START),
                            last_consume_epoch=time.time(), records_behind=int(time.time() - START))],
    topic="clickstream", consumer_group="analytics", sla_threshold_seconds=30))
# dbt: last run was 10 minutes ago (breaches a 5 min SLA)
monitor.add_probe(PipelineFreshnessProbe(lambda: time.time() - 600, model="mart.daily_revenue",
                  system=sc.System.DBT, sla_threshold_seconds=300))
# Redis cache: written 2s ago
monitor.add_probe(CacheFreshnessProbe(lambda: time.time() - 2, key_namespace="sessions",
                  system=sc.System.REDIS, sla_threshold_seconds=120))
monitor.start()

print("Emitting data-staleness metrics to console. Ctrl-C to stop.")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

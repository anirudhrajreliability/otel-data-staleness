#!/usr/bin/env python3
"""SDK-in-the-live-path service.

Runs the otel-staleness **SDK** against REAL Postgres + Redis and exports
`data.staleness.*` over OTLP to the Collector — so the SDK producer path is
validated end-to-end against live backends, not merely unit-tested with fakes.

`build_monitor()` keeps all heavy/optional imports lazy so the module can be
imported and unit-tested (see test_sdk_probe_service.py) without psycopg2, redis,
or the OTLP exporter installed.
"""
from __future__ import annotations

import os
import time


def make_pg_epoch_fetch(dsn: str, query: str):
    """Return a callable that yields the freshest epoch (float seconds) from PG."""
    import psycopg2  # lazy

    def fetch():
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
                return row[0] if row and row[0] is not None else None
        finally:
            conn.close()

    return fetch


def make_redis_epoch_fetch(url: str, key: str):
    """Return a callable that yields the last-write epoch stored at `key`."""
    import redis  # lazy

    def fetch():
        r = redis.from_url(url)
        v = r.get(key)
        return float(v) if v is not None else None

    return fetch


def build_monitor(meter, pg_fetch, redis_fetch, sla: float = 60.0):
    """Wire an SDK monitor with a SQL probe (Postgres) and a cache probe (Redis).

    `pg_fetch` / `redis_fetch` are plain callables returning Unix seconds, so this
    is fully testable without live backends.
    """
    from otel_staleness import StalenessMonitor, conventions as sc
    from otel_staleness.probes import SQLFreshnessProbe, CacheFreshnessProbe

    mon = StalenessMonitor(meter)
    mon.add_probe(SQLFreshnessProbe(
        pg_fetch, source_name="sdk_orders", system=sc.System.POSTGRESQL,
        namespace="demo", sla_threshold_seconds=sla))
    mon.add_probe(CacheFreshnessProbe(
        redis_fetch, key_namespace="sdk_cache", system=sc.System.REDIS,
        sla_threshold_seconds=sla))
    return mon


def main():
    from opentelemetry import metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter

    endpoint = os.environ.get("OTLP_ENDPOINT", "http://otel-collector:4318/v1/metrics")
    pg_dsn = os.environ.get(
        "PG_DSN", "host=postgres user=postgres password=demopw dbname=postgres")
    redis_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=endpoint), export_interval_millis=10000)
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    meter = metrics.get_meter("otel-staleness-sdk-service")

    pg_fetch = make_pg_epoch_fetch(
        pg_dsn, "SELECT EXTRACT(EPOCH FROM MAX(updated_at)) FROM demo.sdk_orders")
    redis_fetch = make_redis_epoch_fetch(redis_url, "sdk_cache:last_write")
    build_monitor(meter, pg_fetch, redis_fetch).start()
    print(f"sdk-service: exporting data.staleness.* via OTLP -> {endpoint}", flush=True)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from otel_staleness import StalenessMonitor, conventions as sc
from otel_staleness.probes import (
    SQLFreshnessProbe, KafkaFreshnessProbe, PipelineFreshnessProbe,
    CacheFreshnessProbe, ObjectStoreFreshnessProbe,
)
from otel_staleness.probes.kafka import PartitionState
from otel_staleness._timeutil import to_epoch


def test_sql_probe_reading():
    p = SQLFreshnessProbe(lambda: 900.0, source_name="orders",
                          system=sc.System.POSTGRESQL, namespace="public",
                          sla_threshold_seconds=300)
    r = list(p.read())[0]
    assert r.source_name == "orders"
    assert r.method == sc.Method.MAX_TIMESTAMP
    assert r.last_update_epoch == 900.0
    assert r.compute_age(now=1000.0) == 100.0


def test_sql_probe_accepts_decimal_epoch():
    # PostgreSQL EXTRACT(EPOCH FROM ...) returns numeric -> psycopg2 Decimal.
    p = SQLFreshnessProbe(lambda: Decimal("900.5"), source_name="orders",
                          system=sc.System.POSTGRESQL, sla_threshold_seconds=300)
    r = list(p.read())[0]
    assert r.last_update_epoch == 900.5
    assert r.compute_age(now=1000.5) == 100.0


def test_to_epoch_types():
    assert to_epoch(Decimal("1700000000")) == 1700000000.0
    assert to_epoch(5) == 5.0
    assert to_epoch(None) is None
    with pytest.raises(TypeError):
        to_epoch(True)          # bool is never a valid timestamp
    with pytest.raises(TypeError):
        to_epoch("not-a-number")


def test_sql_probe_accepts_datetime():
    dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
    p = SQLFreshnessProbe(lambda: dt, source_name="t")
    r = list(p.read())[0]
    assert r.last_update_epoch == dt.timestamp()


def test_kafka_probe_per_partition_lag():
    def fetch():
        return [
            PartitionState("0", last_event_epoch=950.0, last_consume_epoch=952.0, records_behind=5),
            PartitionState("1", last_event_epoch=900.0, last_consume_epoch=905.0, records_behind=20),
        ]
    p = KafkaFreshnessProbe(fetch, topic="events", consumer_group="g1")
    rs = list(p.read())
    assert len(rs) == 2
    assert rs[0].partition == "0"
    assert rs[0].lag_seconds == 2.0
    assert rs[1].records_behind == 20
    assert rs[0].method == sc.Method.CONSUMER_LAG


def test_pipeline_probe():
    p = PipelineFreshnessProbe(lambda: 800.0, model="mart.revenue",
                               system=sc.System.DBT, sla_threshold_seconds=3600)
    r = list(p.read())[0]
    assert r.method == sc.Method.RUN_COMPLETION
    assert r.pipeline_stage == "transform"


def test_cache_and_object_probes():
    c = CacheFreshnessProbe(lambda: 990.0, key_namespace="sessions",
                            system=sc.System.REDIS)
    rc = list(c.read())[0]
    assert rc.method == sc.Method.TTL_AGE

    o = ObjectStoreFreshnessProbe(lambda: 500.0, prefix="raw/events/",
                                  bucket="lake", system=sc.System.S3)
    ro = list(o.read())[0]
    assert ro.method == sc.Method.OBJECT_MTIME
    assert ro.namespace == "lake"


def test_probe_end_to_end_with_monitor():
    mon = StalenessMonitor(now_fn=lambda: 1000.0)
    mon.add_probe(SQLFreshnessProbe(lambda: 940.0, source_name="orders"))
    readings = mon.collect_readings()
    assert readings[0].compute_age(1000.0) == 60.0


# --- differential (source-relative) freshness ------------------------------
from otel_staleness.probes import IndexFreshnessProbe, ReplicationFreshnessProbe


def test_index_probe_differential_and_absolute():
    # source newest = 1000, index reflects up to 970 -> 30s behind corpus
    p = IndexFreshnessProbe(lambda: 1000.0, lambda: 970.0,
                            index_name="docs-v1", source="public.documents",
                            system=sc.System.PINECONE, sla_threshold_seconds=60)
    r = list(p.read())[0]
    assert r.method == sc.Method.INDEX_LAG
    assert r.relative_to == "public.documents"
    assert r.lag_seconds == 30.0                    # differential
    assert r.last_update_epoch == 970.0             # absolute basis
    assert r.compute_age(now=1000.0) == 30.0
    assert sc.ATTR_RELATIVE_TO in r.attributes()


def test_index_probe_lag_never_negative():
    # index somehow ahead of reported source -> clamp to 0
    p = IndexFreshnessProbe(lambda: 900.0, lambda: 950.0,
                            index_name="i", source="s")
    r = list(p.read())[0]
    assert r.lag_seconds == 0.0


def test_replication_probe():
    p = ReplicationFreshnessProbe(lambda: 1000.0, lambda: 985.0,
                                  dataset="orders", source="primary",
                                  system=sc.System.POSTGRESQL, sla_threshold_seconds=10)
    r = list(p.read())[0]
    assert r.method == sc.Method.REPLICATION_LAG
    assert r.relative_to == "primary"
    assert r.lag_seconds == 15.0
    assert r.last_update_epoch == 985.0


def test_relative_to_in_reading_attributes():
    from otel_staleness import FreshnessReading
    r = FreshnessReading(source_system="qdrant", source_name="idx",
                         relative_to="corpus")
    assert r.attributes()[sc.ATTR_RELATIVE_TO] == "corpus"

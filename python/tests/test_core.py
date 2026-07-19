import time

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from otel_staleness import StalenessMonitor, FreshnessReading
from otel_staleness import conventions as sc


def _collect(monitor_setup):
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("test")
    monitor = StalenessMonitor(meter, now_fn=lambda: 1000.0)
    monitor_setup(monitor)
    monitor.start()
    data = reader.get_metrics_data()
    points = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                pts = []
                for p in m.data.data_points:
                    pts.append((dict(p.attributes), getattr(p, "value", None)))
                points[m.name] = pts
    return points


def test_age_from_last_update():
    def setup(mon):
        mon.add_probe(lambda: [FreshnessReading(
            source_system="postgresql", source_name="orders",
            last_update_epoch=940.0, method=sc.Method.MAX_TIMESTAMP)])
    pts = _collect(setup)
    assert sc.METRIC_AGE in pts
    attrs, value = pts[sc.METRIC_AGE][0]
    assert value == 60.0  # 1000 - 940
    assert attrs[sc.ATTR_SOURCE_SYSTEM] == "postgresql"
    assert attrs[sc.ATTR_SOURCE_NAME] == "orders"


def test_explicit_age_preferred():
    def setup(mon):
        mon.add_probe(lambda: [FreshnessReading(
            source_system="kafka", source_name="t", age_seconds=12.5)])
    pts = _collect(setup)
    _, value = pts[sc.METRIC_AGE][0]
    assert value == 12.5


def test_age_never_negative():
    def setup(mon):
        mon.add_probe(lambda: [FreshnessReading(
            source_system="s3", source_name="p", last_update_epoch=1500.0)])
    pts = _collect(setup)
    _, value = pts[sc.METRIC_AGE][0]
    assert value == 0.0


def test_sla_breached_flag():
    def setup(mon):
        mon.add_probe(lambda: [FreshnessReading(
            source_system="dbt", source_name="m",
            last_update_epoch=800.0, sla_threshold_seconds=100.0)])  # age 200 > 100
    pts = _collect(setup)
    _, breached = pts[sc.METRIC_SLA_BREACHED][0]
    assert breached == 1
    _, thr = pts[sc.METRIC_SLA_THRESHOLD][0]
    assert thr == 100.0


def test_sla_not_breached():
    def setup(mon):
        mon.add_probe(lambda: [FreshnessReading(
            source_system="dbt", source_name="m",
            last_update_epoch=950.0, sla_threshold_seconds=100.0)])  # age 50 < 100
    pts = _collect(setup)
    _, breached = pts[sc.METRIC_SLA_BREACHED][0]
    assert breached == 0


def test_failing_probe_isolated():
    def boom():
        raise RuntimeError("nope")
    def setup(mon):
        mon.add_probe(boom)
        mon.add_probe(lambda: [FreshnessReading(
            source_system="redis", source_name="k", last_update_epoch=990.0)])
    pts = _collect(setup)
    # the good probe still produced a reading
    assert pts[sc.METRIC_AGE][0][1] == 10.0


def test_lag_and_records_behind():
    def setup(mon):
        mon.add_probe(lambda: [FreshnessReading(
            source_system="kafka", source_name="t", partition="0",
            last_update_epoch=980.0, lag_seconds=3.0, records_behind=42)])
    pts = _collect(setup)
    assert pts[sc.METRIC_LAG][0][1] == 3.0
    assert pts[sc.METRIC_RECORDS_BEHIND][0][1] == 42


def test_collect_caches_from_completion():
    from otel_staleness import StalenessMonitor
    calls = {"n": 0}

    def probe():
        calls["n"] += 1
        return [FreshnessReading(source_system="p", source_name="t", age_seconds=1)]

    # now() advances 10s "during" the first collection (start=1000, completion=1010),
    # then tiny steps. With the fix, the window is measured from completion.
    seq = iter([1000.0, 1010.0, 1010.05])
    mon = StalenessMonitor(now_fn=lambda: next(seq))
    mon.add_probe(probe)
    mon.collect_readings()   # runs the probe; cache stamped at completion (1010)
    mon.collect_readings()   # now=1010.05, within TTL of 1010 -> cache hit
    assert calls["n"] == 1


def test_add_probe_names_function():
    from otel_staleness import StalenessMonitor
    mon = StalenessMonitor()

    def my_named_probe():
        return []

    mon.add_probe(my_named_probe)
    assert mon._probes[0][1] == "my_named_probe"   # not the generic "function"

from opentelemetry import context as otel_context
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from otel_staleness import conventions as sc
from otel_staleness.freshness_context import (
    stamp_origin, get_origin, end_to_end_age, EndToEndFreshness, ORIGIN_TIME_KEY,
)

NOW = 1_000_000.0


def test_stamp_and_read_roundtrip():
    ctx = stamp_origin(NOW - 42)
    assert get_origin(ctx) == NOW - 42


def test_end_to_end_age_across_hops():
    # origin stamped at ingest; measured much later downstream
    ctx = stamp_origin(NOW - 300)
    assert end_to_end_age(now_fn=lambda: NOW, ctx=ctx) == 300.0


def test_unstamped_context_returns_none():
    assert get_origin(otel_context.Context()) is None
    assert end_to_end_age(now_fn=lambda: NOW, ctx=otel_context.Context()) is None


def test_age_never_negative():
    ctx = stamp_origin(NOW + 10)   # clock skew: origin "in the future"
    assert end_to_end_age(now_fn=lambda: NOW, ctx=ctx) == 0.0


def test_recorder_emits_age_with_end_to_end_method():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    e2e = EndToEndFreshness(provider.get_meter("t"), now_fn=lambda: NOW)

    ctx = stamp_origin(NOW - 120)
    age = e2e.record("orders-pipeline", system="pipeline", stage="serve", ctx=ctx)
    assert age == 120.0

    data = reader.get_metrics_data()
    pts = []
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == sc.METRIC_AGE:
                    for p in m.data.data_points:
                        pts.append((dict(p.attributes), p.value))
    assert pts, "expected an age datapoint"
    attrs, value = pts[0]
    assert value == 120.0
    assert attrs[sc.ATTR_METHOD] == "end_to_end"
    assert attrs[sc.ATTR_PIPELINE_STAGE] == "serve"
    assert attrs[sc.ATTR_SOURCE_NAME] == "orders-pipeline"


def test_end_to_end_via_monitor_single_age_instrument():
    # Wiring EndToEndFreshness through a monitor must NOT create a second
    # data.staleness.age instrument; the monitor's single gauge emits it.
    from otel_staleness import StalenessMonitor
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    mon = StalenessMonitor(provider.get_meter("t"), now_fn=lambda: NOW)
    e2e = EndToEndFreshness(monitor=mon, now_fn=lambda: NOW)
    mon.start()
    ctx = stamp_origin(NOW - 77)
    e2e.record("pipeline-x", stage="serve", ctx=ctx)

    data = reader.get_metrics_data()
    age_metrics = [m for rm in data.resource_metrics for sm in rm.scope_metrics
                   for m in sm.metrics if m.name == sc.METRIC_AGE]
    # exactly one age instrument, and our end_to_end series is present in it
    assert len(age_metrics) == 1
    methods = {dict(p.attributes).get(sc.ATTR_METHOD) for p in age_metrics[0].data.data_points}
    assert "end_to_end" in methods

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from otel_staleness import StalenessMonitor, FreshnessReading, conventions as sc


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def tick(self, d):
        self.t += d


def _setup(clock):
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    mon = StalenessMonitor(provider.get_meter("ext"), now_fn=clock)
    return reader, mon


def _values(reader, name):
    out = []
    data = reader.get_metrics_data()
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == name:
                    for p in m.data.data_points:
                        out.append(p)
    return out


def test_probe_errors_counted_once_per_export():
    clock = Clock(1000.0)
    reader, mon = _setup(clock)

    def boom():
        raise RuntimeError("nope")

    mon.add_probe(boom, name="boomer")
    mon.add_probe(lambda: [FreshnessReading(source_system="pg", source_name="t", last_update_epoch=990)])
    mon.start()

    _values(reader, sc.METRIC_AGE)  # export 1 (triggers collection)
    clock.tick(60)
    pts = _values(reader, sc.METRIC_PROBE_ERRORS)  # export 2
    assert pts, "expected probe.errors"
    # cumulative across 2 exports = 2, tagged with a low-cardinality error.type
    assert pts[0].value == 2
    assert pts[0].attributes[sc.ATTR_ERROR_TYPE] == "probe_error"
    # identity carried via the declared source.name attribute (not a stray key)
    assert pts[0].attributes[sc.ATTR_SOURCE_NAME] == "boomer"


def test_empty_reading_surfaces_probe_error():
    # A probe that returns an indeterminable reading (no age / last_update /
    # records_behind / lag) MUST surface probe.errors, not be silently dropped.
    clock = Clock(1000.0)
    reader, mon = _setup(clock)
    mon.add_probe(lambda: [FreshnessReading(
        source_system="postgresql", source_name="orders",
        sla_threshold_seconds=60)])  # NULL MAX() -> no freshness signal
    mon.start()

    age_pts = _values(reader, sc.METRIC_AGE)  # export 1
    assert age_pts == [], "an empty reading must NOT emit an age"
    clock.tick(60)
    pts = _values(reader, sc.METRIC_PROBE_ERRORS)  # export 2
    assert pts, "expected probe.errors for the empty reading"
    assert pts[0].attributes[sc.ATTR_ERROR_TYPE] == "no_value"
    assert pts[0].attributes[sc.ATTR_SOURCE_NAME] == "orders"


def test_timeout_maps_to_snake_case():
    clock = Clock(1000.0)
    reader, mon = _setup(clock)

    def slow():
        raise TimeoutError("deadline exceeded")

    mon.add_probe(slow, name="warehouse")
    mon.start()
    _values(reader, sc.METRIC_AGE)
    clock.tick(60)
    pts = _values(reader, sc.METRIC_PROBE_ERRORS)
    assert pts and pts[0].attributes[sc.ATTR_ERROR_TYPE] == "timeout"


def test_age_peak_tracks_and_resets():
    clock = Clock(1000.0)
    reader, mon = _setup(clock)
    state = {"last_update": 940.0}  # age 60 at t=1000
    mon.add_probe(lambda: [FreshnessReading(source_system="pg", source_name="t",
                                            last_update_epoch=state["last_update"])])
    mon.start()

    def peak():
        return _values(reader, sc.METRIC_AGE_PEAK)[0].value

    assert peak() == 60.0
    clock.tick(60)                       # no new data -> age 120
    assert peak() == 120.0
    clock.tick(60); state["last_update"] = clock() - 5   # fresh data -> age 5
    assert peak() == 5.0                 # peak reset on fresh data


def test_update_interval_histogram():
    clock = Clock(1000.0)
    reader, mon = _setup(clock)
    state = {"lu": 1000.0}
    mon.add_probe(lambda: [FreshnessReading(source_system="pg", source_name="t",
                                            last_update_epoch=state["lu"])])
    mon.start()
    _values(reader, sc.METRIC_AGE_PEAK)          # export 1: sets baseline lu=1000
    clock.tick(30); state["lu"] = 1030.0
    _values(reader, sc.METRIC_AGE_PEAK)          # export 2: record delta 30
    clock.tick(70); state["lu"] = 1100.0
    _values(reader, sc.METRIC_AGE_PEAK)          # export 3: record delta 70
    hp = _values(reader, sc.METRIC_UPDATE_INTERVAL)
    assert hp, "expected update.interval histogram"
    assert hp[0].count == 2
    assert abs(hp[0].sum - 100.0) < 1e-6


def test_partition_skew():
    clock = Clock(1000.0)
    reader, mon = _setup(clock)
    mon.add_probe(lambda: [
        FreshnessReading(source_system="kafka", source_name="topic", partition="0", last_update_epoch=990),   # age 10
        FreshnessReading(source_system="kafka", source_name="topic", partition="1", last_update_epoch=800),   # age 200
    ])
    mon.start()
    pts = _values(reader, sc.METRIC_PARTITION_SKEW)
    assert pts, "expected partition.skew"
    assert pts[0].value == 190.0    # 200 - 10
    assert sc.ATTR_PARTITION not in dict(pts[0].attributes)  # source-level aggregate

"""Unit tests for the pure assertion helpers in validate.py.

These verify the accuracy math without a live Prometheus, so the integration
harness's correctness logic is itself tested. Run: python -m pytest test_validate.py
"""
import validate as v


def test_parse_prom_success():
    payload = {
        "status": "success",
        "data": {"result": [
            {"metric": {"__name__": "data_staleness_age", "data_source_name": "orders"},
             "value": [1700000000, "123.5"]},
        ]},
    }
    series = v.parse_prom(payload)
    assert series == [({"__name__": "data_staleness_age", "data_source_name": "orders"}, 123.5)]


def test_parse_prom_error_raises():
    try:
        v.parse_prom({"status": "error", "error": "bad query"})
    except ValueError as e:
        assert "bad query" in str(e)
    else:
        raise AssertionError("expected ValueError on non-success response")


def test_pick_series_filters_by_labels():
    series = [
        ({"data_source_name": "orders", "data_staleness_partition": "0"}, 10.0),
        ({"data_source_name": "orders", "data_staleness_partition": "1"}, 20.0),
        ({"data_source_name": "other"}, 99.0),
    ]
    assert v.pick_series(series, data_source_name="orders") == [10.0, 20.0]
    assert v.pick_series(series, data_source_name="orders", data_staleness_partition="1") == [20.0]
    assert v.pick_series(series, data_source_name="missing") == []


def test_within_tolerance_absolute():
    assert v.within_tolerance(120.0, 121.0, abs_tol=2.0)
    assert not v.within_tolerance(120.0, 130.0, abs_tol=2.0)
    # exact match always passes
    assert v.within_tolerance(5.0, 5.0)


def test_within_tolerance_relative():
    # 5% of 1000 = 50, so 1040 is within, 1060 is not
    assert v.within_tolerance(1040.0, 1000.0, rel_tol=0.05)
    assert not v.within_tolerance(1060.0, 1000.0, rel_tol=0.05)


def test_within_tolerance_uses_looser_of_abs_or_rel():
    # abs_tol=1 is tight, rel_tol=0.1 of 1000 = 100 is loose -> loose wins
    assert v.within_tolerance(1090.0, 1000.0, abs_tol=1.0, rel_tol=0.1)


def test_records_behind_sum_across_partitions():
    # emulate what cmd_records_behind does: sum partitions
    series = [
        ({"data_source_name": "clickstream", "data_staleness_partition": "0"}, 400.0),
        ({"data_source_name": "clickstream", "data_staleness_partition": "1"}, 600.0),
    ]
    total = sum(v.pick_series(series, data_source_name="clickstream"))
    assert total == 1000.0
    assert v.within_tolerance(total, 1000.0, abs_tol=50.0)

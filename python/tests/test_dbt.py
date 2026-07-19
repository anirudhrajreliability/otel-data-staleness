import json
import os

from otel_staleness import conventions as sc
from otel_staleness.integrations import DbtSourceFreshnessProbe, DbtRunResultsProbe


def _write(tmp_path, name, doc):
    p = os.path.join(tmp_path, name)
    with open(p, "w") as fh:
        json.dump(doc, fh)
    return p


def test_dbt_source_freshness(tmp_path):
    doc = {
        "results": [
            {
                "unique_id": "source.proj.raw.orders",
                "max_loaded_at": "2026-06-29T00:00:00Z",
                "max_loaded_at_time_ago_in_s": 3600.0,
                "status": "pass",
                "criteria": {"error_after": {"count": 1, "period": "day"}},
            }
        ]
    }
    p = _write(str(tmp_path), "sources.json", doc)
    rs = DbtSourceFreshnessProbe(p).read()
    assert len(rs) == 1
    r = rs[0]
    assert r.source_system == sc.System.DBT
    assert r.source_name == "source.proj.raw.orders"
    assert r.method == sc.Method.MAX_TIMESTAMP
    from datetime import datetime, timezone
    expected = datetime(2026, 6, 29, tzinfo=timezone.utc).timestamp()
    assert abs(r.last_update_epoch - expected) < 1
    assert r.sla_threshold_seconds == 86400.0


def test_dbt_source_age_fallback(tmp_path):
    # no parseable timestamp -> use age field
    doc = {"results": [{"unique_id": "source.p.s.t", "max_loaded_at": None,
                        "max_loaded_at_time_ago_in_s": 120.0, "criteria": {}}]}
    p = _write(str(tmp_path), "sources.json", doc)
    r = DbtSourceFreshnessProbe(p).read()[0]
    assert r.last_update_epoch is None
    assert r.age_seconds == 120.0
    assert r.compute_age() == 120.0


def test_dbt_run_results_models_only(tmp_path):
    doc = {
        "results": [
            {"unique_id": "model.proj.mart_revenue", "status": "success",
             "timing": [{"name": "compile", "completed_at": "2026-06-29T01:00:00Z"},
                        {"name": "execute", "completed_at": "2026-06-29T01:05:00Z"}]},
            {"unique_id": "test.proj.not_null", "status": "pass", "timing": []},
        ]
    }
    p = _write(str(tmp_path), "run_results.json", doc)
    rs = DbtRunResultsProbe(p).read()
    assert len(rs) == 1  # the test row is filtered out
    assert rs[0].source_name == "model.proj.mart_revenue"
    assert rs[0].method == sc.Method.RUN_COMPLETION


def test_dbt_missing_file_raises():
    import pytest
    # a missing/corrupt artifact must be a visible failure, not a silent []
    with pytest.raises(RuntimeError):
        DbtSourceFreshnessProbe("/no/such.json").read()

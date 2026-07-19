from otel_staleness import conventions as sc
from otel_staleness.probes import VersionFreshnessProbe, VersionInfo
from otel_staleness.probes.version import compute_version_staleness

NOW = 1_000_000.0


def _info():
    # k8s-like: documented 1.28, current 1.31; release times ascending
    return VersionInfo(current="1.31.0", releases={
        "1.28.0": NOW - 400_000,
        "1.29.0": NOW - 300_000,   # oldest newer than 1.28
        "1.30.0": NOW - 200_000,
        "1.31.0": NOW - 100_000,
    })


def test_versions_behind_and_age():
    behind, age, current = compute_version_staleness("1.28.0", _info(), NOW)
    assert behind == 3
    assert age == 300_000.0            # now - release(1.29.0), the oldest newer
    assert current == "1.31.0"


def test_current_is_fresh():
    behind, age, _ = compute_version_staleness("1.31.0", _info(), NOW)
    assert behind == 0 and age == 0.0


def test_documented_ahead_of_release():
    behind, age, _ = compute_version_staleness("2.5.0", _info(), NOW)
    assert behind == 0 and age == 0.0


def test_prereleases_excluded_by_default():
    info = VersionInfo(current="1.31.0", releases={
        "1.31.0": NOW - 100_000, "2.0.0rc1": NOW - 50_000})
    behind, _, _ = compute_version_staleness("1.31.0", info, NOW)
    assert behind == 0            # rc not counted
    behind2, _, _ = compute_version_staleness("1.31.0", info, NOW, include_prereleases=True)
    assert behind2 == 1


def test_unparseable_documented_raises():
    import pytest
    with pytest.raises(ValueError):
        compute_version_staleness("not-a-version", _info(), NOW)


def test_age_none_when_no_date():
    info = VersionInfo(current="2.0.0", releases={"1.0.0": None, "2.0.0": None})
    behind, age, _ = compute_version_staleness("1.0.0", info, NOW)
    assert behind == 1 and age is None


def test_leading_v_and_tags():
    info = VersionInfo(current="v1.5.0", releases={
        "v1.4.0": NOW - 200_000, "v1.5.0": NOW - 100_000})
    behind, age, current = compute_version_staleness("v1.4.0", info, NOW)
    assert behind == 1 and age == 100_000.0


def test_probe_end_to_end():
    probe = VersionFreshnessProbe(
        documented_version="1.28.0",
        fetch_current=_info,
        source_name="kubernetes-docs",
        system=sc.System.DOCS,
        sla_threshold_seconds=1_209_600,   # 14 days
        now_fn=lambda: NOW,
    )
    r = probe.read()[0]
    assert r.method == sc.Method.VERSION_DRIFT
    assert r.records_behind == 3
    assert r.age_seconds == 300_000.0
    attrs = r.attributes()
    assert attrs[sc.ATTR_VERSION_DOCUMENTED] == "1.28.0"
    assert attrs[sc.ATTR_VERSION_CURRENT] == "1.31.0"
    assert attrs[sc.ATTR_SOURCE_NAME] == "kubernetes-docs"


def test_probe_documented_from_callable():
    probe = VersionFreshnessProbe(
        documented_version=lambda: "1.30.0",
        fetch_current=_info, source_name="docs", now_fn=lambda: NOW)
    r = probe.read()[0]
    assert r.records_behind == 1        # only 1.31 is newer


def test_current_not_in_releases_still_counts():
    # BUG repro: registry 'current' is newer than documented but absent from the
    # releases map -> must still report behind, not "up to date".
    info = VersionInfo(current="1.31.0", releases={"1.28.0": NOW - 300_000})
    behind, age, cur = compute_version_staleness("1.28.0", info, NOW)
    assert behind == 1
    assert cur == "1.31.0"
    # age unknown (current has no release date in the map) -> None, not a fake 0
    assert age is None

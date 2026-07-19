"""Unit test for the SDK-live service wiring.

Verifies build_monitor registers the two expected probes and that, given fake
fetchers returning known epochs, the monitor collects correct readings — all
without a live Postgres/Redis. Run: python -m pytest test_sdk_probe_service.py
"""
import time

import sdk_probe_service as svc


def test_build_monitor_wires_two_probes_and_reads_correctly():
    now = time.time()
    pg_epoch = now - 30.0     # sdk_orders is 30s old
    cache_epoch = now - 5.0   # sdk_cache is 5s old

    mon = svc.build_monitor(
        meter=None,  # falls back to the global no-op meter; fine for collection
        pg_fetch=lambda: pg_epoch,
        redis_fetch=lambda: cache_epoch,
        sla=60.0,
    )

    readings = mon.collect_readings()
    by_name = {r.source_name: r for r in readings}

    assert set(by_name) == {"sdk_orders", "sdk_cache"}

    # SQL probe -> Postgres, ~30s old
    age_orders = by_name["sdk_orders"].compute_age(now)
    assert 29.0 <= age_orders <= 31.0

    # Cache probe -> Redis, ~5s old
    age_cache = by_name["sdk_cache"].compute_age(now)
    assert 4.0 <= age_cache <= 6.0


def test_build_monitor_isolates_a_failing_probe():
    # A probe whose fetch raises must not stop the other from producing a reading.
    def boom():
        raise RuntimeError("db down")

    mon = svc.build_monitor(
        meter=None,
        pg_fetch=boom,
        redis_fetch=lambda: time.time() - 2.0,
        sla=60.0,
    )
    readings = mon.collect_readings()
    names = {r.source_name for r in readings}
    # sdk_orders failed; sdk_cache still collected.
    assert "sdk_cache" in names

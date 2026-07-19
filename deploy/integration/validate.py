#!/usr/bin/env python3
"""Query Prometheus and assert `data.staleness.*` metrics match expectations.

This is the assertion engine shared by the accuracy, load, SDK, and chaos
integration tests. It talks to a running Prometheus (populated by the Collector
scraping REAL backends) and checks that the emitted metrics are numerically
correct — not merely present.

Design notes
------------
* Accuracy is asserted primarily on ``data_staleness_last_update_timestamp``,
  which is the exact epoch the scraper read from the source and does NOT drift
  between scrape and query. ``data_staleness_age`` is checked within a tolerance
  that accounts for the collection interval and query delay.
* The pure helpers (``parse_prom``, ``pick_series``, ``within_tolerance``) carry
  the real logic and are unit-tested in ``test_validate.py`` — so the assertion
  math is verified even without a live stack.

Only the Python standard library is used, so this runs on a bare EC2 box.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request


# --------------------------------------------------------------------------
# Pure helpers (unit-tested in test_validate.py)
# --------------------------------------------------------------------------
def parse_prom(payload: dict) -> list[tuple[dict, float]]:
    """Turn a Prometheus /api/v1/query JSON response into [(labels, value), ...].

    Raises ValueError on a non-success response so a broken query fails loudly
    rather than silently returning zero series.
    """
    status = payload.get("status")
    if status != "success":
        raise ValueError(f"prometheus query failed: {payload.get('error', status)}")
    result = payload.get("data", {}).get("result", [])
    out: list[tuple[dict, float]] = []
    for r in result:
        labels = dict(r.get("metric", {}))
        # instant-vector: value = [ts, "float-as-string"]
        raw = r.get("value", [None, "nan"])[1]
        out.append((labels, float(raw)))
    return out


def pick_series(series: list[tuple[dict, float]], **filters) -> list[float]:
    """Return the values of series whose labels match ALL given filters."""
    vals: list[float] = []
    for labels, v in series:
        if all(str(labels.get(k)) == str(want) for k, want in filters.items()):
            vals.append(v)
    return vals


def within_tolerance(actual: float, expected: float,
                     abs_tol: float = 0.0, rel_tol: float = 0.0) -> bool:
    """True if actual is within abs_tol OR rel_tol*|expected| of expected."""
    return abs(actual - expected) <= max(abs_tol, rel_tol * abs(expected))


# --------------------------------------------------------------------------
# Live query (thin wrapper over Prometheus HTTP API)
# --------------------------------------------------------------------------
def prom_query(prom_url: str, query: str, timeout: float = 5.0) -> list[tuple[dict, float]]:
    url = prom_url.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (trusted local URL)
        payload = json.load(resp)
    return parse_prom(payload)


def poll(prom_url: str, query: str, predicate, *, wait_s: int, interval_s: int = 5,
         label: str = ""):
    """Poll `query` until `predicate(series)` is truthy or `wait_s` elapses.

    Returns (ok, last_series). Prints progress so the run is legible in CI logs.
    """
    deadline = time.time() + wait_s
    last: list[tuple[dict, float]] = []
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            last = prom_query(prom_url, query)
        except Exception as e:  # network hiccup while the stack warms up
            print(f"  [{attempt}] {label or query}: query error ({e}); retrying")
            time.sleep(interval_s)
            continue
        ok, detail = predicate(last)
        print(f"  [{attempt}] {label or query}: {detail}")
        if ok:
            return True, last
        time.sleep(interval_s)
    return False, last


# --------------------------------------------------------------------------
# CLI assertions
# --------------------------------------------------------------------------
def _fail(msg: str):
    print(f"FAIL ❌  {msg}")
    sys.exit(1)


def _pass(msg: str):
    print(f"PASS ✅  {msg}")
    sys.exit(0)


def cmd_age(a):
    """Assert age AND last_update.timestamp for a source are numerically right."""
    metric = "data_staleness_last_update_timestamp"
    q = f'{metric}{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        if not vals:
            return False, "no last_update series yet"
        got = vals[0]
        ok = within_tolerance(got, a.expect_epoch, abs_tol=a.ts_tol)
        return ok, f"last_update={got:.0f} want={a.expect_epoch:.0f} (±{a.ts_tol}s)"

    ok, _ = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} last_update")
    if not ok:
        _fail(f"{a.source}: last_update.timestamp never matched the injected epoch "
              f"{a.expect_epoch:.0f} within ±{a.ts_tol}s")

    # age should equal now - injected_epoch within a looser tolerance (drift +
    # collection interval). We recompute "expected age" at query time.
    age = pick_series(prom_query(a.prom, f'data_staleness_age{{data_source_name="{a.source}"}}'),
                      data_source_name=a.source)
    if not age:
        _fail(f"{a.source}: no data_staleness_age series")
    expected_age = time.time() - a.expect_epoch
    if not within_tolerance(age[0], expected_age, abs_tol=a.age_tol):
        _fail(f"{a.source}: age={age[0]:.1f}s but expected ~{expected_age:.1f}s "
              f"(±{a.age_tol}s)")
    _pass(f"{a.source}: last_update matches injected epoch AND age={age[0]:.1f}s "
          f"is correct (expected ~{expected_age:.1f}s)")


def cmd_records_behind(a):
    q = f'data_staleness_records_behind{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        if not vals:
            return False, "no records_behind series yet"
        total = sum(vals)  # sum across partitions
        ok = within_tolerance(total, a.expect, abs_tol=a.tol)
        return ok, f"records_behind(sum)={total:.0f} want={a.expect:.0f} (±{a.tol})"

    ok, series = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} lag")
    if not ok:
        _fail(f"{a.source}: records_behind never reached {a.expect}±{a.tol}")
    _pass(f"{a.source}: consumer lag matches injected backlog "
          f"({sum(pick_series(series, data_source_name=a.source)):.0f} records)")


def cmd_probe_errors(a):
    q = f'data_staleness_probe_errors{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        total = sum(vals) if vals else 0
        return total >= a.min, f"probe_errors(sum)={total:.0f} want>={a.min}"

    ok, _ = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} probe_errors")
    if not ok:
        _fail(f"{a.source}: probe_errors did not reach >={a.min} — a broken "
              f"source should be VISIBLE, not silent")
    _pass(f"{a.source}: failure surfaced as data_staleness_probe_errors (>= {a.min})")


def cmd_breached(a):
    q = f'data_staleness_sla_breached{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        if not vals:
            return False, "no sla_breached series yet"
        return vals[0] == a.expect, f"sla_breached={vals[0]:.0f} want={a.expect}"

    ok, _ = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} sla")
    if not ok:
        _fail(f"{a.source}: sla_breached never became {a.expect}")
    _pass(f"{a.source}: sla_breached == {a.expect} as expected")


def cmd_nonnegative(a):
    """Assert age is clamped to >= 0 even under a future (clock-skewed) timestamp."""
    q = f'data_staleness_age{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        if not vals:
            return False, "no age series yet"
        return vals[0] >= 0, f"age={vals[0]:.2f} (must be >= 0)"

    ok, series = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} clamp")
    if not ok:
        _fail(f"{a.source}: age went NEGATIVE under a future timestamp — clock "
              f"skew must clamp to 0, never fabricate a negative age")
    _pass(f"{a.source}: age clamped to >= 0 under a future/skewed timestamp")


def cmd_present(a):
    """Assert a source is emitting an age series at all (used for SDK-live check)."""
    q = f'data_staleness_age{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        return len(vals) >= 1, f"age series count={len(vals)} want>=1"

    ok, _ = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} present")
    if not ok:
        _fail(f"{a.source}: no data_staleness_age series appeared")
    _pass(f"{a.source}: emitting data_staleness_age")


def cmd_series(a):
    """Assert an arbitrary metric has >=min series for a source (e.g. version-drift
    sources emit data_staleness_records_behind, not age)."""
    q = f'{a.metric}{{data_source_name="{a.source}"}}'

    def pred(series):
        vals = pick_series(series, data_source_name=a.source)
        return len(vals) >= a.min, f"{a.metric} series count={len(vals)} want>={a.min}"

    ok, _ = poll(a.prom, q, pred, wait_s=a.wait, label=f"{a.source} {a.metric}")
    if not ok:
        _fail(f"{a.source}: metric {a.metric} did not appear (>= {a.min} series)")
    _pass(f"{a.source}: emitting {a.metric} (live scrape works)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Assert data.staleness.* metrics in Prometheus.")
    p.add_argument("--prom", default="http://localhost:9090", help="Prometheus base URL")
    p.add_argument("--wait", type=int, default=90, help="max seconds to poll")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("age", help="assert last_update + age accuracy")
    a.add_argument("--source", required=True)
    a.add_argument("--expect-epoch", type=float, required=True, dest="expect_epoch")
    a.add_argument("--ts-tol", type=float, default=2.0, dest="ts_tol")
    a.add_argument("--age-tol", type=float, default=20.0, dest="age_tol")
    a.set_defaults(func=cmd_age)

    r = sub.add_parser("records-behind", help="assert consumer lag == known backlog")
    r.add_argument("--source", required=True)
    r.add_argument("--expect", type=float, required=True)
    r.add_argument("--tol", type=float, default=50.0)
    r.set_defaults(func=cmd_records_behind)

    e = sub.add_parser("probe-errors", help="assert a broken source is visible")
    e.add_argument("--source", required=True)
    e.add_argument("--min", type=float, default=1.0)
    e.set_defaults(func=cmd_probe_errors)

    b = sub.add_parser("breached", help="assert sla_breached value")
    b.add_argument("--source", required=True)
    b.add_argument("--expect", type=int, default=1)
    b.set_defaults(func=cmd_breached)

    n = sub.add_parser("nonnegative", help="assert age clamps to >= 0 under skew")
    n.add_argument("--source", required=True)
    n.set_defaults(func=cmd_nonnegative)

    pr = sub.add_parser("present", help="assert a source emits an age series")
    pr.add_argument("--source", required=True)
    pr.set_defaults(func=cmd_present)

    s = sub.add_parser("series", help="assert a metric has >=min series for a source")
    s.add_argument("--source", required=True)
    s.add_argument("--metric", required=True)
    s.add_argument("--min", type=int, default=1)
    s.set_defaults(func=cmd_series)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Conformance runner for the data-staleness convention.

Validates an implementation against ``vectors.json``. This reference runner
drives the ``otel-staleness`` Python SDK; other implementations can reuse the
same vectors by emitting metrics for each case and comparing to ``expected``.

    python conformance/runner.py        # exits non-zero on any mismatch
"""
import json
import os
import sys

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from otel_staleness import StalenessMonitor, FreshnessReading

HERE = os.path.dirname(__file__)


def emit_for_case(case):
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    mon = StalenessMonitor(provider.get_meter("conformance"), now_fn=lambda: case["now"])
    mon.add_probe(lambda c=case: [FreshnessReading(**c["input"])])
    mon.start()
    data = reader.get_metrics_data()
    out = {}
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                for p in m.data.data_points:
                    out[m.name] = getattr(p, "value", None)
    return out


def main():
    with open(os.path.join(HERE, "vectors.json")) as fh:
        suite = json.load(fh)
    failures = 0
    for case in suite["cases"]:
        got = emit_for_case(case)
        for metric, want in case["expected"].items():
            actual = got.get(metric)
            ok = actual is not None and abs(float(actual) - float(want)) < 1e-9
            status = "ok" if ok else "FAIL"
            if not ok:
                failures += 1
            print(f"[{status}] {case['name']}: {metric} want={want} got={actual}")
    print(f"\n{'PASS' if failures == 0 else 'FAIL'} — {failures} mismatch(es)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

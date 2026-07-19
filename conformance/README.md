> Part of **otel-data-staleness** — see the [root README](../README.md) for the project overview and the SDK-vs-Collector comparison.

# Conformance suite

`vectors.json` is a language-agnostic specification test for the data-staleness
convention: each case gives a `now` and a reading's `input`, and the `expected`
metric values any conforming implementation MUST produce.

`runner.py` is the reference validator (drives the Python SDK). Other
implementations should load the same vectors, emit metrics per case, and compare
against `expected`. This is what lets the convention be a *standard* rather than
one library's behavior.

```bash
python conformance/runner.py   # exits non-zero on mismatch
```

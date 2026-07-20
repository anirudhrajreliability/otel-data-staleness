> Part of **otel-data-staleness** — see the [root README](../../README.md) for the project overview and the SDK-vs-Collector comparison.

# datastaleness processor

An OpenTelemetry Collector **metrics processor** that implements the server-side
half of the [data-staleness semantic conventions](../../spec/semantic-conventions.md).

It does two things as telemetry flows through the Collector:

1. **Derives `data.staleness.age`** from `data.staleness.last_update.timestamp`
   data points (`age = now − timestamp`). This lets lightweight producers emit
   only a *timestamp* and have the freshness age computed centrally — no clock
   logic in every app.
2. **Evaluates freshness SLAs** — for every age point (incoming or derived) it
   emits `data.staleness.sla.threshold` and `data.staleness.sla.breached` (`0/1`)
   based on configurable per-source thresholds.

## Configuration

```yaml
processors:
  datastaleness:
    # Derive data.staleness.age from data.staleness.last_update.timestamp.
    compute_age_from_last_update: true
    # Emit data.staleness.sla.threshold and data.staleness.sla.breached.
    evaluate_sla: true
    # Used when no rule below matches.
    default_threshold: 5m
    # First matching rule wins. Empty selector fields are wildcards.
    slas:
      - source_system: kafka
        threshold: 30s
      - source_name: orders
        threshold: 1m
```

| Field | Default | Meaning |
|-------|---------|---------|
| `compute_age_from_last_update` | `true` | Derive `age` from `last_update.timestamp`. |
| `evaluate_sla` | `true` | Emit threshold + breached metrics. |
| `default_threshold` | `5m` | Threshold when no rule matches (set `0` to disable). |
| `slas[].source_system` | — | Match on `data.source.system` (wildcard if empty). |
| `slas[].source_name` | — | Match on `data.source.name` (wildcard if empty). |
| `slas[].threshold` | — | Max acceptable age for this rule (must be `> 0`). |

## Example pipeline

```yaml
receivers:
  otlp:
    protocols: { grpc: {}, http: {} }
processors:
  datastaleness:
    compute_age_from_last_update: true
    evaluate_sla: true
    default_threshold: 5m
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [datastaleness]
      exporters: [prometheus]
```

## Building into a Collector

Add the component to an [OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/custom-collector/) manifest:

```yaml
processors:
  - gomod: github.com/otel-data-staleness/datastalenessprocessor v0.4.0
```

Register `NewFactory()` in your distribution's component list.

## Development

```bash
go test ./...        # unit tests
go vet ./...
gofmt -l .           # should print nothing
```

Stability: **development**. Tested against Collector API `v0.110.0` /
pdata `v1.16.0`.

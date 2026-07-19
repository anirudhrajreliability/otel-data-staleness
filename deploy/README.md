> Part of **otel-data-staleness** — see the [root README](../README.md) for the project overview and the SDK-vs-Collector comparison.

# Demo deployment

A runnable stack showing data-staleness metrics end to end:

```
otel-staleness SDK (generator) --OTLP--> Collector --> Prometheus --> Grafana
```

This demo uses the **stock** Collector contrib image (no custom build needed),
because the SDK emits the metrics directly over OTLP. To run the zero-code
*receiver* or the SLA *processor* instead, build a custom Collector (see
`../collector/*/README.md`).

## Run

```bash
docker compose up -d
# in another shell, on the host:
pip install otel-staleness opentelemetry-exporter-otlp
python demo/generator.py
```

Then open:

- **Grafana**: http://localhost:3000 (anonymous admin) -> dashboard **"Data Staleness"**
- **Prometheus**: http://localhost:9090 (alerts under Status -> Rules)

The `clickstream` source is rigged to fall behind and breach its 30s SLA after
~30 seconds, so you can watch the "Sources breaching SLA" stat flip and the
alert fire.

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yaml` | Collector + Prometheus + Grafana |
| `otel-collector-config.yaml` | OTLP in, Prometheus out (`add_metric_suffixes: false`) |
| `prometheus.yml` / `prometheus-alerts.yml` | scrape + freshness alert rules |
| `grafana/dashboard.json` | importable dashboard (also auto-provisioned) |
| `demo/generator.py` | simulates four sources, one going stale |

## Useful PromQL

```promql
data_staleness_sla_breached == 1                       # breaching sources
max by (data_source_system) (data_staleness_age)       # worst per system
increase(data_staleness_probe_errors[10m]) > 0         # failing probes
```

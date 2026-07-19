> Part of **otel-data-staleness** — see the [root README](../../README.md) for the project overview and the SDK-vs-Collector comparison.

# EC2 turnkey demo — real backends

This stands up the whole system against **real** Postgres and Kafka (not fakes),
so you can watch the zero-code Collector receiver detect staleness end to end:

```
custom Collector (receiver)  ──►  Prometheus  ──►  Grafana
   scraping real Postgres + Kafka + files
```

Generators create a clear contrast:

| Source | Behaviour | Expected |
|--------|-----------|----------|
| `orders_fresh` (Postgres) | a sidecar inserts every 10s | age ~0–10s, **not** breaching |
| `orders_stale` (Postgres) | seeded once, never updated | age climbs, **breaches** the 60s SLA |
| `events` (Kafka) | producer sends a burst then stops | topic age climbs, breaches |
| `export_fresh` (file) | touched every 10s | age ~0, fine |
| `export_stale` (file) | created once | age climbs, breaches |

## One command

On a fresh **Ubuntu 24.04** EC2 instance (t3.large, 30 GB), from the repo root:

```bash
bash scripts/ec2-bootstrap.sh
```

That installs Go 1.25 + Docker + Python, runs **every** test suite, builds and
validates the custom Collector, brings this stack up, and runs the smoke test
that asserts `data.staleness.*` metrics are flowing and a stale source is
breaching its SLA. Exit code 0 = everything works.

## Manual

```bash
sudo docker compose -f deploy/ec2-demo/docker-compose.yaml up -d --build
bash scripts/smoke-test.sh
```

View from your laptop via SSH tunnel (keep the ports closed publicly):

```bash
ssh -i KEY.pem -N -L 3000:localhost:3000 -L 9090:localhost:9090 ubuntu@<EC2_IP>
```

- **Grafana** http://localhost:3000 (anonymous admin) → dashboard **"Data Staleness"**
- **Prometheus** http://localhost:9090 → try `data_staleness_age`,
  `data_staleness_sla_breached == 1`, `data_staleness_probe_errors`

Within ~60–90s the two "stale" sources cross their 60s SLA: the *Sources
breaching SLA* stat rises and the alert fires, while the fresh sources stay low.

## Notes

- The Collector image is built from `collector/Dockerfile` (OCB build, Go 1.25) —
  the first `up --build` compiles it, which takes a few minutes.
- Security group: open only SSH (22) to your IP; reach the UIs via the tunnel.
- Teardown: `sudo docker compose -f deploy/ec2-demo/docker-compose.yaml down -v`
  then terminate the instance.
- This exercises the SQL, Kafka, and file scrapers against live services. The
  AWS-native paths (Kinesis, MSK IAM, Schema Registry) need those managed
  services; point additional sources at them per the receiver README.

> Part of **otel-data-staleness** — see the [root README](../README.md) for the project overview and the SDK-vs-Collector comparison.

# OpenTelemetry submission

This folder holds the artifacts for proposing the data-staleness convention to
OpenTelemetry.

- [`0000-data-staleness.md`](0000-data-staleness.md) — the formal **OTEP** draft
  (renumber once a PR number is assigned).
- [`SIG-DISCUSSION-ISSUE.md`](SIG-DISCUSSION-ISSUE.md) — a ready-to-post issue to
  **socialize** the idea and find a sponsor before any PR.

## Recommended order (don't open a PR cold)

OpenTelemetry's Semantic Conventions project explicitly deprioritizes
off-roadmap contributions without a sponsor, so lead with socialization:

1. **Publish the repo** (public) so the links in the issue resolve.
2. **Open the discussion issue** (`SIG-DISCUSSION-ISSUE.md`) in
   `open-telemetry/semantic-conventions`. This is your public flag-plant and the
   originator record.
3. **Join a Semantic Conventions SIG meeting** (schedule is on the OTel
   community calendar) and raise it. Aim to find a **sponsor/approver**.
4. **Only then** open the OTEP PR (`0000-data-staleness.md` → the assigned
   number) and, in parallel, a semantic-conventions PR modeling it in **Weaver**
   (we already have `model/registry/data-staleness.yaml`).
5. Iterate at **Development** stability; expect naming/scope changes before it
   stabilizes.

## Why this is well-positioned

- A complete, tested **reference implementation** in two languages already
  exists (Python SDK + Go Collector components) — the SIG values prototype-backed
  proposals.
- A **Weaver model** and a **conformance suite** are ready, which is what turns a
  proposal into a real, machine-checkable, multi-language standard.
- The framing is **consolidation of a known quantity** (Age of Information + what
  vendors already compute), not an unproven new metric — a much easier sell.

The honest gap is **adoption**: the strongest argument to the SIG is evidence
that multiple parties want this (stars, downloads, named users). Publishing and
socializing is how that starts.

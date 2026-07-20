> Part of **otel-data-staleness** — see the [root README](../README.md) for the project overview and the SDK-vs-Collector comparison.

# Path to OpenTelemetry adoption

This folder holds the artifacts for proposing the data-staleness convention to
OpenTelemetry, plus the current, verified process for getting it there.

- [`SIG-DISCUSSION-ISSUE.md`](SIG-DISCUSSION-ISSUE.md) — a ready-to-post issue to
  **socialize** the idea and find area owners/sponsors *before* any PR.
- [`0000-data-staleness.md`](0000-data-staleness.md) — a formal **OTEP-style
  design doc**. Note: semantic conventions are **not** submitted primarily via
  OTEP anymore (see below); keep this as the background/design record and only
  turn it into an OTEP PR if a maintainer asks for one (likely for the new
  `data.*` namespace decision).

> Process verified against the live semantic-conventions `CONTRIBUTING.md`
> (July 2026). The mechanism changed from the older "write an OTEP" model.

## The gate that governs everything: area ownership

The `semantic-conventions` repo runs an **automated area-ownership check**: a PR
that touches a `model/` or `docs/` area with **no active SIG/project** (status
`inactive` in [`AREAS.md`](https://github.com/open-telemetry/semantic-conventions/blob/main/AREAS.md))
is **automatically closed**. `data.staleness.*` is a brand-new area (a new
`data.*` root namespace), so **you cannot open a convention PR cold — it will be
auto-closed.** You must establish an active area/project *first*. The whole
sequence below is built around that.

## Phase 0 — Publish & sign (mechanical)

1. **Make this repo public** (GitHub → Settings → Danger Zone → Change visibility)
   so the links in the discussion issue resolve.
2. **Sign the CNCF CLA** — <https://identity.linuxfoundation.org/projects/cncf>.
   Required for every contribution; do it once now.

## Phase 1 — Socialize & find co-owners (the real work)

3. Join the **CNCF Slack** (<https://slack.cncf.io>), channel
   **`#otel-semantic-conventions`**. Introduce the problem + link the repo
   (spec, Weaver model, conformance suite, two-language reference implementation,
   real-backend validation).
4. **Attend a Semantic Conventions SIG meeting** (schedule on the OTel community
   calendar via <https://github.com/open-telemetry/community>); add it to the
   meeting agenda doc beforehand and raise it live.
5. **Open the discussion issue** (`SIG-DISCUSSION-ISSUE.md`) in
   `open-telemetry/semantic-conventions`. Explicitly ask: is there appetite, and
   who would **co-own** a `data.*`/freshness area?

Goal of this phase: a small group (ideally 2–3 people involved in instrumentation)
willing to be the point of contact. **Solo, off-roadmap proposals for new areas
are explicitly deprioritized** — the co-owners are the unlock.

## Phase 2 — Establish the area/project

6. With interest in hand, follow
   [`community/project-management.md`](https://github.com/open-telemetry/community/blob/main/project-management.md)
   and the semconv
   [`docs/how-to-write-conventions`](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/how-to-write-conventions/README.md)
   to register the area. Outcome you need: the area listed as **active with
   owners** in `AREAS.md`, which is what stops the auto-close.

## Phase 3 — Prepare the semantic-conventions PR

7. Fork `open-telemetry/semantic-conventions`. Map our artifacts onto their layout:
   - `model/registry/data-staleness.yaml` → `model/data/registry.yaml` (attributes)
     and `model/data/metrics.yaml` (the `data.staleness.*` metrics). Conform to the
     [semconv YAML schema](https://github.com/open-telemetry/weaver/blob/main/schemas/semconv-syntax.md)
     (close to ours, not identical).
   - `spec/semantic-conventions.md` → `docs/data/README.md` (tables auto-generated
     via `make generate-all`).
8. Tooling before pushing: `npm install`, then `make check` (style/spell/links),
   `make check-policies` (naming + backward-compat), and `make chlog-new` for the
   changelog entry.
9. **Link the prototype in the PR description** — non-trivial conventions must be
   prototyped in instrumentation, and we have exactly that. This is our strongest
   asset; lead with it.

## Phase 4 — Naming decision (may need an OTEP)

10. A new top-level **`data.*`** root namespace, and `staleness` vs `freshness`,
    are cross-cutting decisions the SIG will debate. If maintainers want a formal
    design record, convert `0000-data-staleness.md` into an OTEP PR in
    [`open-telemetry/oteps`](https://github.com/open-telemetry/oteps). **Don't
    lead with the OTEP** — only if asked.

## Phase 5 — Review & merge

11. Merge-ready = **two code-owner approvals** + a required review from
    **@specs-semconv-approvers** + no open discussions + **≥2 working days** since
    the last change. Iterate at **Development** stability; expect names to change
    before they stabilize.

## Pragmatic alternative: a decentralized registry first

The repo now actively points newcomers toward **third-party conventions built on
OTel's tooling** ("Want to define your own conventions outside this repo while
building on OTel's?"). We already have the Weaver model, so a realistic first move
is to publish `data.staleness.*` as a **decentralized Weaver registry**, drive real
adoption (stars, downloads, named users), and use that traction as the evidence to
upstream into core later. For a solo-origin proposal this is often faster than the
new-area gate, and **adoption is the single strongest argument** to bring to the SIG.

## Why this is well-positioned

- A complete, tested **reference implementation** in two languages (Python SDK +
  Go Collector receiver & processor), **validated end-to-end** against real
  backends — the SIG values prototype-backed proposals.
- A **Weaver model** and a **conformance suite** already exist — what turns a
  proposal into a machine-checkable, multi-language standard.
- The framing is **consolidation of a known quantity** (Age of Information + what
  observability vendors already compute), not an unproven new metric.

The honest remaining gap is **adoption**. Everything technical is in place; the
work ahead is social — Slack, a SIG meeting, and converting the prototype into a
couple of committed co-owners.

## Sources

- semantic-conventions `CONTRIBUTING.md` — <https://github.com/open-telemetry/semantic-conventions/blob/main/CONTRIBUTING.md>
- OTeps repo — <https://github.com/open-telemetry/oteps>
- OTel community / project management — <https://github.com/open-telemetry/community/blob/main/project-management.md>

"""End-to-end (cross-hop) freshness via OpenTelemetry baggage.

Point probes measure freshness *at one place*. This module carries the
**origin event-time** through a pipeline using OTEL baggage, so any downstream
stage can compute the *true end-to-end age* across every hop the event
traversed — not just the last one. Because it rides OTEL context/baggage, it
propagates across HTTP/gRPC/messaging with the standard OTEL propagators, with
no bespoke transport.

    # at ingest, stamp the event's origin time:
    ctx = stamp_origin(event_epoch)          # attach to the current context
    # ...work flows across services carrying baggage...
    # at any later stage:
    age = end_to_end_age()                    # now - origin_time, across all hops

`EndToEndFreshness` emits the standardized `data.staleness.age` gauge
(method=`end_to_end`) with the pipeline stage, so end-to-end age lands in the
same metric as every other freshness signal.
"""
from __future__ import annotations

import time
from typing import Dict, Optional, Tuple

from opentelemetry import baggage, context as otel_context, metrics
from opentelemetry.metrics import CallbackOptions, Observation

from . import conventions as sc
from .core import StalenessProbe

# Baggage key carrying the origin event time (Unix seconds, as a string).
ORIGIN_TIME_KEY = "data.staleness.origin_time"


def stamp_origin(event_epoch: float, ctx: Optional[otel_context.Context] = None) -> otel_context.Context:
    """Return a context with the origin event-time attached to baggage.

    Attach it to the active context with ``otel_context.attach(...)`` or pass it
    explicitly; it then propagates downstream via the OTEL propagators.
    """
    return baggage.set_baggage(ORIGIN_TIME_KEY, repr(float(event_epoch)), context=ctx)


def get_origin(ctx: Optional[otel_context.Context] = None) -> Optional[float]:
    v = baggage.get_baggage(ORIGIN_TIME_KEY, context=ctx)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def end_to_end_age(now_fn=time.time, ctx: Optional[otel_context.Context] = None) -> Optional[float]:
    """True end-to-end age = now - origin event time, or None if unstamped."""
    origin = get_origin(ctx)
    if origin is None:
        return None
    return max(0.0, now_fn() - origin)


class EndToEndFreshness(StalenessProbe):
    """Emits end-to-end age (from baggage) as the standard `data.staleness.age`
    with ``method=end_to_end``.

    Call :meth:`record` at each pipeline stage; the latest end-to-end age per
    (source, stage) is reported.

    Two ways to wire it up, so there is never a duplicate `data.staleness.age`
    instrument:

    - **With a monitor (recommended):** ``EndToEndFreshness(monitor=mon)`` — it
      registers itself as a probe on the monitor, so the monitor's single
      `data.staleness.age` gauge emits end-to-end readings too.
    - **Standalone:** ``EndToEndFreshness(meter)`` — registers its own gauge.
      Do *not* also share that meter with a ``StalenessMonitor`` (that would
      create a second `data.staleness.age` instrument).
    """

    def __init__(self, meter: Optional[metrics.Meter] = None, *, monitor=None, now_fn=time.time):
        self._now = now_fn
        # (source_system, source_name, stage) -> latest end-to-end age
        self._latest: Dict[Tuple[str, str, str], float] = {}
        if monitor is not None:
            monitor.add_probe(self, name="end_to_end")  # single shared age gauge
        else:
            meter = meter or metrics.get_meter("otel-staleness", sc.CONVENTION_VERSION)
            meter.create_observable_gauge(
                sc.METRIC_AGE, callbacks=[self._cb], unit=sc.UNIT_SECONDS,
                description="Current data staleness (now - event time of freshest record).",
            )

    def read(self):
        """Probe interface: emit the latest end-to-end age per (source, stage)."""
        from .core import FreshnessReading
        return [
            FreshnessReading(source_system=system, source_name=name,
                             method=sc.Method.END_TO_END, pipeline_stage=stage,
                             age_seconds=age)
            for (system, name, stage), age in self._latest.items()
        ]

    def record(
        self,
        source_name: str,
        *,
        system: str = "pipeline",
        stage: str = "serve",
        ctx: Optional[otel_context.Context] = None,
    ) -> Optional[float]:
        """Read the baggage origin time and update this stage's end-to-end age.

        Returns the age (or None if the context was not stamped upstream).
        """
        age = end_to_end_age(self._now, ctx)
        if age is not None:
            self._latest[(system, source_name, stage)] = age
        return age

    def _cb(self, options: CallbackOptions):
        out = []
        for (system, name, stage), age in self._latest.items():
            out.append(Observation(age, {
                sc.ATTR_SOURCE_SYSTEM: system,
                sc.ATTR_SOURCE_NAME: name,
                sc.ATTR_METHOD: sc.Method.END_TO_END,
                sc.ATTR_PIPELINE_STAGE: stage,
            }))
        return out

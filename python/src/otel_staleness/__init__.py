"""otel-staleness: vendor-neutral OpenTelemetry data-staleness instrumentation.

Quick start::

    from otel_staleness import StalenessMonitor, FreshnessReading
    from otel_staleness.probes import SQLFreshnessProbe

See ``spec/semantic-conventions.md`` for the metric/attribute definitions.
"""
from . import conventions
from .core import FreshnessReading, StalenessMonitor, StalenessProbe
from . import version_extract
from . import freshness_context

__version__ = "0.4.0"
__all__ = [
    "FreshnessReading",
    "StalenessMonitor",
    "StalenessProbe",
    "conventions",
    "version_extract",
    "freshness_context",
]

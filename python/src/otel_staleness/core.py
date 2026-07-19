"""Core data model and monitor for data-staleness instrumentation.

The design follows the Age-of-Information (AoI) model: a *probe* reports the
event time of the freshest record it can see for a logical source, and the
monitor turns that into the standardized OpenTelemetry metrics defined in
``conventions.py``.

    age = now - last_update_time
    lag = processing_time - event_time   (reported directly by the probe)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

from . import conventions as sc


@dataclass
class FreshnessReading:
    """A single point-in-time freshness observation for one logical source.

    Only ``source_system`` and ``source_name`` are required. Provide
    ``last_update_epoch`` (Unix seconds) and the monitor derives ``age``;
    alternatively provide ``age_seconds`` directly when the source exposes age
    natively. ``lag_seconds`` and ``records_behind`` are optional extra signals.
    """

    source_system: str
    source_name: str
    namespace: Optional[str] = None
    method: Optional[str] = None
    partition: Optional[str] = None
    pipeline_stage: Optional[str] = None
    relative_to: Optional[str] = None

    last_update_epoch: Optional[float] = None
    age_seconds: Optional[float] = None
    lag_seconds: Optional[float] = None
    records_behind: Optional[int] = None
    sla_threshold_seconds: Optional[float] = None
    extra_attributes: Optional[Dict[str, str]] = None

    def attributes(self) -> Dict[str, str]:
        attrs: Dict[str, str] = {
            sc.ATTR_SOURCE_SYSTEM: self.source_system,
            sc.ATTR_SOURCE_NAME: self.source_name,
        }
        if self.namespace is not None:
            attrs[sc.ATTR_SOURCE_NAMESPACE] = self.namespace
        if self.method is not None:
            attrs[sc.ATTR_METHOD] = self.method
        if self.partition is not None:
            attrs[sc.ATTR_PARTITION] = self.partition
        if self.pipeline_stage is not None:
            attrs[sc.ATTR_PIPELINE_STAGE] = self.pipeline_stage
        if self.relative_to is not None:
            attrs[sc.ATTR_RELATIVE_TO] = self.relative_to
        if self.extra_attributes:
            for k, v in self.extra_attributes.items():
                attrs[k] = v
        return attrs

    def key(self) -> tuple:
        a = self.attributes()
        return tuple(sorted(a.items()))

    def compute_age(self, now: Optional[float] = None) -> Optional[float]:
        """Resolve age in seconds, preferring an explicit ``age_seconds``."""
        if self.age_seconds is not None:
            return max(0.0, float(self.age_seconds))
        if self.last_update_epoch is not None:
            now = time.time() if now is None else now
            return max(0.0, now - float(self.last_update_epoch))
        return None


# A probe is any callable returning readings, or an object with ``read()``.
ProbeFn = Callable[[], Iterable[FreshnessReading]]


class StalenessProbe:
    """Base class for probes. Subclasses implement :meth:`read`."""

    def read(self) -> Iterable[FreshnessReading]:  # pragma: no cover
        raise NotImplementedError


def _as_probe_fn(probe) -> ProbeFn:
    if isinstance(probe, StalenessProbe):
        return probe.read
    if callable(probe):
        return probe
    raise TypeError("probe must be a StalenessProbe or a callable returning readings")


class StalenessMonitor:
    """Registers the data-staleness instruments and drives them from probes.

    Usage::

        monitor = StalenessMonitor(meter)
        monitor.add_probe(my_sql_probe)
        monitor.start()   # registers observable gauges; SDK collects on export
    """

    def __init__(self, meter: Optional[metrics.Meter] = None, *, now_fn: Callable[[], float] = time.time):
        self._meter = meter or metrics.get_meter("otel-staleness", sc.CONVENTION_VERSION)
        self._now_fn = now_fn
        self._probes: List[tuple] = []  # (fn, name)
        self._started = False
        # breach edge-detection state, keyed by series identity
        self._breached_state: Dict[tuple, bool] = {}
        self._breaches_counter = None
        self._probe_errors_counter = None
        self._update_interval_hist = None
        # stateful extension-metric bookkeeping, keyed by series identity
        self._peak: Dict[tuple, float] = {}
        self._last_age: Dict[tuple, float] = {}
        self._last_update: Dict[tuple, float] = {}
        # short-lived snapshot cache so all observable callbacks within one
        # export cycle share a single probe collection (probes run once, and a
        # failing probe is counted once).
        self._cache: Optional[List[FreshnessReading]] = None
        self._cache_at: float = 0.0
        self._cache_ttl = 0.1

    # -- probe registration -------------------------------------------------
    def add_probe(self, probe, name: Optional[str] = None) -> "StalenessMonitor":
        fn = _as_probe_fn(probe)
        if name is None:
            name = getattr(probe, "__name__", None) or type(probe).__name__
        self._probes.append((fn, name))
        return self

    def collect_readings(self) -> List[FreshnessReading]:
        # Serve a fresh snapshot within the export cycle to avoid re-running
        # probes (and double-counting errors) across the several callbacks.
        now = self._now_fn()
        if self._cache is not None and (now - self._cache_at) < self._cache_ttl:
            return self._cache
        readings: List[FreshnessReading] = []
        for fn, name in self._probes:
            try:
                result = fn()
            except Exception as exc:
                # a failing probe must not break the others, but the failure
                # MUST be visible (never silently swallowed).
                if self._probe_errors_counter is not None:
                    self._probe_errors_counter.add(
                        1, {sc.ATTR_ERROR_TYPE: type(exc).__name__, "probe": name})
                continue
            if result:
                readings.extend(result)
        self._cache = readings
        self._cache_at = self._now_fn()  # measure window from completion, not start
        return readings

    # -- instrument callbacks ----------------------------------------------
    def _cb_age(self, options: CallbackOptions) -> Iterable[Observation]:
        now = self._now_fn()
        out = []
        for r in self.collect_readings():
            age = r.compute_age(now)
            if age is not None:
                out.append(Observation(age, r.attributes()))
        return out

    def _cb_lag(self, options: CallbackOptions) -> Iterable[Observation]:
        return [
            Observation(float(r.lag_seconds), r.attributes())
            for r in self.collect_readings()
            if r.lag_seconds is not None
        ]

    def _cb_last_update(self, options: CallbackOptions) -> Iterable[Observation]:
        return [
            Observation(float(r.last_update_epoch), r.attributes())
            for r in self.collect_readings()
            if r.last_update_epoch is not None
        ]

    def _cb_records_behind(self, options: CallbackOptions) -> Iterable[Observation]:
        return [
            Observation(int(r.records_behind), r.attributes())
            for r in self.collect_readings()
            if r.records_behind is not None
        ]

    def _cb_threshold(self, options: CallbackOptions) -> Iterable[Observation]:
        return [
            Observation(float(r.sla_threshold_seconds), r.attributes())
            for r in self.collect_readings()
            if r.sla_threshold_seconds is not None
        ]

    def _cb_breached(self, options: CallbackOptions) -> Iterable[Observation]:
        now = self._now_fn()
        out = []
        for r in self.collect_readings():
            if r.sla_threshold_seconds is None:
                continue
            age = r.compute_age(now)
            if age is None:
                continue
            breached = age > float(r.sla_threshold_seconds)
            attrs = r.attributes()
            k = r.key()
            was = self._breached_state.get(k, False)
            if breached and not was and self._breaches_counter is not None:
                self._breaches_counter.add(1, attrs)
            self._breached_state[k] = breached
            out.append(Observation(1 if breached else 0, attrs))
        return out

    def _cb_age_peak(self, options: CallbackOptions) -> Iterable[Observation]:
        """Peak AoI: the max age reached before fresh data resets it. Also
        drives the update.interval histogram (both need cross-cycle state, so
        they share this one pass)."""
        now = self._now_fn()
        out = []
        for r in self.collect_readings():
            age = r.compute_age(now)
            k = r.key()
            if r.last_update_epoch is not None and self._update_interval_hist is not None:
                prev_lu = self._last_update.get(k)
                if prev_lu is not None and r.last_update_epoch > prev_lu:
                    self._update_interval_hist.record(r.last_update_epoch - prev_lu, r.attributes())
                self._last_update[k] = r.last_update_epoch
            if age is None:
                continue
            prev_age = self._last_age.get(k)
            if prev_age is None or age >= prev_age:
                peak = max(self._peak.get(k, age), age)
            else:
                peak = age  # age dropped -> fresh data arrived -> reset the peak
            self._peak[k] = peak
            self._last_age[k] = age
            out.append(Observation(peak, r.attributes()))
        return out

    def _cb_partition_skew(self, options: CallbackOptions) -> Iterable[Observation]:
        """max(age) - min(age) across the partitions of a single source."""
        now = self._now_fn()
        groups: Dict[tuple, List[float]] = {}
        meta: Dict[tuple, FreshnessReading] = {}
        for r in self.collect_readings():
            if r.partition is None:
                continue
            age = r.compute_age(now)
            if age is None:
                continue
            gk = (r.source_system, r.source_name, r.namespace)
            groups.setdefault(gk, []).append(age)
            meta.setdefault(gk, r)
        out = []
        for gk, ages in groups.items():
            if len(ages) < 2:
                continue
            r = meta[gk]
            attrs = {sc.ATTR_SOURCE_SYSTEM: r.source_system, sc.ATTR_SOURCE_NAME: r.source_name}
            if r.namespace is not None:
                attrs[sc.ATTR_SOURCE_NAMESPACE] = r.namespace
            out.append(Observation(max(ages) - min(ages), attrs))
        return out

    # -- lifecycle ----------------------------------------------------------
    def start(self) -> "StalenessMonitor":
        if self._started:
            return self
        m = self._meter
        m.create_observable_gauge(
            sc.METRIC_AGE, callbacks=[self._cb_age], unit=sc.UNIT_SECONDS,
            description="Current data staleness (now - event time of freshest record).",
        )
        m.create_observable_gauge(
            sc.METRIC_LAG, callbacks=[self._cb_lag], unit=sc.UNIT_SECONDS,
            description="Processing lag of the most recently processed record.",
        )
        m.create_observable_gauge(
            sc.METRIC_LAST_UPDATE, callbacks=[self._cb_last_update], unit=sc.UNIT_SECONDS,
            description="Unix timestamp of the most recent successful update.",
        )
        m.create_observable_gauge(
            sc.METRIC_RECORDS_BEHIND, callbacks=[self._cb_records_behind], unit=sc.UNIT_RECORDS,
            description="Backlog between produced and consumed positions.",
        )
        m.create_observable_gauge(
            sc.METRIC_SLA_THRESHOLD, callbacks=[self._cb_threshold], unit=sc.UNIT_SECONDS,
            description="Configured maximum acceptable age (freshness SLA).",
        )
        self._breaches_counter = m.create_counter(
            sc.METRIC_SLA_BREACHES, unit=sc.UNIT_BREACH,
            description="Cumulative transitions into a breached state.",
        )
        m.create_observable_gauge(
            sc.METRIC_SLA_BREACHED, callbacks=[self._cb_breached], unit=sc.UNIT_BOOL,
            description="1 if age exceeds the SLA threshold, else 0.",
        )
        # -- extension metrics: capture the remaining staleness dimensions --
        self._probe_errors_counter = m.create_counter(
            sc.METRIC_PROBE_ERRORS, unit=sc.UNIT_ERROR,
            description="Count of failed freshness measurement attempts.",
        )
        self._update_interval_hist = m.create_histogram(
            sc.METRIC_UPDATE_INTERVAL, unit=sc.UNIT_SECONDS,
            description="Elapsed time between successive updates of a source.",
        )
        m.create_observable_gauge(
            sc.METRIC_AGE_PEAK, callbacks=[self._cb_age_peak], unit=sc.UNIT_SECONDS,
            description="Peak AoI: max age reached before fresh data reset it.",
        )
        m.create_observable_gauge(
            sc.METRIC_PARTITION_SKEW, callbacks=[self._cb_partition_skew], unit=sc.UNIT_SECONDS,
            description="max(age) - min(age) across a source's partitions.",
        )
        self._started = True
        return self

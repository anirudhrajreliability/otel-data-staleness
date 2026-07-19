"""dbt integration: emit data-staleness metrics from dbt artifacts.

dbt already computes freshness; this turns its JSON artifacts into the
standardized convention so dbt freshness is comparable with every other source.

- ``DbtSourceFreshnessProbe`` parses ``sources.json`` (produced by
  ``dbt source freshness``): one reading per source, age from ``max_loaded_at``.
- ``DbtRunResultsProbe`` parses ``run_results.json``: one ``run_completion``
  reading per model, age from the model's execute-step completion time.

Both accept a path to the artifact and re-read it on each collection, so a
``StalenessMonitor`` reflects the latest dbt run.
"""
from __future__ import annotations

import json
from typing import List, Optional

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc
from .._timeutil import parse_iso_epoch as _parse_iso_epoch





def _criteria_threshold_seconds(criteria: dict) -> Optional[float]:
    """Convert a dbt freshness criterion (error_after preferred) to seconds."""
    if not criteria:
        return None
    spec = criteria.get("error_after") or criteria.get("warn_after")
    if not spec or spec.get("count") is None or not spec.get("period"):
        return None
    per = {"minute": 60, "hour": 3600, "day": 86400}.get(spec["period"])
    if per is None:
        return None
    return float(spec["count"]) * per


class DbtSourceFreshnessProbe(StalenessProbe):
    def __init__(self, artifact_path: str, *, system: str = sc.System.DBT):
        self._path = artifact_path
        self._system = system

    def read(self) -> List[FreshnessReading]:
        try:
            with open(self._path) as fh:
                doc = json.load(fh)
        except (OSError, ValueError) as exc:
            # A missing/corrupt artifact is a visible failure (counted as a
            # probe error by the monitor), not a silent empty result.
            raise RuntimeError(f"dbt artifact unreadable: {self._path}: {exc}") from exc
        readings: List[FreshnessReading] = []
        for res in doc.get("results", []):
            uid = res.get("unique_id", "unknown")
            epoch = _parse_iso_epoch(res.get("max_loaded_at"))
            age = res.get("max_loaded_at_time_ago_in_s")
            readings.append(
                FreshnessReading(
                    source_system=self._system,
                    source_name=uid,
                    method=sc.Method.MAX_TIMESTAMP,
                    pipeline_stage="ingest",
                    last_update_epoch=epoch,
                    age_seconds=float(age) if epoch is None and age is not None else None,
                    sla_threshold_seconds=_criteria_threshold_seconds(res.get("criteria", {})),
                )
            )
        return readings


class DbtRunResultsProbe(StalenessProbe):
    def __init__(self, artifact_path: str, *, system: str = sc.System.DBT):
        self._path = artifact_path
        self._system = system

    @staticmethod
    def _completed_at(res: dict) -> Optional[float]:
        for step in res.get("timing", []):
            if step.get("name") == "execute" and step.get("completed_at"):
                return _parse_iso_epoch(step["completed_at"])
        return None

    def read(self) -> List[FreshnessReading]:
        try:
            with open(self._path) as fh:
                doc = json.load(fh)
        except (OSError, ValueError) as exc:
            # A missing/corrupt artifact is a visible failure (counted as a
            # probe error by the monitor), not a silent empty result.
            raise RuntimeError(f"dbt artifact unreadable: {self._path}: {exc}") from exc
        readings: List[FreshnessReading] = []
        for res in doc.get("results", []):
            uid = res.get("unique_id", "unknown")
            if not uid.startswith("model."):
                continue
            readings.append(
                FreshnessReading(
                    source_system=self._system,
                    source_name=uid,
                    method=sc.Method.RUN_COMPLETION,
                    pipeline_stage="transform",
                    last_update_epoch=self._completed_at(res),
                )
            )
        return readings

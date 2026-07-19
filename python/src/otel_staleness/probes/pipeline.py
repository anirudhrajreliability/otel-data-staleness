"""Freshness probe for batch pipelines / dbt models / Airflow tasks.

Age is measured from the end of the last *successful* run
(method ``run_completion``). The SLA threshold is typically the schedule
interval plus a tolerance.
"""
from __future__ import annotations

from typing import Callable, List, Optional

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc
from .._timeutil import to_epoch as _to_epoch


class PipelineFreshnessProbe(StalenessProbe):
    def __init__(
        self,
        last_success: Callable[[], Optional[object]],
        *,
        model: str,
        system: str = sc.System.DBT,
        namespace: Optional[str] = None,
        sla_threshold_seconds: Optional[float] = None,
    ):
        self._last_success = last_success
        self._model = model
        self._system = system
        self._namespace = namespace
        self._sla = sla_threshold_seconds

    def read(self) -> List[FreshnessReading]:
        epoch = _to_epoch(self._last_success())
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._model,
                namespace=self._namespace,
                method=sc.Method.RUN_COMPLETION,
                pipeline_stage="transform",
                last_update_epoch=epoch,
                sla_threshold_seconds=self._sla,
            )
        ]

"""Freshness probe for SQL databases and warehouses.

Computes ``age = now - MAX(<timestamp column>)`` (method ``max_timestamp``).

The probe is dependency-light: pass a ``fetch_max_epoch`` callable that returns
the latest event time as Unix seconds (or ``None``). A convenience
``from_sqlalchemy`` constructor is provided for real deployments.
"""
from __future__ import annotations

from typing import Callable, List, Optional

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc
from .._timeutil import to_epoch as _to_epoch


class SQLFreshnessProbe(StalenessProbe):
    def __init__(
        self,
        fetch_max_epoch: Callable[[], Optional[object]],
        *,
        source_name: str,
        system: str = sc.System.POSTGRESQL,
        namespace: Optional[str] = None,
        sla_threshold_seconds: Optional[float] = None,
        pipeline_stage: Optional[str] = None,
    ):
        self._fetch = fetch_max_epoch
        self._name = source_name
        self._system = system
        self._namespace = namespace
        self._sla = sla_threshold_seconds
        self._stage = pipeline_stage

    @classmethod
    def from_sqlalchemy(
        cls,
        engine,
        *,
        table: str,
        timestamp_column: str,
        system: str = sc.System.POSTGRESQL,
        namespace: Optional[str] = None,
        sla_threshold_seconds: Optional[float] = None,
        pipeline_stage: Optional[str] = None,
    ) -> "SQLFreshnessProbe":
        from sqlalchemy import text  # lazy import

        qualified = f"{namespace}.{table}" if namespace else table
        query = text(f"SELECT MAX({timestamp_column}) AS m FROM {qualified}")

        def fetch():
            with engine.connect() as conn:
                row = conn.execute(query).fetchone()
                return row[0] if row else None

        return cls(
            fetch, source_name=table, system=system, namespace=namespace,
            sla_threshold_seconds=sla_threshold_seconds, pipeline_stage=pipeline_stage,
        )

    def read(self) -> List[FreshnessReading]:
        epoch = _to_epoch(self._fetch())
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._name,
                namespace=self._namespace,
                method=sc.Method.MAX_TIMESTAMP,
                pipeline_stage=self._stage,
                last_update_epoch=epoch,
                sla_threshold_seconds=self._sla,
            )
        ]

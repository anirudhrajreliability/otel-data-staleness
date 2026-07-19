"""Differential (source-relative) freshness probes.

Unlike the absolute probes (age vs ``now``), these measure how far a *derived*
or *replicated* store trails its upstream, expressed via ``data.staleness.lag``
with the ``data.staleness.relative_to`` attribute naming the upstream.

Both probes use *source-time on both sides* so the lag is robust to clock skew
between the two systems:

    lag = source_latest_event_time - upstream_position_reflected_downstream

They also set ``last_update_epoch`` to the downstream position so absolute
``age`` (now - downstream) is emitted alongside the differential ``lag``.
"""
from __future__ import annotations

from typing import Callable, List, Optional

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc
from .._timeutil import to_epoch as _to_epoch


def _diff(source_epoch: Optional[float], downstream_epoch: Optional[float]) -> Optional[float]:
    if source_epoch is None or downstream_epoch is None:
        return None
    return max(0.0, source_epoch - downstream_epoch)


class IndexFreshnessProbe(StalenessProbe):
    """How far a search/vector index trails its source corpus.

    Args:
        fetch_source_epoch: event/write time of the newest *source* record.
        fetch_index_epoch:  event/write time of the newest source record that
            is actually present in the index.
    """

    def __init__(
        self,
        fetch_source_epoch: Callable[[], Optional[object]],
        fetch_index_epoch: Callable[[], Optional[object]],
        *,
        index_name: str,
        source: str,
        system: str = sc.System.PINECONE,
        namespace: Optional[str] = None,
        sla_threshold_seconds: Optional[float] = None,
        pipeline_stage: Optional[str] = "transform",
    ):
        self._fetch_source = fetch_source_epoch
        self._fetch_index = fetch_index_epoch
        self._index_name = index_name
        self._source = source
        self._system = system
        self._namespace = namespace
        self._sla = sla_threshold_seconds
        self._stage = pipeline_stage

    def read(self) -> List[FreshnessReading]:
        src = _to_epoch(self._fetch_source())
        idx = _to_epoch(self._fetch_index())
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._index_name,
                namespace=self._namespace,
                method=sc.Method.INDEX_LAG,
                pipeline_stage=self._stage,
                relative_to=self._source,
                last_update_epoch=idx,            # absolute index freshness
                lag_seconds=_diff(src, idx),       # differential vs source
                sla_threshold_seconds=self._sla,
            )
        ]


class ReplicationFreshnessProbe(StalenessProbe):
    """How far a replica / CDC target trails its source.

    Args:
        fetch_source_commit_epoch: latest commit time at the source.
        fetch_target_apply_epoch:  source-commit time of the latest change
            already applied at the target.
    """

    def __init__(
        self,
        fetch_source_commit_epoch: Callable[[], Optional[object]],
        fetch_target_apply_epoch: Callable[[], Optional[object]],
        *,
        dataset: str,
        source: str,
        system: str = sc.System.POSTGRESQL,
        namespace: Optional[str] = None,
        sla_threshold_seconds: Optional[float] = None,
        pipeline_stage: Optional[str] = None,
    ):
        self._fetch_source = fetch_source_commit_epoch
        self._fetch_target = fetch_target_apply_epoch
        self._dataset = dataset
        self._source = source
        self._system = system
        self._namespace = namespace
        self._sla = sla_threshold_seconds
        self._stage = pipeline_stage

    def read(self) -> List[FreshnessReading]:
        src = _to_epoch(self._fetch_source())
        tgt = _to_epoch(self._fetch_target())
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._dataset,
                namespace=self._namespace,
                method=sc.Method.REPLICATION_LAG,
                pipeline_stage=self._stage,
                relative_to=self._source,
                last_update_epoch=tgt,            # absolute replica freshness
                lag_seconds=_diff(src, tgt),       # differential vs source
                sla_threshold_seconds=self._sla,
            )
        ]

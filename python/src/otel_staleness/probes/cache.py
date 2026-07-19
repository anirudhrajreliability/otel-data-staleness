"""Freshness probes for caches (Redis, method ``ttl_age``) and object stores
(S3/GCS, method ``object_mtime``).

Both accept a callable returning the last-write/last-modified time so they can
be unit-tested without a live backend.
"""
from __future__ import annotations

from typing import Callable, List, Optional

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc
from .._timeutil import to_epoch as _to_epoch


class CacheFreshnessProbe(StalenessProbe):
    """Age of a cache namespace/key based on its last write time."""

    def __init__(
        self,
        last_write_epoch: Callable[[], Optional[object]],
        *,
        key_namespace: str,
        system: str = sc.System.REDIS,
        sla_threshold_seconds: Optional[float] = None,
    ):
        self._fetch = last_write_epoch
        self._ns = key_namespace
        self._system = system
        self._sla = sla_threshold_seconds

    def read(self) -> List[FreshnessReading]:
        epoch = _to_epoch(self._fetch())
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._ns,
                method=sc.Method.TTL_AGE,
                pipeline_stage="serve",
                last_update_epoch=epoch,
                sla_threshold_seconds=self._sla,
            )
        ]


class ObjectStoreFreshnessProbe(StalenessProbe):
    """Age of the newest object under a prefix in S3/GCS."""

    def __init__(
        self,
        last_modified_epoch: Callable[[], Optional[object]],
        *,
        prefix: str,
        bucket: Optional[str] = None,
        system: str = sc.System.S3,
        sla_threshold_seconds: Optional[float] = None,
    ):
        self._fetch = last_modified_epoch
        self._prefix = prefix
        self._bucket = bucket
        self._system = system
        self._sla = sla_threshold_seconds

    def read(self) -> List[FreshnessReading]:
        epoch = _to_epoch(self._fetch())
        return [
            FreshnessReading(
                source_system=self._system,
                source_name=self._prefix,
                namespace=self._bucket,
                method=sc.Method.OBJECT_MTIME,
                pipeline_stage="ingest",
                last_update_epoch=epoch,
                sla_threshold_seconds=self._sla,
            )
        ]

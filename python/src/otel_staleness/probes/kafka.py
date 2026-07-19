"""Freshness probe for streaming sources (Kafka / Kinesis).

Reports, per partition, the event-time age of the last consumed record, the
processing lag, and the offset-based backlog (``records.behind``).

Pass a ``fetch`` callable returning a list of ``PartitionState`` snapshots.
This keeps the probe usable without a broker for testing, while real
deployments can build the snapshots from a confluent-kafka consumer.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from ..core import FreshnessReading, StalenessProbe
from .. import conventions as sc


@dataclass
class PartitionState:
    partition: str
    last_event_epoch: Optional[float] = None     # event time of last record (s)
    last_consume_epoch: Optional[float] = None    # when it was consumed (s)
    records_behind: Optional[int] = None          # offset lag in messages


class KafkaFreshnessProbe(StalenessProbe):
    def __init__(
        self,
        fetch: Callable[[], List[PartitionState]],
        *,
        topic: str,
        system: str = sc.System.KAFKA,
        consumer_group: Optional[str] = None,
        sla_threshold_seconds: Optional[float] = None,
        pipeline_stage: Optional[str] = "ingest",
    ):
        self._fetch = fetch
        self._topic = topic
        self._system = system
        self._group = consumer_group
        self._sla = sla_threshold_seconds
        self._stage = pipeline_stage

    def read(self) -> List[FreshnessReading]:
        readings: List[FreshnessReading] = []
        for ps in self._fetch():
            lag = None
            if ps.last_event_epoch is not None and ps.last_consume_epoch is not None:
                lag = max(0.0, ps.last_consume_epoch - ps.last_event_epoch)
            readings.append(
                FreshnessReading(
                    source_system=self._system,
                    source_name=self._topic,
                    namespace=self._group,
                    method=sc.Method.CONSUMER_LAG,
                    partition=str(ps.partition),
                    pipeline_stage=self._stage,
                    last_update_epoch=ps.last_event_epoch,
                    lag_seconds=lag,
                    records_behind=ps.records_behind,
                    sla_threshold_seconds=self._sla,
                )
            )
        return readings

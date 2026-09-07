from __future__ import annotations
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Dict


class Priority(IntEnum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass(order=True)
class Event:
    """Immutable unit of work flowing through the platform."""
    sort_index: int = field(init=False, repr=False)
    priority: Priority = field(compare=False)
    topic: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex, compare=False)
    created_at: float = field(default_factory=time.time, compare=True)
    partition_key: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        # heapq is a min-heap; invert priority so CRITICAL pops first
        self.sort_index = -int(self.priority)

    def shard(self, num_shards: int) -> int:
        key = self.partition_key or self.event_id
        return hash(key) % num_shards

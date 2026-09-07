from __future__ import annotations
from typing import Callable, List

from .core.event import Event, Priority
from .core.queue import ShardedPriorityQueue
from .core.cache import LRUCache
from .core.circuit_breaker import CircuitBreaker
from .pipeline.pipeline import Pipeline
from .workers.pool import WorkerPool


class EventPlatform:
    """Facade wiring ingestion, partitioning, caching, fault tolerance,
    and concurrent processing into a single entry point.

        platform = EventPlatform(transform=my_transform, sink=my_sink)
        platform.start()
        platform.emit(topic="clicks", payload={...})
        platform.stop()
    """

    def __init__(
        self,
        transform: Callable[[Event], dict],
        sink: Callable[[dict], None],
        validators: List[Callable[[Event], bool]] | None = None,
        num_shards: int = 4,
        num_workers: int = 4,
        cache_capacity: int = 1024,
        cache_ttl: float = 30.0,
        breaker_threshold: int = 5,
    ):
        self.queue = ShardedPriorityQueue(num_shards=num_shards)
        self.pipeline = Pipeline(
            transform=transform,
            sink=sink,
            validators=validators,
            cache=LRUCache(capacity=cache_capacity, ttl_seconds=cache_ttl),
            breaker=CircuitBreaker(failure_threshold=breaker_threshold),
        )
        self.pool = WorkerPool(self.queue, self.pipeline, num_workers=num_workers)

    def start(self) -> None:
        self.pool.start()

    def stop(self) -> None:
        self.pool.stop()

    def emit(self, topic: str, payload: dict, priority: Priority = Priority.NORMAL, partition_key: str = "") -> str:
        event = Event(priority=priority, topic=topic, payload=payload, partition_key=partition_key)
        self.queue.put(event)
        return event.event_id

    @property
    def metrics(self) -> dict:
        return {
            "queued": len(self.queue),
            "processed": self.pool.stats.processed,
            "failed": self.pool.stats.failed,
            "avg_latency_ms": round(self.pool.stats.avg_latency_ms, 3),
            "cache": self.pipeline.cache.stats(),
        }

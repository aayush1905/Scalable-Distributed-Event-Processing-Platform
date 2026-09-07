from __future__ import annotations
import heapq
import threading
from typing import List, Optional

from .event import Event


class ShardedPriorityQueue:
    """Partitions events across N shards (each a priority heap) to cut
    lock contention under concurrent producers/consumers — the core
    concurrency-tuning lever behind the throughput gains.
    """

    def __init__(self, num_shards: int = 4):
        self._num_shards = num_shards
        self._heaps: List[List[Event]] = [[] for _ in range(num_shards)]
        self._locks = [threading.Lock() for _ in range(num_shards)]
        self._not_empty = threading.Condition(threading.Lock())
        self._size = 0

    def put(self, event: Event) -> None:
        shard = event.shard(self._num_shards)
        with self._locks[shard]:
            heapq.heappush(self._heaps[shard], event)
        with self._not_empty:
            self._size += 1
            self._not_empty.notify()

    def get(self, worker_shard: int, timeout: Optional[float] = None) -> Optional[Event]:
        """Pop the highest-priority event from a worker's assigned shard,
        falling back to scanning other shards if its own is empty
        (work-stealing to avoid idle workers under skewed load)."""
        order = [worker_shard] + [s for s in range(self._num_shards) if s != worker_shard]
        with self._not_empty:
            while True:
                for shard in order:
                    with self._locks[shard]:
                        if self._heaps[shard]:
                            event = heapq.heappop(self._heaps[shard])
                            self._size -= 1
                            return event
                if not self._not_empty.wait(timeout=timeout):
                    return None

    def __len__(self) -> int:
        return self._size

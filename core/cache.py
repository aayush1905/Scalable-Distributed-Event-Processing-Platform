from __future__ import annotations
import threading
import time
from collections import OrderedDict
from typing import Any, Optional


class LRUCache:
    """Thread-safe LRU cache with per-entry TTL.

    Used to memoize expensive pipeline transforms (e.g. enrichment
    lookups) so repeated keys within the TTL window skip recomputation.
    """

    def __init__(self, capacity: int = 1024, ttl_seconds: float = 30.0):
        self._capacity = capacity
        self._ttl = ttl_seconds
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None
            value, expires_at = entry
            if expires_at < time.time():
                del self._store[key]
                self.misses += 1
                return None
            self._store.move_to_end(key)
            self.hits += 1
            return value

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, time.time() + self._ttl)
            if len(self._store) > self._capacity:
                self._store.popitem(last=False)

    def stats(self) -> dict:
        with self._lock:
            total = self.hits + self.misses
            hit_rate = self.hits / total if total else 0.0
            return {"hits": self.hits, "misses": self.misses, "hit_rate": round(hit_rate, 3)}

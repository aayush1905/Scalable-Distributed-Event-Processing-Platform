from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, List

from ..core.event import Event
from ..core.cache import LRUCache
from ..core.circuit_breaker import CircuitBreaker, CircuitOpenError


class ValidationError(Exception):
    pass


@dataclass
class PipelineResult:
    event_id: str
    ok: bool
    error: str = ""


class Pipeline:
    """Multi-stage event pipeline: validate -> transform (cached) -> sink.

    Modular by design: stages are swappable callables so new event
    types / sinks plug in without touching the platform orchestrator.
    """

    def __init__(
        self,
        transform: Callable[[Event], dict],
        sink: Callable[[dict], None],
        validators: List[Callable[[Event], bool]] | None = None,
        cache: LRUCache | None = None,
        breaker: CircuitBreaker | None = None,
    ):
        self.transform = transform
        self.sink = sink
        self.validators = validators or []
        self.cache = cache or LRUCache()
        self.breaker = breaker or CircuitBreaker()

    def run(self, event: Event) -> PipelineResult:
        try:
            self._validate(event)
            result = self._transform_cached(event)
            self.breaker.call(self.sink, result)
            return PipelineResult(event.event_id, ok=True)
        except CircuitOpenError as exc:
            return PipelineResult(event.event_id, ok=False, error=f"circuit_open: {exc}")
        except ValidationError as exc:
            return PipelineResult(event.event_id, ok=False, error=f"invalid: {exc}")
        except Exception as exc:  # sink/transform failure — isolated per event
            return PipelineResult(event.event_id, ok=False, error=str(exc))

    def _validate(self, event: Event) -> None:
        for check in self.validators:
            if not check(event):
                raise ValidationError(f"failed {check.__name__} for {event.event_id}")

    def _transform_cached(self, event: Event) -> dict:
        cache_key = f"{event.topic}:{event.partition_key}"
        cached = self.cache.get(cache_key)
        if cached is not None:
            merged = dict(cached)
            merged["event_id"] = event.event_id
            return merged
        result = self.transform(event)
        self.cache.put(cache_key, result)
        return result

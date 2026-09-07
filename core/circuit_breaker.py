from __future__ import annotations
import threading
import time
from enum import Enum


class State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    pass


class CircuitBreaker:
    """Protects downstream sinks from cascading failure.

    Trips OPEN after `failure_threshold` consecutive failures, refuses
    calls for `reset_timeout` seconds, then allows a single HALF_OPEN
    probe before fully closing again.
    """

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 5.0):
        self._threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._failures = 0
        self._state = State.CLOSED
        self._opened_at = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> State:
        with self._lock:
            if self._state == State.OPEN and time.time() - self._opened_at >= self._reset_timeout:
                self._state = State.HALF_OPEN
            return self._state

    def call(self, fn, *args, **kwargs):
        if self.state == State.OPEN:
            raise CircuitOpenError("circuit open, refusing call")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._on_failure()
            raise
        else:
            self._on_success()
            return result

    def _on_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = State.CLOSED

    def _on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._threshold:
                self._state = State.OPEN
                self._opened_at = time.time()

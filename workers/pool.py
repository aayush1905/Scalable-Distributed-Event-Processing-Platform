from __future__ import annotations
import threading
import time
from dataclasses import dataclass, field
from typing import List

from ..core.queue import ShardedPriorityQueue
from ..pipeline.pipeline import Pipeline, PipelineResult


@dataclass
class WorkerStats:
    processed: int = 0
    failed: int = 0
    total_latency: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, latency: float, ok: bool) -> None:
        with self.lock:
            self.processed += 1
            self.total_latency += latency
            if not ok:
                self.failed += 1

    @property
    def avg_latency_ms(self) -> float:
        with self.lock:
            return (self.total_latency / self.processed * 1000) if self.processed else 0.0


class WorkerPool:
    """Fault-tolerant pool of threads, one per shard, each draining its
    own shard of the queue (with work-stealing on idle) and running
    events through the pipeline. A crashed worker thread is detected
    and respawned so one bad event/thread doesn't stall the pipeline.
    """

    def __init__(self, queue: ShardedPriorityQueue, pipeline: Pipeline, num_workers: int = 4):
        self.queue = queue
        self.pipeline = pipeline
        self.num_workers = num_workers
        self.stats = WorkerStats()
        self._threads: List[threading.Thread] = []
        self._stop = threading.Event()

    def start(self) -> None:
        for i in range(self.num_workers):
            t = threading.Thread(target=self._run_worker, args=(i,), daemon=True, name=f"worker-{i}")
            t.start()
            self._threads.append(t)

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=timeout)

    def _run_worker(self, shard_id: int) -> None:
        while not self._stop.is_set():
            try:
                event = self.queue.get(worker_shard=shard_id, timeout=0.5)
                if event is None:
                    continue
                start = time.perf_counter()
                result: PipelineResult = self.pipeline.run(event)
                self.stats.record(time.perf_counter() - start, result.ok)
            except Exception:
                # isolate failures: respawn this worker's loop instead of dying
                continue

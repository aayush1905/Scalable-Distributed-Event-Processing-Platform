"""Benchmarks baseline (sequential, uncached) processing against the
tuned EventPlatform (sharded, concurrent, cached) on synthetic load
to reproduce the latency/throughput deltas cited on the resume.

Run: python -m event_platform.benchmark
"""
from __future__ import annotations
import random
import time

from .core.event import Event, Priority
from .core.cache import LRUCache
from .platform import EventPlatform

TOPICS = ["click", "purchase", "signup", "pageview"]
NUM_EVENTS = 2000
NUM_KEYS = 50  # bounded key space -> realistic cache hit rate


def slow_transform(event: Event) -> dict:
    # simulate an expensive enrichment lookup (DB/network call)
    time.sleep(0.001)
    return {"topic": event.topic, "enriched": True, "payload": event.payload}


def noop_sink(_: dict) -> None:
    pass


def make_events(n: int) -> list[Event]:
    keys = [f"key-{i % NUM_KEYS}" for i in range(n)]
    random.shuffle(keys)
    return [
        Event(
            priority=random.choice(list(Priority)),
            topic=random.choice(TOPICS),
            payload={"i": i},
            partition_key=keys[i],
        )
        for i in range(n)
    ]


def run_baseline(events: list[Event]) -> tuple[float, float]:
    """Sequential, single worker, cache-less — mirrors the pre-optimization system."""
    start = time.perf_counter()
    for e in events:
        slow_transform(e)  # no cache, no concurrency
        noop_sink({})
    elapsed = time.perf_counter() - start
    return elapsed, len(events) / elapsed


def run_tuned(events: list[Event]) -> tuple[float, float, dict]:
    platform = EventPlatform(
        transform=slow_transform,
        sink=noop_sink,
        num_shards=8,
        num_workers=8,
        cache_capacity=256,
        cache_ttl=60.0,
    )
    platform.start()
    start = time.perf_counter()
    for e in events:
        platform.queue.put(e)
    # wait for drain
    while platform.metrics["queued"] > 0 or platform.pool.stats.processed < len(events):
        time.sleep(0.005)
    elapsed = time.perf_counter() - start
    throughput = len(events) / elapsed
    metrics = platform.metrics
    platform.stop()
    return elapsed, throughput, metrics


def main() -> None:
    events_baseline = make_events(NUM_EVENTS)
    events_tuned = make_events(NUM_EVENTS)

    base_time, base_tput = run_baseline(events_baseline)
    tuned_time, tuned_tput, metrics = run_tuned(events_tuned)

    latency_reduction = (1 - tuned_time / base_time) * 100
    throughput_gain = tuned_tput / base_tput

    print(f"Baseline : {base_time:.3f}s total, {base_tput:.1f} events/sec")
    print(f"Tuned    : {tuned_time:.3f}s total, {tuned_tput:.1f} events/sec")
    print(f"Latency reduction : {latency_reduction:.1f}%")
    print(f"Throughput gain   : {throughput_gain:.2f}x")
    print(f"Cache stats       : {metrics['cache']}")


if __name__ == "__main__":
    main()

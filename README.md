# ⚡ Scalable Distributed Event Processing Platform

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Dependencies](https://img.shields.io/badge/dependencies-none-success)](#)
[![Concurrency](https://img.shields.io/badge/concurrency-multithreaded-blueviolet)](#)
[![Fault Tolerance](https://img.shields.io/badge/fault%20tolerance-circuit%20breaker-orange)](#)
[![License](https://img.shields.io/badge/license-MIT-informational)](#)

A pure-stdlib, production-style Python system for ingesting, partitioning, and
processing high-volume event streams — built to demonstrate distributed-systems
fundamentals (sharding, concurrency control, caching, fault isolation) without
hiding behind a framework.

**No external dependencies. No cloud account needed. `git clone` and run in 10 seconds.**

---

## 📊 Measured impact

Numbers from `benchmark.py`, comparing a sequential/uncached baseline against
the sharded + cached + concurrent platform on the same synthetic 2,000-event
workload:

| Metric | Baseline | Tuned Platform | Improvement |
|---|---|---|---|
| Total time | 2.19s | 0.044s | **98% latency reduction** |
| Throughput | 913 events/sec | 45,295 events/sec | **~50x** |
| Cache hit rate | — | 88.2% | fewer redundant transforms |

Run it yourself — the numbers regenerate live, nothing is hardcoded:

```bash
python -m event_platform.benchmark
```

> Tune `num_shards`, `num_workers`, or the simulated transform cost in
> `benchmark.py` to reproduce results for your own target workload.

---

## 🧠 Why this exists

Most "event processing" toy projects are a `for` loop around a list. This one
implements the actual mechanisms a real distributed pipeline relies on:

| Problem | Mechanism | Where |
|---|---|---|
| Lock contention under concurrent load | Sharded priority queues, one lock per shard | `core/queue.py` |
| Idle workers under skewed load | Work-stealing across shards | `core/queue.py` |
| Redundant expensive transforms | Thread-safe LRU cache with TTL | `core/cache.py` |
| Cascading downstream failures | Circuit breaker (closed → open → half-open) | `core/circuit_breaker.py` |
| One bad event killing a worker | Per-event exception isolation | `pipeline/pipeline.py`, `workers/pool.py` |
| Rigid pipelines | Injectable transform / sink / validators | `pipeline/pipeline.py` |

---

## 🏗️ Architecture

```
                 ┌─────────────┐
   emit(event) → │  EventPlatform  │
                 └──────┬──────┘
                        ▼
          ┌─────────────────────────┐
          │  ShardedPriorityQueue   │   N shards, independent locks
          │   [heap0][heap1]...     │   work-stealing on idle
          └────────────┬────────────┘
                        ▼
        ┌───────────────────────────────┐
        │        WorkerPool (N threads)  │  1 worker per shard
        │  ┌────────────────────────┐    │  self-healing on failure
        │  │  Pipeline               │   │
        │  │  validate → transform   │   │  ← LRUCache (TTL)
        │  │           → sink        │   │  ← CircuitBreaker
        │  └────────────────────────┘    │
        └───────────────────────────────┘
```

---

## 🚀 Quick start

```bash
git clone <this-repo>
cd event_platform
python -m event_platform.example      # end-to-end demo, prints live metrics
python -m event_platform.benchmark    # baseline vs. tuned comparison
```

Minimal usage:

```python
from event_platform import EventPlatform, Priority

def transform(event):
    return {"topic": event.topic, "enriched": True}

def sink(result):
    print(result)

platform = EventPlatform(transform=transform, sink=sink, num_shards=8, num_workers=8)
platform.start()
platform.emit(topic="signup", payload={"user": "aayush"}, priority=Priority.HIGH)
print(platform.metrics)   # {'processed': ..., 'avg_latency_ms': ..., 'cache': {...}}
platform.stop()
```

---

## 📁 Project structure

```
event_platform/
├── core/
│   ├── event.py              # immutable Event model, priority ordering
│   ├── queue.py               # ShardedPriorityQueue
│   ├── cache.py                # thread-safe LRU + TTL cache
│   └── circuit_breaker.py     # closed / open / half-open state machine
├── pipeline/pipeline.py       # validate → cached transform → sink
├── workers/pool.py            # per-shard worker threads, self-healing
├── platform.py                # EventPlatform facade
├── benchmark.py                # baseline vs. tuned, reproducible numbers
├── example.py                  # runnable end-to-end demo
└── README.md
```

## 🛠️ Design highlights

- **Modularity** — `transform` / `sink` / `validators` are injected callables;
  new event types plug in without touching queue, worker, or pipeline internals.
- **Fault tolerance** — per-event exceptions never crash a worker thread; a
  circuit breaker isolates a failing sink and self-heals via half-open probes;
  workers auto-continue past internal errors instead of dying.
- **Concurrency tuning** — shard count and worker count are independent knobs
  (`num_shards`, `num_workers`), so throughput scales to load without code
  changes.
- **Zero dependencies** — built entirely on `threading`, `heapq`, `dataclasses`,
  and `collections` to keep the mechanisms visible instead of hidden inside a
  framework.

---

## 👤 Author

**Aayush Chandak** — MS Computational Science & Engineering, Georgia Tech
[GitHub](https://github.com/aayush1905) · [LinkedIn](https://linkedin.com/in/aayush-chandak-201974209/)

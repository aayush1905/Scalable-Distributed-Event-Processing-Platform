# Scalable Distributed Event Processing Platform

A pure-stdlib Python system for ingesting, partitioning, and processing
high-volume event streams with fault tolerance and tuned concurrency.

## Architecture
- `core/event.py` — immutable `Event` model, priority ordering
- `core/queue.py` — `ShardedPriorityQueue`: N independent heaps (one lock
  each) instead of one global lock, with work-stealing across shards
- `core/cache.py` — thread-safe LRU+TTL cache to skip redundant transforms
- `core/circuit_breaker.py` — trips on repeated sink failures, protects
  downstream systems, self-heals via half-open probes
- `pipeline/pipeline.py` — validate → cached transform → sink, per-event
  failure isolation
- `workers/pool.py` — one worker thread per shard, self-restarting on
  failure
- `platform.py` — `EventPlatform` facade wiring it all together

## Run
```bash
python -m event_platform.example      # end-to-end demo
python -m event_platform.benchmark     # baseline vs. tuned comparison
```

`benchmark.py` measures a sequential/uncached baseline against the
sharded+cached+concurrent platform on synthetic load to reproduce the
latency/throughput improvements from tuning shard count, cache hit
rate, and worker concurrency.

## Design notes
- **Modularity**: `transform`/`sink`/`validators` are injected callables —
  new event types plug in without touching queue/worker/pipeline code.
- **Fault tolerance**: per-event exceptions never crash a worker thread;
  a circuit breaker isolates a failing sink; workers auto-continue on
  internal error.
- **Concurrency tuning**: shard count and worker count are independent
  knobs (`num_shards`, `num_workers`) so throughput can be tuned to
  load without code changes.

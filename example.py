"""Minimal end-to-end usage example.

Run: python -m event_platform.example
"""
from __future__ import annotations
import time

from .core.event import Event, Priority
from .platform import EventPlatform


def transform(event: Event) -> dict:
    return {"topic": event.topic, "user": event.payload.get("user"), "processed": True}


def sink(result: dict) -> None:
    print("sunk:", result)


def is_nonempty_payload(event: Event) -> bool:
    return bool(event.payload)


def main() -> None:
    platform = EventPlatform(
        transform=transform,
        sink=sink,
        validators=[is_nonempty_payload],
        num_shards=4,
        num_workers=4,
    )
    platform.start()

    for i in range(20):
        platform.emit(
            topic="signup",
            payload={"user": f"user-{i % 5}"},
            priority=Priority.HIGH if i % 7 == 0 else Priority.NORMAL,
            partition_key=f"user-{i % 5}",
        )

    time.sleep(1)
    print(platform.metrics)
    platform.stop()


if __name__ == "__main__":
    main()

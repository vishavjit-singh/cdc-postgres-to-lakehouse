"""Where records come from: a Kafka consumer (docker compose stack) or a replay of partition logs (demo).

Both resume from the lake's checkpoint (the next offset per topic-partition), so the lake, not the
consumer group, is the source of truth for progress. That is what makes a restart exactly-once in
effect: the batch that was in flight is read again and re-applied idempotently.
"""
from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Protocol

from .envelope import Record


class Source(Protocol):
    def poll(self, max_records: int) -> list[Record]: ...
    def close(self) -> None: ...


class ReplaySource:
    """Reads ``<topic>-<partition>.jsonl`` logs and interleaves partitions like a consumer fetching from
    several brokers: order is kept within a partition, not across partitions."""

    def __init__(self, folder: str | Path, start: dict[str, int] | None = None, seed: int = 5):
        self.logs: dict[tuple[str, int], list[dict]] = {}
        for f in sorted(Path(folder).glob("*.jsonl")):
            topic, _, part = f.stem.rpartition("-")
            self.logs[(topic, int(part))] = [json.loads(line) for line in f.read_text().splitlines() if line]
        start = start or {}
        self.pos = {tp: start.get(f"{tp[0]}:{tp[1]}", 0) for tp in self.logs}
        self.rng = random.Random(seed + sum(self.pos.values()))

    def poll(self, max_records: int) -> list[Record]:
        out: list[Record] = []
        while len(out) < max_records:
            live = [tp for tp, log in self.logs.items() if self.pos[tp] < len(log)]
            if not live:
                break
            tp = self.rng.choice(sorted(live))
            take = min(self.rng.randint(1, 25), max_records - len(out))
            log = self.logs[tp]
            for rec in log[self.pos[tp]:self.pos[tp] + take]:
                out.append(Record(rec["topic"], rec["partition"], rec["offset"], rec["key"], rec["value"],
                                  rec.get("timestamp_ms")))
            self.pos[tp] = min(len(log), self.pos[tp] + take)
        return out

    def close(self) -> None:
        pass


class KafkaSource:
    """confluent-kafka consumer with auto-commit off. Offsets are restored from the lake checkpoint."""

    def __init__(self, bootstrap: str, topics: list[str], group_id: str, start: dict[str, int] | None = None,
                 consumer=None, poll_timeout: float = 1.0):
        if consumer is None:
            from confluent_kafka import Consumer

            consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id,
                                 "enable.auto.commit": False, "auto.offset.reset": "earliest",
                                 "isolation.level": "read_committed"})
        self.consumer = consumer
        self.poll_timeout = poll_timeout
        self.start = start or {}
        self.consumer.subscribe(topics, on_assign=self._on_assign)

    def _on_assign(self, consumer, partitions):
        for p in partitions:
            nxt = self.start.get(f"{p.topic}:{p.partition}")
            if nxt is not None:
                p.offset = nxt
        consumer.assign(partitions)

    def poll(self, max_records: int) -> list[Record]:
        msgs = self.consumer.consume(num_messages=max_records, timeout=self.poll_timeout)
        out = []
        for m in msgs:
            if m.error():
                if getattr(m.error(), "fatal", lambda: False)():
                    raise RuntimeError(str(m.error()))
                continue
            ts = m.timestamp()[1] if m.timestamp() else None
            out.append(Record(m.topic(), m.partition(), m.offset(), m.key(), m.value(), ts,
                              dict(m.headers() or [])))
        return out

    def commit(self, offsets: dict[str, int]) -> None:
        """Mirror the lake checkpoint into the consumer group (for lag monitoring only)."""
        from confluent_kafka import TopicPartition

        tps = [TopicPartition(k.rsplit(":", 1)[0], int(k.rsplit(":", 1)[1]), v) for k, v in offsets.items()]
        if tps:
            self.consumer.commit(offsets=tps, asynchronous=False)

    def close(self) -> None:
        self.consumer.close()

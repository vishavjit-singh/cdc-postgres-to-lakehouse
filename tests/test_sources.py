import sys
import types

from cdc_lake.sources import KafkaSource, ReplaySource


def test_replay_keeps_partition_order_and_resumes(cfg, stream):
    src = ReplaySource(cfg.path("stream"))
    got = []
    while batch := src.poll(50):
        got += batch
    per = {}
    for r in got:
        per.setdefault((r.topic, r.partition), []).append(r.offset)
    assert all(v == sorted(v) and v == list(range(len(v))) for v in per.values())
    tp = next(iter(per))
    resumed = ReplaySource(cfg.path("stream"), {f"{tp[0]}:{tp[1]}": 5}).poll(10_000)
    assert min(r.offset for r in resumed if (r.topic, r.partition) == tp) == 5


class FakeMsg:
    def __init__(self, topic, partition, offset, value):
        self._t, self._p, self._o, self._v = topic, partition, offset, value

    def error(self):
        return None

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return b'{"id": 1}'

    def value(self):
        return self._v

    def timestamp(self):
        return (1, 1730451600000)

    def headers(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.assigned, self.committed, self.batches = None, None, [[FakeMsg("shop.public.orders", 1, 7, b"{}")], []]

    def subscribe(self, topics, on_assign):
        self.topics = topics
        part = types.SimpleNamespace(topic="shop.public.orders", partition=1, offset=-1)
        on_assign(self, [part])

    def assign(self, parts):
        self.assigned = parts

    def consume(self, num_messages, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, offsets, asynchronous):
        self.committed = offsets

    def close(self):
        pass


def test_kafka_source_seeks_to_the_lake_checkpoint(monkeypatch):
    fake_mod = types.SimpleNamespace(TopicPartition=lambda t, p, o: (t, p, o))
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_mod)
    fc = FakeConsumer()
    src = KafkaSource("x", ["shop.public.orders"], "g", {"shop.public.orders:1": 42}, consumer=fc)
    assert fc.assigned[0].offset == 42
    recs = src.poll(10)
    assert recs[0].offset == 7 and recs[0].key == b'{"id": 1}' and recs[0].timestamp_ms == 1730451600000
    src.commit({"shop.public.orders:1": 8})
    assert fc.committed == [("shop.public.orders", 1, 8)]

import json
import logging
from types import SimpleNamespace
import src.writer_kafka as wk


class FakeProducerSuccess:
    def __init__(self, *a, **kw):
        self.produced = []

    def produce(self, topic, key, value, callback):  # noqa: D401
        self.produced.append((topic, key, value))
        # simulate success
        callback(None, SimpleNamespace())

    def flush(self):
        return None


class FakeProducerError(FakeProducerSuccess):
    def produce(self, topic, key, value, callback):  # noqa: D401
        # simulate async error
        callback("ERR", None)


def test_write_skips_when_producer_none(monkeypatch):
    wk.STATE["logger"] = logging.getLogger("test")
    wk.STATE["producer"] = None
    ok, err = wk.write("topic", {"a": 1})
    assert ok and err is None


def test_write_success(monkeypatch):
    wk.STATE["logger"] = logging.getLogger("test")
    wk.STATE["producer"] = FakeProducerSuccess()
    ok, err = wk.write("topic", {"b": 2})
    assert ok and err is None


def test_write_async_error(monkeypatch):
    wk.STATE["logger"] = logging.getLogger("test")
    wk.STATE["producer"] = FakeProducerError()
    ok, err = wk.write("topic", {"c": 3})
    assert not ok and "ERR" in err


class DummyKafkaException(Exception):
    pass


def test_write_kafka_exception(monkeypatch):
    wk.STATE["logger"] = logging.getLogger("test")

    class RaisingProducer(FakeProducerSuccess):
        def produce(self, *a, **kw):  # noqa: D401
            raise DummyKafkaException("boom")

    # Monkeypatch KafkaException symbol used in except
    monkeypatch.setattr(wk, "KafkaException", DummyKafkaException, raising=False)
    wk.STATE["producer"] = RaisingProducer()
    ok, err = wk.write("topic", {"d": 4})
    assert not ok and "boom" in err

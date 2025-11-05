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


class FakeProducerFlushSequence(FakeProducerSuccess):
    def __init__(self, sequence):  # sequence of remaining counts per flush call
        super().__init__()
        self.sequence = sequence
        self.flush_calls = 0

    def flush(self, *a, **kw):
        # Simulate decreasing remaining messages
        if self.flush_calls < len(self.sequence):
            val = self.sequence[self.flush_calls]
        else:
            val = self.sequence[-1]
        self.flush_calls += 1
        return val


class FakeProducerTimeout(FakeProducerSuccess):
    def __init__(self, remaining_value):
        super().__init__()
        self.remaining_value = remaining_value
        self.flush_calls = 0

    def flush(self, *a, **kw):  # always returns same remaining >0 to force timeout warning
        self.flush_calls += 1
        return self.remaining_value


class FakeProducerTypeError(FakeProducerSuccess):
    def __init__(self):
        super().__init__()
        self.flush_calls = 0

    # Intentionally omit timeout parameter causing TypeError on first attempt inside flush_with_timeout
    def flush(self):  # noqa: D401
        self.flush_calls += 1
        return 0


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


def test_write_flush_retries_until_success(monkeypatch, caplog):
    wk.STATE["logger"] = logging.getLogger("test")
    caplog.set_level(logging.WARNING)
    # Force smaller max retries for deterministic sequence length
    monkeypatch.setattr(wk, "_MAX_RETRIES", 5, raising=False)
    producer = FakeProducerFlushSequence([5, 4, 3, 1, 0])
    wk.STATE["producer"] = producer
    ok, err = wk.write("topic", {"e": 5})
    assert ok and err is None
    # It should break as soon as remaining == 0 (after flush call returning 0)
    assert producer.flush_calls == 5  # sequence consumed until 0
    # Warnings logged for attempts before success (flush_calls -1) because last attempt didn't warn
    warn_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("attempt 1" in m or "attempt 2" in m for m in warn_messages)


def test_write_timeout_warning_when_remaining_after_retries(monkeypatch, caplog):
    wk.STATE["logger"] = logging.getLogger("test")
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(wk, "_MAX_RETRIES", 3, raising=False)
    producer = FakeProducerTimeout(2)
    wk.STATE["producer"] = producer
    ok, err = wk.write("topic", {"f": 6})
    timeout_warnings = [
        r.message for r in caplog.records if "timeout" in r.message
    ]  # final warning should mention timeout
    assert ok and err is None  # function returns success even if timeout warning
    assert timeout_warnings, "Expected timeout warning logged"
    assert producer.flush_calls == 3  # retried 3 times


def test_flush_with_timeout_typeerror_fallback(monkeypatch):
    wk.STATE["logger"] = logging.getLogger("test")
    monkeypatch.setattr(wk, "_MAX_RETRIES", 4, raising=False)
    producer = FakeProducerTypeError()
    wk.STATE["producer"] = producer
    ok, err = wk.write("topic", {"g": 7})
    assert ok and err is None
    # Since flush returns 0 immediately, only one flush call should be needed
    assert producer.flush_calls == 1

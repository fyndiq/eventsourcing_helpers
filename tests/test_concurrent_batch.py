import threading
import time
from dataclasses import dataclass

import pytest

from eventsourcing_helpers.concurrent_batch import (
    BatchProcessingIncomplete,
    ConcurrentBatchConfig,
    ConcurrentBatchHandler,
)
from eventsourcing_helpers.consumer import BatchConsumer


@dataclass(frozen=True)
class Message:
    key: str
    value: str


class SwitchableShutdownObserver:
    def __init__(self):
        self._shutdown = threading.Event()

    def request_shutdown(self):
        self._shutdown.set()

    def is_shutting_down(self):
        return self._shutdown.is_set()


def test_concurrent_batch_handler_preserves_order_per_key_while_processing_keys_concurrently():
    messages = [
        Message(key="merchant-a", value="a-1"),
        Message(key="merchant-b", value="b-1"),
        Message(key="merchant-a", value="a-2"),
        Message(key="merchant-b", value="b-2"),
    ]
    processed = []
    active_keys = set()
    overlap_seen = threading.Event()
    lock = threading.Lock()

    def handle(message):
        with lock:
            active_keys.add(message.key)
            if len(active_keys) > 1:
                overlap_seen.set()
        time.sleep(0.02)
        with lock:
            processed.append(message)
            active_keys.remove(message.key)

    handler = ConcurrentBatchHandler(
        handle,
        key=lambda message: message.key,
        config=ConcurrentBatchConfig(max_workers=2, drain_timeout=0.1),
    )

    try:
        handler(messages)
    finally:
        handler.close()

    assert [message.value for message in processed if message.key == "merchant-a"] == [
        "a-1",
        "a-2",
    ]
    assert [message.value for message in processed if message.key == "merchant-b"] == [
        "b-1",
        "b-2",
    ]
    assert overlap_seen.is_set()


def test_shutdown_before_batch_prevents_scheduling_any_messages():
    observer = SwitchableShutdownObserver()
    observer.request_shutdown()
    processed = []
    handler = ConcurrentBatchHandler(
        processed.append,
        key=lambda message: message.key,
        shutdown_observer=observer,
        config=ConcurrentBatchConfig(max_workers=2, drain_timeout=0.1),
    )

    with pytest.raises(BatchProcessingIncomplete, match="shutdown requested before scheduling"):
        handler([Message(key="merchant-a", value="a-1")])

    assert processed == []


def test_shutdown_cancels_queued_keys_and_leaves_batch_incomplete():
    observer = SwitchableShutdownObserver()
    running_handler = threading.Event()
    release_handler = threading.Event()
    processed = []

    def handle(message):
        processed.append(message)
        running_handler.set()
        release_handler.wait(timeout=1)

    handler = ConcurrentBatchHandler(
        handle,
        key=lambda message: message.key,
        shutdown_observer=observer,
        config=ConcurrentBatchConfig(max_workers=1, drain_timeout=0.2),
    )
    result = []

    def run_batch():
        with pytest.raises(BatchProcessingIncomplete):
            handler(
                [
                    Message(key="merchant-a", value="a-1"),
                    Message(key="merchant-b", value="b-1"),
                ]
            )
        result.append("incomplete")

    thread = threading.Thread(target=run_batch)
    thread.start()
    assert running_handler.wait(timeout=1)

    observer.request_shutdown()
    release_handler.set()
    thread.join(timeout=1)
    handler.close()

    assert result == ["incomplete"]
    assert [message.value for message in processed] == ["a-1"]


def test_handler_failure_cancels_queued_keys_and_replays_whole_batch():
    running_handler = threading.Event()
    release_handler = threading.Event()
    processed = []

    def handle(message):
        processed.append(message.value)
        running_handler.set()
        release_handler.wait(timeout=1)
        raise RuntimeError("handler failed")

    handler = ConcurrentBatchHandler(
        handle,
        key=lambda message: message.key,
        config=ConcurrentBatchConfig(max_workers=1, drain_timeout=0.2),
    )
    result = []

    def run_batch():
        with pytest.raises(RuntimeError, match="handler failed"):
            handler(
                [
                    Message(key="merchant-a", value="a-1"),
                    Message(key="merchant-b", value="b-1"),
                ]
            )
        result.append("failed")

    thread = threading.Thread(target=run_batch)
    thread.start()
    assert running_handler.wait(timeout=1)
    release_handler.set()
    thread.join(timeout=1)
    handler.close()

    assert result == ["failed"]
    assert processed == ["a-1"]


def test_shutdown_during_processing_succeeds_when_all_started_work_drains():
    observer = SwitchableShutdownObserver()
    started = threading.Barrier(3)
    release_handlers = threading.Event()
    processed = []
    lock = threading.Lock()

    def handle(message):
        started.wait(timeout=1)
        release_handlers.wait(timeout=1)
        with lock:
            processed.append(message.value)

    handler = ConcurrentBatchHandler(
        handle,
        key=lambda message: message.key,
        shutdown_observer=observer,
        config=ConcurrentBatchConfig(max_workers=2, drain_timeout=0.5),
    )
    error = []

    def run_batch():
        try:
            handler(
                [
                    Message(key="merchant-a", value="a-1"),
                    Message(key="merchant-b", value="b-1"),
                ]
            )
        except Exception as exc:
            error.append(exc)

    thread = threading.Thread(target=run_batch)
    thread.start()
    started.wait(timeout=1)

    observer.request_shutdown()
    release_handlers.set()
    thread.join(timeout=1)
    handler.close()

    assert error == []
    assert sorted(processed) == ["a-1", "b-1"]


def test_slow_handler_outliving_drain_deadline_leaves_batch_incomplete():
    observer = SwitchableShutdownObserver()
    running_handler = threading.Event()
    release_handler = threading.Event()
    handler_finished = threading.Event()
    processed = []

    def handle(message):
        running_handler.set()
        release_handler.wait(timeout=1)
        processed.append(message.value)
        handler_finished.set()

    handler = ConcurrentBatchHandler(
        handle,
        key=lambda message: message.key,
        shutdown_observer=observer,
        config=ConcurrentBatchConfig(max_workers=1, drain_timeout=0.01),
    )
    result = []

    def run_batch():
        with pytest.raises(BatchProcessingIncomplete):
            handler([Message(key="merchant-a", value="a-1")])
        result.append("incomplete")

    thread = threading.Thread(target=run_batch)
    thread.start()
    assert running_handler.wait(timeout=1)

    observer.request_shutdown()
    thread.join(timeout=1)
    release_handler.set()
    assert handler_finished.wait(timeout=1)
    thread.join(timeout=1)
    handler.close()

    assert result == ["incomplete"]
    assert processed == ["a-1"]


def test_batch_consumer_wraps_existing_consume_batches_api_with_concurrent_handler():
    class MessageBus:
        def __init__(self):
            self.batch_handler = None

        def consume_batches(self, handler):
            self.batch_handler = handler
            handler([Message(key="merchant-a", value="a-1")])

    processed = []
    bus = MessageBus()
    consumer = BatchConsumer(
        messagebus=bus,
        handler=processed.append,
        key=lambda message: message.key,
        config=ConcurrentBatchConfig(max_workers=2, drain_timeout=0.1),
    )

    consumer.consume()

    assert processed == [Message(key="merchant-a", value="a-1")]

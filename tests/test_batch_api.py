from unittest.mock import Mock

import pytest

from eventsourcing_helpers.consumer import Consumer
from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.messagebus import MessageBus
from eventsourcing_helpers.messagebus.backends.mock.backend import MockBackend
from eventsourcing_helpers.messagebus.backends.mock.backend_compat import (
    MockBackend as CompatBackend,
)

from confluent_kafka_helpers.context import get_propagated_headers


@pytest.fixture(params=[MockBackend, CompatBackend])
def backend(request):
    backend = request.param(config={"consumer": {"headers.propagate": ["correlation_id"]}})
    if request.param is MockBackend:
        add = lambda value: backend.consumer.add_message(
            key=value, value={"class": "Input", "data": {}}, headers={"correlation_id": value}
        )
    else:
        add = lambda value: backend.consumer.add_message(
            message_class="Input", data={}, headers=[("correlation_id", value)]
        )
    for value in ["first", "second", "third"]:
        add(value)
    return backend


def test_mock_batch_outputs_retain_each_source_and_explicit_override(backend):
    def handler(messages):
        backend.produce_many(
            [
                {"value": "output", "source": messages[0]},
                {
                    "value": "override",
                    "source": messages[0],
                    "headers": {"correlation_id": "explicit"},
                },
            ]
        )
        for message in messages[1:]:
            with message.process():
                backend.produce("fallback")
        backend.produce("unassociated")

    backend.consume_batches(handler)
    outputs = list(backend.producer.messages)
    assert [output.get("headers", {}).get("correlation_id") for output in outputs] == [
        "first",
        "explicit",
        "second",
        "third",
        None,
    ]
    assert all("source" not in output for output in outputs)
    assert get_propagated_headers() == {}


def test_mock_direct_consumer_keeps_uncommitted_batch_for_replay(backend):
    with backend.get_batch_consumer(batch_size=2)() as consumer:
        batches = iter(consumer)
        first = next(batches)
        consumer.commit_batch(first)
        second = next(batches)
        assert len(second) == 1
    assert list(backend.consumer.messages) == second


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": 0},
        {"timeout": float("nan")},
        {"delivery_timeout": 0},
    ],
)
def test_mocks_validate_batch_options(backend, kwargs):
    handler = Mock()
    with pytest.raises(ValueError):
        backend.consume_batches(handler, **kwargs)
    handler.assert_not_called()


def test_messagebus_forwards_batch_options_and_collection():
    backend = Mock()
    bus = MessageBus({"backend_config": {}}, importer=lambda _: lambda **kwargs: backend)
    handler = Mock()
    bus.consume_batches(handler, batch_size=5, timeout=0.2, delivery_timeout=8)
    backend.consume_batches.assert_called_once_with(
        handler, batch_size=5, timeout=0.2, delivery_timeout=8
    )
    records = [{"value": "one"}]
    bus.produce_many(records, timeout=7)
    backend.produce_many.assert_called_once_with(records, timeout=7)
    bus.get_batch_consumer(batch_size=3, timeout=0.4)
    backend.get_batch_consumer.assert_called_once_with(batch_size=3, timeout=0.4)


def test_consumer_uses_explicit_batch_handler():
    bus = Mock()
    handler = Mock(spec=Handler)
    consumer = Consumer(bus, handler)
    consumer.consume_batches(batch_size=3, timeout=0.2, delivery_timeout=9)
    bus.consume_batches.assert_called_once_with(
        handler=handler.handle_batch, batch_size=3, timeout=0.2, delivery_timeout=9
    )
    handler.handle.assert_not_called()


def test_base_handler_does_not_silently_fall_back():
    class SingleOnly(Handler):
        handlers = {"input": Mock()}

    with pytest.raises(NotImplementedError, match="handle_batch"):
        SingleOnly().handle_batch([])

"""Exercise both libraries together with only Kafka transport/schema lookup mocked."""

from typing import NamedTuple
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.avro import AvroProducer as NativeProducer

from eventsourcing_helpers.message import Event
from eventsourcing_helpers.messagebus.backends.kafka import KafkaAvroBackend

from confluent_kafka_helpers import clear_shutdown_requested
from confluent_kafka_helpers.context import get_propagated_headers
from confluent_kafka_helpers.exceptions import KafkaDeliveryError
from confluent_kafka_helpers.producer import AvroProducer


@Event
class Output(NamedTuple):
    offset: int


@pytest.fixture
def pipeline():
    clear_shutdown_requested()
    timeline = []
    queued = []
    native = Mock()
    native._serializer.decode_message.side_effect = lambda value, **kwargs: value
    native.assignment.return_value = [TopicPartition("input", 0)]

    def raw(offset, action, error=None):
        record = Mock()
        record.topic.return_value = "input"
        record.partition.return_value = 0
        record.offset.return_value = offset
        record.value.return_value = {"class": "Input", "data": {"offset": offset}}
        record.key.return_value = str(offset)
        record.headers.return_value = [("origin_action_type", action.encode())]
        record.timestamp.return_value = (0, -1)
        record.error.return_value = error
        return record

    native.consume.return_value = [
        raw(4, "first"),
        raw(6, "second"),
        raw(7, "eof", KafkaError(KafkaError._PARTITION_EOF)),
    ]
    native.commit.side_effect = lambda **kwargs: timeline.append("commit") or []
    registry = Mock()

    def publish(**kwargs):
        timeline.append("publish")
        queued.append(kwargs)

    with (
        patch("confluent_kafka_helpers.consumer.ConfluentAvroConsumer", return_value=native),
        patch.object(NativeProducer, "__init__", return_value=None),
        patch.object(NativeProducer, "produce", side_effect=publish),
        patch.object(NativeProducer, "flush") as flush,
        patch.object(NativeProducer, "poll") as poll,
        patch.object(AvroProducer, "_close"),
    ):

        def delivered(timeout):
            timeline.append("flush")
            for args in queued:
                args["on_delivery"](None, Mock())
            return 0

        flush.side_effect = delivered
        backend = KafkaAvroBackend(
            {
                "producer": {
                    "topics": ["output"],
                    "bootstrap.servers": "unused:9092",
                    "schema.registry.url": "unused",
                    "flush": True,
                },
                "consumer": {
                    "topics": ["input"],
                    "group.id": "test",
                    "bootstrap.servers": "unused:9092",
                    "schema.registry.url": "unused",
                    "stop_on_eof": True,
                },
            },
            producer=lambda config, **kwargs: AvroProducer(
                config, schema_registry=lambda _: registry, **kwargs
            ),
            get_offset_watchdog_config=lambda _: None,
        )
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.return_value = False
        backend.offset_watchdog.set_seen.side_effect = lambda message: timeline.append("seen")
        yield backend, native, queued, timeline, flush, poll
    clear_shutdown_requested()


def test_fallback_and_bulk_output_confirmed_before_watchdog_and_commit(pipeline):
    backend, native, queued, timeline, flush, poll = pipeline

    def handler(messages):
        timeline.append("database")
        with messages[0].process():
            assert get_propagated_headers() == {"origin_action_type": "first"}
            backend.produce(Output(offset=4))
        backend.produce_many([{"value": Output(offset=6), "source": messages[1]}])

    backend.consume_batches(handler)
    assert timeline == ["database", "publish", "publish", "flush", "seen", "seen", "commit"]
    assert [args["headers"]["origin_action_type"] for args in queued] == ["first", "second"]
    flush.assert_called_once()
    poll.assert_not_called()
    assert [
        (p.topic, p.partition, p.offset) for p in native.commit.call_args.kwargs["offsets"]
    ] == [("input", 0, 7)]
    assert native.commit.call_args.kwargs["asynchronous"] is False


@pytest.mark.parametrize("failure", ["handler", "delivery", "callback", "pending", "commit"])
def test_failures_stop_consumption_without_premature_acknowledgement(pipeline, failure):
    backend, native, queued, timeline, flush, poll = pipeline
    if failure == "commit":
        native.commit.side_effect = KafkaException(KafkaError(KafkaError._ASSIGNMENT_LOST))

    def delivered(timeout):
        if failure == "pending":
            return 1
        for args in queued:
            args["on_delivery"]("delivery failed" if failure == "delivery" else None, Mock())
        return 0

    flush.side_effect = delivered

    def handler(messages):
        callback = Mock(
            side_effect=ValueError("callback failed") if failure == "callback" else None
        )
        with messages[0].process():
            backend.produce(Output(offset=4), on_delivery=callback)
        if failure == "handler":
            raise ValueError("handler failed")

    with pytest.raises((ValueError, KafkaDeliveryError, TimeoutError, KafkaException)):
        backend.consume_batches(handler)
    if failure != "commit":
        native.commit.assert_not_called()
        backend.offset_watchdog.set_seen.assert_not_called()
    else:
        native.commit.assert_called_once()
    native.consume.assert_called_once()
    assert not backend.producer.in_delivery_scope
    assert not backend._processing_batch
    assert get_propagated_headers() == {}


def test_early_delivery_failure_during_fallback_queue_wait_prevents_ack(pipeline):
    backend, native, queued, timeline, flush, poll = pipeline
    attempts = []

    def publish(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 2:
            raise BufferError()
        queued.append(kwargs)

    def fail_during_poll(timeout):
        queued[0]["on_delivery"]("delivery failed", Mock())

    poll.side_effect = fail_during_poll

    def handler(messages):
        for message in messages:
            with message.process():
                backend.produce(Output(offset=message._meta.offset))

    with patch.object(NativeProducer, "produce", side_effect=publish):
        with pytest.raises(KafkaDeliveryError, match="delivery failed"):
            backend.consume_batches(handler)
    assert len(attempts) == 2
    poll.assert_called_once()
    flush.assert_not_called()
    native.commit.assert_not_called()
    backend.offset_watchdog.set_seen.assert_not_called()

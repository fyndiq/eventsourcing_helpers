from functools import partial
from unittest.mock import Mock, call

import pytest

from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.messagebus.backends.kafka import KafkaAvroBackend

from confluent_kafka_helpers.context import get_propagated_headers


def create_message(offset, headers=None):
    message = Mock()
    message._meta = Mock()
    message._meta.offset = offset
    message._meta.partition = 0
    message._meta.topic = "topic"
    message._meta.headers = headers or {}
    return message


class AvroConsumerMock:
    last_instance = None

    def __init__(self, config):
        message = create_message(offset=1)
        self.config = config
        self.is_auto_commit = False
        self.commit = Mock()
        self.messages = iter([message, message])
        self.batch_values = []
        type(self).last_instance = self

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.messages)

    def batches(self):
        return iter(self.batch_values)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        pass


def make_batch_consumer(batches, is_auto_commit=False):
    class BatchConsumerMock(AvroConsumerMock):
        def __init__(self, config):
            super().__init__(config=config)
            self.is_auto_commit = is_auto_commit
            self.batch_values = batches

    return BatchConsumerMock


class RecordingHandler(Handler):
    handlers = {"message": Mock()}

    def __init__(self):
        self.handled = []
        self.propagated_headers = []

    def handle(self, message):
        self.handled.append(message)
        self.propagated_headers.append(get_propagated_headers())


class KafkaBackendTests:

    def setup_method(self):
        self.consumer = AvroConsumerMock
        self.producer = Mock()
        self.value_serializer = Mock()
        self.config = {"producer": {"foo": "bar"}, "consumer": {"group.id": "consumer.group.1"}}
        self.get_producer_config = Mock(return_value=self.config["producer"])
        self.get_consumer_config = Mock(return_value=self.config["consumer"])
        self.get_offset_watchdog_config = Mock(return_value=None)
        self.id, self.event = 1, {"event": "value"}
        self.backend = partial(
            KafkaAvroBackend,
            self.config,
            self.producer,
            self.consumer,
            self.value_serializer,
            self.get_producer_config,
            self.get_consumer_config,
        )

    def test_init(self):
        """
        Test that the dependencies are setup correctly when
        initializing the backend.
        """
        self.backend()

        self.producer.assert_called_once_with(
            self.config["producer"], value_serializer=self.value_serializer
        )

    def test_consume(self):
        backend = self.backend()
        handler = Mock()
        backend.consume(handler=handler)
        assert handler.call_count == 1

    def _backend_with_consumer(self, consumer):
        return KafkaAvroBackend(
            self.config,
            self.producer,
            consumer,
            self.value_serializer,
            self.get_producer_config,
            self.get_consumer_config,
            self.get_offset_watchdog_config,
        )

    def test_consume_batches_commits_once_per_batch(self):
        batches = [
            [create_message(offset=1), create_message(offset=2)],
            [create_message(offset=3)],
        ]
        self.consumer = make_batch_consumer(batches)
        backend = self._backend_with_consumer(self.consumer)
        handler = Mock()

        backend.consume_batches(handler=handler)

        assert handler.call_args_list == [call(batches[0]), call(batches[1])]
        assert self.consumer.last_instance.commit.call_count == 2

    def test_consume_batches_skips_empty_batches(self):
        batches = [[], [create_message(offset=1)]]
        self.consumer = make_batch_consumer(batches)
        backend = self._backend_with_consumer(self.consumer)
        handler = Mock()

        backend.consume_batches(handler=handler)

        handler.assert_called_once_with(batches[1])
        self.consumer.last_instance.commit.assert_called_once_with(asynchronous=False)

    def test_consume_batches_does_not_commit_on_error(self):
        self.consumer = make_batch_consumer([[create_message(offset=1)]])
        backend = self._backend_with_consumer(self.consumer)
        handler = Mock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError):
            backend.consume_batches(handler=handler)

        self.consumer.last_instance.commit.assert_not_called()

    def test_consume_batches_skips_messages_seen_by_watchdog(self):
        messages = [create_message(offset=1), create_message(offset=2), create_message(offset=3)]
        self.consumer = make_batch_consumer([messages])
        backend = self._backend_with_consumer(self.consumer)
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.side_effect = [False, True, False]
        handler = Mock()

        backend.consume_batches(handler=handler)

        handler.assert_called_once_with([messages[0], messages[2]])
        backend.offset_watchdog.set_seen.assert_any_call(messages[0])
        backend.offset_watchdog.set_seen.assert_any_call(messages[2])
        assert backend.offset_watchdog.set_seen.call_count == 2
        self.consumer.last_instance.commit.assert_called_once_with(asynchronous=False)

    def test_consume_batches_calls_per_message_handlers_with_message_headers(self):
        messages = [
            create_message(offset=1, headers={"x-request-id": "one", "other": "ignored"}),
            create_message(offset=2, headers={"x-request-id": "two"}),
        ]
        self.config["consumer"]["headers.propagate"] = ["x-request-id"]
        self.consumer = make_batch_consumer([messages])
        backend = self._backend_with_consumer(self.consumer)
        handler = RecordingHandler()

        backend.consume_batches(handler=handler.handle)

        assert handler.handled == messages
        assert handler.propagated_headers == [
            {"x-request-id": "one"},
            {"x-request-id": "two"},
        ]
        assert get_propagated_headers() == {}
        self.consumer.last_instance.commit.assert_called_once_with(asynchronous=False)

from functools import partial
from unittest.mock import Mock

import pytest

from eventsourcing_helpers.messagebus.backends.kafka import KafkaAvroBackend


class AvroConsumerMock:
    def __init__(self, config):
        message = Mock()
        message._meta = Mock()
        message._meta.offset = 1
        self.messages = iter([message, message])

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.messages)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        pass

    is_auto_commit = False
    commit = Mock()


class KafkaBackendTests:

    def setup_method(self):
        self.consumer = AvroConsumerMock
        self.producer = Mock()
        self.value_serializer = Mock()
        self.config = {"producer": {"foo": "bar"}, "consumer": {"group.id": "consumer.group.1"}}
        self.get_producer_config = Mock(return_value=self.config["producer"])
        self.get_consumer_config = Mock(return_value=self.config["consumer"])
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

    def test_handle_stores_offset_after_success_when_auto_commit_is_enabled(self):
        backend = self.backend()
        backend.offset_watchdog = None
        handler = Mock()
        message = Mock(_raw=Mock())
        consumer = Mock(is_auto_commit=True)
        consumer.store_offsets.side_effect = lambda **_: handler.assert_called_once_with(message)

        backend._handle(handler, message, consumer)

        consumer.store_offsets.assert_called_once_with(message=message._raw)
        consumer.commit.assert_not_called()

    def test_handle_commits_synchronously_when_auto_commit_is_disabled(self):
        backend = self.backend()
        backend.offset_watchdog = None
        handler = Mock()
        message = Mock(_raw=Mock())
        consumer = Mock(is_auto_commit=False)

        backend._handle(handler, message, consumer)

        consumer.commit.assert_called_once_with(asynchronous=False)
        consumer.store_offsets.assert_not_called()

    def test_handle_does_not_advance_offset_when_handler_fails(self):
        backend = self.backend()
        backend.offset_watchdog = None
        handler = Mock(side_effect=RuntimeError("handler failed"))
        message = Mock(_raw=Mock())
        consumer = Mock(is_auto_commit=True)

        with pytest.raises(RuntimeError, match="handler failed"):
            backend._handle(handler, message, consumer)

        consumer.store_offsets.assert_not_called()
        consumer.commit.assert_not_called()

    def test_handle_stores_offset_for_message_skipped_by_offset_watchdog(self):
        backend = self.backend()
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.return_value = True
        handler = Mock()
        message = Mock(_raw=Mock())
        consumer = Mock(is_auto_commit=True)

        backend._handle(handler, message, consumer)

        handler.assert_not_called()
        backend.offset_watchdog.set_seen.assert_not_called()
        consumer.store_offsets.assert_called_once_with(message=message._raw)

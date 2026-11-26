from functools import partial
from unittest.mock import MagicMock, Mock, call

import pytest

from eventsourcing_helpers.messagebus.backends.kafka import KafkaAvroBackend


def create_message(offset, headers=None):
    message = Mock()
    message._raw = Mock()
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
        self.commit_batch = Mock()
        self.store_offsets = Mock()
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


def make_batch_consumer(batches, *, is_auto_commit, calls=None):
    class BatchConsumerMock(AvroConsumerMock):
        def __init__(self, config, **kwargs):
            super().__init__(config=config)
            self.is_auto_commit = is_auto_commit
            if calls is not None:
                self.commit_batch = calls.commit_batch
                self.store_offsets = calls.store_offsets
            self.batch_values = batches
            self.messages = iter(batches)

    return BatchConsumerMock


class KafkaBackendTests:

    def setup_method(self):
        self.consumer = AvroConsumerMock
        self.producer = MagicMock()
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

    def test_handle_stores_offset_after_success_when_auto_commit_is_enabled(self):
        backend = self.backend()
        backend.offset_watchdog = None
        parent = Mock()
        message = Mock(_raw=Mock())
        consumer = Mock(is_auto_commit=True)
        consumer.store_offsets = parent.store_offsets

        backend._handle(parent.handler, message, consumer)

        assert parent.mock_calls == [
            call.handler(message),
            call.store_offsets(message=message._raw),
        ]
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

    @pytest.mark.parametrize("is_auto_commit", [True, False])
    def test_handle_does_not_advance_offset_when_handler_fails(self, is_auto_commit):
        backend = self.backend()
        backend.offset_watchdog = None
        handler = Mock(side_effect=RuntimeError("handler failed"))
        message = Mock(_raw=Mock())
        consumer = Mock(is_auto_commit=is_auto_commit)

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

    def _backend_with_consumer(self, consumer):
        return KafkaAvroBackend(
            self.config,
            self.producer,
            consumer,
            self.value_serializer,
            self.get_producer_config,
            self.get_consumer_config,
            self.get_offset_watchdog_config,
            batch_consumer=consumer,
        )

    def test_consume_batches_commits_once_per_batch(self):
        batches = [
            [create_message(offset=1), create_message(offset=2)],
            [create_message(offset=3)],
        ]
        self.consumer = make_batch_consumer(batches, is_auto_commit=False)
        backend = self._backend_with_consumer(self.consumer)
        handler = Mock()

        backend.consume_batches(handler=handler)

        assert handler.call_args_list == [call(batches[0]), call(batches[1])]
        assert self.consumer.last_instance.commit_batch.call_args_list == [
            call(batches[0]),
            call(batches[1]),
        ]

    def test_consume_batches_confirms_delivery_before_commit(self):
        messages = [create_message(offset=1), create_message(offset=2)]
        calls = Mock()
        self.consumer = make_batch_consumer([messages], is_auto_commit=False, calls=calls)
        backend = self._backend_with_consumer(self.consumer)
        scope = MagicMock()
        scope.__enter__.side_effect = lambda: calls.delivery_start()
        scope.__exit__.side_effect = lambda *args: calls.delivery_confirm() and False
        backend.producer.delivery_scope.return_value = scope
        backend.offset_watchdog = None

        backend.consume_batches(handler=calls.handler)

        assert calls.mock_calls == [
            call.delivery_start(),
            call.handler(messages),
            call.delivery_confirm(),
            call.commit_batch(messages),
        ]

    def test_consume_batches_commits_after_success_when_auto_commit_is_disabled(self):
        messages = [create_message(offset=1), create_message(offset=2)]
        calls = Mock()
        self.consumer = make_batch_consumer([messages], is_auto_commit=False, calls=calls)
        backend = self._backend_with_consumer(self.consumer)

        backend.consume_batches(handler=calls.handler)

        assert calls.mock_calls == [
            call.handler(messages),
            call.commit_batch(messages),
        ]

    def test_consume_batches_skips_empty_batches(self):
        batches = [[], [create_message(offset=1)]]
        self.consumer = make_batch_consumer(batches, is_auto_commit=False)
        backend = self._backend_with_consumer(self.consumer)
        handler = Mock()

        backend.consume_batches(handler=handler)

        handler.assert_called_once_with(batches[1])
        self.consumer.last_instance.commit_batch.assert_called_once_with(batches[1])

    @pytest.mark.parametrize(
        "is_auto_commit", [True, False], ids=["auto-commit", "explicit-commit"]
    )
    def test_consume_batches_does_not_advance_offsets_on_partial_failure(self, is_auto_commit):
        messages = [create_message(offset=1), create_message(offset=2)]
        self.consumer = make_batch_consumer([messages], is_auto_commit=is_auto_commit)
        backend = self._backend_with_consumer(self.consumer)
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.return_value = False
        process_message = Mock(side_effect=[None, RuntimeError("second message failed")])

        def handler(batch):
            for message in batch:
                process_message(message)

        with pytest.raises(RuntimeError, match="second message failed"):
            backend.consume_batches(handler=handler)

        assert process_message.call_args_list == [call(messages[0]), call(messages[1])]
        backend.offset_watchdog.set_seen.assert_not_called()
        self.consumer.last_instance.store_offsets.assert_not_called()
        self.consumer.last_instance.commit.assert_not_called()
        self.consumer.last_instance.commit_batch.assert_not_called()

    def test_consume_batches_skips_messages_seen_by_watchdog(self):
        messages = [create_message(offset=1), create_message(offset=2), create_message(offset=3)]
        self.consumer = make_batch_consumer([messages], is_auto_commit=False)
        backend = self._backend_with_consumer(self.consumer)
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.side_effect = [False, True, False]
        handler = Mock()

        backend.consume_batches(handler=handler)

        handler.assert_called_once_with([messages[0], messages[2]])
        backend.offset_watchdog.set_seen.assert_any_call(messages[0])
        backend.offset_watchdog.set_seen.assert_any_call(messages[2])
        assert backend.offset_watchdog.set_seen.call_count == 2
        self.consumer.last_instance.commit_batch.assert_called_once_with(messages)

    def test_all_seen_batch_commits_without_handler_or_delivery_scope(self):
        messages = [create_message(offset=1)]
        self.consumer = make_batch_consumer([messages], is_auto_commit=False)
        backend = self._backend_with_consumer(self.consumer)
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.return_value = True
        handler = Mock()
        backend.consume_batches(handler)
        handler.assert_not_called()
        backend.producer.delivery_scope.assert_not_called()
        backend.offset_watchdog.set_seen.assert_not_called()
        self.consumer.last_instance.commit_batch.assert_called_once_with(messages)

    def test_delivery_failure_does_not_advance_watchdog_or_offsets(self):
        messages = [create_message(offset=1)]
        self.consumer = make_batch_consumer([messages], is_auto_commit=False)
        backend = self._backend_with_consumer(self.consumer)
        backend.offset_watchdog = Mock()
        backend.offset_watchdog.seen.return_value = False
        backend.producer.delivery_scope.return_value.__exit__.side_effect = TimeoutError("output")
        with pytest.raises(TimeoutError):
            backend.consume_batches(Mock())
        backend.offset_watchdog.set_seen.assert_not_called()
        self.consumer.last_instance.commit_batch.assert_not_called()
        assert backend._processing_batch is False

    def test_consume_only_backend_does_not_require_producer(self):
        messages = [create_message(offset=1)]
        self.consumer = make_batch_consumer([messages], is_auto_commit=False)
        backend = self._backend_with_consumer(self.consumer)
        backend.producer = None
        handler = Mock()
        backend.consume_batches(handler)
        handler.assert_called_once_with(messages)
        self.consumer.last_instance.commit_batch.assert_called_once_with(messages)

    def test_batch_produce_suppresses_per_message_flush_and_poll(self):
        backend = self.backend()
        backend.flush = True
        backend._processing_batch = True
        backend.produce("output")
        backend.producer.produce.assert_called_once_with(key=None, value="output", topic=None)
        backend.producer.flush.assert_not_called()
        backend.producer.poll.assert_not_called()

    def test_collection_flushes_once_outside_batch(self):
        backend = self.backend()
        backend.flush = True
        records = [{"value": "one"}, {"value": "two"}]
        backend.produce_many(records, timeout=5)
        backend.producer.produce_many.assert_called_once_with(records, timeout=5)
        backend.producer.flush.assert_called_once_with()

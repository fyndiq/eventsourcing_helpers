import copy
import time
from contextvars import ContextVar
from functools import partial
from itertools import islice
from typing import Optional, Union
from unittest.mock import MagicMock

from eventsourcing_helpers.messagebus.backends import MessageBusBackend

from confluent_kafka_helpers import is_shutdown_requested
from confluent_kafka_helpers.message import Message

_in_batch = ContextVar("mock_batch", default=False)


class MockBatchConsumer:
    def __init__(self, messages, config, batch_size, timeout):
        from confluent_kafka_helpers.consumer import get_propagate_header_keys

        self.messages = messages
        self.batch_size = batch_size
        self.timeout = timeout
        self.stop_on_eof = config.get("stop_on_eof", True)
        self.propagate_header_keys = get_propagate_header_keys(config.get("headers.propagate"))
        self.position = 0

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        while not is_shutdown_requested():
            batch = list(islice(self.messages, self.position, self.position + self.batch_size))
            if batch:
                self.position += len(batch)
                return [Message(message._raw, self.propagate_header_keys) for message in batch]
            if self.stop_on_eof:
                break
            time.sleep(self.timeout)
        raise StopIteration

    def commit_batch(self, messages):
        raw_ids = {id(message._raw) for message in messages}
        committed = max(
            (
                index
                for index, message in enumerate(self.messages, 1)
                if id(message._raw) in raw_ids
            ),
            default=0,
        )
        for _ in range(committed):
            self.messages.popleft()
            self.position -= 1


class MockBatchBackend(MessageBusBackend):
    def get_batch_consumer(self, batch_size=100, timeout=0.1):
        from confluent_kafka_helpers.consumer import validate_batch_options

        validate_batch_options(batch_size, timeout)
        return partial(
            MockBatchConsumer,
            self.consumer.get_messages(),
            self.config.get("consumer", {}),
            batch_size,
            timeout,
        )

    def consume_batch(self, handler, batch_size=100, timeout=0.1, delivery_timeout=30.0):
        from confluent_kafka_helpers.context import propagated_headers_context

        consumer_factory = self.get_batch_consumer(batch_size, timeout)
        with consumer_factory() as consumer:
            for batch in consumer:
                token = _in_batch.set(True)
                try:
                    with propagated_headers_context({}):
                        handler(list(batch))
                finally:
                    _in_batch.reset(token)
                consumer.commit_batch(batch)

    def produce_many(self, records):
        for record in records:
            self.produce(**record)


def apply_source_headers(kwargs):
    source = kwargs.pop("source", None)
    if source is not None:
        propagated_headers = source.propagated_headers
    elif _in_batch.get():
        from confluent_kafka_helpers.context import get_propagated_headers

        propagated_headers = get_propagated_headers()
    else:
        return
    if propagated_headers:
        kwargs["headers"] = {**propagated_headers, **(kwargs.get("headers") or {})}


def create_message(
    key: str, value: dict, topic: Optional[str], headers: Optional[Union[dict, list]]
) -> Message:
    if isinstance(headers, dict):
        headers = list(headers.items())
    kafka_message = MagicMock()
    kafka_message.configure_mock(
        **{
            "key.return_value": key,
            "value.return_value": copy.deepcopy(value),
            "topic.return_value": topic,
            "headers.return_value": headers,
            "timestamp.return_value": (0, time.time()),
        }
    )
    message = Message(kafka_message=kafka_message)
    return message

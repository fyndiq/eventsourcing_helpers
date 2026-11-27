import copy
import time
from collections import deque
from dataclasses import dataclass, field
from functools import partial
from itertools import islice
from typing import Iterable, Optional, Union
from unittest.mock import MagicMock

from eventsourcing_helpers.messagebus.backends import MessageBusBackend

from confluent_kafka_helpers import is_shutdown_requested
from confluent_kafka_helpers.consumer import get_propagate_header_keys, validate_batch_options
from confluent_kafka_helpers.context import get_propagated_headers, propagated_headers
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.utils import validate_timeout


@dataclass
class MockConsumer:
    messages: deque[Message] = field(default_factory=deque)
    _next_offsets: dict[tuple[str | None, int], int] = field(
        default_factory=dict, init=False, repr=False
    )

    def get_messages(self) -> deque[Message]:
        return self.messages

    def _append_message(self, key, value, topic, headers, partition: int) -> None:
        offset = self._next_offsets.get((topic, partition), 0)
        self.messages.append(create_message(key, value, topic, headers, partition, offset))
        self._next_offsets[topic, partition] = offset + 1

    def assert_one_message_added_with(
        self, message_class: str, data: dict, headers: dict = None
    ) -> None:
        assert len(self.messages) == 1
        assert {"class": message_class, "data": data} == self.messages[0].value
        assert (headers or {}) == self.messages[0]._meta.headers


class MockBatchConsumer:
    def __init__(self, messages, config, batch_size, timeout):
        for key in ("enable.auto.commit", "enable.auto.offset.store"):
            if str(config.get(key, False)).lower() not in {"false", "0"}:
                raise ValueError(f"Batch consumption requires {key}=false")
        for key in ("poll_timeout", "non_blocking", "batch_max_size", "batch_max_wait"):
            if key in config:
                raise ValueError(f"Use batch_size and timeout; batch consumer rejects {key}")
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

    def commit_batch(self, messages: Iterable[Message]) -> None:
        offsets: dict[tuple[str | None, int], int] = {}
        for message in messages:
            meta = message._meta
            partition = (meta.topic, meta.partition)
            offsets[partition] = max(offsets.get(partition, 0), meta.offset + 1)
        if not offsets:
            return

        remaining: deque[Message] = deque()
        position = self.position
        for index, message in enumerate(self.messages):
            meta = message._meta
            if meta.offset >= offsets.get((meta.topic, meta.partition), 0):
                remaining.append(message)
            elif index < self.position:
                position -= 1
        self.messages.clear()
        self.messages.extend(remaining)
        self.position = position


class MockBatchBackend(MessageBusBackend):
    def get_batch_consumer(self, batch_size=100, timeout=0.1):
        validate_batch_options(batch_size, timeout)
        return partial(
            MockBatchConsumer,
            self.consumer.get_messages(),
            self.config.get("consumer", {}),
            batch_size,
            timeout,
        )

    def consume_batch(self, handler, batch_size=100, timeout=0.1, delivery_timeout=30.0):
        validate_timeout(delivery_timeout, name="delivery_timeout", allow_zero=False)
        consumer_factory = self.get_batch_consumer(batch_size, timeout)
        with consumer_factory() as consumer:
            for batch in consumer:
                with propagated_headers({}):
                    handler(batch)
                consumer.commit_batch(batch)

    def produce_many(self, records, *, timeout=30.0):
        validate_timeout(timeout)
        for record in records:
            self.produce(**record)


def apply_source_headers(kwargs):
    source = kwargs.pop("source", None)
    if source is not None:
        propagated_headers = source.propagated_headers
    else:
        propagated_headers = get_propagated_headers()
    if propagated_headers:
        kwargs["headers"] = {**propagated_headers, **(kwargs.get("headers") or {})}


def create_message(
    key: Optional[str],
    value: dict,
    topic: Optional[str],
    headers: Optional[Union[dict, list]],
    partition: int = 0,
    offset: int = 0,
) -> Message:
    if isinstance(headers, dict):
        headers = list(headers.items())
    kafka_message = MagicMock()
    kafka_message.configure_mock(
        **{
            "key.return_value": key,
            "value.return_value": copy.deepcopy(value),
            "topic.return_value": topic,
            "partition.return_value": partition,
            "offset.return_value": offset,
            "headers.return_value": headers,
            "timestamp.return_value": (0, time.time()),
        }
    )
    message = Message(kafka_message=kafka_message)
    return message

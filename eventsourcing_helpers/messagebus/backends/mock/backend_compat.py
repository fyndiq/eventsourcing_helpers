from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, List, Union

import structlog

from eventsourcing_helpers.messagebus.backends.mock.utils import (
    MockBatchBackend,
    MockConsumer,
    apply_source_headers,
)
from eventsourcing_helpers.messagebus.backends.mock.utils import (
    create_message as create_kafka_message,
)

from confluent_kafka_helpers.message import Message

logger = structlog.get_logger(__name__)


def create_message(message_class: str, data: dict, headers: Union[None, dict]) -> Message:
    return create_kafka_message(None, {"class": message_class, "data": data}, None, headers)


class Consumer(MockConsumer):
    def add_message(
        self,
        message_class: str,
        data: dict,
        headers: dict = None,
        *,
        topic: str | None = None,
        partition: int = 0,
    ) -> None:
        self._append_message(
            None, {"class": message_class, "data": data}, topic, headers, partition
        )


@dataclass
class Producer:
    messages: Deque[dict] = field(default_factory=deque)

    def add_message(self, message: dict) -> None:
        self.messages.append(message)

    def clear_messages(self) -> None:
        self.messages.clear()

    def assert_message_produced_with(self, key: str, value: dict, **kwargs) -> None:
        assert dict(key=key, value=value, **kwargs) in self.messages

    def assert_one_message_produced_with(self, key: str, value: dict, **kwargs) -> None:
        assert len(self.messages) == 1
        self.assert_message_produced_with(key=key, value=value, **kwargs)

    def assert_multiple_messages_produced_with(self, messages: List) -> None:
        assert len(self.messages) == len(messages)
        for message in messages:
            self.assert_message_produced_with(**message)

    def assert_no_messages_produced(self) -> None:
        assert len(self.messages) == 0

    def assert_one_message_produced(self) -> None:
        assert len(self.messages) == 1


class MockBackend(MockBatchBackend):
    def __init__(self, config: dict) -> None:
        self.config = config
        self.consumer = Consumer()
        self.producer = Producer()

    def produce(self, value: dict, key: str = None, **kwargs) -> None:
        apply_source_headers(kwargs)
        self.producer.add_message(dict(value=value, key=key, **kwargs))

    def consume(self, handler: Callable) -> None:
        messages = self.consumer.get_messages()
        while messages:
            handler(messages.popleft())

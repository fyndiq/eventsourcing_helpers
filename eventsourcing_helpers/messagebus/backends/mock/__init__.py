from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, List

import structlog

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.mock.utils import create_message

logger = structlog.get_logger(__name__)


@dataclass
class Consumer:
    messages: Deque[Message] = field(default_factory=deque)

    def add_message(self, message_class: str, data: dict, headers: dict = None) -> None:
        message = create_message(
            message_class=message_class, data=data, headers=headers
        )
        self.messages.append(message)

    def get_messages(self) -> Deque[Message]:
        return self.messages

    def assert_one_message_added_with(
        self, message_class: str, data: dict, headers: dict = None
    ) -> None:
        if headers is None:
            headers = {}
        assert len(self.messages) == 1
        assert {'class': message_class, 'data': data} == self.messages[0].value
        assert headers == self.messages[0]._meta.headers


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


class MockBackend(MessageBusBackend):
    def __init__(self, config: dict) -> None:
        self.consumer = Consumer()
        self.producer = Producer()

    def produce(self, value: dict, key: str = None, **kwargs) -> None:
        self.producer.add_message(dict(value=value, key=key, **kwargs))

    def consume(self, handler: Callable) -> None:
        messages = self.consumer.get_messages()
        while messages:
            handler(messages.popleft())

from dataclasses import dataclass, field
from typing import Callable, List

import structlog

from confluent_kafka_helpers.message import Message

from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.mock.utils import create_kafka_message

logger = structlog.get_logger(__name__)


@dataclass
class Consumer:
    messages: List[Message] = field(default_factory=list)

    def add_message(self, message_class: str, data: dict) -> None:
        message = Message(
            kafka_message=create_kafka_message(message_class=message_class, data=data)
        )
        self.messages.append(message)

    def get_messages(self) -> List[Message]:
        return self.messages


@dataclass
class Producer:
    messages: List[dict] = field(default_factory=list)

    def add_message(self, message: dict) -> None:
        self.messages.append(message)

    def assert_one_message_produced_with(self, key: str, value: dict) -> None:
        assert len(self.messages) == 1
        assert dict(key=key, value=value) in self.messages

    def assert_no_messages_produced(self) -> None:
        assert len(self.messages) == 0


class MockBackend(MessageBusBackend):
    def __init__(self, config: dict) -> None:
        self.consumer = Consumer()
        self.producer = Producer()

    def produce(self, value: dict, key: str = None, **kwargs) -> None:
        self.producer.add_message(dict(value=value, key=key, **kwargs))

    def consume(self, handler: Callable) -> None:
        messages = self.consumer.get_messages()
        while messages:
            handler(messages.pop())

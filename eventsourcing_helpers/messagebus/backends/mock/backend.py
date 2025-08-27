import warnings
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Optional

import structlog

from eventsourcing_helpers.message import Message as MessageToKafka
from eventsourcing_helpers.message.pydantic import PydanticMixin
from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.mock.utils import create_message
from eventsourcing_helpers.serializers import to_message_from_dto

from confluent_kafka_helpers.message import Message as MessageFromKafka

logger = structlog.get_logger(__name__)


@dataclass
class Consumer:
    messages: Deque[MessageFromKafka] = field(default_factory=deque)

    def add_message(
        self,
        key: str,
        value: dict,
        topic: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> None:
        message = create_message(key=key, value=value, topic=topic, headers=headers)
        self.messages.append(message)

    def add_messages(self, messages: list) -> None:
        for message in messages:
            self.add_message(**message)

    def get_messages(self) -> Deque[MessageFromKafka]:
        return self.messages

    def assert_one_message_added_with(
        self, message_class: str, data: dict, headers: dict = None
    ) -> None:
        if headers is None:
            headers = {}
        assert len(self.messages) == 1
        assert {"class": message_class, "data": data} == self.messages[0].value
        assert headers == self.messages[0]._meta.headers


@dataclass
class Producer:
    messages: Deque[dict] = field(default_factory=deque)

    @property
    def num_messages(self):
        return len(self.messages)

    def add_message(self, message: dict) -> None:
        self.messages.append(message)

    def clear_messages(self) -> None:
        self.messages.clear()

    def assert_message_produced_with(self, key: str, value: dict, **kwargs) -> None:
        warnings.warn(
            "assert_message_produced_with is deprecated, use assert_messages_produced_with instead",
            DeprecationWarning,
            stacklevel=2,
        )
        assert dict(key=key, value=value, **kwargs) in self.messages

    def assert_one_message_produced_with(self, key: str, value: dict, **kwargs) -> None:
        warnings.warn(
            (
                "assert_one_message_produced_with is deprecated, "
                "use assert_messages_produced_with instead"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        assert len(self.messages) == 1
        self.assert_message_produced_with(key=key, value=value, **kwargs)

    def assert_multiple_messages_produced_with(self, messages: list) -> None:
        warnings.warn(
            (
                "assert_multiple_messages_produced_with is deprecated, "
                "use assert_messages_produced_with instead"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        assert len(self.messages) == len(messages)
        for message in messages:
            self.assert_message_produced_with(**message)

    def assert_no_messages_produced(self) -> None:
        assert self.num_messages == 0

    def assert_one_message_produced(self) -> None:
        assert self.num_messages == 1

    def assert_num_messages_produced(self, num: int) -> None:
        assert self.num_messages == num

    def assert_messages_produced_with(self, messages: list) -> None:
        assert messages == list(self.messages)

    def assert_messages_produced_with_class_names(self, *class_names) -> None:
        try:
            assert class_names == tuple(m["value"]["class"] for m in self.messages)
        except (TypeError, KeyError):
            raise AssertionError("Invalid message envelope", self.messages)


class MockBackend(MessageBusBackend):
    def __init__(self, config: dict) -> None:
        self.consumer = Consumer()
        self.producer = Producer()

    def produce(
        self,
        value: dict,
        key: str = None,
        value_serializer: Callable = to_message_from_dto,
        **kwargs,
    ) -> None:
        if isinstance(value, (MessageToKafka, PydanticMixin)):
            value = value_serializer(value)
        self.producer.add_message(dict(value=value, key=key, **kwargs))

    def consume(self, handler: Callable) -> None:
        messages = self.consumer.get_messages()
        while messages:
            handler(messages.popleft())

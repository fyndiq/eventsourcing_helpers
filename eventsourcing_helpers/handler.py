from typing import Callable, Iterable

from eventsourcing_helpers.serializers import from_message_to_dto

from confluent_kafka_helpers.message import Message


class Handler:
    handlers: dict = {}

    def __init__(self, message_deserializer: Callable = from_message_to_dto) -> None:
        assert self.handlers
        self.message_deserializer = message_deserializer

    def handle(self, message: Message) -> None:
        raise NotImplementedError("You need to implement handle method in your subclass")

    def handle_batch(self, messages: Iterable[Message]) -> None:
        for message in messages:
            with message.process():
                self.handle(message)

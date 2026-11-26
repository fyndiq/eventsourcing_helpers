from typing import Callable

from eventsourcing_helpers.serializers import from_message_to_dto

from confluent_kafka_helpers.message import Message


class Handler:
    handlers: dict = {}

    def __init__(self, message_deserializer: Callable = from_message_to_dto) -> None:
        assert self.handlers
        self.message_deserializer = message_deserializer

    def handle(self, message: dict) -> None:
        raise NotImplementedError("You need to implement handle method in your subclass")

    def handle_batch(self, messages: list[Message]) -> None:
        raise NotImplementedError("Batch handlers must implement handle_batch")

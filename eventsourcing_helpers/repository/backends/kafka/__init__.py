from typing import Callable, Iterator

from confluent_kafka_helpers.loader import AvroMessageLoader, MessageGenerator
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.repository.backends import RepositoryBackend
from eventsourcing_helpers.repository.backends.kafka.config import (
    get_loader_config, get_producer_config
)
from eventsourcing_helpers.serializers import to_message_from_dto


class KafkaAvroBackend(RepositoryBackend):
    def __init__(
        self, config: dict, producer=AvroProducer, loader=AvroMessageLoader,
        value_serializer: Callable = to_message_from_dto,
        get_producer_config: Callable = get_producer_config,
        get_loader_config: Callable = get_loader_config
    ) -> None:
        producer_config = get_producer_config(config)
        loader_config = get_loader_config(config)

        self.producer, self.loader = None, None
        if producer_config:
            self.producer = producer(producer_config, value_serializer=value_serializer)
        if loader_config:
            self.loader = loader(loader_config)

    def commit(self, id: str, events: list, **kwargs) -> None:
        """
        Commit staged events.

        Args:
            id: ID of the aggregate root to be used as key in the message.
            events: List of staged events to be committed.
        """
        assert self.producer is not None, "Producer is not configured"

        for event in events:
            self.producer.produce(key=id, value=event, **kwargs)

    def load(self, id: str, **kwargs) -> MessageGenerator:
        """
        Returns the repository message loader.

        Args:
            id: ID of the aggregate root.

        Returns:
            MessageGenerator: Repository message loader.
        """
        assert self.loader is not None, "Loader is not configured"
        return self.loader.load(id, **kwargs)

    def get_events(self, id: str, max_offset: int = None) -> Iterator[Message]:  # type: ignore
        """
        Get all aggregate events from the repository one at a time.

        Args:
            id: ID of the aggregate root.
            max_offset: Stop loading events at this offset.

        Yields:
            Message: The next available event.
        """
        with self.load(id) as events:  # type:ignore
            for event in events:
                if max_offset is not None and event._meta.offset > max_offset:
                    break
                yield event

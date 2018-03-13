from typing import Callable

from confluent_kafka_helpers.loader import AvroMessageLoader
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
            self.producer = producer(
                producer_config, value_serializer=value_serializer
            )
        if loader_config:
            self.loader = loader(loader_config)

    def commit(self, id: str, events: list, **kwargs) -> None:
        """
        Commit events to Kafka.

        Args:
            id: Aggregate root id to be used as key in the message.
            events: List of staged events to be committed.
        """
        assert self.producer is not None, "Producer is not configured"

        # TODO: investigate how "exactly once" delivery works
        # right now there is a potential risk that one of the produce's
        # fails and leaving the aggregate in an invalid state.
        for event in events:
            self.producer.produce(key=id, value=event, **kwargs)

    def load(self, id: str, **kwargs) -> list:
        """
        Load events from Kafka.

        Args:
            id: Aggregate root id to load.

        Returns:
            list: Loaded events.
        """
        assert self.loader is not None, "Loader is not configured"
        return self.loader.load(id, **kwargs)

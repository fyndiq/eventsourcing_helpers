from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.repository.backends.base import RepositoryBackend


class KafkaBackend(RepositoryBackend):

    def __init__(self, config: dict, producer=AvroProducer,
                 loader=AvroMessageLoader) -> None:
        self.producer = producer(config.get('producer'))
        self.loader = loader(config.get('loader'))

    def commit(self, guid: str, events: list, **kwargs) -> None:
        """
        Commit events to Kafka.

        Args:
            guid: Aggregate root guid to be used as key in the message.
            events: List of staged events to be committed.
        """
        for event in events:
            self.producer.produce(key=guid, value=event, **kwargs)

    def load(self, guid: str, **kwargs) -> list:
        """
        Load events from Kafka.

        Args:
            guid: Aggregate root guid to load.

        Returns:
            list: Loaded events.
        """
        return self.loader.load(guid, **kwargs)

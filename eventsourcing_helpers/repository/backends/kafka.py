from typing import Callable

from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer
from confluent_kafka_helpers.consumer import AvroConsumer

from eventsourcing_helpers.repository.backends import RepositoryBackend
from eventsourcing_helpers.serializers import to_message_from_dto


class KafkaAvroBackend(RepositoryBackend):

    def __init__(self, config: dict,
                 consumer: AvroConsumer, producer=AvroProducer,
                 loader=AvroMessageLoader,
                 value_serializer: Callable=to_message_from_dto) -> None:  # yapf: disable
        producer_config = config.pop('producer', None)
        loader_config = config.pop('loader', None)

        self.consumer = consumer
        self.producer, self.loader = None, None
        if producer_config:
            self.producer = producer(
                producer_config, value_serializer=value_serializer
            )
        if loader_config:
            self.loader = loader(loader_config)

    def commit(self, guid: str, events: list, **kwargs) -> None:
        """
        Commit events to Kafka.

        Args:
            guid: Aggregate root guid to be used as key in the message.
            events: List of staged events to be committed.
        """
        assert self.producer is not None, "Producer is not configured"

        # TODO: investigate how "exactly once" delivery works
        # right now there is a potential risk that one of the produce's
        # fails and leaving the aggregate in an invalid state.
        for event in events:
            self.producer.produce(key=guid, value=event, **kwargs)

        # at this point the events are hopefully committed to Kafka.
        #
        # the last step is to also commit the consumer offset to
        # mark the command as handled.
        #
        # if we don't do this the same command will be processed on
        # the next rebalance.
        if self.consumer.is_auto_commit is False:
            self.consumer.commit()

    def load(self, guid: str, **kwargs) -> list:
        """
        Load events from Kafka.

        Args:
            guid: Aggregate root guid to load.

        Returns:
            list: Loaded events.
        """
        assert self.loader is not None, "Loader is not configured"
        return self.loader.load(guid, **kwargs)

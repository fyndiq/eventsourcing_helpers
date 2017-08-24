from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers import logger
from eventsourcing_helpers.message import to_message_from_dto


class KafkaBackend:

    def __init__(self, producer_config, loader_config):
        self.producer = AvroProducer(producer_config)
        self.loader = AvroMessageLoader(loader_config)

    def save(self, aggregate, **kwargs):
        for event in aggregate._uncommitted_events:
            message = to_message_from_dto(event)
            self.producer.produce(key=aggregate._id, value=message,
                                  **kwargs)

        aggregate.mark_events_as_commited()

    def load(self, key, *args, **kwargs):
        logger.info("Loading messages from event store")
        messages = self.loader.load(key, *args, **kwargs)
        return messages

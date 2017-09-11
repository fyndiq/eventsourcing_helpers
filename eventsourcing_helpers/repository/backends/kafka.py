from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers import logger


class KafkaBackend:

    def __init__(self, producer_config, loader_config):
        self.producer = AvroProducer(producer_config)
        self.loader = AvroMessageLoader(loader_config)

    def save(self, aggregate, **kwargs):
        for event in aggregate._events:
            self.producer.produce(key=aggregate._guid, value=event, **kwargs)

        aggregate.clear_commited_events()

    def load(self, key, *args, **kwargs):
        logger.info("Loading messages from event store")
        messages = self.loader.load(key, *args, **kwargs)
        return messages

from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers import logger
from eventsourcing_helpers.message import to_message_from_dto


class KafkaBackend:

    def __init__(self, topic, producer_config, loader_config):
        self.topic = topic
        self.producer = AvroProducer(producer_config)
        self.loader = AvroMessageLoader(loader_config)

    def save(self, aggregate, topic=None, **kwargs):
        if not topic:
            topic = self.topic

        for event in aggregate._uncommitted_events:
            message = to_message_from_dto(event)
            self.producer.publish(topic=topic, key=aggregate._id,
                                  value=message, **kwargs)

        aggregate.mark_events_as_commited()

    def load(self, key, *args, **kwargs):
        logger.info("Loading messages from event store")
        messages = self.loader.load(key, *args, **kwargs)
        return messages

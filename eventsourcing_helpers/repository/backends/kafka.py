from confluent_kafka.avro import AvroProducer

from confluent_kafka_helpers.loader import AvroMessageLoader

from eventsourcing_helpers import logger
from eventsourcing_helpers.message import to_message_from_dto


class KafkaBackend:

    def __init__(self, topic, producer_config, loader_config,
                 default_value_schema, default_key_schema):
        self.topic = topic
        self.producer_config = producer_config
        self.default_value_schema = default_value_schema
        self.default_key_schema = default_key_schema

        self.producer = AvroProducer(
            producer_config,
            default_value_schema=default_value_schema,
            default_key_schema=default_key_schema
        )
        self.loader = AvroMessageLoader(loader_config)

    def _publish(self, topic, key, value):
        logger.info("Publishing message", topic=topic, key=key, value=value)
        self.producer.produce(topic=topic, key=key, value=value)
        self.producer.flush()

    def save(self, aggregate, topic=None):
        if not topic:
            topic = self.topic

        for event in aggregate._uncommitted_events:
            message = to_message_from_dto(event)
            self._publish(topic=topic, key=aggregate._id, value=message)

        aggregate.mark_events_as_commited()

    def load(self, key, *args, **kwargs):
        logger.info("Loading messages from event store")
        messages = self.loader.load(key, *args, **kwargs)
        return messages

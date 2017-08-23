from confluent_kafka.avro import AvroProducer

from confluent_kafka_helpers.loader import AvroMessageLoader
from confluent_kafka_helpers.schema_registry import SchemaRegistry

from eventsourcing_helpers import logger
from eventsourcing_helpers.message import to_message_from_dto


class KafkaBackend:

    def __init__(self, topic, producer_config, loader_config):
        self.topic = topic
        self.producer_config = producer_config

        schema_registry_url = producer_config['schema.registry.url']
        key_subject_name = producer_config.pop('key_subject_name')
        value_subject_name = producer_config.pop('value_subject_name')

        # fetch latest schemas from schema registry
        schema_registry = SchemaRegistry(schema_registry_url)
        key_schema = schema_registry.get_latest_schema(key_subject_name)
        value_schema = schema_registry.get_latest_schema(value_subject_name)

        self.producer = AvroProducer(
            producer_config,
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )
        self.loader = AvroMessageLoader(loader_config)

    def _publish(self, topic, key, value):
        logger.info("Publishing message", topic=topic, key=key,
                    value=value)
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

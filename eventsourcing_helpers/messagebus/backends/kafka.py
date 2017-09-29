from functools import partial

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.messagebus.backends import MessageBusBackend


class KafkaAvroBackend(MessageBusBackend):

    def __init__(self, config: dict, producer: AvroProducer=AvroProducer,
                 consumer: AvroConsumer=AvroConsumer) -> None:
        producer_config = config.pop('producer', None)
        consumer_config = config.pop('consumer', None)
        self.producer = producer(producer_config) if producer_config else None
        self.consumer = None
        if consumer_config:
            self.consumer = partial(consumer, config=consumer_config)

    def produce(self, key: str, value: dict, topic=None) -> None:
        assert self.producer is not None, "Producer is not configured"
        self.producer.produce(key, value, topic)

    def get_consumer(self) -> AvroConsumer:
        assert self.consumer is not None, "Consumer is not configured"
        return self.consumer

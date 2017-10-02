from functools import partial

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.serializers import to_message_from_dto


class KafkaAvroBackend(MessageBusBackend):

    def __init__(self, config: dict, producer: AvroProducer=AvroProducer,
                 consumer: AvroConsumer=AvroConsumer) -> None:
        producer_config = config.pop('producer', None)
        consumer_config = config.pop('consumer', None)

        self.consumer, self.producer = None, None
        if consumer_config:
            self.consumer = partial(consumer, config=consumer_config)
        if producer_config:
            self.producer = producer(
                producer_config, value_serializer=to_message_from_dto
            )

    def produce(self, key: str, value: dict, topic: str=None) -> None:
        assert self.producer is not None, "Producer is not configured"
        self.producer.produce(key, value, topic)

    def get_consumer(self) -> AvroConsumer:
        assert self.consumer is not None, "Consumer is not configured"
        return self.consumer

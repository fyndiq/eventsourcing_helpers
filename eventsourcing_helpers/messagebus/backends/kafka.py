from functools import partial
from typing import Callable

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.serializers import to_message_from_dto


class KafkaAvroBackend(MessageBusBackend):

    def __init__(self, config: dict, producer: AvroProducer=AvroProducer,
                 consumer: AvroConsumer=AvroConsumer,
                 value_serializer: Callable=to_message_from_dto) -> None:
        producer_config = config.pop('producer', None)
        consumer_config = config.pop('consumer', None)

        self.flush = producer_config.pop('flush', True)
        self.consumer, self.producer = None, None
        if consumer_config:
            self.consumer = partial(consumer, config=consumer_config)
        if producer_config:
            self.producer = producer(
                producer_config, value_serializer=value_serializer
            )

    def produce(self, key: str, value: dict, topic: str=None, **kwargs) -> None:
        assert self.producer is not None, "Producer is not configured"
        # produce message to a topic.
        #
        # this is an asynchronous operation, an application may use the
        # callback argument to pass a function that will be called from poll()
        # when the message has been successfully delivered or permanently fails
        # delivery.
        self.producer.produce(key, value, topic, **kwargs)

        # polls the producer for events and calls the corresponding callbacks.
        #
        # NOTE: since produce() is an asynchronous API this call
        #       will most likely not serve the delivery callback for the
        #       last produced message.
        self.producer.poll(0)

        # block until all messages are delivered/failed.
        # TODO: we should probably not flush every time
        # https://github.com/confluentinc/confluent-kafka-python/issues/137
        if self.flush:
            self.producer.flush()

    def get_consumer(self) -> AvroConsumer:
        assert self.consumer is not None, "Consumer is not configured"
        return self.consumer

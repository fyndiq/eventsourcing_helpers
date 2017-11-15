from functools import partial
from typing import Callable

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.kafka.config import (
    get_consumer_config, get_producer_config)
from eventsourcing_helpers.serializers import to_message_from_dto


class KafkaAvroBackend(MessageBusBackend):

    def __init__(self, config: dict,
                 handler: Callable=None,
                 producer: AvroProducer=AvroProducer,
                 consumer: AvroConsumer=AvroConsumer,
                 value_serializer: Callable=to_message_from_dto,
                 get_producer_config: Callable=get_producer_config,
                 get_consumer_config: Callable=get_consumer_config) -> None:  # yapf: disable
        self.handler = handler
        self.consumer, self.producer = None, None

        producer_config = get_producer_config(config)
        consumer_config = get_consumer_config(config)

        if consumer_config:
            self.consumer = partial(consumer, config=consumer_config)
        if producer_config:
            self.flush = producer_config.pop('flush', False)
            self.producer = producer(
                producer_config, value_serializer=value_serializer
            )

    def produce(self, value: dict, key: str=None, topic: str=None,
                **kwargs) -> None:  # yapf:disable
        assert self.producer is not None, "Producer is not configured"
        # produce a message to a topic.
        #
        # this is an asynchronous operation, an application may use the
        # callback argument to pass a function that will be called from poll()
        # when the message has been successfully delivered or permanently fails
        # delivery.
        self.producer.produce(key=key, value=value, topic=topic, **kwargs)

        # polls the producer for events and calls the corresponding callbacks.
        #
        # NOTE: since produce() is an asynchronous API this call
        #       will most likely not serve the delivery callback for the
        #       last produced message.
        self.producer.poll(0)

        # block until all messages are delivered/failed.
        # probably not required if we run poll after each produce.
        if self.flush:
            self.producer.flush()

    def get_consumer(self) -> AvroConsumer:
        assert self.consumer is not None, "Consumer is not configured"
        return self.consumer

    def consume(self) -> None:
        assert self.handler is not None, "You must set a handler"

        Consumer = self.get_consumer()
        with Consumer() as consumer:
            for message in consumer:
                if message:
                    self.handler(message.value())
                    if consumer.is_auto_commit is False:
                        consumer.commit()

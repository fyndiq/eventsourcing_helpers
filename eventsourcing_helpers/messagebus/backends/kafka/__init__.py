import time
from functools import partial
from typing import Any, Callable

import structlog

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.kafka.config import (
    get_consumer_config, get_producer_config)
from eventsourcing_helpers.serializers import to_message_from_dto

logger = structlog.get_logger(__name__)


class KafkaAvroBackend(MessageBusBackend):

    def __init__(self, config: dict,
                 producer: AvroProducer=AvroProducer,
                 consumer: AvroConsumer=AvroConsumer,
                 value_serializer=None,
                 get_producer_config: Callable=get_producer_config,
                 get_consumer_config: Callable=get_consumer_config) -> None:  # yapf: disable
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

    def _consume(self, handler: Callable, message: Message,
                 consumer: AvroConsumer) -> None:  # yapf: disable
        start_time = time.time()
        handler(message)
        if consumer.is_auto_commit is False:
            consumer.commit()
        end_time = time.time() - start_time
        logger.debug(f"Message processed in {end_time:.5f}s")

    def produce(self, value: dict, key: str=None, topic: str=None,
                **kwargs) -> None:  # yapf:disable
        assert self.producer is not None, "Producer is not configured"

        self.producer.produce(key=key, value=value, topic=topic, **kwargs)
        self.producer.poll(0)
        if self.flush:
            self.producer.flush()

    def get_consumer(self) -> AvroConsumer:
        assert self.consumer is not None, "Consumer is not configured"
        return self.consumer

    def consume(self, handler: Callable) -> None:
        assert callable(handler), "You must pass a message handler"
        Consumer = self.get_consumer()
        with Consumer() as consumer:
            for message in consumer:
                self._consume(handler, message, consumer)

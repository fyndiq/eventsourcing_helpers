import time
from functools import partial
from typing import Callable, Type

import structlog
from confluent_kafka import KafkaError, KafkaException

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.consumer.message import Message as ConsumerMessage
from confluent_kafka_helpers.producer import AvroProducer

from eventsourcing_helpers import metrics
from eventsourcing_helpers.message import Message as ProducerMessage
from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.kafka import utils
from eventsourcing_helpers.messagebus.backends.kafka.config import (
    get_consumer_config, get_offset_watchdog_config, get_producer_config
)
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog import OffsetWatchdog
from eventsourcing_helpers.serializers import to_message_from_dto

logger = structlog.get_logger(__name__)


class KafkaAvroBackend(MessageBusBackend):
    def __init__(
        self, config: dict, producer: AvroProducer = AvroProducer,
        consumer: AvroConsumer = AvroConsumer, value_serializer: Callable = to_message_from_dto,
        get_producer_config: Callable = get_producer_config,
        get_consumer_config: Callable = get_consumer_config,
        get_offset_watchdog_config: Callable = get_offset_watchdog_config
    ) -> None:
        self.consumer = None
        self.producer = None
        self.offset_watchdog = None
        self.value_serializer = value_serializer

        self.producer_config = get_producer_config(config)
        self.consumer_config = get_consumer_config(config)
        self.offset_wd_config = get_offset_watchdog_config(config)

        if self.producer_config:
            self.flush = self.producer_config.pop('flush', False)
            self.producer = producer(self.producer_config)
        if self.consumer_config:
            self.consumer = partial(consumer, config=self.consumer_config)
        if self.offset_wd_config:
            self.offset_watchdog = OffsetWatchdog(self.offset_wd_config)

    def _shall_handle(self, message: Type[ConsumerMessage]) -> bool:
        if not self.offset_watchdog:
            return True
        return not self.offset_watchdog.seen(message)

    def _set_handled(self, message: Type[ConsumerMessage]):
        if self.offset_watchdog:
            try:
                self.offset_watchdog.set_seen(message)
            except Exception:
                logger.exception("Failed to set offset, but will continue")

    @metrics.call_counter('eventsourcing_helpers.messagebus.kafka.handle.count')
    @metrics.timed('eventsourcing_helpers.messagebus.kafka.handle.time')
    def _handle(
        self, handler: Callable, message: Type[ConsumerMessage], consumer: AvroConsumer
    ) -> None:
        start_time = time.time()
        if self._shall_handle(message):
            handler(message)
            self._set_handled(message)
        if consumer.is_auto_commit is False:
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as e:
                error_code = e.args[0].code()
                if error_code == KafkaError._NO_OFFSET:
                    logger.warning("Offset already committed")
                else:
                    raise
        end_time = time.time() - start_time
        logger.debug(f"Message processed in {end_time:.5f}s")

    def produce(
        self, message: Type[ProducerMessage], key: str = None, topic: str = None, **kwargs
    ) -> None:
        assert self.producer is not None, "Producer is not configured"
        while True:
            try:
                self.producer.produce(
                    key=key, value=self.value_serializer(message), topic=topic,
                    key_schema=utils.get_key_schema(meta=message.Meta),
                    value_schema=utils.get_value_schema(meta=message.Meta), **kwargs
                )
                break
            except BufferError:
                self.producer.poll(timeout=0.5)
            continue

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
                self._handle(handler, message, consumer)

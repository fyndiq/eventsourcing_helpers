import time
from contextlib import nullcontext
from functools import partial
from typing import Callable

import structlog
from confluent_kafka import KafkaError, KafkaException

from eventsourcing_helpers import metrics
from eventsourcing_helpers.messagebus.backends import MessageBusBackend
from eventsourcing_helpers.messagebus.backends.kafka.config import (
    get_consumer_config,
    get_offset_watchdog_config,
    get_producer_config,
)
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog import OffsetWatchdog
from eventsourcing_helpers.serializers import to_message_from_dto

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.producer import AvroProducer
from confluent_kafka_helpers.utils import validate_timeout

logger = structlog.get_logger(__name__)


class KafkaAvroBackend(MessageBusBackend):
    def __init__(
        self,
        config: dict,
        producer: AvroProducer = AvroProducer,
        consumer: AvroConsumer = AvroConsumer,
        value_serializer: Callable = to_message_from_dto,
        get_producer_config: Callable = get_producer_config,
        get_consumer_config: Callable = get_consumer_config,
        get_offset_watchdog_config: Callable = get_offset_watchdog_config,
    ) -> None:
        self.consumer = None
        self.producer = None
        self.offset_watchdog = None

        producer_config = get_producer_config(config)
        consumer_config = get_consumer_config(config)
        self._batch_consumer_config = consumer_config.copy() if consumer_config else None
        offset_wd_config = get_offset_watchdog_config(config)

        if producer_config:
            self.flush = producer_config.pop("flush", False)
            self.producer = producer(producer_config, value_serializer=value_serializer)
        if consumer_config:
            self.consumer = partial(consumer, config=consumer_config)
        if offset_wd_config:
            self.offset_watchdog = OffsetWatchdog(offset_wd_config)

    def _shall_handle(self, message: Message) -> bool:
        if not self.offset_watchdog:
            return True
        return not self.offset_watchdog.seen(message)

    def _set_handled(self, message: Message):
        if self.offset_watchdog:
            try:
                self.offset_watchdog.set_seen(message)
            except Exception:
                logger.exception("Failed to set offset, but will continue")

    @metrics.call_counter("eventsourcing_helpers.messagebus.kafka.handle.count")
    @metrics.timed("eventsourcing_helpers.messagebus.kafka.handle.time")
    def _handle(self, handler: Callable, message: Message, consumer: AvroConsumer) -> None:
        start_time = time.time()
        if self._shall_handle(message):
            handler(message)
            self._set_handled(message)
        if consumer.is_auto_commit:
            # librdkafka requires its native message, not the helpers wrapper.
            consumer.store_offsets(message=message._raw)
        else:
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as e:
                error_code = e.args[0].code()
                if error_code == KafkaError._NO_OFFSET:  # type: ignore[attr-defined]
                    logger.warning("Offset already committed")
                else:
                    raise
        end_time = time.time() - start_time
        logger.debug(f"Message processed in {end_time:.5f}s")

    def produce(self, value: dict, key: str = None, topic: str = None, **kwargs) -> None:
        assert self.producer is not None, "Producer is not configured"

        while True:
            try:
                self.producer.produce(key=key, value=value, topic=topic, **kwargs)
                break
            except BufferError:
                self.producer.poll(timeout=0.5)
        self.producer.finish_producing(flush=self.flush)

    def produce_many(self, records, *, timeout=30.0) -> None:
        assert self.producer is not None, "Producer is not configured"
        self.producer.produce_many(records, timeout=timeout)
        self.producer.finish_producing(flush=self.flush)

    def get_consumer(self) -> AvroConsumer:
        assert self.consumer is not None, "Consumer is not configured"
        return self.consumer

    def get_batch_consumer(self, batch_size=100, timeout=0.1):
        from confluent_kafka_helpers.consumer import AvroBatchConsumer, validate_batch_options

        validate_batch_options(batch_size, timeout)
        assert self._batch_consumer_config is not None, "Consumer is not configured"
        return partial(
            AvroBatchConsumer,
            config=self._batch_consumer_config,
            batch_size=batch_size,
            timeout=timeout,
        )

    def consume(self, handler: Callable) -> None:
        assert callable(handler), "You must pass a message handler"
        Consumer = self.get_consumer()
        with Consumer() as consumer:
            for message in consumer:
                self._handle(handler, message, consumer)

    def consume_batch(self, handler: Callable, batch_size=100, timeout=0.1, delivery_timeout=30.0):
        assert callable(handler), "You must pass a batch handler"
        validate_timeout(delivery_timeout, name="delivery_timeout", allow_zero=False)
        Consumer = self.get_batch_consumer(batch_size, timeout)
        with Consumer() as consumer:
            for batch in consumer:
                messages = [message for message in batch if self._shall_handle(message)]
                if messages:
                    self._handle_batch(handler, messages, delivery_timeout)
                    for message in messages:
                        self._set_handled(message)
                consumer.commit_batch(batch)

    @metrics.call_counter("eventsourcing_helpers.messagebus.kafka.handle_batch.count")
    @metrics.timed("eventsourcing_helpers.messagebus.kafka.handle_batch.time")
    def _handle_batch(self, handler: Callable, messages: list[Message], delivery_timeout: float):
        scope = (
            self.producer.delivery_scope(delivery_timeout)
            if self.producer is not None
            else nullcontext()
        )
        with scope:
            handler(messages)

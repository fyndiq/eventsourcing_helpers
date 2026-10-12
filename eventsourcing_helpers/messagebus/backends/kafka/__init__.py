import time
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
from eventsourcing_helpers.messagebus.backends.kafka.context import message_handler_context
from eventsourcing_helpers.messagebus.backends.kafka.offset_watchdog import OffsetWatchdog
from eventsourcing_helpers.serializers import to_message_from_dto

from confluent_kafka_helpers.consumer import AvroConsumer
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.producer import AvroProducer

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
        self._commit(consumer, [message])
        end_time = time.time() - start_time
        logger.debug(f"Message processed in {end_time:.5f}s")

    def _commit(self, consumer: AvroConsumer, messages: list[Message]) -> None:
        if consumer.is_auto_commit:
            for message in messages:
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

    def _handle_batch(
        self, handler: Callable, messages: list[Message], consumer: AvroConsumer
    ) -> None:
        """
        If the handler raises, nothing is committed or stored and Kafka redelivers
        the whole batch, so handlers must be idempotent.
        """
        start_time = time.time()
        messages_to_handle = [message for message in messages if self._shall_handle(message)]
        if messages_to_handle:
            handler(messages_to_handle)

            for message in messages_to_handle:
                self._set_handled(message)

        self._commit(consumer, messages)
        end_time = time.time() - start_time
        logger.debug(f"Batch processed in {end_time:.5f}s", batch_size=len(messages))

    def produce(self, value: dict, key: str = None, topic: str = None, **kwargs) -> None:
        assert self.producer is not None, "Producer is not configured"

        while True:
            try:
                self.producer.produce(key=key, value=value, topic=topic, **kwargs)
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

    def consume_batches(self, handler: Callable) -> None:
        """
        Consume and handle message batches indefinitely.

        If handling raises, offsets are not committed or stored and Kafka can
        redeliver the whole batch.
        The handler receives ``list[Message]``.
        """
        assert callable(handler), "You must pass a message handler"
        Consumer = self.get_consumer()
        with Consumer() as consumer:
            for messages in consumer.batches():
                if not messages:
                    continue
                self._handle_batch(handler, messages, consumer)


__all__ = ["KafkaAvroBackend", "message_handler_context"]

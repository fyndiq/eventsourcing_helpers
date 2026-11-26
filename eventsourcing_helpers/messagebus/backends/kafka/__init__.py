import math
import time
from contextvars import ContextVar
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
from confluent_kafka_helpers.exceptions import KafkaDeliveryError
from confluent_kafka_helpers.message import Message
from confluent_kafka_helpers.producer import AvroProducer

logger = structlog.get_logger(__name__)


class BatchDelivery:
    def __init__(self, producer: AvroProducer, timeout: float):
        self.producer = producer
        self.timeout = timeout
        self.deadline: float | None = None
        self.pending = 0
        self.sent = False
        self.failure: Exception | None = None

    def _remaining(self) -> float:
        if self.deadline is None:
            self.deadline = time.monotonic() + self.timeout
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Kafka batch delivery timed out")
        return remaining

    def _track(self, records):
        for record in records:
            if self.failure is not None:
                raise self.failure
            args = dict(record)
            callback = args.pop("callback", None)
            callback = args.pop("on_delivery", None) or callback
            self.sent = True
            self.pending += 1
            yield {**args, "on_delivery": partial(self._delivered, callback)}

    def _delivered(self, callback, error, message):
        try:
            try:
                self.producer.delivery_callback(error, message)
            finally:
                if callback is not None:
                    callback(error, message)
            if error:
                raise KafkaDeliveryError(str(error), message)
        except Exception as failure:
            if self.failure is None:
                self.failure = failure
        finally:
            self.pending -= 1

    def produce_many(self, records):
        try:
            self.producer.produce_many(self._track(records), timeout=self._remaining())
        except Exception as failure:
            if self.failure is None:
                self.failure = failure
            raise

    def wait(self):
        if self.failure is not None:
            raise self.failure
        if not self.sent:
            return
        outstanding = self.producer.flush(self._remaining())
        if self.failure is not None:
            raise self.failure
        if outstanding or self.pending:
            raise TimeoutError("Kafka batch still has unconfirmed output")


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
        self._batch_delivery: ContextVar[BatchDelivery | None] = ContextVar(
            "batch_delivery", default=None
        )

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

        if delivery := self._batch_delivery.get():
            delivery.produce_many([dict(key=key, value=value, topic=topic, **kwargs)])
            return

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

    def produce_many(self, records) -> None:
        assert self.producer is not None, "Producer is not configured"
        if delivery := self._batch_delivery.get():
            delivery.produce_many(records)
            return

        self.producer.produce_many(records)
        self.producer.poll(0)
        if self.flush:
            self.producer.flush()

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
        if not math.isfinite(delivery_timeout) or delivery_timeout <= 0:
            raise ValueError("delivery_timeout must be finite and positive")
        Consumer = self.get_batch_consumer(batch_size, timeout)
        with Consumer() as consumer:
            for batch in consumer:
                messages = [message for message in batch if self._shall_handle(message)]
                if messages:
                    self._handle_batch(handler, messages, delivery_timeout)
                    if self.offset_watchdog:
                        for message in messages:
                            self.offset_watchdog.set_seen(message)
                consumer.commit_batch(batch)

    @metrics.call_counter("eventsourcing_helpers.messagebus.kafka.handle_batch.count")
    @metrics.timed("eventsourcing_helpers.messagebus.kafka.handle_batch.time")
    def _handle_batch(self, handler: Callable, messages: list[Message], delivery_timeout: float):
        delivery = (
            BatchDelivery(self.producer, delivery_timeout) if self.producer is not None else None
        )
        token = self._batch_delivery.set(delivery)
        try:
            handler(list(messages))
            if delivery is not None:
                delivery.wait()
        finally:
            self._batch_delivery.reset(token)

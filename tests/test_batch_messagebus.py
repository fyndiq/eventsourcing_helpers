from contextlib import nullcontext
from unittest.mock import Mock

import pytest

from eventsourcing_helpers.consumer import Consumer
from eventsourcing_helpers.handler import Handler
from eventsourcing_helpers.messagebus import MessageBus
from eventsourcing_helpers.messagebus.backends.kafka import KafkaAvroBackend

from confluent_kafka_helpers.exceptions import KafkaDeliveryError
from confluent_kafka_helpers.producer import AvroProducer


class Producer(AvroProducer):
    def __init__(self, sequence):
        self.sequence = sequence
        self.callbacks = []
        self.capacity = 1
        self.error = None
        self.flush_result = 0
        self.complete_at_flush = True
        self._delivery_callback = Mock()
        self._active_delivery_scope = None
        self._delivery_report_only_error = False

    def __len__(self):
        return len(self.callbacks)

    def _produce_once(self, value, *_args, on_delivery=None, **kwargs):
        if len(self.callbacks) >= self.capacity:
            self.sequence.append("full")
            raise BufferError
        self.sequence.append(value)
        self.callbacks.append(on_delivery)

    def poll(self, timeout):
        self.sequence.append("poll")
        if self.callbacks:
            callback = self.callbacks.pop(0)
            if callback:
                callback(self.error, "delivered")

    def flush(self, timeout=None):
        self.sequence.append("flush")
        if self.complete_at_flush:
            while self.callbacks:
                self.poll(0)
        return self.flush_result


@pytest.fixture
def setup_batch():
    sequence = []
    producer = Producer(sequence)
    config = {"backend_config": {"producer": {"flush": True}}}
    bus = MessageBus(config, producer=Mock(return_value=producer))
    backend = bus.backend
    consumer = Mock()
    consumer.commit_batch.side_effect = lambda batch: sequence.append(("commit", batch))
    backend.get_batch_consumer = Mock(return_value=lambda: nullcontext(consumer))
    backend.offset_watchdog = Mock()
    backend.offset_watchdog.seen.return_value = False
    backend.offset_watchdog.set_seen.side_effect = lambda msg: sequence.append(("seen", msg))

    def fetch(*batches):
        consumer.__iter__ = Mock(return_value=iter(batches))

    return bus, producer, backend.offset_watchdog, consumer, sequence, fetch


def test_delivery_from_early_poll_and_final_barrier_precede_ack(setup_batch):
    bus, producer, watchdog, consumer, sequence, fetch = setup_batch
    first, second, seen = Mock(), Mock(), Mock()
    watchdog.seen.side_effect = lambda msg: msg is seen
    original = [first, seen, second]
    fetch(original, [seen])
    per_record_callback = Mock()

    def handler(batch):
        assert batch == [first, second]
        bus.produce("initial", on_delivery=per_record_callback)
        bus.produce_many([{"value": "bulk", "source": second}])
        sequence.append("handler returned")

    bus.consume_batch(handler, batch_size=3)
    assert sequence == [
        "initial",
        "full",
        "poll",
        "bulk",
        "handler returned",
        "flush",
        "poll",
        ("seen", first),
        ("seen", second),
        ("commit", original),
        ("commit", [seen]),
    ]
    per_record_callback.assert_called_once_with(None, "delivered")
    assert producer._delivery_callback.call_count == 2


@pytest.mark.parametrize(
    "failure,expected",
    [
        ("handler", RuntimeError),
        ("delivery", KafkaDeliveryError),
        ("early delivery", KafkaDeliveryError),
        ("configured callback", ValueError),
        ("per-record callback", ValueError),
        ("outstanding", TimeoutError),
        ("missing callback", TimeoutError),
        ("queue timeout", TimeoutError),
    ],
)
def test_batch_does_not_ack_on_failure(setup_batch, failure, expected):
    bus, producer, watchdog, consumer, sequence, fetch = setup_batch
    original = [Mock()]
    fetch(original, [Mock()])
    callback = None
    if failure in {"delivery", "early delivery"}:
        producer.error = "delivery rejected"
    elif failure == "configured callback":
        producer._delivery_callback.side_effect = ValueError("configured callback failed")
    elif failure == "per-record callback":
        callback = Mock(side_effect=ValueError("per-record callback failed"))
    elif failure == "outstanding":
        producer.flush_result = 1
    elif failure == "missing callback":
        producer.complete_at_flush = False
    elif failure == "queue timeout":
        producer.capacity = 0

    def handler(batch):
        assert batch == original
        if failure == "handler":
            raise RuntimeError("handler failed")
        bus.produce("output", on_delivery=callback)
        if failure == "early delivery":
            bus.produce_many([{"value": "second"}])

    timeout = 0.005 if failure == "queue timeout" else 30.0
    with pytest.raises(expected):
        bus.consume_batch(handler, delivery_timeout=timeout)
    watchdog.set_seen.assert_not_called()
    consumer.commit_batch.assert_not_called()


def test_batch_replays_after_delivery_error_then_skips_acknowledged_messages(setup_batch):
    bus, producer, watchdog, consumer, _, fetch = setup_batch
    batch = [Mock(), Mock()]
    handled = []
    watchdog.seen.side_effect = lambda message: message in handled
    watchdog.set_seen.side_effect = handled.append
    producer.error = "not delivered"

    def handler(records):
        assert records == batch
        bus.produce("output")

    fetch(batch)
    with pytest.raises(KafkaDeliveryError):
        bus.consume_batch(handler)
    producer.error = None
    fetch(batch, batch)
    bus.consume_batch(handler)
    assert handled == batch
    assert consumer.commit_batch.call_count == 2


def test_commit_error_stops_batch_iteration(setup_batch):
    bus, _, _, consumer, _, fetch = setup_batch
    fetch([Mock()], [Mock()])
    consumer.commit_batch.side_effect = RuntimeError("commit failed")
    handler = Mock()
    with pytest.raises(RuntimeError, match="commit failed"):
        bus.consume_batch(handler)
    handler.assert_called_once()


def test_watchdog_update_failure_does_not_block_input_commit(setup_batch):
    bus, _, watchdog, consumer, _, fetch = setup_batch
    batch = [Mock(), Mock()]
    fetch(batch)
    watchdog.set_seen.side_effect = [RuntimeError("watchdog unavailable"), None]
    bus.consume_batch(lambda _: bus.produce("output"))
    assert watchdog.set_seen.call_count == 2
    consumer.commit_batch.assert_called_once_with(batch)


def test_default_batch_handler_keeps_context_and_failed_batch_replays():
    bus = MessageBus(
        {
            "backend": "eventsourcing_helpers.messagebus.backends.mock.backend.MockBackend",
            "backend_config": {},
        }
    )

    class SequentialHandler(Handler):
        handlers = {"Example": Mock()}
        fail = True

        def handle(self, message):
            index = message.value["data"]["id"]
            if index == 1 and self.fail:
                raise RuntimeError("handler failed")
            bus.produce({"id": index})

    for index in range(3):
        bus.backend.consumer.add_message(
            key=str(index),
            value={"class": "Example", "data": {"id": index}},
            headers={"origin_action_type": str(index)},
        )

    handler = SequentialHandler()
    consumer = Consumer(bus, handler)
    with pytest.raises(RuntimeError, match="handler failed"):
        consumer.consume_batch()
    assert len(bus.backend.consumer.messages) == 3
    handler.fail = False
    consumer.consume_batch()
    assert [
        (event["value"]["id"], event["headers"]["origin_action_type"])
        for event in bus.backend.producer.messages
    ] == [(0, "0"), (0, "0"), (1, "1"), (2, "2")]
    assert not bus.backend.consumer.messages


def test_bulk_flushing_outside_consumer_uses_config_once():
    sequence = []
    producer = Producer(sequence)
    backend = KafkaAvroBackend({"producer": {"flush": True}}, producer=Mock(return_value=producer))
    producer.capacity = 3
    backend.produce_many([{"value": "one"}, {"value": "two"}])
    assert sequence == ["one", "two", "poll", "flush", "poll"]


@pytest.mark.parametrize("backend", ["mock", "mock_compat"])
def test_mock_batches_preserve_dto_metadata_and_batch_order(backend):
    module = "backend" if backend == "mock" else "backend_compat"
    path = f"eventsourcing_helpers.messagebus.backends.mock.{module}"
    config = {"backend": f"{path}.MockBackend", "backend_config": {}}
    bus = MessageBus(config)

    class BatchHandler(Handler):
        handlers = {"Example": Mock()}

        def __init__(self):
            super().__init__()
            self.batches = []

        def handle_batch(self, messages):
            commands = [self.message_deserializer(message) for message in messages]
            self.batches.append([command.id for command in commands])
            for command, source in zip(commands, messages):
                assert command.Meta.headers["origin_action_type"] == str(command.id)
                bus.produce_many([{"value": {"id": command.id}, "source": source}])
                with source.process():
                    bus.produce({"fallback": command.id})

    for number in range(3):
        value = {"class": "Example", "data": {"id": number}}
        headers = {"origin_action_type": str(number)}
        if backend == "mock":
            bus.backend.consumer.add_message(key=str(number), value=value, headers=headers)
        else:
            bus.backend.consumer.add_message("Example", value["data"], headers)

    handler = BatchHandler()
    Consumer(bus, handler).consume_batch(batch_size=2)
    assert handler.batches == [[0, 1], [2]]
    assert [
        message["headers"]["origin_action_type"] for message in bus.backend.producer.messages
    ] == ["0", "0", "1", "1", "2", "2"]
    assert not bus.backend.consumer.messages


@pytest.mark.parametrize("backend", ["mock", "mock_compat"])
def test_mock_commit_only_acknowledges_offsets_in_the_committed_partition(backend):
    module = "backend" if backend == "mock" else "backend_compat"
    bus = MessageBus(
        {
            "backend": f"eventsourcing_helpers.messagebus.backends.mock.{module}.MockBackend",
            "backend_config": {},
        }
    )
    for number, partition in enumerate([0, 1, 0, 1]):
        data = {"id": number}
        if backend == "mock":
            value = {"class": "Example", "data": data}
            bus.backend.consumer.add_message(str(number), value, "commands", partition=partition)
        else:
            bus.backend.consumer.add_message("Example", data, topic="commands", partition=partition)

    consumer_factory = bus.get_batch_consumer(batch_size=3)
    with consumer_factory() as consumer:
        batch = next(consumer)
        assert [(m._meta.partition, m._meta.offset) for m in batch] == [(0, 0), (1, 0), (0, 1)]
        consumer.commit_batch([batch[2]])
        assert [m.value["data"]["id"] for m in next(consumer)] == [3]
    with consumer_factory() as consumer:
        replayed = next(consumer)
        assert [m.value["data"]["id"] for m in replayed] == [1, 3]
        consumer.commit_batch(replayed)
    assert not bus.backend.consumer.messages

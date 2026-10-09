# Event sourcing helpers
[![build](https://circleci.com/gh/fyndiq/eventsourcing_helpers/tree/master.svg?style=shield)](https://circleci.com/gh/fyndiq/eventsourcing_helpers/tree/master)
[![coverage](https://codecov.io/gh/fyndiq/eventsourcing_helpers/branch/master/graph/badge.svg)](https://codecov.io/gh/fyndiq/eventsourcing_helpers)
[![version](https://img.shields.io/pypi/v/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)
[![downloads](https://img.shields.io/pypi/dm/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)
[![license](https://img.shields.io/pypi/l/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)

A Python library for practicing the event sourcing pattern using DDD.

## Installation

    pip install .
    pip install -r requirements.txt

## Batch consumption

`consume_batches(handler)` is available on `KafkaAvroBackend`, `MessageBus`, and
`eventsourcing_helpers.consumer.Consumer`.

```python
def handle_batch(messages):
    bulk_write([message.value for message in messages])


messagebus.consume_batches(handle_batch)
```

Kafka batch size and wait time come from the consumer config keys supported by
`confluent-kafka-helpers>=1.5.0`:

```python
{
    "consumer": {
        "batch_max_size": 100,  # default
        "batch_max_wait": 1.0,  # seconds, default
    }
}
```

Existing `EventHandler` and `CommandHandler` instances can be used with
`consume_batches`; each message is handled individually, then the backend commits
once for the whole batch.

Batch handlers that receive `list[Message]` can opt in to per-message propagated
headers and handler-span links:

```python
from eventsourcing_helpers.messagebus.backends.kafka import message_handler_context


def handle_batch(messages):
    for message in messages:
        with message_handler_context(message, propagate_header_keys=["x-request-id"]):
            handle_one(message)
```

If handling raises, the batch is not committed and the exception propagates.
Kafka may redeliver any message in that batch, so batch handlers must be
idempotent. The whole batch must finish before `max.poll.interval.ms`, and
OpenTelemetry keeps at most 128 span links by default; raise
`OTEL_SPAN_LINK_COUNT_LIMIT` if `batch_max_size` is higher than that.

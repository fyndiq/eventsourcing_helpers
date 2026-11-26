# Event sourcing helpers
[![CI](https://github.com/fyndiq/eventsourcing_helpers/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/fyndiq/eventsourcing_helpers/actions/workflows/ci.yml)
[![coverage](https://codecov.io/gh/fyndiq/eventsourcing_helpers/branch/master/graph/badge.svg)](https://codecov.io/gh/fyndiq/eventsourcing_helpers)
[![version](https://img.shields.io/pypi/v/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)
[![downloads](https://img.shields.io/pypi/dm/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)
[![license](https://img.shields.io/pypi/l/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)

A Python library for practicing the event sourcing pattern using DDD.

## Installation

Python 3.11 or newer is required.

```sh
uv venv
uv pip install --index https://pypi.org/simple eventsourcing-helpers
```

See [Releases](docs/releases.md) for installation from qlty, prereleases, and
publication recovery.

## Development

Use Python 3.11 and uv 0.13.1 for development. Run `just setup` to install the
locked development and optional dependencies, then `just test`. Run `just build` to build a wheel
and source distribution. `just fix` formats library code and tests;
`just pip-update` updates the lockfile and environment.

Use Conventional Commit messages. Package changes need `fix:`, `feat:`, or a
breaking-change marker so Commitizen can calculate the next version. CI validates
all introduced commit messages, builds distributions, and previews the release.
Merges to `master` publish through the [Release workflow](docs/releases.md).
Direct uploads through `just publish` are disabled.

## Kafka offset commits

For single-message consumption (`consume`), the Kafka backend marks a message as consumed
only after its handler returns. How depends on
`enable.auto.commit` in the consumer config:

- `False` (default): the offset is committed synchronously after each handled message.
- `True`: the offset is stored with `store_offsets()` after each handled message, and librdkafka
  commits stored offsets in the background. **You must also set
  `"enable.auto.offset.store": False`.** Otherwise librdkafka stores each offset as soon as the
  message is polled, before the handler runs, so a crash or shutdown can commit messages that
  were never handled.

If the handler produces Kafka events, also set the producer `flush` option to `True`. In the
auto-commit case above, the offset is stored after the handler and committed in the background.
Without a flush, output events may still be in the producer's memory when that offset is stored. If
the process hard-crashes then, the input message counts as done but its output events are lost.
Flushing first makes the broker acknowledge the output events before the offset is stored, so a crash
can only replay the input message.

```python
"backend_config": {
    "producer": {
        "flush": True,
    },
    "consumer": {
        "enable.auto.commit": True,
        "enable.auto.offset.store": False,
    },
}
```

## Batch consumption

`consume_batches(handler, batch_size=100, timeout=0.1, delivery_timeout=30.0)` is
available on `KafkaAvroBackend` and `MessageBus`. It uses the separate
[`AvroBatchConsumer`](https://github.com/fyndiq/confluent_kafka_helpers/pull/110).
The batch APIs are available in the pinned `confluent-kafka-helpers==1.8.0a4`
alpha release.

```python
def handle_batch(messages):
    bulk_write([message.value for message in messages])
    messagebus.produce_many(
        {"value": build_event(message), "source": message}
        for message in messages
    )


messagebus.consume_batches(handle_batch, batch_size=100, timeout=0.1, delivery_timeout=30.0)
```

Pass size and timeout as method arguments, not `batch_max_size` / `batch_max_wait`
consumer settings. Size must be an integer from 1 to 1,000,000; the read timeout
must be finite and nonnegative. Batch consumers disable automatic commits **and**
automatic offset storage, rejecting settings that enable either. Single-message
APIs retain their existing behaviour.

One handler receives a `list[Message]` per nonempty batch of messages that need
processing. The backend treats a normal return as success for every message
passed to the handler. If any message fails, raise instead of returning normally:
the exception propagates without advancing offsets for that batch. Kafka may
redeliver messages whose processing already succeeded, so batch handlers must be
idempotent.

The backend filters watchdog-seen input without changing the remaining order.
After successful handling, it confirms all output, marks handled records in the
watchdog and synchronously commits the original batch's explicit partition offsets.
An all-seen batch commits without invoking the handler. Handler, delivery or callback
failures and delivery timeouts prevent watchdog advancement and input commits.
Commit and assignment failures propagate and stop consumption for replay.

Every publication on the configured producer is tracked during a batch, including
ordinary `messagebus.produce()` calls from fallback handlers. The backend suppresses
per-record flushes even when `producer.flush=True`, then confirms output once.
The delivery timeout must be finite and positive; it starts before the handler and
bounds queue waits and final delivery confirmation, but cannot interrupt application
work or schema-registry requests. Join workers before the handler returns. Do not
share the producer with unrelated work while handling a batch.

`produce_many(records, timeout=30.0)` accepts ordinary production arguments and an
optional `source=message` per record. It preserves source propagation headers and
trace context; explicit business headers win. Without a source, output has no
implicit input provenance. Outside batch consumption, it only queues output and
honours the existing `flush` setting once per collection.

For existing per-message handlers, explicitly preserve order and activate each
input's processing span and headers:

```python
def handle_batch(messages):
    for message in messages:
        with message.process():
            existing_handler(message)
```

`eventsourcing_helpers.consumer.Consumer.consume_batches(...)` calls
`Handler.handle_batch(messages)`. Implement it explicitly; the default raises
`NotImplementedError`. It never automatically groups commands or dispatches them
through the single-message handler. Both mock backends support batch options,
collection publishing and source headers; use `consume_message_batches` in tests.

For custom loops, `messagebus.get_batch_consumer(batch_size=100, timeout=0.1)`
returns a context-managed consumer factory. Direct callers own output confirmation
and `consumer.commit_batch(messages)`. Commit only successful prefixes per
partition: acknowledging a later offset also acknowledges earlier records.

Kafka guarantees ordering within a partition, not across partitions or topics.
Applications must preserve that order when applying changes. Processing is at
least once: database writes and output can succeed before an input commit fails.
Make both replay-safe. Producer idempotence does not deduplicate application replay
or make database writes and Kafka publication atomic.

The whole batch must finish before `max.poll.interval.ms`, and
OpenTelemetry keeps at most 128 span links by default; raise
`OTEL_SPAN_LINK_COUNT_LIMIT` if `batch_size` is higher than that.

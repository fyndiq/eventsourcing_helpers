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

Use Python 3.11 and uv 0.13.1 for development. Run `make setup` to install the
locked development and optional dependencies, then `make test`. Run `make build` to build a wheel
and source distribution. `make fix` formats library code and tests;
`make pip-update` updates the lockfile and environment.

Use Conventional Commit messages. Package changes need `fix:`, `feat:`, or a
breaking-change marker so Commitizen can calculate the next version. CI validates
all introduced commit messages, builds distributions, and previews the release.
Merges to `master` publish through the [Release workflow](docs/releases.md).
Direct uploads through `make publish` are disabled.

## Kafka offset commits

The Kafka backend marks a message as consumed only after its handler returns. How depends on
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

`consume_batches(handler)` is available on `KafkaAvroBackend`, `MessageBus`, and
`eventsourcing_helpers.consumer.Consumer`.

```python
def handle_batch(messages):
    bulk_write([message.value for message in messages])


messagebus.consume_batches(handle_batch)
```

Kafka batch consumption requires a release of `confluent-kafka-helpers` that
includes [`AvroConsumer.batches()`](https://github.com/fyndiq/confluent_kafka_helpers/pull/110).
Batch size and wait time come from these consumer config keys:

```python
{
    "consumer": {
        "batch_max_size": 100,  # default
        "batch_max_wait": 1.0,  # seconds, default
    }
}
```

One handler receives a `list[Message]` per nonempty batch of messages that need
processing. The backend treats a normal return as success for every message
passed to the handler. If any message fails, raise instead of returning normally:
the exception propagates without advancing offsets for that batch. Kafka may
redeliver messages whose processing already succeeded, so batch handlers must be
idempotent.

With `enable.auto.commit` enabled, set `enable.auto.offset.store` to `false` so
polling doesn't advance offsets before processing. The backend stores offsets
after the handler returns so Kafka can commit them later. With `enable.auto.commit`
disabled, leave `enable.auto.offset.store` enabled: polling tracks the offsets
locally and the backend commits them synchronously after the handler returns.

The whole batch must finish before `max.poll.interval.ms`, and
OpenTelemetry keeps at most 128 span links by default; raise
`OTEL_SPAN_LINK_COUNT_LIMIT` if `batch_max_size` is higher than that.

### Concurrent batch handling

Use `BatchConsumer` when one fixed Kafka batch should be processed with
concurrency while preserving ordering for messages that share a key:

```python
from eventsourcing_helpers.concurrent_batch import (
    ConcurrentBatchConfig,
    SignalShutdownObserver,
)
from eventsourcing_helpers.consumer import BatchConsumer


consumer = BatchConsumer(
    messagebus=messagebus,
    handler=event_handler.handle,
    key=lambda message: message.value["data"]["merchant_id"],
    config=ConcurrentBatchConfig(
        max_workers=8,
        drain_timeout=25.0,
    ),
    shutdown_observer=SignalShutdownObserver(),
)
consumer.consume()
```

`BatchConsumer` still uses the existing `consume_batches()` API. It wraps the
application's per-message handler in a `ConcurrentBatchHandler`, groups a fixed
batch by `key`, and handles one group sequentially in each worker. Messages with
different keys may run concurrently, up to `max_workers`. If `key` is omitted,
the Kafka message key from `message._meta.key` is used.

`ConcurrentBatchConfig` has these keys:

- `max_workers`: maximum number of key groups processed at the same time; the
  default is `1`
- `drain_timeout`: seconds to wait for already-running handlers after shutdown
  is observed; the default is `30.0`

On handler failure, shutdown before scheduling, queued work that cannot start, or
a drain timeout, `BatchConsumer` raises instead of returning normally. The Kafka
backend then leaves the whole batch uncommitted and does not advance the offset
watchdog for any message in the batch. Kafka redelivery is expected, including
for messages whose handlers already returned successfully, so handlers must be
idempotent.

`SignalShutdownObserver` is the current shutdown seam. It records SIGTERM inside
eventsourcing-helpers so queued messages stop starting and partial batches
returned during shutdown are not scheduled. Kafka polling and consumer close
remain the responsibility of confluent-kafka-helpers; when that library exposes a
dedicated shutdown state, pass an observer backed by that API instead of the
signal observer.

Thread cancellation is best-effort. Cancelling a `Future` only prevents work that
has not started. It does not stop a handler that is already running, and
`ThreadPoolExecutor.shutdown(wait=False)` does not kill running threads or
guarantee immediate process exit. Keep outbound HTTP/OpenSearch timeouts and
retry budgets below `drain_timeout`, and set Kubernetes
`terminationGracePeriodSeconds` high enough to cover Kafka shutdown, the drain
period, and application cleanup.

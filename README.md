# Event sourcing helpers
[![CI](https://github.com/fyndiq/eventsourcing_helpers/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/fyndiq/eventsourcing_helpers/actions/workflows/ci.yml)
[![coverage](https://codecov.io/gh/fyndiq/eventsourcing_helpers/branch/master/graph/badge.svg)](https://codecov.io/gh/fyndiq/eventsourcing_helpers)
[![version](https://img.shields.io/pypi/v/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)
[![downloads](https://img.shields.io/pypi/dm/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)
[![license](https://img.shields.io/pypi/l/eventsourcing-helpers.svg)](https://pypi.org/project/eventsourcing-helpers/)

A Python library for practicing the event sourcing pattern using DDD.

## Installation

    pip install .
    pip install -r requirements.txt

## Flush produced events before saving a Kafka consumer offset

If a Kafka consumer handles a message and produces new events, the producer may keep those
events in its local buffer for a short time. When the consumer stores or commits its offset
before that buffer reaches Kafka, a hard crash can lose the produced events: Kafka thinks the
input message is done, but the output events never arrived.

Set `flush_producer_before_offset_store` to `True` in the Kafka messagebus backend config to
flush the producer once after the handler succeeds and before the consumer offset is stored or
committed:

```python
{
    "backend": "kafka_avro",
    "backend_config": {
        "flush_producer_before_offset_store": True,
        "producer": {
            # producer config
        },
        "consumer": {
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            # consumer config
        },
    },
}
```

This is useful with async commits, especially when `enable.auto.commit` is `True` and
`enable.auto.offset.store` is `False`. Prefer this option over producer `"flush": True`: producer
`"flush": True` flushes after every produce call, while this option flushes only once after the
message handler finishes.

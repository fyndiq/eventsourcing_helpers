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

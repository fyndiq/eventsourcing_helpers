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

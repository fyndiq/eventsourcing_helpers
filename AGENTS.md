# Agent guidelines

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`eventsourcing_helpers` is a Python library (PyPI: `eventsourcing-helpers`) for practicing the Event Sourcing pattern with Domain-Driven Design. It is a library, not an application — it provides building blocks (aggregate roots, command/event handlers, repositories, a message bus) that consuming services wire together. Kafka (via `confluent-kafka-helpers`) is the default backend for both the event store and the message bus.

## Commands

All common tasks go through the `Makefile`, which delegates to `scripts/*.sh`:

- `make setup` — create `.venv` and install deps + editable package with all extras
- `make test` — runs `unit-test` then `lint` (the full CI gate)
- `make unit-test` — `pytest tests/` with coverage against `eventsourcing_helpers/`
- `make lint` — `flake8` then `mypy eventsourcing_helpers/`
- `make fix` — auto-format with `isort` then `black` (line length 100)
- `make build` / `make publish` — build wheel / upload to PyPI via twine

Run a single test:

```sh
pytest tests/repository/test_repository.py
pytest tests/repository/test_repository.py -k "test_load"
```

CI (`.circleci/config.yml`) runs on Python 3.10, installs `.[mongo,redis,cnamedtuple]`, then `make lint` and `make unit-test`. Note: CI does **not** install the `pydantic` extra, but `make setup` does.

## Architecture

The library models the standard event-sourcing write/read flow. Data moves as **DTOs** (deserialized messages) between these layers:

**Message flow (consume side):** `Consumer` → `MessageBus` (backend) → `Handler.handle` → domain handler. A `Consumer` couples a `MessageBus` with a `Handler` and loops via `messagebus.consume(handler)`.

**Handlers** (`handler.py`, `command_handler.py`, `event_handler.py`) — all subclass `Handler` and dispatch on the message's `class` field via a `handlers` dict (keyed by command/event class name or class object):
- `CommandHandler` — dispatches a command to a registered callable.
- `ESCommandHandler` — event-sourced: loads the `AggregateRoot` from the `Repository`, runs the command method *on* the aggregate, then commits staged events. On handler exception it deletes the snapshot (cache invalidation) and re-raises.
- `EventHandler` — application service that routes events to domain handlers; supports optional per-handler deserialization classes.

**Domain models** (`models.py`):
- `Entity` — base domain object with identity (`id`) and `_version`. Events are applied via convention: event class `FooCreated` → method `apply_foo_created`. **Critical gotcha:** `Entity._events` is a *class-level shared list* — all staged events across all entity instances accumulate there and are cleared globally via `_clear_staged_events()`. `apply_event(event, is_new=True)` applies and stages; `is_new=False` (used when replaying from storage) applies without staging.
- `EntityDict` — dict of child entities for fast keyed lookup; participates in the entity-tree traversal (`_get_all_entities`).
- `AggregateRoot` — the entity that acts as the façade/gateway; all commands must go through it.

**Repository** (`repository/__init__.py`) — mediates between domain and storage. `load(id)` tries the snapshot store first, falling back to replaying events from the backend (applying each with `is_new=False`). `commit(aggregate_root)` saves a snapshot, writes events to the backend, and clears staged events — rolling back the snapshot if the Kafka commit fails. `AggregateBuilder` (`builder.py`) rebuilds an aggregate up to a `max_offset` (for read-side/projection rebuilds), tolerating missing apply methods.

**Pluggable backends** — `MessageBus`, `Repository`, and `Snapshot` each resolve a backend by string path from a `BACKENDS` dict + config, imported dynamically via `utils.import_backend`. Adding a backend = implement the interface and register its dotted path.
- Repository event store: `kafka_avro` (default).
- Message bus: `kafka_avro` (default), `mock`, `mock_compat`.
- Snapshot: `null` (default, no-op), plus `mongo`/`redis` backends (optional extras).

**Serialization** (`serializers.py`, `message/`) — `from_message_to_dto` / `to_message_from_dto` convert between Kafka messages and DTOs. Messages serialize as `{"class": <name>, "data": {...}}`. Messages are immutable proxy objects (`message/message.py`): `NewMessage` raises on missing attributes, while `OldMessage` (loaded from the store) returns `None` for missing attributes so apply methods stay forward-compatible as Avro schemas gain fields. DTOs are `namedtuple`-based by default; `cnamedtuple` is used if installed. Alternatively, define messages with `PydanticMixin` (`message/pydantic.py`, requires `pydantic>=2`).

**Metrics & tracing** — `metrics.py` exposes a Datadog statsd client that is a no-op `StatsdNullClient` unless `DATADOG_ENABLE_METRICS=1`. `tracing/` wraps `confluent_kafka_helpers`' OpenTelemetry backend; handlers create spans around message handling.

## Testing

- `setup.cfg` registers `python_classes = *Tests`, so test classes must end in `Tests`. Tests live in `tests/` mirroring the package layout.
- The package ships a **pytest plugin** (`entry_points["pytest11"]` → `messagebus/backends/mock/test.py`). Consuming projects get `consumer`, `messagebus`, and `consume_messages` fixtures; the `consumer` fixture must be overridden to return the app's `Consumer`. Use the `mock`/`mock_compat` message bus backends in tests.
- See `MIGRATION.md` for the 1.x→2.x changes (mock backend path moved to `backend_compat`, `PydanticMixin` import moved).

## Conventions

- Format with `black` + `isort` (black profile), line length 100, enforced by `flake8` (with `flake8-black`, `flake8-isort`, `flake8-eradicate`). `mypy` runs with the pydantic plugin.
- Releases: bump `version` in `setup.py`, follow `docs/release.md`.

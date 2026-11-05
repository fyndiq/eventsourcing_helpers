#!/usr/bin/env sh
set -eu
uv run --locked flake8
uv run --locked mypy eventsourcing_helpers/

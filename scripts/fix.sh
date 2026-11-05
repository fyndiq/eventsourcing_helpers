#!/usr/bin/env sh
set -eu
uv run --locked isort eventsourcing_helpers/ tests/
uv run --locked black eventsourcing_helpers/ tests/

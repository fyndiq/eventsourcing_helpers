#!/usr/bin/env sh
set -eu
uv run --locked pytest tests/ --cov=eventsourcing_helpers/ --junitxml=/tmp/test-results/report.xml --no-cov-on-fail --cov-report term-missing

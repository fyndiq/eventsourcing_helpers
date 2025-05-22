#!/usr/bin/env sh
set -e
flake8
mypy eventsourcing_helpers/

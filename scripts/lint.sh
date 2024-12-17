#!/usr/bin/env bash
set -e
flake8
mypy eventsourcing_helpers/

#!/bin/bash
set -e
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate

command -v flake8 >/dev/null 2>&1 || echo "flake8 is required"
command -v mypy >/dev/null 2>&1 || echo "mypy is required"

echo "Running flake8..."
flake8

echo "Running mypy..."
mypy eventsourcing_helpers/

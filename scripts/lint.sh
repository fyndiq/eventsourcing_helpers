#!/bin/bash
set -e
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
flake8
mypy eventsourcing_helpers/

#!/usr/bin/env sh
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
mypy eventsourcing_helpers/ --ignore-missing-imports --show-error-context

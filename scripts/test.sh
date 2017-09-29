#!/usr/bin/env sh
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
pytest --cov=eventsourcing_helpers/

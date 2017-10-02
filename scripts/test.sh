#!/usr/bin/env sh
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
pytest --cov=eventsourcing_helpers/

if [ "$1" == "ci" ]; then
    codecov -t $CODECOV_TOKEN
fi

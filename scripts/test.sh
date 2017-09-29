#!/usr/bin/env sh
[[ -z "${VIRTUAL_ENV}" ]] && . .venv/bin/activate
pytest --cov=eventsourcing_helpers/

if [ "$1" == "ci" ]; then
    COVERALLS_REPO_TOKEN=$COVERALLS_REPO_TOKEN coveralls
fi

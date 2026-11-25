#!/usr/bin/env sh
set -eu
uv sync --locked --all-extras
uv run pre-commit install --hook-type pre-push
uv run pre-commit install --hook-type commit-msg

#!/usr/bin/env sh
set -eu
uv lock --upgrade
uv sync --locked --all-extras

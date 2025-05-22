#!/usr/bin/env sh
set -e
rm -rf dist/* build/*
python -m build -w

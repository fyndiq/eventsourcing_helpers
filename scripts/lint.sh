#!/bin/bash
set -e
flake8
mypy eventsourcing_helpers/

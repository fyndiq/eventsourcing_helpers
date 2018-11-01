#!/bin/bash
virtualenv -p python3 .venv
source .venv/bin/activate
pip install --process-dependency-links --no-cache-dir -r requirements/dev.txt

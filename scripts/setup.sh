#!/usr/bin/env sh
set -e
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install '.[mongo,redis,cnamedtuple]'

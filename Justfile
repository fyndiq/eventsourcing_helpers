default:
    @just --list

setup:
    @./scripts/setup.sh

lint:
    @./scripts/lint.sh

fix:
    @./scripts/fix.sh

unit-test:
    @./scripts/unit-test.sh

test: lint unit-test

build:
    @./scripts/build.sh

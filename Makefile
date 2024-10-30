setup:
	./scripts/setup.sh

lint:
	./scripts/lint.sh

unit-test:
	./scripts/unit-test.sh

build:
	./scripts/build.sh

publish:
	./scripts/publish.sh

pip-update:
	./scripts/pip-update.sh

test: unit-test lint

.PHONY: test build

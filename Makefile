setup:
	./scripts/setup.sh

lint:
	./scripts/lint.sh

unit-test:
	./scripts/unit-test.sh

unit-test-ci:
	./scripts/unit-test.sh ci

publish:
	./scripts/publish.sh

pip-update:
	./scripts/pip-update.sh

test: unit-test lint

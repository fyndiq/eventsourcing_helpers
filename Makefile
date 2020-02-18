setup:
	./scripts/setup.sh

lint:
	./scripts/lint.sh

test: lint
	./scripts/test.sh

test-ci:
	./scripts/test.sh ci

publish:
	./scripts/publish.sh

pip-update:
	pip-compile -U requirements.in --output-file requirements.txt

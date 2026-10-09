# Lists all available commands.
default:
    @just --list

# Sets up the local development environment.
setup:
    ./scripts/setup.sh

# Lints the codebase.
lint:
    ./scripts/lint.sh

# Runs the unit tests.
unit-test:
    ./scripts/unit-test.sh

# Builds the package.
build:
    ./scripts/build.sh

# Publishes the package.
publish:
    ./scripts/publish.sh

# Updates pinned dependencies.
pip-update:
    ./scripts/pip-update.sh

# Fixes formatting issues.
fix:
    ./scripts/fix.sh

# Runs the full test suite.
test: unit-test lint

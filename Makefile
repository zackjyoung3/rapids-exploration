# Variables
POETRY = poetry
BLACK = black
FLAKE8 = flake8
MYPY = mypy
SRC_DIR = rapids_exploration
TEST_DIR = tests
VENV := $(shell $(POETRY) env info -p)

# Default target
.PHONY: help
help:
	@echo "Makefile commands:"
	@echo "  make install            - Install production dependencies only"
	@echo "  make install-dev        - Install production + development dependencies"
	@echo "  make install-test       - Install production + test dependencies"
	@echo "  make install-all        - Install all dependencies (dev + test)"
	@echo "  make update             - Update dependencies"
	@echo "  make run                - Run the application"
	@echo "  make test               - Run tests"
	@echo "  make format             - Format code with black"
	@echo "  make lint               - Run flake8 for linting"
	@echo "  make check              - Check formatting and linting"
	@echo "  make type-check         - Run mypy for type checking"
	@echo "  make clean              - Clean up generated files"
	@echo "  make requirements       - Export dependencies to requirements.txt"

# Install dependencies
.PHONY: install
install:
	set -x; $(POETRY) install

# Install production + development dependencies
.PHONY: install-dev
install-dev:
	$(POETRY) install --with dev 

# Install production + test dependencies
.PHONY: install-test
install-test:
	$(POETRY) install --with test 

# Install all dependencies (dev + test)
.PHONY: install-all
install-all:
	$(POETRY) install --with dev,test 

# Update dependencies
.PHONY: update
update:
	set -x; $(POETRY) update

# Run tests
.PHONY: test
test:
	set -x; $(VENV)/bin/pytest $(TEST_DIR)

# Format code with black
.PHONY: format
format:
	set -x; $(BLACK) $(SRC_DIR) $(TEST_DIR)

# Lint code with flake8
.PHONY: lint
lint:
	set -x; $(FLAKE8) $(SRC_DIR) $(TEST_DIR)

# Check code formatting and linting
.PHONY: check
check: lint
	set -x; $(BLACK) --check $(SRC_DIR) $(TEST_DIR)

# Run mypy for type checking
.PHONY: type-check
type-check:
	set -x; $(MYPY) $(SRC_DIR)

# Clean up cache and build artifacts
.PHONY: clean
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Export dependencies to requirements.txt
.PHONY: requirements
requirements:
	set -x; $(POETRY) export -f requirements.txt --output requirements.txt --without-hashes
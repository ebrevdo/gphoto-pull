set positional-arguments

default:
    @just --list

sync:
    uv sync --dev

format:
    uv run ruff format .

fmt: format

format-check:
    uv run ruff format --check .

lint: format-check
    uv run ruff check .
    uv run pyright

test *args:
    uv run pytest "$@"

clean-dist:
    rm -rf dist

build: clean-dist
    uv build

check: lint test build

publish-dry-run: build
    UV_PUBLISH_TOKEN='op://Private/gphoto-pull pypi API token/password' op run -- uv publish --dry-run --trusted-publishing never dist/*

# DANGEROUS
publish: build
    UV_PUBLISH_TOKEN='op://Private/gphoto-pull pypi API token/password' op run -- uv publish --trusted-publishing never dist/*

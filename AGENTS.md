# Repository Guidelines

## Project Structure & Module Organization

This repository uses a `src/` layout. Application code lives in `src/gphoto_pull/`:

- `cli.py` exposes the `gphoto-pull` entrypoint.
- `config.py` loads optional TOML config and CLI overrides.
- `browser.py` launches the app-owned persistent Playwright profile.
- `automation.py` holds the Google Photos workflow.
- `download.py`, `enumeration.py`, `rpc_payloads.py`, and `state.py` handle download
  targets, media discovery, Google Photos payload parsing, and resumable SQLite state.

Tests live in `tests/` and run under `pytest`. Generated or local-only paths include `.venv/`, `.playwright/`, `.state/`, `downloads/`, `dist/`, and `gphoto-pull.toml`; do not commit their contents. Default runtime data lives under `~/.local/state/gphoto-pull/`.

## Build, Test, and Development Commands

- `uv sync --dev` installs runtime and dev dependencies into `.venv`.
- `uv run gphoto-pull install-browser` installs browser binaries for the selected config directory.
- `uv run gphoto-pull config` writes a local `gphoto-pull.toml` interactively.
- `uv run gphoto-pull doctor` checks local config and prerequisites.
- `uv run gphoto-pull login` opens the persistent browser profile for manual Google login.
- `uv run gphoto-pull pull --after 2026-01-01T00:00:00-08:00` runs a pull for a date window.
- `just lint` runs Ruff format check, Ruff lint, and Pyright.
- `just test` runs pytest. Extra args pass through, e.g. `just test tests/test_config.py`.
- `just check` runs lint, tests, and `uv build`.
- `uv run ruff check .` runs lint checks.
- `uv run ruff format .` formats the codebase.
- `uv run pyright` runs strict type checking.
- `uv run pytest` runs tests.
- `uv build` produces the sdist and wheel.

## Coding Style & Naming Conventions

Target Python 3.13. Use 4-space indentation, UTF-8 text, and LF line endings per `.editorconfig`. Ruff enforces import sorting and a 100-character line limit. Follow standard Python naming: `snake_case` for functions and modules, `PascalCase` for classes, and clear config keys like `download_concurrency`.

Document maintained modules, classes, and public functions with concise docstrings. Include `Attributes`, `Args`, `Returns`, and `Side Effects` only when they add real information; do not add empty `None` sections. Inline trivial one-line helpers instead of wrapping them. Do not use `getattr` or `hasattr`; prefer explicit typed interfaces and direct attributes.

## Testing Guidelines

Add unit tests in `tests/test_*.py`. Keep tests focused on deterministic logic such as config parsing, path handling, and future selector helpers. New behavior should ship with tests when practical; at minimum, run `just lint` and `just test` before opening a PR.

## Commit & Pull Request Guidelines

Use short, imperative commit subjects such as `Add Playwright login state capture`. Keep commits scoped to one concern. PRs should include a brief summary, notable config or selector changes, test/typecheck results, and screenshots only if browser-visible behavior changes.

## Security & Configuration Tips

Do not add environment-variable based configuration. Treat `gphoto-pull.toml`, config directories, downloads, diagnostics, sync DBs, browser binaries, and browser profile directories as local-only runtime data.

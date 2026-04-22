# Contributing

Thanks for improving `gphoto-pull`.

## Development Setup

```bash
uv sync --dev
just lint
just test
```

Use `uv run ...` directly if `just` is not installed:

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest
```

## Pull Requests

- Keep changes scoped to one concern.
- Add or update tests for deterministic behavior.
- Do not commit local runtime data such as browser profiles, diagnostics,
  downloaded media, sync databases, or `.codex/` notes.
- Mention any Google Photos selector, route, or payload-shape assumptions in the
  PR description.
- Include local verification results.

## Browser-Automation Changes

Google Photos is not a stable public API. Prefer small, isolated changes around
selectors, payload parsing, and diagnostics. When browser-visible behavior
changes, include enough detail for reviewers to reproduce the path.

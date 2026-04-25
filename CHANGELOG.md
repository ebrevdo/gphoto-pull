# Changelog

All notable changes to `gphoto-pull` will be documented in this file.

Before a public release, move relevant entries from `Unreleased` into a
versioned section. The first release tag is `v0.0.1`.

## Unreleased

- Nothing yet.

## v0.0.2

- Added CI, release, lint, typecheck, test, and packaging workflows.
- Added PyPI publishing through GitHub Trusted Publishing.
- Added local `just` recipes for common development commands.
- Added MIT licensing and richer package metadata.
- Added default authentication checks for `pull`, `refresh`, and `doctor`; `doctor --dry-run`
  skips the live browser check.
- Added app-owned Chromium/profile handling improvements, including login completion marking,
  stale profile lock cleanup, and `--allow-browser-signin=false` for launches.
- Added account-scoped SQLite state directories so different Google accounts can share a
  configured state template without mixing media indexes.
- Added earlier auth failure detection for Google Photos routes instead of waiting for long
  page timeouts.
- Changed `pull` to require a live authenticated session and removed `pull --dry-run`.
- Changed progress rendering to use fixed active worker rows, no visible slot column, and
  single-line ellipsized filenames.
- Changed download status labels and timing logs so `download` covers Playwright `save_as`
  byte saving and `finalize` covers local rename, sidecar, and state updates.
- Removed temporary login/browser debug logging and extra Chromium workaround flags.

## v0.0.1

- Initial public package metadata and release infrastructure.

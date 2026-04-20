# gphoto-pull

`gphoto-pull` is a Python CLI for downloading original Google Photos photos and videos
uploaded or shared inside a date/time window. It uses Playwright with an app-owned persistent
Chromium profile, so the normal flow is manual Google login once and headless pulls after that.

Google Photos is not a stable public API. This project treats the browser session, downloaded
files, diagnostics, and sync database as local runtime state and keeps enough diagnostics to debug
site drift.

## Quick Start

```bash
gphoto-pull config --defaults
gphoto-pull install-browser
gphoto-pull doctor
gphoto-pull login
gphoto-pull pull --after 2026-01-01T00:00:00-08:00 --dry-run
gphoto-pull pull --after "Jan 1 2026" --before "Feb 1 2026"
```

`gphoto-pull login` opens a normal Chromium window using the persistent profile, without attaching
Playwright to the login browser. Complete Google login and MFA in that browser, then press Enter in
the terminal. Later `pull` runs reuse that same profile with Playwright and run headless by default.

## Runtime Directory

By default, everything lives under:

```text
~/.local/state/gphoto-pull/
```

That directory owns:

- `gphoto-pull.toml`: optional local defaults.
- `browsers/`: Playwright-installed Chromium binaries.
- `chrome-profile/`: persistent Google login profile.
- `diagnostics/`: HTML, screenshots, RPC payloads, and download traces.
- `state/pull-state.sqlite3`: local media index.

Use `--config-dir path` for a fully isolated second setup. Relative paths in `gphoto-pull.toml`
resolve under that config directory.

By default, downloaded originals are written to the current working directory. If
`download_dir` is set in `gphoto-pull.toml`, it must be absolute or start with `~/`. Relative
download paths are only allowed with the `--download-dir` CLI option, where they resolve from the
current working directory.

## Commands

- `gphoto-pull config`: interactively write `gphoto-pull.toml`.
- `gphoto-pull config --defaults --force`: write defaults without prompts.
- `gphoto-pull install-browser`: install Playwright Chromium into the configured runtime directory.
- `gphoto-pull doctor`: check config, browser binaries, and runtime paths.
- `gphoto-pull login`: launch the persistent headed browser profile for manual login.
- `gphoto-pull refresh --after TIMESTAMP`: refresh the local media index without downloading.
- `gphoto-pull refresh --after TIMESTAMP --reset`: delete and rebuild the local media index.
- `gphoto-pull pull --dry-run --after TIMESTAMP`: inspect config and enumeration without downloading.
- `gphoto-pull pull --after START --before END`: enumerate and download originals in a window.
- `gphoto-pull reset --yes`: delete the persistent browser profile and local media index.
- `gphoto-pull reset --target index --yes`: delete only the local media index.

`--after` is inclusive and `--before` is exclusive. Both accept common date/time text.
Valid examples include `2026-01-01`, `2026/01/01 00:00 -0800`, and
`Jan 1 2026 12:00 AM`. If no time is provided, midnight is used. If no timezone is
provided, the local machine timezone is used.

Useful pull overrides:

```bash
gphoto-pull --config-dir ~/.local/state/gphoto-pull-work pull \
  --after "Jan 1 2026" \
  --before "Feb 1 2026" \
  --download-dir downloads \
  --concurrency 4 \
  --headed
```

## Configuration

Example `gphoto-pull.toml`:

```toml
# Omit this to download into the current working directory.
# download_dir = "downloads"
download_concurrency = 3
enrichment_concurrency = 5
sync_db_path = "state/pull-state.sqlite3"
diagnostics_dir = "diagnostics"
browsers_path = "browsers"
browser_profile_dir = "chrome-profile"
headless = true
enrich_metadata = true

# Usually pass these on the command line instead.
# after = "Jan 1 2026"
# before = "Feb 1 2026"

# Optional when Playwright Chromium is not enough.
# browser_binary = "/path/to/chromium"
```

Do not use environment variables for normal configuration. Put durable defaults in
`gphoto-pull.toml` and use CLI flags for one-off overrides.

Use `--no-enrich-metadata` to skip post-download detail-page metadata enrichment for a faster
pull, or `--enrich-metadata` to force it on when the config disables it. Enrichment runs on a
separate worker pool controlled by `enrichment_concurrency`.

## How Pull Works

1. Launches the persistent browser profile.
2. Captures fresh `Recently added` and `/updates` diagnostics.
3. Parses Google Photos payload shapes for exact upload/share times when available.
4. Persists candidates in SQLite and skips already-downloaded files.
5. Downloads originals with concurrent workers, preferring direct URLs when proven and falling
   back to the Google Photos detail-page Download action.
6. Finalizes downloads atomically, writes sidecar metadata, and optionally enriches sidecars from
   the detail page.

## Troubleshooting

- If `doctor` reports missing browser binaries, run the Playwright install command from Quick Start.
- If `pull` lands on a Google marketing page or account chooser, run `gphoto-pull login` again.
- If `login` crashes because the browser profile is stale or locked, run `gphoto-pull reset --yes`.
- If downloads fail, inspect `diagnostics/pull_failures/` and `diagnostics/download_traces/`.
- If a run is interrupted, rerun the same command. The SQLite state skips finalized files.
- If older UUID-named duplicate files are reported by `doctor`, they are leftovers from early builds
  and can be deleted after you confirm the named downloads exist.

## Development

```bash
uv sync --dev
uv run gphoto-pull --help
uv run ruff check .
uv run ruff format .
uv run pyright
uv run python -m unittest discover -s tests
uv build
```

The source uses a `src/` layout. Main modules are:

- `cli.py`: typed Tyro CLI.
- `config.py`: `msgspec` TOML config and path resolution.
- `browser.py`: Playwright profile/session lifecycle.
- `automation.py`: high-level pull orchestration.
- `photos_ui.py`: Google Photos selectors and route classification.
- `rpc_payloads.py`: Google Photos payload parsers.
- `enumeration.py`: candidate enumeration and cutoff matching.
- `state.py`: SQLite sync state.
- `download.py`: final path planning and atomic file moves.

"""Configuration loading and validation for the gphoto-pull CLI.

Description:
    Decodes `gphoto-pull.toml`, applies CLI overrides, validates scalar values,
    and resolves runtime paths under the selected config directory.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import msgspec
import msgspec.toml
from dateutil.parser import ParserError
from dateutil.parser import parse as parse_datetime
from platformdirs import user_state_path

DEFAULT_CONFIG_DIR = user_state_path("gphoto-pull", appauthor=False)
DEFAULT_CONFIG_FILE_NAME = "gphoto-pull.toml"


class ConfigError(ValueError):
    """Invalid project configuration.

    Description:
        Raised when TOML, CLI overrides, or resolved config values are invalid.
    """


@dataclass(slots=True, frozen=True)
class ConfigOverrides:
    """Runtime config values supplied by the CLI.

    Description:
        Holds per-run override values before they are merged with the TOML file and
        defaults. `None` means "do not override".

    Attributes:
        after: Optional inclusive lower-bound timestamp as text or an aware `datetime`.
        before: Optional exclusive upper-bound timestamp as text or an aware `datetime`.
        download_dir: Optional output directory override.
        download_concurrency: Optional parallel download worker count.
        enrichment_concurrency: Optional parallel enrichment worker count.
        sync_db_path: Optional SQLite state database path.
        diagnostics_dir: Optional diagnostics artifact directory.
        browsers_path: Optional Playwright browser-install directory.
        browser_binary: Optional Chromium executable path.
        browser_profile_dir: Optional persistent browser profile directory.
        headless: Optional default browser visibility for pull runs.
        enrich_metadata: Optional post-download detail metadata enrichment setting.
        progress_interactive: Optional override for Rich live progress rendering.
    """

    after: str | datetime | None = None
    before: str | datetime | None = None
    download_dir: Path | str | None = None
    download_concurrency: int | None = None
    enrichment_concurrency: int | None = None
    sync_db_path: Path | str | None = None
    diagnostics_dir: Path | str | None = None
    browsers_path: Path | str | None = None
    browser_binary: str | None = None
    browser_profile_dir: Path | str | None = None
    headless: bool | None = None
    enrich_metadata: bool | None = None
    progress_interactive: bool | None = None


class ConfigFile(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Typed representation of `gphoto-pull.toml`.

    Description:
        Mirrors supported TOML keys exactly. Unknown keys fail decoding so typos do
        not silently change runtime behavior.

    Attributes:
        after: Optional default inclusive lower-bound timestamp as text.
        before: Optional default exclusive upper-bound timestamp as text.
        download_dir: Optional output directory.
        download_concurrency: Optional parallel download worker count.
        enrichment_concurrency: Optional parallel enrichment worker count.
        sync_db_path: Optional SQLite state database path.
        diagnostics_dir: Optional diagnostics artifact directory.
        browsers_path: Optional Playwright browser-install directory.
        browser_binary: Optional Chromium executable path.
        browser_profile_dir: Optional persistent browser profile directory.
        headless: Optional default browser visibility for pull runs.
        enrich_metadata: Optional post-download detail metadata enrichment setting.
    """

    after: str | None = None
    before: str | None = None
    download_dir: str | None = None
    download_concurrency: int | None = None
    enrichment_concurrency: int | None = None
    sync_db_path: str | None = None
    diagnostics_dir: str | None = None
    browsers_path: str | None = None
    browser_binary: str | None = None
    browser_profile_dir: str | None = None
    headless: bool | None = None
    enrich_metadata: bool | None = None


def _parse_datetime_field(
    raw: str | datetime | None,
    *,
    field_name: str,
) -> datetime | None:
    """Description:
    Parse and validate an optional datetime field.

    Args:
        raw: Timestamp text, datetime, or missing value.
        field_name: Config field name used in errors.

    Returns:
        Aware cutoff datetime, or `None` when missing/blank.
    """

    if raw is None:
        return None
    if isinstance(raw, str):
        value = raw.strip()
        if value == "":
            return None
        try:
            parsed = parse_datetime(
                value,
                fuzzy=False,
                default=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
            )
        except (ParserError, OverflowError) as exc:
            raise ConfigError(
                f"`{field_name}` must be a date/time, for example `2026-01-01`, "
                "`Jan 1 2026 12:00 AM`, or `2026/01/01 00:00 -0800`."
            ) from exc
    else:
        parsed = raw
    if parsed.tzinfo is None:
        return parsed.astimezone()
    if parsed.tzinfo.utcoffset(parsed) is None:
        return parsed.replace(tzinfo=None).astimezone()
    return parsed


def _parse_positive_int(raw: int | None, *, default: int, field_name: str) -> int:
    """Description:
    Resolve an optional positive integer config value.

    Args:
        raw: Optional configured integer.
        default: Value used when `raw` is missing.
        field_name: Config field name for error messages.

    Returns:
        Resolved positive integer.
    """

    if raw is None:
        return default
    if raw <= 0:
        raise ConfigError(f"{field_name} must be greater than zero.")
    return raw


def _resolve_path(path: Path, *, base_dir: Path) -> Path:
    """Description:
    Resolve a user path against the config directory.

    Args:
        path: Path to expand and resolve.
        base_dir: Base directory for relative paths.

    Returns:
        Expanded absolute or base-relative path.
    """

    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded
    return base_dir / expanded


def _parse_path(
    raw: Path | str | None,
    *,
    default: Path | str,
    field_name: str,
    base_dir: Path,
) -> Path:
    """Description:
    Parse and resolve a path config value.

    Args:
        raw: Optional path value from config or overrides.
        default: Default path when `raw` is missing.
        field_name: Config field name for error messages.
        base_dir: Base directory for relative paths.

    Returns:
        Resolved path.
    """

    if raw is None:
        return _resolve_path(Path(default), base_dir=base_dir)
    if isinstance(raw, Path):
        return _resolve_path(raw, base_dir=base_dir)
    if raw.strip() == "":
        raise ConfigError(f"{field_name} cannot be empty.")
    return _resolve_path(Path(raw), base_dir=base_dir)


def _parse_download_path(raw: Path | str | None, *, source: str) -> Path:
    """Description:
    Parse the final download directory path.

    Args:
        raw: Optional configured or overridden download path.
        source: Source label, either `config` or `override`.

    Returns:
        Absolute path, user-expanded path, or current-working-directory-relative path.
    """

    if raw is None:
        return Path.cwd()
    path = raw if isinstance(raw, Path) else Path(raw.strip())
    if not isinstance(raw, Path) and raw.strip() == "":
        raise ConfigError("download_dir cannot be empty.")
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded
    if source == "config":
        raise ConfigError("download_dir in gphoto-pull.toml must be absolute or start with ~/")
    return Path.cwd() / expanded


def _parse_optional_str(raw: str | None) -> str | None:
    """Description:
    Normalize optional string config values.

    Args:
        raw: Optional string from config.

    Returns:
        Stripped non-empty text, or `None`.
    """

    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _read_config_file(path: Path) -> ConfigFile:
    """Description:
    Decode a TOML config file if it exists.

    Args:
        path: TOML file path.

    Returns:
        Decoded config values, or defaults when the file is missing.

    Side Effects:
        Reads the config file when present.
    """

    if not path.exists():
        return ConfigFile()
    try:
        raw_config = msgspec.toml.decode(path.read_bytes(), type=ConfigFile)
    except msgspec.DecodeError as exc:
        raise ConfigError(f"{path}: invalid TOML: {exc}") from exc
    return raw_config


def _apply_overrides(values: ConfigFile, overrides: ConfigOverrides | None) -> ConfigFile:
    """Description:
    Merge CLI override values over TOML config values.

    Args:
        values: Decoded TOML values.
        overrides: Optional CLI overrides.

    Returns:
        Config values with overrides applied.
    """

    if overrides is None:
        return values

    override_after = overrides.after
    if isinstance(override_after, datetime):
        override_after_text = override_after.isoformat()
    else:
        override_after_text = override_after
    override_before = overrides.before
    if isinstance(override_before, datetime):
        override_before_text = override_before.isoformat()
    else:
        override_before_text = override_before

    return ConfigFile(
        after=override_after_text if override_after_text is not None else values.after,
        before=override_before_text if override_before_text is not None else values.before,
        download_dir=(
            str(overrides.download_dir)
            if overrides.download_dir is not None
            else values.download_dir
        ),
        download_concurrency=(
            overrides.download_concurrency
            if overrides.download_concurrency is not None
            else values.download_concurrency
        ),
        enrichment_concurrency=(
            overrides.enrichment_concurrency
            if overrides.enrichment_concurrency is not None
            else values.enrichment_concurrency
        ),
        sync_db_path=(
            str(overrides.sync_db_path)
            if overrides.sync_db_path is not None
            else values.sync_db_path
        ),
        diagnostics_dir=(
            str(overrides.diagnostics_dir)
            if overrides.diagnostics_dir is not None
            else values.diagnostics_dir
        ),
        browsers_path=(
            str(overrides.browsers_path)
            if overrides.browsers_path is not None
            else values.browsers_path
        ),
        browser_binary=(
            overrides.browser_binary
            if overrides.browser_binary is not None
            else values.browser_binary
        ),
        browser_profile_dir=(
            str(overrides.browser_profile_dir)
            if overrides.browser_profile_dir is not None
            else values.browser_profile_dir
        ),
        headless=overrides.headless if overrides.headless is not None else values.headless,
        enrich_metadata=(
            overrides.enrich_metadata
            if overrides.enrich_metadata is not None
            else values.enrich_metadata
        ),
    )


def _download_path_source(values: ConfigFile, overrides: ConfigOverrides | None) -> str:
    """Description:
    Determine whether `download_dir` came from config or CLI override.

    Args:
        values: Decoded TOML values before overrides.
        overrides: Optional CLI overrides.

    Returns:
        Source label for download path validation.
    """

    if overrides is not None and overrides.download_dir is not None:
        return "override"
    if values.download_dir is not None:
        return "config"
    return "default"


@dataclass(slots=True)
class ProjectConfig:
    """Fully resolved configuration used by the application.

    Description:
        Stores validated, absolute-or-config-relative runtime paths and scalar
        settings after TOML values and CLI overrides have been merged.

    Attributes:
        config_dir: Root directory that owns config and runtime state.
        after: Optional aware inclusive lower-bound timestamp.
        before: Optional aware exclusive upper-bound timestamp.
        download_dir: Final download directory.
        download_concurrency: Number of concurrent download workers.
        enrichment_concurrency: Number of concurrent metadata enrichment workers.
        sync_db_path: SQLite state database path.
        diagnostics_dir: Diagnostics artifact directory.
        browsers_path: Playwright browser-install directory.
        browser_binary: Optional Chromium executable path.
        browser_profile_dir: Persistent Google login profile directory.
        headless: Whether pull runs should hide the browser by default.
        enrich_metadata: Whether pull runs should fetch detail metadata after
            direct downloads.
        progress_interactive: Whether pull progress may use Rich live rendering.
        config_file: TOML file path considered for this run.
        config_file_loaded: Whether `config_file` existed and was decoded.
    """

    config_dir: Path
    after: datetime | None
    before: datetime | None
    download_dir: Path
    download_concurrency: int
    enrichment_concurrency: int
    sync_db_path: Path
    diagnostics_dir: Path
    browsers_path: Path
    browser_binary: str | None
    browser_profile_dir: Path
    headless: bool
    enrich_metadata: bool
    progress_interactive: bool
    config_file: Path
    config_file_loaded: bool

    @classmethod
    def from_sources(
        cls,
        *,
        config_dir: Path | str | None = None,
        config_path: Path | str | None = None,
        overrides: ConfigOverrides | None = None,
    ) -> ProjectConfig:
        """Description:
        Resolve project configuration from defaults, TOML, and CLI overrides.

        Args:
            config_dir: Runtime/config directory to use. Defaults to the platform
                state directory for `gphoto-pull`.
            config_path: Specific TOML file to read. Relative paths resolve under
                `config_dir`.
            overrides: Per-run values that take precedence over the TOML file.

        Returns:
            A validated `ProjectConfig` with paths resolved relative to
            `config_dir` where appropriate.

        Side Effects:
            Reads `config_path` when it exists.
        """

        if config_dir is not None:
            resolved_config_dir = Path(config_dir).expanduser()
        elif config_path is not None:
            resolved_config_dir = Path(config_path).expanduser().parent
        else:
            resolved_config_dir = DEFAULT_CONFIG_DIR
        if config_path is None:
            config_file = resolved_config_dir / DEFAULT_CONFIG_FILE_NAME
        else:
            config_file = Path(config_path).expanduser()
            if not config_file.is_absolute():
                config_file = _resolve_config_file_path(
                    config_file,
                    config_dir=resolved_config_dir,
                )
        config_values = _read_config_file(config_file)
        download_source = _download_path_source(config_values, overrides)
        values = _apply_overrides(config_values, overrides)
        download_dir = _parse_download_path(values.download_dir, source=download_source)
        after = _parse_datetime_field(values.after, field_name="after")
        before = _parse_datetime_field(values.before, field_name="before")
        if after is not None and before is not None and before <= after:
            raise ConfigError("`before` must be after `after`.")

        return cls(
            config_dir=resolved_config_dir,
            after=after,
            before=before,
            download_dir=download_dir,
            download_concurrency=_parse_positive_int(
                values.download_concurrency,
                default=3,
                field_name="download_concurrency",
            ),
            enrichment_concurrency=_parse_positive_int(
                values.enrichment_concurrency,
                default=5,
                field_name="enrichment_concurrency",
            ),
            sync_db_path=_parse_path(
                values.sync_db_path,
                default="state/pull-state.sqlite3",
                field_name="sync_db_path",
                base_dir=resolved_config_dir,
            ),
            diagnostics_dir=_parse_path(
                values.diagnostics_dir,
                default="diagnostics",
                field_name="diagnostics_dir",
                base_dir=resolved_config_dir,
            ),
            browsers_path=_parse_path(
                values.browsers_path,
                default="browsers",
                field_name="browsers_path",
                base_dir=resolved_config_dir,
            ),
            browser_binary=_parse_optional_str(
                values.browser_binary,
            ),
            browser_profile_dir=_parse_path(
                values.browser_profile_dir,
                default="chrome-profile",
                field_name="browser_profile_dir",
                base_dir=resolved_config_dir,
            ),
            headless=True if values.headless is None else values.headless,
            enrich_metadata=True if values.enrich_metadata is None else values.enrich_metadata,
            progress_interactive=(
                True
                if overrides is None or overrides.progress_interactive is None
                else overrides.progress_interactive
            ),
            config_file=config_file,
            config_file_loaded=config_file.exists(),
        )

    def ensure_runtime_paths(self) -> None:
        """Description:
        Create directories needed by a normal login or pull run.
        Side Effects:
            Creates download, diagnostics, browser, profile, and state parent
            directories if they do not already exist.
        """

        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.sync_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.diagnostics_dir.mkdir(parents=True, exist_ok=True)
        self.browsers_path.mkdir(parents=True, exist_ok=True)
        self.browser_profile_dir.mkdir(parents=True, exist_ok=True)


def _resolve_config_file_path(config_path: Path, *, config_dir: Path) -> Path:
    """Description:
    Resolve a relative config path without duplicating an existing directory prefix.

    Args:
        config_path: Relative config file path.
        config_dir: Effective config directory.

    Returns:
        Relative path resolved under `config_dir` when needed.
    """

    if config_path.parts[: len(config_dir.parts)] == config_dir.parts:
        return config_path
    return config_dir / config_path

"""Typed command-line interface for gphoto-pull.

Description:
    Defines Tyro dataclasses, config/bootstrap commands, and service command
    dispatch for the `gphoto-pull` executable.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, override

import tyro

from gphoto_pull.automation import GooglePhotosPuller
from gphoto_pull.browser import BrowserSessionError
from gphoto_pull.config import (
    DEFAULT_CONFIG_DIR,
    DEFAULT_CONFIG_FILE_NAME,
    ConfigError,
    ConfigOverrides,
    ProjectConfig,
)
from gphoto_pull.interrupts import cooperative_sigint_handling, raise_if_interrupt_requested

DEFAULT_CONFIG_VALUES = {
    "download_concurrency": "4",
    "sync_db_path": "state/pull-state.sqlite3",
    "diagnostics_dir": "diagnostics",
    "browsers_path": "browsers",
    "browser_profile_dir": "chrome-profile",
}

_LOG_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


class _MillisecondsFormatter(logging.Formatter):
    """Logging formatter with ISO-like local timestamps and milliseconds."""

    default_msec_format = "%s.%03d"

    @override
    def formatTime(
        self,
        record: logging.LogRecord,
        datefmt: str | None = None,
    ) -> str:
        """Description:
        Format a log record timestamp with milliseconds.

        Args:
            record: Log record to format.
            datefmt: Optional `strftime` format.

        Returns:
            Local timestamp string.
        """

        rendered = super().formatTime(record, datefmt)
        if "." in rendered:
            return rendered
        return f"{rendered}.{int(record.msecs):03d}"


def _configure_logging(*, verbose: bool, quiet: bool) -> None:
    """Description:
    Configure CLI logging to stderr.

    Args:
        verbose: Emit detailed phase/timing logs.
        quiet: Suppress non-error logs.

    Side Effects:
        Installs a root logging handler when none exists.
    """

    root_level = logging.INFO
    app_level = logging.INFO
    if verbose:
        app_level = logging.DEBUG
    if quiet:
        root_level = logging.ERROR
        app_level = logging.ERROR

    if logging.getLogger().handlers:
        logging.getLogger().setLevel(root_level)
        logging.getLogger("gphoto_pull").setLevel(app_level)
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_MillisecondsFormatter("%(asctime)s %(message)s", _LOG_TIME_FORMAT))
    logging.basicConfig(level=root_level, handlers=[handler])
    logging.getLogger("gphoto_pull").setLevel(app_level)


@dataclass(slots=True)
class ConfigCommand:
    """Config file command.

    Description:
        Creates or updates `gphoto-pull.toml`.

    Attributes:
        force: Overwrite an existing config file without prompting.
        defaults: Write default values without asking interactive questions.
    """

    force: bool = False
    defaults: bool = False


@dataclass(slots=True)
class DoctorCommand:
    """Doctor command.

    Description:
        Validates local prerequisites and config.
    """


@dataclass(slots=True)
class InstallBrowserCommand:
    """Browser install command.

    Description:
        Installs Playwright Chromium into the configured browser directory.
    """


@dataclass(slots=True)
class LoginCommand:
    """Login command.

    Description:
        Launches the persistent browser profile for interactive Google login.
    """


@dataclass(slots=True)
class PullCommand:
    """Pull command.

    Description:
        Runs the pull workflow.

    Attributes:
        after: Optional ISO-8601 cutoff timestamp override.
        before: Optional exclusive upper-bound timestamp override.
        download_dir: Optional output directory override.
        concurrency: Optional parallel worker count override.
        headed: Show the pull browser instead of running headless.
        dry_run: Enumerate/report without starting downloads.
    """

    after: str | None = None
    before: str | None = None
    download_dir: str | None = None
    concurrency: int | None = None
    headed: bool = False
    dry_run: bool = False


@dataclass(slots=True)
class RefreshCommand:
    """Refresh command.

    Description:
        Refreshes the local media index without downloading files.

    Attributes:
        after: Optional inclusive lower-bound timestamp override.
        before: Optional exclusive upper-bound timestamp override.
        reset: Delete the existing media index before refreshing.
        headed: Show the refresh browser instead of running headless.
    """

    after: str | None = None
    before: str | None = None
    reset: bool = False
    headed: bool = False


@dataclass(slots=True)
class ResetCommand:
    """Reset command.

    Description:
        Deletes selected local runtime data. Defaults to the browser profile
        plus media index.

    Attributes:
        target: Runtime data to reset. Supported values: `all`, `profile`,
            `index`, `browsers`, or `browser`.
        yes: Skip interactive confirmation.
    """

    target: str = "all"
    yes: bool = False


Command = Annotated[
    Annotated[
        ConfigCommand,
        tyro.conf.subcommand("config", description="Create or update `gphoto-pull.toml`."),
    ]
    | Annotated[
        DoctorCommand,
        tyro.conf.subcommand("doctor", description="Validate local prerequisites and config."),
    ]
    | Annotated[
        InstallBrowserCommand,
        tyro.conf.subcommand(
            "install-browser",
            description="Install Playwright Chromium into the configured browser directory.",
        ),
    ]
    | Annotated[
        LoginCommand,
        tyro.conf.subcommand(
            "login",
            description="Launch the persistent browser profile for interactive Google login.",
        ),
    ]
    | Annotated[
        PullCommand,
        tyro.conf.subcommand("pull", description="Run the pull workflow."),
    ]
    | Annotated[
        RefreshCommand,
        tyro.conf.subcommand(
            "refresh",
            description="Refresh the local media index without downloading files.",
        ),
    ]
    | Annotated[
        ResetCommand,
        tyro.conf.subcommand(
            "reset",
            description="Reset local runtime data. Defaults to profile plus media index.",
        ),
    ],
    tyro.conf.OmitSubcommandPrefixes,
]


@dataclass(slots=True)
class CliArgs:
    """Top-level CLI argument model.

    Description:
        Tyro converts this dataclass and the command union into the `gphoto-pull`
        command line.

    Attributes:
        command: Subcommand to execute.
        config_dir: Optional runtime/config directory root.
        config: Optional explicit TOML config path.
        browser_profile_dir: Optional persistent Chromium profile override.
        browser_binary: Optional Chromium executable override.
        browsers_path: Optional Playwright browser-install directory override.
        verbose: Emit detailed phase/timing logs.
        quiet: Suppress non-error logs.
    """

    command: Command
    config_dir: str | None = None
    config: str | None = None
    browser_profile_dir: str | None = None
    browser_binary: str | None = None
    browsers_path: str | None = None
    verbose: bool = False
    quiet: bool = False


def parse_args(argv: Sequence[str] | None = None) -> CliArgs:
    """Description:
    Parse CLI arguments into typed dataclasses.

    Args:
        argv: Optional argument sequence. Uses `sys.argv` when omitted.

    Returns:
        Parsed top-level CLI arguments.

    Side Effects:
        Tyro may print help or parse errors and raise `SystemExit`.
    """

    parsed_args = tyro.cli(
        CliArgs,
        prog="gphoto-pull",
        description="Pull recent media from Google Photos with Playwright.",
        args=None if argv is None else list(argv),
    )
    return parsed_args


def _config_dir_from_args(args: CliArgs) -> Path:
    """Description:
    Resolve the effective config/runtime directory from top-level CLI args.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Config directory path.
    """

    if args.config_dir is not None:
        return Path(args.config_dir)
    if args.config is not None:
        return Path(args.config).expanduser().parent
    return DEFAULT_CONFIG_DIR


def _config_path_from_args(args: CliArgs, *, config_dir: Path) -> Path | None:
    """Description:
    Resolve an explicit TOML path from CLI args.

    Args:
        args: Parsed CLI arguments.
        config_dir: Effective config directory for relative paths.

    Returns:
        Explicit config file path, or `None` to use the default.
    """

    if args.config is None:
        return None
    config_path = Path(args.config).expanduser()
    if config_path.is_absolute():
        return config_path
    return config_dir / config_path


def _config_overrides(args: CliArgs) -> ConfigOverrides:
    """Description:
    Convert CLI options into project config overrides.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Override values suitable for `ProjectConfig.from_sources`.
    """

    command = args.command
    after: str | None = None
    before: str | None = None
    download_dir: str | None = None
    download_concurrency: int | None = None
    headless: bool | None = None

    if isinstance(command, PullCommand | RefreshCommand):
        after = command.after
        before = command.before
        download_dir = command.download_dir if isinstance(command, PullCommand) else None
        download_concurrency = command.concurrency if isinstance(command, PullCommand) else None
        if command.headed:
            headless = False

    return ConfigOverrides(
        after=after,
        before=before,
        download_dir=download_dir,
        download_concurrency=download_concurrency,
        browser_profile_dir=args.browser_profile_dir,
        browser_binary=args.browser_binary,
        browsers_path=args.browsers_path,
        headless=headless,
        progress_interactive=False if args.verbose else None,
    )


def _load_config(args: CliArgs) -> ProjectConfig:
    """Description:
    Load the project config selected by CLI arguments.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Resolved project configuration.

    Side Effects:
        Reads the TOML config file when it exists.
    """

    config_dir = _config_dir_from_args(args)
    return ProjectConfig.from_sources(
        config_dir=config_dir,
        config_path=_config_path_from_args(args, config_dir=config_dir),
        overrides=_config_overrides(args),
    )


def _prompt_text(prompt: str, *, default: str) -> str:
    """Description:
    Prompt for a text value with a default.

    Args:
        prompt: Prompt label.
        default: Value used when input is blank.

    Returns:
        Entered or default text.

    Side Effects:
        Reads from stdin and writes the prompt to stdout.
    """

    raw_value = input(f"{prompt} [{default}]: ").strip()
    return raw_value or default


def _prompt_optional_text(prompt: str) -> str | None:
    """Description:
    Prompt for optional text.

    Args:
        prompt: Prompt label.

    Returns:
        Entered text, or `None` when blank.

    Side Effects:
        Reads from stdin and writes the prompt to stdout.
    """

    raw_value = input(f"{prompt} [blank]: ").strip()
    return raw_value or None


def _prompt_bool(prompt: str, *, default: bool) -> bool:
    """Description:
    Prompt for a yes/no value with validation.

    Args:
        prompt: Prompt label.
        default: Value used when input is blank.

    Returns:
        Parsed boolean answer.

    Side Effects:
        Reads from stdin and writes prompts/validation messages to stdout.
    """

    suffix = "Y/n" if default else "y/N"
    while True:
        raw_value = input(f"{prompt} [{suffix}]: ").strip().lower()
        if raw_value == "":
            return default
        if raw_value in {"y", "yes", "true", "1"}:
            return True
        if raw_value in {"n", "no", "false", "0"}:
            return False
        print("Please answer yes or no.")


def _toml_string(value: str) -> str:
    """Description:
    Render a minimal TOML double-quoted string literal.

    Args:
        value: Raw string value.

    Returns:
        Escaped TOML string literal.
    """

    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _render_config_toml(values: dict[str, str | bool]) -> str:
    """Description:
    Render config prompt values as `gphoto-pull.toml` content.

    Args:
        values: Config values gathered from defaults or prompts.

    Returns:
        TOML document text.
    """

    lines = [
        "# Local defaults for gphoto-pull.",
        "# Command-line options override these values.",
        "",
        f"download_concurrency = {values['download_concurrency']}",
        f"sync_db_path = {_toml_string(str(values['sync_db_path']))}",
        f"diagnostics_dir = {_toml_string(str(values['diagnostics_dir']))}",
        f"browsers_path = {_toml_string(str(values['browsers_path']))}",
        f"browser_profile_dir = {_toml_string(str(values['browser_profile_dir']))}",
        f"headless = {str(values['headless']).lower()}",
    ]

    after = values.get("after")
    if isinstance(after, str) and after:
        lines.extend(["", f"after = {_toml_string(after)}"])
    before = values.get("before")
    if isinstance(before, str) and before:
        lines.extend(["", f"before = {_toml_string(before)}"])

    browser_binary = values.get("browser_binary")
    if isinstance(browser_binary, str) and browser_binary:
        lines.extend(["", f"browser_binary = {_toml_string(browser_binary)}"])

    return "\n".join(lines) + "\n"


def _config_values_from_prompts(*, use_defaults: bool) -> dict[str, str | bool]:
    """Description:
    Gather config values from defaults or interactive prompts.

    Args:
        use_defaults: Whether to skip prompts and use default values.

    Returns:
        Config values ready for TOML rendering.

    Side Effects:
        Reads from stdin when `use_defaults` is false.
    """

    if use_defaults:
        return {
            **DEFAULT_CONFIG_VALUES,
            "headless": True,
        }

    values: dict[str, str | bool] = {
        "download_concurrency": _prompt_text(
            "Download concurrency",
            default=DEFAULT_CONFIG_VALUES["download_concurrency"],
        ),
        "sync_db_path": _prompt_text(
            "Sync database path, relative to config dir unless absolute",
            default=DEFAULT_CONFIG_VALUES["sync_db_path"],
        ),
        "diagnostics_dir": _prompt_text(
            "Diagnostics directory, relative to config dir unless absolute",
            default=DEFAULT_CONFIG_VALUES["diagnostics_dir"],
        ),
        "browsers_path": _prompt_text(
            "Playwright browsers path, relative to config dir unless absolute",
            default=DEFAULT_CONFIG_VALUES["browsers_path"],
        ),
        "browser_profile_dir": _prompt_text(
            "Persistent browser profile directory, relative to config dir unless absolute",
            default=DEFAULT_CONFIG_VALUES["browser_profile_dir"],
        ),
        "headless": _prompt_bool("Run pull headless by default", default=True),
    }

    after = _prompt_optional_text("Default cutoff timestamp, or leave blank to pass --after")
    if after is not None:
        values["after"] = after
    before = _prompt_optional_text("Default upper-bound timestamp, or leave blank")
    if before is not None:
        values["before"] = before

    browser_binary = _prompt_optional_text(
        "Browser binary override, or leave blank for Playwright Chromium"
    )
    if browser_binary is not None:
        values["browser_binary"] = browser_binary

    return values


def _run_config_command(args: CliArgs, command: ConfigCommand) -> int:
    """Description:
    Execute the `config` subcommand.

    Args:
        args: Parsed top-level CLI arguments.
        command: Parsed config subcommand options.

    Returns:
        Process-style exit code.

    Side Effects:
        May prompt, write TOML config, validate it, and print next steps.
    """

    config_dir = _config_dir_from_args(args).expanduser()
    config_path = (
        config_dir / DEFAULT_CONFIG_FILE_NAME
        if args.config is None
        else Path(args.config).expanduser()
    )
    if not config_path.is_absolute():
        config_path = config_dir / config_path
    if (
        config_path.exists()
        and not command.force
        and not _prompt_bool(f"{config_path} exists. Overwrite it", default=False)
    ):
        print(f"Left existing config unchanged: {config_path}")
        return 0

    values = _config_values_from_prompts(use_defaults=command.defaults)
    rendered = _render_config_toml(values)

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(rendered, encoding="utf-8")

    # Validate the generated file before reporting success.
    ProjectConfig.from_sources(config_dir=config_dir, config_path=config_path)
    print(f"Wrote config: {config_path}")
    print("Next: run `gphoto-pull install-browser`, then `gphoto-pull login`.")
    return 0


def _run_install_browser_command(config: ProjectConfig) -> int:
    """Description:
    Install Playwright Chromium into the configured browser directory.

    Args:
        config: Resolved project configuration.

    Returns:
        Subprocess exit code from Playwright.

    Side Effects:
        Creates the browser directory, runs Playwright's installer subprocess,
        and streams installer output to the terminal.
    """

    config.browsers_path.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["PLAYWRIGHT_BROWSERS_PATH"] = str(config.browsers_path)
    print(f"Installing Playwright Chromium into: {config.browsers_path}")
    completed = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        check=False,
        env=env,
    )
    return completed.returncode


def _reset_paths_for_target(config: ProjectConfig, *, target: str) -> tuple[Path, ...]:
    """Description:
    Resolve reset target names to configured paths.

    Args:
        config: Resolved project configuration.
        target: Reset target name.

    Returns:
        Paths to delete for the target.
    """

    if target == "all":
        return (config.browser_profile_dir, config.sync_db_path)
    if target == "profile":
        return (config.browser_profile_dir,)
    if target == "index":
        return (config.sync_db_path,)
    if target == "browsers":
        return (config.browsers_path,)
    if target == "browser":
        return (config.browser_profile_dir, config.browsers_path)
    raise ConfigError("reset target must be one of: all, profile, index, browsers, browser.")


def _run_reset_command(config: ProjectConfig, command: ResetCommand) -> int:
    """Description:
    Execute the `reset` subcommand.

    Args:
        config: Resolved project configuration.
        command: Parsed reset command options.

    Returns:
        Process-style exit code.

    Side Effects:
        Deletes configured runtime directories after confirmation.
    """

    paths = _reset_paths_for_target(config, target=command.target)
    rendered_paths = "\n".join(f"- {path}" for path in paths)
    if not command.yes and not _prompt_bool(
        f"Delete {command.target} runtime data?\n{rendered_paths}",
        default=False,
    ):
        print("Reset cancelled.")
        return 0

    for path in paths:
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            print(f"Deleted: {path}")
        else:
            print(f"Already absent: {path}")
    if command.target in {"browsers", "browser"}:
        print("Next: run `gphoto-pull install-browser`.")
    if command.target in {"all", "profile", "browser"}:
        print("Next: run `gphoto-pull login`.")
    return 0


def _run_doctor(service: GooglePhotosPuller) -> int:
    """Description:
    Execute the `doctor` service command.

    Args:
        service: Application service.

    Returns:
        Process-style exit code.

    Side Effects:
        Prints doctor checks.
    """

    for check in service.doctor():
        status = "WARN" if check.warning else "OK" if check.ok else "MISSING"
        print(f"[{status}] {check.name}: {check.detail}")
    return 0


def _run_pull(service: GooglePhotosPuller, *, dry_run: bool) -> int:
    """Description:
    Execute the `pull` service command.

    Args:
        service: Application service.
        dry_run: Whether to report without downloading.

    Returns:
        Process-style exit code.

    Side Effects:
        Runs the pull workflow and prints result lines.
    """

    lines = service.pull(dry_run=dry_run)
    label = "Dry run plan" if dry_run else "Pull result"
    print(label, file=sys.stderr)
    for line in lines:
        print(f"- {line}", file=sys.stderr)
    return 0


def _run_refresh(config: ProjectConfig, service: GooglePhotosPuller, *, reset: bool) -> int:
    """Description:
    Execute the `refresh` service command.

    Args:
        config: Resolved project configuration.
        service: Application service.
        reset: Whether to remove the current media index before refreshing.

    Returns:
        Process-style exit code.

    Side Effects:
        Optionally deletes the media index DB, refreshes metadata, and writes
        result lines to stderr.
    """

    if reset and config.sync_db_path.exists():
        config.sync_db_path.unlink()
        print(f"Deleted media index: {config.sync_db_path}", file=sys.stderr)
    lines = service.refresh()
    print("Refresh result", file=sys.stderr)
    for line in lines:
        print(f"- {line}", file=sys.stderr)
    return 0


def _run_login(service: GooglePhotosPuller) -> int:
    """Description:
    Execute the `login` service command.

    Args:
        service: Application service.

    Returns:
        Process-style exit code.

    Side Effects:
        Launches the interactive login workflow and prints result lines.
    """

    print("Session capture")
    for line in service.login():
        print(f"- {line}")
    return 0


def _run_service_command(args: CliArgs) -> int:
    """Description:
    Dispatch non-config subcommands to the application service.

    Args:
        args: Parsed CLI arguments.

    Returns:
        Process-style exit code.

    Side Effects:
        Loads config and runs the selected service command.
    """

    config = _load_config(args)
    command = args.command

    if isinstance(command, InstallBrowserCommand):
        return _run_install_browser_command(config)
    if isinstance(command, ResetCommand):
        return _run_reset_command(config, command)

    service = GooglePhotosPuller(config)
    if isinstance(command, DoctorCommand):
        return _run_doctor(service)
    if isinstance(command, LoginCommand):
        return _run_login(service)
    if isinstance(command, PullCommand):
        return _run_pull(service, dry_run=command.dry_run)
    if isinstance(command, RefreshCommand):
        return _run_refresh(config, service, reset=command.reset)

    raise AssertionError(f"Unsupported service command: {command!r}")


def main(argv: Sequence[str] | None = None) -> int:
    """Description:
    Run the `gphoto-pull` command-line program.

    Args:
        argv: Optional argument sequence for tests or programmatic invocation.

    Returns:
        Process-style exit code.

    Side Effects:
        Reads config, may launch browsers, may write config/state/download files,
        and prints command output.
    """

    try:
        raw_args = sys.argv[1:] if argv is None else list(argv)
        if not raw_args:
            try:
                parse_args(["--help"])
            except SystemExit as exc:
                return int(exc.code or 0)
            return 0
        args = parse_args(raw_args)
        _configure_logging(verbose=args.verbose, quiet=args.quiet)
        with cooperative_sigint_handling():
            if isinstance(args.command, ConfigCommand):
                exit_code = _run_config_command(args, args.command)
            else:
                exit_code = _run_service_command(args)
            raise_if_interrupt_requested()
            return exit_code
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    except BrowserSessionError as exc:
        print(f"Browser session error: {exc}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130

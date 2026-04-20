import io
import os
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

from gphoto_pull.automation import GooglePhotosPuller
from gphoto_pull.browser import BrowserSessionError, BrowserSessionPaths
from gphoto_pull.cli import (
    ConfigCommand,
    InstallBrowserCommand,
    LoginCommand,
    PullCommand,
    RefreshCommand,
    ResetCommand,
    main,
    parse_args,
)
from gphoto_pull.config import ConfigOverrides, ProjectConfig

MISSING_CONFIG_PATH = Path.cwd() / ".missing-gphoto-pull.toml"


class CliTests(unittest.TestCase):
    def test_main_without_args_prints_help(self) -> None:
        with io.StringIO() as stdout, redirect_stdout(stdout):
            exit_code = main([])
            rendered = stdout.getvalue()

        self.assertEqual(exit_code, 0)
        self.assertIn("usage: gphoto-pull", rendered)
        self.assertIn("subcommands", rendered)

    def test_parse_args_accepts_login_command(self) -> None:
        args = parse_args(["login"])

        self.assertIsInstance(args.command, LoginCommand)

    def test_parse_args_accepts_config_command(self) -> None:
        args = parse_args(["config", "--defaults", "--force"])

        command = args.command
        self.assertIsInstance(command, ConfigCommand)
        assert isinstance(command, ConfigCommand)
        self.assertTrue(command.defaults)
        self.assertTrue(command.force)

    def test_parse_args_accepts_install_browser_command(self) -> None:
        args = parse_args(["install-browser"])

        self.assertIsInstance(args.command, InstallBrowserCommand)

    def test_parse_args_defaults_reset_to_all(self) -> None:
        args = parse_args(["reset", "--yes"])

        command = args.command
        self.assertIsInstance(command, ResetCommand)
        assert isinstance(command, ResetCommand)
        self.assertEqual(command.target, "all")
        self.assertTrue(command.yes)

    def test_parse_args_accepts_refresh_command(self) -> None:
        args = parse_args(["refresh", "--after", "2026-01-02", "--reset", "--headed"])

        command = args.command
        self.assertIsInstance(command, RefreshCommand)
        assert isinstance(command, RefreshCommand)
        self.assertEqual(command.after, "2026-01-02")
        self.assertTrue(command.reset)
        self.assertTrue(command.headed)

    def test_parse_args_accepts_pull_overrides(self) -> None:
        args = parse_args(
            [
                "--browser-profile-dir",
                "profile-override",
                "pull",
                "--after",
                "2026-01-02T03:04:05-08:00",
                "--before",
                "2026-01-03T03:04:05-08:00",
                "--download-dir",
                "archive",
                "--concurrency",
                "8",
                "--enrichment-concurrency",
                "11",
                "--headed",
                "--no-enrich-metadata",
            ]
        )

        self.assertEqual(args.browser_profile_dir, "profile-override")
        command = args.command
        self.assertIsInstance(command, PullCommand)
        assert isinstance(command, PullCommand)
        self.assertEqual(command.after, "2026-01-02T03:04:05-08:00")
        self.assertEqual(command.before, "2026-01-03T03:04:05-08:00")
        self.assertEqual(command.download_dir, "archive")
        self.assertEqual(command.concurrency, 8)
        self.assertEqual(command.enrichment_concurrency, 11)
        self.assertTrue(command.headed)
        self.assertFalse(command.enrich_metadata)
        self.assertTrue(command.no_enrich_metadata)

    def test_main_runs_login_command(self) -> None:
        config = ProjectConfig.from_sources(config_path=MISSING_CONFIG_PATH)
        service = MagicMock()
        service.login.return_value = ["Opened persistent browser profile"]

        with (
            patch("gphoto_pull.cli._load_config", return_value=config),
            patch("gphoto_pull.cli.GooglePhotosPuller", return_value=service),
            io.StringIO() as stdout,
            redirect_stdout(stdout),
        ):
            exit_code = main(["login"])
            rendered = stdout.getvalue()

        self.assertEqual(exit_code, 0)
        self.assertIn("Session capture", rendered)
        self.assertIn("Opened persistent browser profile", rendered)

    def test_main_reports_browser_session_errors(self) -> None:
        config = ProjectConfig.from_sources(config_path=MISSING_CONFIG_PATH)
        service = MagicMock()
        service.login.side_effect = BrowserSessionError("missing chromium")

        with (
            patch("gphoto_pull.cli._load_config", return_value=config),
            patch("gphoto_pull.cli.GooglePhotosPuller", return_value=service),
            io.StringIO() as stdout,
            redirect_stdout(stdout),
            io.StringIO() as stderr,
            redirect_stderr(stderr),
        ):
            exit_code = main(["login"])
            rendered = stderr.getvalue()

        self.assertEqual(exit_code, 3)
        self.assertIn("Browser session error: missing chromium", rendered)

    def test_main_returns_interrupted_exit_code(self) -> None:
        config = ProjectConfig.from_sources(config_path=MISSING_CONFIG_PATH)
        service = MagicMock()
        service.pull.side_effect = KeyboardInterrupt()

        with (
            patch("gphoto_pull.cli._load_config", return_value=config),
            patch("gphoto_pull.cli.GooglePhotosPuller", return_value=service),
            io.StringIO() as stderr,
            redirect_stderr(stderr),
        ):
            exit_code = main(["pull"])
            rendered = stderr.getvalue()

        self.assertEqual(exit_code, 130)
        self.assertIn("Interrupted.", rendered)

    def test_main_applies_pull_cli_overrides(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            with (
                patch("gphoto_pull.cli.GooglePhotosPuller") as puller_class,
                io.StringIO() as stderr,
                redirect_stderr(stderr),
            ):
                service = MagicMock()
                service.pull.return_value = ["planned"]
                puller_class.return_value = service

                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "pull",
                        "--after",
                        "2026-01-02T03:04:05-08:00",
                        "--before",
                        "2026-01-03T03:04:05-08:00",
                        "--download-dir",
                        "archive",
                        "--concurrency",
                        "8",
                        "--enrichment-concurrency",
                        "11",
                        "--headed",
                        "--no-enrich-metadata",
                        "--dry-run",
                    ]
                )

            self.assertEqual(exit_code, 0)
            created_config = puller_class.call_args.args[0]
            self.assertEqual(created_config.config_dir, config_path.parent)
            self.assertEqual(created_config.download_dir, Path.cwd() / "archive")
            self.assertEqual(created_config.download_concurrency, 8)
            self.assertEqual(created_config.enrichment_concurrency, 11)
            self.assertFalse(created_config.headless)
            self.assertFalse(created_config.enrich_metadata)
            assert created_config.after is not None
            assert created_config.before is not None
            self.assertEqual(created_config.after.isoformat(), "2026-01-02T03:04:05-08:00")
            self.assertEqual(created_config.before.isoformat(), "2026-01-03T03:04:05-08:00")

    def test_main_uses_relative_config_path_once(self) -> None:
        original_cwd = Path.cwd()
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            config_path = root / "configs" / "dev.toml"
            config_path.parent.mkdir()
            config_path.write_text('sync_db_path = "state.sqlite3"\n', encoding="utf-8")
            with (
                patch("gphoto_pull.cli.GooglePhotosPuller") as puller_class,
                io.StringIO() as stderr,
                redirect_stderr(stderr),
            ):
                service = MagicMock()
                service.pull.return_value = ["planned"]
                puller_class.return_value = service
                try:
                    os.chdir(root)
                    exit_code = main(["--config", "configs/dev.toml", "pull", "--dry-run"])
                finally:
                    os.chdir(original_cwd)

            created_config = puller_class.call_args.args[0]

        self.assertEqual(exit_code, 0)
        self.assertEqual(created_config.config_file, Path("configs/dev.toml"))
        self.assertTrue(created_config.config_file_loaded)
        self.assertEqual(created_config.sync_db_path, Path("configs/state.sqlite3"))

    def test_main_runs_refresh_reset_command(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            index_path = Path(tmp_dir) / "state.sqlite3"
            config_path.write_text('sync_db_path = "state.sqlite3"\n', encoding="utf-8")
            index_path.write_text("index", encoding="utf-8")
            with (
                patch("gphoto_pull.cli.GooglePhotosPuller") as puller_class,
                io.StringIO() as stderr,
                redirect_stderr(stderr),
            ):
                service = MagicMock()
                service.refresh.return_value = ["refreshed"]
                puller_class.return_value = service

                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "refresh",
                        "--after",
                        "2026-01-02T03:04:05-08:00",
                        "--reset",
                    ]
                )
                rendered = stderr.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertFalse(index_path.exists())
            service.refresh.assert_called_once()
            self.assertIn("Deleted media index:", rendered)
            self.assertIn("Refresh result", rendered)

    def test_config_command_writes_default_toml(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"

            with (
                io.StringIO() as stdout,
                redirect_stdout(stdout),
            ):
                exit_code = main(["--config", str(config_path), "config", "--defaults"])
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertTrue(config_path.exists())
            contents = config_path.read_text(encoding="utf-8")
            self.assertNotIn("download_dir =", contents)
            self.assertIn('browsers_path = "browsers"', contents)
            self.assertIn(
                'browser_profile_dir = "chrome-profile"',
                contents,
            )
            self.assertIn("headless = true", contents)
            self.assertIn("enrichment_concurrency = 5", contents)
            self.assertIn("enrich_metadata = true", contents)
            self.assertNotIn("browser_binary =", contents)
            self.assertIn("gphoto-pull install-browser", rendered)

    def test_install_browser_command_runs_playwright_installer(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            config_path.write_text('browsers_path = "pw-browsers"\n', encoding="utf-8")

            with (
                patch("gphoto_pull.cli.subprocess.run") as run,
                io.StringIO() as stdout,
                redirect_stdout(stdout),
            ):
                run.return_value.returncode = 0
                exit_code = main(["--config", str(config_path), "install-browser"])
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertIn(str(config_path.parent / "pw-browsers"), rendered)
            run.assert_called_once()
            self.assertEqual(
                run.call_args.args[0],
                [sys.executable, "-m", "playwright", "install", "chromium"],
            )
            self.assertEqual(
                run.call_args.kwargs["env"]["PLAYWRIGHT_BROWSERS_PATH"],
                str(config_path.parent / "pw-browsers"),
            )

    def test_config_command_prompts_for_values(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            answers = iter(
                [
                    "8",
                    "12",
                    "state.sqlite3",
                    "diag",
                    "pw-browsers",
                    "profile-override",
                    "n",
                    "n",
                    "2026-01-02T03:04:05-08:00",
                    "2026-01-03T03:04:05-08:00",
                    "custom-chromium",
                ]
            )

            def answer_prompt(prompt: str) -> str:
                del prompt
                return next(answers)

            with (
                patch("builtins.input", side_effect=answer_prompt),
                io.StringIO() as stdout,
                redirect_stdout(stdout),
            ):
                exit_code = main(["--config", str(config_path), "config"])

            self.assertEqual(exit_code, 0)
            contents = config_path.read_text(encoding="utf-8")
            self.assertNotIn("download_dir =", contents)
            self.assertIn("download_concurrency = 8", contents)
            self.assertIn("enrichment_concurrency = 12", contents)
            self.assertIn('sync_db_path = "state.sqlite3"', contents)
            self.assertIn('diagnostics_dir = "diag"', contents)
            self.assertIn('browsers_path = "pw-browsers"', contents)
            self.assertIn('browser_profile_dir = "profile-override"', contents)
            self.assertIn("headless = false", contents)
            self.assertIn("enrich_metadata = false", contents)
            self.assertIn('after = "2026-01-02T03:04:05-08:00"', contents)
            self.assertIn('before = "2026-01-03T03:04:05-08:00"', contents)
            self.assertIn('browser_binary = "custom-chromium"', contents)

    def test_config_command_does_not_overwrite_without_confirmation(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            config_path.write_text("existing = true\n", encoding="utf-8")

            with (
                patch("builtins.input", return_value=""),
                io.StringIO() as stdout,
                redirect_stdout(stdout),
            ):
                exit_code = main(["--config", str(config_path), "config", "--defaults"])
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertEqual(config_path.read_text(encoding="utf-8"), "existing = true\n")
            self.assertIn("Left existing config unchanged", rendered)

    def test_reset_command_defaults_to_profile_and_index(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            profile_dir = Path(tmp_dir) / "chrome-profile"
            browsers_dir = Path(tmp_dir) / "browsers"
            index_path = Path(tmp_dir) / "state" / "pull-state.sqlite3"
            profile_dir.mkdir()
            browsers_dir.mkdir()
            index_path.parent.mkdir()
            index_path.write_text("index", encoding="utf-8")
            config_path.write_text(
                "\n".join(
                    [
                        'browser_profile_dir = "chrome-profile"',
                        'browsers_path = "browsers"',
                        'sync_db_path = "state/pull-state.sqlite3"',
                    ]
                ),
                encoding="utf-8",
            )

            with io.StringIO() as stdout, redirect_stdout(stdout):
                exit_code = main(["--config", str(config_path), "reset", "--yes"])
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertFalse(profile_dir.exists())
            self.assertFalse(index_path.exists())
            self.assertTrue(browsers_dir.exists())
            self.assertIn("Deleted:", rendered)
            self.assertIn("gphoto-pull login", rendered)

    def test_reset_index_removes_media_index_only(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            profile_dir = Path(tmp_dir) / "chrome-profile"
            index_path = Path(tmp_dir) / "state" / "pull-state.sqlite3"
            profile_dir.mkdir()
            index_path.parent.mkdir()
            index_path.write_text("index", encoding="utf-8")
            config_path.write_text(
                "\n".join(
                    [
                        'browser_profile_dir = "chrome-profile"',
                        'sync_db_path = "state/pull-state.sqlite3"',
                    ]
                ),
                encoding="utf-8",
            )

            with io.StringIO() as stdout, redirect_stdout(stdout):
                exit_code = main(
                    ["--config", str(config_path), "reset", "--target", "index", "--yes"]
                )
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertTrue(profile_dir.exists())
            self.assertFalse(index_path.exists())
            self.assertIn("Deleted:", rendered)

    def test_reset_browser_removes_profile_and_browser_install(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            profile_dir = Path(tmp_dir) / "chrome-profile"
            browsers_dir = Path(tmp_dir) / "browsers"
            profile_dir.mkdir()
            browsers_dir.mkdir()
            config_path.write_text(
                "\n".join(
                    [
                        'browser_profile_dir = "chrome-profile"',
                        'browsers_path = "browsers"',
                    ]
                ),
                encoding="utf-8",
            )

            with io.StringIO() as stdout, redirect_stdout(stdout):
                exit_code = main(
                    ["--config", str(config_path), "reset", "--target", "browser", "--yes"]
                )
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertFalse(profile_dir.exists())
            self.assertFalse(browsers_dir.exists())
            self.assertIn("gphoto-pull install-browser", rendered)
            self.assertIn("gphoto-pull login", rendered)

    def test_reset_command_requires_confirmation_without_yes(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            profile_dir = Path(tmp_dir) / "chrome-profile"
            profile_dir.mkdir()
            config_path.write_text('browser_profile_dir = "chrome-profile"\n', encoding="utf-8")

            with (
                patch("builtins.input", return_value=""),
                io.StringIO() as stdout,
                redirect_stdout(stdout),
            ):
                exit_code = main(["--config", str(config_path), "reset", "--target", "profile"])
                rendered = stdout.getvalue()

            self.assertEqual(exit_code, 0)
            self.assertTrue(profile_dir.exists())
            self.assertIn("Reset cancelled.", rendered)

    def test_reset_command_rejects_unknown_target(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"

            with io.StringIO() as stderr, redirect_stderr(stderr):
                exit_code = main(["--config", str(config_path), "reset", "--target", "state"])
                rendered = stderr.getvalue()

            self.assertEqual(exit_code, 2)
            self.assertIn("reset target", rendered)


class IntegrationWorkflowTests(unittest.TestCase):
    def test_doctor_reports_persistent_profile_strategy(self) -> None:
        config = ProjectConfig.from_sources(
            config_path=MISSING_CONFIG_PATH,
            overrides=ConfigOverrides(browser_profile_dir="gphoto-pull-profile"),
        )

        with (
            patch("gphoto_pull.automation.collect_browser_checks", return_value=[]),
            io.StringIO() as stdout,
            redirect_stdout(stdout),
        ):
            with patch("gphoto_pull.cli._load_config", return_value=config):
                exit_code = main(["doctor"])
            rendered = stdout.getvalue()

        self.assertEqual(exit_code, 0)
        self.assertIn("login/session strategy", rendered)
        self.assertIn("gphoto-pull-profile", rendered)
        self.assertNotIn("CDP", rendered)

    def test_login_uses_persistent_profile_launch(self) -> None:
        config = ProjectConfig.from_sources(
            config_path=MISSING_CONFIG_PATH,
            overrides=ConfigOverrides(browser_profile_dir="gphoto-pull-profile"),
        )

        with patch(
            "gphoto_pull.automation.interactive_login",
            return_value=Path("gphoto-pull-profile"),
        ) as login:
            service = GooglePhotosPuller(config)
            lines = service.login()

        login.assert_called_once_with(
            BrowserSessionPaths(
                download_dir=config.download_dir,
                profile_dir=config.browser_profile_dir,
                diagnostics_dir=config.diagnostics_dir,
                browsers_path=config.browsers_path,
            ),
            browser_binary=None,
            start_url=(
                "https://accounts.google.com/ServiceLogin"
                "?continue=https%3A%2F%2Fphotos.google.com%2F"
            ),
        )
        self.assertIn("Opened persistent browser profile", lines[0])
        self.assertIn("stored in that browser profile", lines[1])

    def test_pull_dry_run_reports_persistent_profile_strategy(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config = ProjectConfig.from_sources(
                config_path=MISSING_CONFIG_PATH,
                overrides=ConfigOverrides(
                    after="2026-01-02T03:04:05-08:00",
                    sync_db_path=Path(tmp_dir) / "pull-state.sqlite3",
                    browser_profile_dir=Path(tmp_dir) / "chrome-profile",
                ),
            )

            with (
                patch("gphoto_pull.cli._load_config", return_value=config),
                io.StringIO() as stderr,
                redirect_stderr(stderr),
            ):
                exit_code = main(["pull", "--dry-run"])
                rendered = stderr.getvalue()

        self.assertEqual(exit_code, 0)
        self.assertIn("Sync DB path:", rendered)
        self.assertIn("Browser profile dir:", rendered)
        self.assertIn("Login/session strategy:", rendered)
        self.assertIn("Headless browser for pull: True", rendered)
        self.assertIn("Photos UI scaffold is active", rendered)
        self.assertIn("Known surface: Updates -> updates /updates", rendered)
        self.assertIn("Saved updates artifact summary:", rendered)
        self.assertIn("Dry-run enumeration candidates persisted:", rendered)
        self.assertIn("Dry-run exact uploaded-time candidates:", rendered)

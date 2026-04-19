import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from gphoto_pull.browser import BrowserSessionPaths
from gphoto_pull.config import ConfigError, ConfigOverrides, ProjectConfig


class ProjectConfigTests(unittest.TestCase):
    def test_from_sources_loads_toml_config_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            config_path.write_text(
                "\n".join(
                    [
                        'after = "2026-01-02T03:04:05-08:00"',
                        'before = "2026-01-03T03:04:05-08:00"',
                        f'download_dir = "{Path("~/gphoto-pull-archive")}"',
                        "download_concurrency = 6",
                        'sync_db_path = "tmp/pull-state.sqlite3"',
                        'diagnostics_dir = "tmp/diagnostics"',
                        'browsers_path = "tmp/playwright"',
                        'browser_binary = "custom-chromium"',
                        'browser_profile_dir = "~/profiles/gphoto-pull"',
                        "headless = false",
                    ]
                ),
                encoding="utf-8",
            )

            config = ProjectConfig.from_sources(config_path=config_path)

            assert config.after is not None
            assert config.before is not None
            self.assertEqual(config.after.isoformat(), "2026-01-02T03:04:05-08:00")
            self.assertEqual(config.before.isoformat(), "2026-01-03T03:04:05-08:00")
            self.assertEqual(config.config_dir, config_path.parent)
            self.assertEqual(config.download_dir, Path("~/gphoto-pull-archive").expanduser())
            self.assertEqual(config.download_concurrency, 6)
            self.assertEqual(config.sync_db_path, config_path.parent / "tmp/pull-state.sqlite3")
            self.assertEqual(config.diagnostics_dir, config_path.parent / "tmp/diagnostics")
            self.assertEqual(config.browsers_path, config_path.parent / "tmp/playwright")
            self.assertEqual(config.browser_binary, "custom-chromium")
            self.assertEqual(
                config.browser_profile_dir,
                Path("~/profiles/gphoto-pull").expanduser(),
            )
            self.assertFalse(config.headless)
            self.assertTrue(config.config_file_loaded)

            browser_paths = BrowserSessionPaths(
                download_dir=config.download_dir,
                profile_dir=config.browser_profile_dir,
                diagnostics_dir=config.diagnostics_dir,
                browsers_path=config.browsers_path,
            )
            self.assertEqual(
                browser_paths.download_dir,
                Path("~/gphoto-pull-archive").expanduser(),
            )
            self.assertEqual(browser_paths.profile_dir, Path("~/profiles/gphoto-pull").expanduser())
            self.assertEqual(browser_paths.diagnostics_dir, config_path.parent / "tmp/diagnostics")
            self.assertEqual(browser_paths.browsers_path, config_path.parent / "tmp/playwright")

    def test_from_sources_applies_cli_overrides(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            config_path.write_text(
                "\n".join(
                    [
                        'after = "2026-01-02T03:04:05-08:00"',
                        f'download_dir = "{Path("~/gphoto-pull-archive")}"',
                        "download_concurrency = 2",
                    ]
                ),
                encoding="utf-8",
            )

            config = ProjectConfig.from_sources(
                config_path=config_path,
                overrides=ConfigOverrides(
                    after="2026-02-03T04:05:06-08:00",
                    before="2026-02-04T04:05:06-08:00",
                    download_dir="override-downloads",
                    download_concurrency=8,
                    headless=False,
                ),
            )

            assert config.after is not None
            assert config.before is not None
            self.assertEqual(config.after.isoformat(), "2026-02-03T04:05:06-08:00")
            self.assertEqual(config.before.isoformat(), "2026-02-04T04:05:06-08:00")
            self.assertEqual(config.download_dir, Path.cwd() / "override-downloads")
            self.assertEqual(config.download_concurrency, 8)
            self.assertFalse(config.headless)

    def test_from_sources_uses_current_year_for_dates_without_year(self) -> None:
        config = ProjectConfig.from_sources(
            overrides=ConfigOverrides(after="april 17", before="april 18"),
        )

        assert config.after is not None
        assert config.before is not None
        self.assertEqual(config.after.year, datetime.now().year)
        self.assertEqual(config.after.month, 4)
        self.assertEqual(config.after.day, 17)
        self.assertEqual(config.after.hour, 0)
        self.assertEqual(config.before.month, 4)
        self.assertEqual(config.before.day, 18)

    def test_from_sources_rejects_before_on_or_before_after(self) -> None:
        with self.assertRaisesRegex(ConfigError, "before.*after"):
            ProjectConfig.from_sources(
                overrides=ConfigOverrides(after="april 18", before="april 18"),
            )

    def test_from_sources_rejects_relative_download_dir_in_config_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            config_path.write_text('download_dir = "downloads"\n', encoding="utf-8")

            with self.assertRaisesRegex(ConfigError, "download_dir.*absolute"):
                ProjectConfig.from_sources(config_path=config_path)

    def test_from_sources_defaults_download_concurrency_to_four(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_dir = Path(tmp_dir)
            config = ProjectConfig.from_sources(config_dir=config_dir)

            self.assertEqual(config.download_concurrency, 4)
            self.assertTrue(config.headless)
            self.assertEqual(config.download_dir, Path.cwd())
            self.assertEqual(config.sync_db_path, config_dir / "state/pull-state.sqlite3")
            self.assertEqual(config.diagnostics_dir, config_dir / "diagnostics")
            self.assertEqual(config.browsers_path, config_dir / "browsers")
            self.assertEqual(config.browser_profile_dir, config_dir / "chrome-profile")

    def test_from_sources_rejects_non_positive_download_concurrency(self) -> None:
        with (
            self.assertRaisesRegex(
                ConfigError,
                "download_concurrency must be greater than zero",
            ),
            TemporaryDirectory() as tmp_dir,
        ):
            ProjectConfig.from_sources(
                config_dir=Path(tmp_dir),
                overrides=ConfigOverrides(download_concurrency=0),
            )

    def test_from_sources_rejects_non_integer_download_concurrency(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "gphoto-pull.toml"
            config_path.write_text('download_concurrency = "four"', encoding="utf-8")

            with self.assertRaisesRegex(
                ConfigError,
                "Expected `int \\| null`, got `str`",
            ):
                ProjectConfig.from_sources(config_path=config_path)

    def test_from_sources_allows_browser_path_overrides(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_dir = Path(tmp_dir)
            config = ProjectConfig.from_sources(
                config_dir=config_dir,
                overrides=ConfigOverrides(
                    sync_db_path="tmp/pull-state.sqlite3",
                    diagnostics_dir="tmp/diagnostics",
                    browsers_path="tmp/playwright",
                    browser_profile_dir="tmp/chrome-profile",
                ),
            )

            self.assertEqual(config.sync_db_path, config_dir / "tmp/pull-state.sqlite3")
            self.assertEqual(config.diagnostics_dir, config_dir / "tmp/diagnostics")
            self.assertEqual(config.browsers_path, config_dir / "tmp/playwright")
            self.assertEqual(config.browser_profile_dir, config_dir / "tmp/chrome-profile")

# pyright: reportPrivateUsage=false

import asyncio
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace, TracebackType
from typing import cast
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import parse_qs, urlparse

from playwright.async_api import Playwright as AsyncPlaywright

from gphoto_pull.browser import (
    DEFAULT_BROWSER_URL,
    BrowserSessionPaths,
    _cleanup_stale_chromium_singleton_files,
    _launch_persistent_context_async,
    browser_binaries_available,
    browser_profile_marked_logged_in,
    collect_browser_checks,
    interactive_login,
    mark_browser_profile_logged_in,
)


class _FakeLoginProcess:
    def __init__(self, *, running: bool) -> None:
        self.running = running
        self.pid = 12345
        self.terminate = Mock()
        self.wait = Mock()

    def poll(self) -> int | None:
        return None if self.running else 0


class BrowserSessionPathsTests(unittest.TestCase):
    def _install_fake_chromium(self, paths: BrowserSessionPaths) -> None:
        executable = paths.browsers_path / "fake-platform" / "chromium"
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_text("#!/bin/sh\n", encoding="utf-8")

    def _install_fake_playwright(
        self,
        *,
        executable_path: Path | None = None,
    ) -> None:
        class FakeChromium:
            @property
            def executable_path(self) -> str:
                return str(executable_path or Path("/missing/fake-platform/chromium"))

        class FakePlaywrightContextManager:
            def __enter__(self) -> SimpleNamespace:
                return SimpleNamespace(chromium=FakeChromium())

            def __exit__(
                self,
                _exc_type: type[BaseException] | None,
                _exc: BaseException | None,
                _tb: TracebackType | None,
            ) -> bool:
                return False

        sync_api = ModuleType("playwright.sync_api")
        sync_api.__dict__["Error"] = RuntimeError
        sync_api.__dict__["sync_playwright"] = FakePlaywrightContextManager

        playwright = ModuleType("playwright")
        playwright.__dict__["sync_api"] = sync_api

        patcher = patch.dict(
            sys.modules,
            {"playwright": playwright, "playwright.sync_api": sync_api},
            clear=False,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_ensure_runtime_directories_creates_expected_paths(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                diagnostics_dir=root / ".state" / "diagnostics",
                browsers_path=root / ".playwright",
            )

            paths.ensure_runtime_directories()

            self.assertTrue(paths.download_dir.exists())
            self.assertTrue(paths.profile_dir.exists())
            self.assertTrue(paths.diagnostics_dir.exists())
            self.assertTrue(paths.browsers_path.exists())

    def test_login_marker_tracks_completed_profile_login(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            profile_dir = Path(tmp_dir) / "chrome-profile"

            self.assertFalse(browser_profile_marked_logged_in(profile_dir))

            mark_browser_profile_logged_in(profile_dir)

            self.assertTrue(browser_profile_marked_logged_in(profile_dir))

    def test_browser_binaries_available_reports_missing_install(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            expected_executable = Path(tmp_dir) / ".playwright" / "fake-platform" / "chromium"
            self._install_fake_playwright(executable_path=expected_executable)

            ok, detail = browser_binaries_available(Path(tmp_dir) / ".playwright")

            self.assertFalse(ok)
            self.assertIn("expected Chromium executable is missing", detail)

    def test_collect_browser_checks_reports_expected_names(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                diagnostics_dir=root / ".state" / "diagnostics",
                browsers_path=root / ".playwright",
            )
            self._install_fake_chromium(paths)
            self._install_fake_playwright(
                executable_path=paths.browsers_path / "fake-platform" / "chromium"
            )

            checks = collect_browser_checks(paths)

            check_names = [check.name for check in checks]
            self.assertEqual(
                check_names,
                [
                    "binaries",
                    "profile",
                    "runtime directories",
                ],
            )
            self.assertTrue(all(check.ok for check in checks))
            self.assertEqual(checks[1].detail, str(paths.profile_dir))

    def test_cleanup_stale_chromium_singleton_files_removes_dead_local_lock(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            profile_dir = Path(tmp_dir)
            (profile_dir / "SingletonLock").symlink_to("test-host-123")
            (profile_dir / "SingletonSocket").symlink_to("/tmp/missing-socket")
            (profile_dir / "SingletonCookie").symlink_to("cookie")

            with (
                patch("gphoto_pull.browser._current_hostname", return_value="test-host"),
                patch("gphoto_pull.browser._process_is_running", return_value=False),
            ):
                _cleanup_stale_chromium_singleton_files(profile_dir)

            self.assertFalse((profile_dir / "SingletonLock").is_symlink())
            self.assertFalse((profile_dir / "SingletonSocket").is_symlink())
            self.assertFalse((profile_dir / "SingletonCookie").is_symlink())

    def test_cleanup_stale_chromium_singleton_files_keeps_live_local_lock(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            profile_dir = Path(tmp_dir)
            (profile_dir / "SingletonLock").symlink_to("test-host-123")
            (profile_dir / "SingletonSocket").symlink_to("/tmp/live-socket")
            (profile_dir / "SingletonCookie").symlink_to("cookie")

            with (
                patch("gphoto_pull.browser._current_hostname", return_value="test-host"),
                patch("gphoto_pull.browser._process_is_running", return_value=True),
            ):
                _cleanup_stale_chromium_singleton_files(profile_dir)

            self.assertTrue((profile_dir / "SingletonLock").is_symlink())
            self.assertTrue((profile_dir / "SingletonSocket").is_symlink())
            self.assertTrue((profile_dir / "SingletonCookie").is_symlink())

    def test_interactive_login_launches_browser_without_playwright_control(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                browsers_path=root / ".playwright",
            )
            self._install_fake_chromium(paths)
            executable_path = paths.browsers_path / "fake-platform" / "chromium"
            self._install_fake_playwright(executable_path=executable_path)
            process = _FakeLoginProcess(running=True)

            with (
                patch("gphoto_pull.browser.os.geteuid", return_value=1000),
                patch("gphoto_pull.browser.subprocess.Popen", return_value=process) as popen,
                patch("builtins.input", return_value=""),
                patch("builtins.print"),
            ):
                result = interactive_login(paths)

            self.assertEqual(result, paths.profile_dir)
            self.assertGreaterEqual(popen.call_count, 1)
            command = popen.call_args.args[0]
            self.assertEqual(command[0], str(executable_path))
            self.assertIn(f"--user-data-dir={paths.profile_dir}", command)
            self.assertIn("--new-window", command)
            self.assertIn("--allow-browser-signin=false", command)
            self.assertNotIn("--no-sandbox", command)
            self.assertIn("--password-store=basic", command)
            self.assertIn("--use-mock-keychain", command)
            process.terminate.assert_called_once_with()
            process.wait.assert_called_once_with(timeout=10)

            login_url = str(command[-1])
            parsed_url = urlparse(login_url)
            self.assertEqual(parsed_url.scheme, "https")
            self.assertEqual(parsed_url.netloc, "accounts.google.com")
            self.assertEqual(parsed_url.path, "/ServiceLogin")
            self.assertEqual(parse_qs(parsed_url.query), {"continue": [DEFAULT_BROWSER_URL]})

    def test_interactive_login_accepts_custom_start_url(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                browsers_path=root / ".playwright",
            )
            self._install_fake_chromium(paths)
            self._install_fake_playwright(
                executable_path=paths.browsers_path / "fake-platform" / "chromium"
            )
            process = _FakeLoginProcess(running=False)

            with (
                patch("gphoto_pull.browser.os.geteuid", return_value=1000),
                patch("gphoto_pull.browser.subprocess.Popen", return_value=process) as popen,
                patch("builtins.input", return_value=""),
                patch("builtins.print"),
            ):
                interactive_login(paths, start_url="https://accounts.google.com/custom")

            command = popen.call_args.args[0]
            self.assertEqual(command[-1], "https://accounts.google.com/custom")
            process.terminate.assert_not_called()
            process.wait.assert_not_called()

    def test_interactive_login_disables_sandbox_when_running_as_root(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                browsers_path=root / ".playwright",
            )
            self._install_fake_chromium(paths)
            self._install_fake_playwright(
                executable_path=paths.browsers_path / "fake-platform" / "chromium"
            )
            process = _FakeLoginProcess(running=False)

            with (
                patch("gphoto_pull.browser.os.geteuid", return_value=0),
                patch("gphoto_pull.browser.subprocess.Popen", return_value=process) as popen,
                patch("builtins.input", return_value=""),
                patch("builtins.print"),
            ):
                interactive_login(paths)

            command = popen.call_args.args[0]
            self.assertIn("--no-sandbox", command)

    def test_persistent_context_disables_browser_signin(self) -> None:
        class AsyncChromium:
            def __init__(self) -> None:
                self.launch_persistent_context = AsyncMock(return_value=object())

        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                browsers_path=root / ".playwright",
            )
            chromium = AsyncChromium()
            playwright = SimpleNamespace(chromium=chromium)

            with patch("gphoto_pull.browser.os.geteuid", return_value=1000):
                result = asyncio.run(
                    _launch_persistent_context_async(
                        cast("AsyncPlaywright", playwright),
                        paths,
                        headless=True,
                        browser_binary=None,
                    )
                )

            self.assertIsNotNone(result)
            chromium.launch_persistent_context.assert_called_once()
            args = chromium.launch_persistent_context.call_args.kwargs["args"]
            self.assertIn("--allow-browser-signin=false", args)
            self.assertNotIn("--no-sandbox", args)

    def test_persistent_context_disables_sandbox_when_running_as_root(self) -> None:
        class AsyncChromium:
            def __init__(self) -> None:
                self.launch_persistent_context = AsyncMock(return_value=object())

        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            paths = BrowserSessionPaths(
                download_dir=root / "downloads",
                profile_dir=root / "chrome-profile",
                browsers_path=root / ".playwright",
            )
            chromium = AsyncChromium()
            playwright = SimpleNamespace(chromium=chromium)

            with patch("gphoto_pull.browser.os.geteuid", return_value=0):
                result = asyncio.run(
                    _launch_persistent_context_async(
                        cast("AsyncPlaywright", playwright),
                        paths,
                        headless=True,
                        browser_binary=None,
                    )
                )

            self.assertIsNotNone(result)
            chromium.launch_persistent_context.assert_called_once()
            self.assertIn(
                "--no-sandbox",
                chromium.launch_persistent_context.call_args.kwargs["args"],
            )

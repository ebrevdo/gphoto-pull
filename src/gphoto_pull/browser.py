"""Playwright browser lifecycle helpers for login and pull sessions.

Description:
    Owns persistent Chromium profile paths, browser prerequisite checks, and
    headed/headless Playwright context startup.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Event, Thread
from typing import TYPE_CHECKING
from urllib.parse import urlencode

DEFAULT_BROWSER_URL = "https://photos.google.com/"
LOGIN_MARKER_FILENAME = ".gphoto-pull-login-complete"
CHROMIUM_PROFILE_COMPAT_ARGS = ("--allow-browser-signin=false",)
LOGIN_PROFILE_COMPAT_ARGS = (
    "--password-store=basic",
    "--use-mock-keychain",
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from playwright.async_api import BrowserContext as AsyncBrowserContext
    from playwright.async_api import Playwright as AsyncPlaywright
    from playwright.sync_api import Playwright


class BrowserSessionError(RuntimeError):
    """Browser setup or login failure.

    Description:
        Raised when Playwright cannot launch, attach, or keep the persistent
        browser session usable.
    """


@dataclass(slots=True, frozen=True)
class BrowserCheck:
    """Result of a browser prerequisite check.

    Description:
        Represents one line in `gphoto-pull doctor` for browser setup.

    Attributes:
        name: Human-readable check name.
        ok: Whether the prerequisite passed.
        detail: Operator-facing explanation.
    """

    name: str
    ok: bool
    detail: str


@dataclass(slots=True, frozen=True)
class BrowserSessionPaths:
    """Filesystem paths required for Playwright browser sessions.

    Description:
        Groups the paths shared by interactive login and headless pull runs.

    Attributes:
        download_dir: Directory Playwright uses for downloaded artifacts.
        profile_dir: Persistent Chromium profile directory.
        diagnostics_dir: Directory for screenshots, HTML, and network captures.
        browsers_path: Directory containing Playwright-managed browser binaries.
    """

    download_dir: Path
    profile_dir: Path
    diagnostics_dir: Path = Path(".state/diagnostics")
    browsers_path: Path = Path(".playwright")

    def ensure_runtime_directories(self) -> None:
        """Description:
        Create all runtime directories needed by Playwright.
        Side Effects:
            Creates directories on disk.
        """

        for path in (
            self.download_dir,
            self.profile_dir,
            self.diagnostics_dir,
            self.browsers_path,
        ):
            path.mkdir(parents=True, exist_ok=True)


def browser_profile_marked_logged_in(profile_dir: Path) -> bool:
    """Description:
    Check whether this profile completed an interactive login session.

    Args:
        profile_dir: Persistent Chromium profile directory.

    Returns:
        Whether `gphoto-pull login` has marked the profile as login-ready.
    """

    return (profile_dir / LOGIN_MARKER_FILENAME).is_file()


def mark_browser_profile_logged_in(profile_dir: Path) -> None:
    """Description:
    Record that the persistent profile completed an interactive login session.

    Args:
        profile_dir: Persistent Chromium profile directory.

    Side Effects:
        Creates or updates a marker file in the browser profile directory.
    """

    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / LOGIN_MARKER_FILENAME).write_text(f"{time.time():.0f}\n", encoding="utf-8")


@contextmanager
def _managed_sync_playwright() -> Iterator[Playwright]:
    """Description:
    Open Playwright's synchronous driver as a context manager.

    Returns:
        Context manager yielding a Playwright driver.

    Side Effects:
        Starts and stops Playwright's driver process.
    """

    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        yield playwright


def browser_binaries_available(browsers_path: Path) -> tuple[bool, str]:
    """Description:
    Inspect a Playwright browser-install directory for Chromium binaries.

    Args:
        browsers_path: Directory expected to contain Playwright browser installs.

    Returns:
        A success flag and an operator-facing detail string.

    Side Effects:
        Reads directory metadata.
    """

    with _playwright_browsers_path_value(browsers_path):
        try:
            with _managed_sync_playwright() as playwright:
                executable_path = Path(playwright.chromium.executable_path)
        except Exception as exc:
            return False, f"could not resolve Chromium executable: {exc}"

    if executable_path.is_file():
        return True, f"found {executable_path}"
    return False, f"expected Chromium executable is missing: {executable_path}"


def chromium_executable_path(browsers_path: Path) -> Path:
    """Description:
    Resolve Playwright's platform-specific Chromium executable path.

    Args:
        browsers_path: Browser install directory to expose to Playwright.

    Returns:
        Chromium executable path for the current platform.
    """

    with _playwright_browsers_path_value(browsers_path), _managed_sync_playwright() as playwright:
        return Path(playwright.chromium.executable_path)


@contextmanager
def _playwright_browsers_path_value(browsers_path: Path) -> Iterator[None]:
    """Description:
    Temporarily point Playwright at a browser install directory.

    Args:
        browsers_path: Browser install directory to expose to Playwright.

    Returns:
        Context manager that restores the previous environment afterward.

    Side Effects:
        Mutates `PLAYWRIGHT_BROWSERS_PATH` for the duration of the context.
    """

    previous_browser_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(browsers_path)

    try:
        yield
    finally:
        if previous_browser_path is None:
            os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
        else:
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = previous_browser_path


def _require_browser_binaries(paths: BrowserSessionPaths, *, browser_binary: str | None) -> None:
    """Description:
    Fail early when Playwright-managed Chromium has not been installed.

    Args:
        paths: Browser session paths containing the browser install directory.
        browser_binary: Optional externally managed Chromium executable.
    """

    if browser_binary is not None:
        return

    installed, detail = browser_binaries_available(paths.browsers_path)
    if installed:
        return

    raise BrowserSessionError(
        "Playwright Chromium is not installed for this gphoto-pull config. "
        f"Run `gphoto-pull install-browser` first. Browser path: {paths.browsers_path}. "
        f"Detail: {detail}"
    )


def require_browser_binaries(paths: BrowserSessionPaths, *, browser_binary: str | None) -> None:
    """Description:
    Validate browser binaries before entering async Playwright.

    Args:
        paths: Browser session paths containing the browser install directory.
        browser_binary: Optional externally managed Chromium executable.

    Side Effects:
        Resolves the Playwright-managed Chromium executable with the sync API.
    """

    _require_browser_binaries(paths, browser_binary=browser_binary)


def _cleanup_stale_chromium_singleton_files(profile_dir: Path) -> None:
    """Description:
    Remove Chromium profile singleton files when they point at a dead local PID.

    Args:
        profile_dir: Persistent Chromium profile directory.

    Side Effects:
        Removes stale Chromium singleton symlinks from the profile directory.
    """

    owner = _chromium_singleton_owner(profile_dir)
    if owner is None:
        return

    hostname, pid = owner
    if hostname != _current_hostname() or _process_is_running(pid):
        return

    for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        with suppress(FileNotFoundError):
            (profile_dir / name).unlink()


def _chromium_singleton_owner(profile_dir: Path) -> tuple[str, int] | None:
    lock_path = profile_dir / "SingletonLock"
    if not lock_path.is_symlink():
        return None

    try:
        lock_target = str(lock_path.readlink())
    except OSError:
        return None

    hostname, separator, pid_text = lock_target.rpartition("-")
    if not hostname or not separator:
        return None

    try:
        return hostname, int(pid_text)
    except ValueError:
        return None


def _current_hostname() -> str:
    return socket.gethostname()


def _process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@contextmanager
def _playwright_browsers_path(paths: BrowserSessionPaths) -> Iterator[None]:
    """Description:
    Temporarily point Playwright at the configured browser install directory.

    Args:
        paths: Browser session paths containing the browser install directory.

    Returns:
        Context manager that restores the previous environment afterward.

    Side Effects:
        Mutates `PLAYWRIGHT_BROWSERS_PATH` for the duration of the context.
    """

    with _playwright_browsers_path_value(paths.browsers_path):
        yield


def collect_browser_checks(paths: BrowserSessionPaths) -> list[BrowserCheck]:
    """Description:
    Build browser-related `doctor` checks.

    Args:
        paths: Runtime paths to inspect and create.

    Returns:
        Ordered check results for display by the CLI.

    Side Effects:
        Creates missing runtime directories through `BrowserSessionPaths`.
    """

    binaries_ok, binaries_detail = browser_binaries_available(paths.browsers_path)
    checks: list[BrowserCheck] = [
        BrowserCheck(
            name="binaries",
            ok=binaries_ok,
            detail=binaries_detail,
        ),
        BrowserCheck(
            name="profile",
            ok=True,
            detail=str(paths.profile_dir),
        ),
    ]

    try:
        paths.ensure_runtime_directories()
    except OSError as exc:
        checks.append(
            BrowserCheck(
                name="runtime directories",
                ok=False,
                detail=str(exc),
            )
        )
    else:
        checks.append(
            BrowserCheck(
                name="runtime directories",
                ok=True,
                detail=", ".join(
                    str(path)
                    for path in (
                        paths.download_dir,
                        paths.profile_dir,
                        paths.diagnostics_dir,
                        paths.browsers_path,
                    )
                ),
            )
        )

    return checks


def default_login_start_url() -> str:
    """Description:
    Return the Google Accounts URL that continues to Google Photos after login.

    Returns:
        A Google login URL with `continue=https://photos.google.com/`.
    """

    query = urlencode({"continue": DEFAULT_BROWSER_URL})
    return f"https://accounts.google.com/ServiceLogin?{query}"


async def _launch_persistent_context_async(
    playwright: AsyncPlaywright,
    paths: BrowserSessionPaths,
    *,
    headless: bool,
    browser_binary: str | None,
) -> AsyncBrowserContext:
    """Description:
    Launch Chromium with the app-owned persistent profile using async Playwright.

    Args:
        playwright: Active async Playwright driver.
        paths: Runtime browser paths.
        headless: Whether Chromium should run without a visible window.
        browser_binary: Optional Chromium executable override.

    Returns:
        Launched persistent browser context.

    Side Effects:
        Starts a Chromium process and binds downloads to the configured path.
    """

    from playwright.async_api import Error as PlaywrightError

    try:
        if browser_binary is not None:
            return await playwright.chromium.launch_persistent_context(
                user_data_dir=str(paths.profile_dir),
                headless=headless,
                accept_downloads=True,
                downloads_path=str(paths.download_dir),
                handle_sigint=True,
                executable_path=browser_binary,
                args=list(CHROMIUM_PROFILE_COMPAT_ARGS),
            )

        return await playwright.chromium.launch_persistent_context(
            user_data_dir=str(paths.profile_dir),
            headless=headless,
            accept_downloads=True,
            downloads_path=str(paths.download_dir),
            handle_sigint=True,
            args=list(CHROMIUM_PROFILE_COMPAT_ARGS),
        )
    except PlaywrightError as exc:
        raise BrowserSessionError(str(exc)) from exc


def interactive_login(
    paths: BrowserSessionPaths,
    *,
    browser_binary: str | None = None,
    start_url: str | None = None,
) -> Path:
    """Description:
    Launch a headed persistent Chromium profile for manual Google login.

    Args:
        paths: Browser session paths to use for profile, downloads, and binaries.
        browser_binary: Optional Chromium executable override.
        start_url: Optional URL to open instead of the default Google login URL.

    Returns:
        The persistent profile directory used for login.

    Side Effects:
        Creates runtime directories, launches Chromium without attaching Playwright,
        waits for terminal input, and terminates the browser process if it is
        still running.
    """

    paths.ensure_runtime_directories()
    _cleanup_stale_chromium_singleton_files(paths.profile_dir)
    _require_browser_binaries(paths, browser_binary=browser_binary)
    login_start_url = default_login_start_url() if start_url is None else start_url
    executable_path = (
        Path(browser_binary)
        if browser_binary is not None
        else chromium_executable_path(paths.browsers_path)
    )
    command = [
        str(executable_path),
        f"--user-data-dir={paths.profile_dir}",
        "--no-first-run",
        "--new-window",
        *CHROMIUM_PROFILE_COMPAT_ARGS,
        *LOGIN_PROFILE_COMPAT_ARGS,
        login_start_url,
    ]
    login_log_path = paths.diagnostics_dir / "login-browser.log"
    with login_log_path.open("ab") as login_log:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=login_log)
        try:
            print("Complete login and MFA in the opened browser, then press Enter here.")
            _wait_for_login_confirmation(
                process,
                login_log_path=login_log_path,
            )
        finally:
            if process.poll() is None:
                process.terminate()
                with suppress(subprocess.TimeoutExpired):
                    process.wait(timeout=10)

    return paths.profile_dir


def _wait_for_login_confirmation(
    process: subprocess.Popen[bytes],
    *,
    login_log_path: Path,
) -> None:
    """Description:
    Wait for Enter while detecting early browser exit.

    Args:
        process: Login browser subprocess.
        login_log_path: Browser stderr log path for troubleshooting.

    Side Effects:
        Reads one line from stdin on a daemon thread.
    """

    confirmed = Event()

    def wait_for_enter() -> None:
        input()
        confirmed.set()

    Thread(target=wait_for_enter, daemon=True).start()
    while not confirmed.is_set():
        exit_code = process.poll()
        if exit_code is not None:
            raise BrowserSessionError(
                "Login browser exited before confirmation. "
                f"Exit code: {exit_code}. Browser log: {login_log_path}"
            )
        time.sleep(0.25)


@asynccontextmanager
async def launched_browser_context_async(
    paths: BrowserSessionPaths,
    *,
    headless: bool,
    browser_binary: str | None = None,
) -> AsyncIterator[AsyncBrowserContext]:
    """Description:
    Launch the persistent Chromium profile for async automated work.

    Args:
        paths: Browser session paths to use for profile, downloads, and binaries.
        headless: Whether Chromium should run without a visible window.
        browser_binary: Optional Chromium executable override.

    Returns:
        Async context manager yielding a Playwright `BrowserContext`.

    Side Effects:
        Creates runtime directories, temporarily sets `PLAYWRIGHT_BROWSERS_PATH`,
        launches Chromium, and closes the browser context when the manager exits.
    """

    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import async_playwright

    paths.ensure_runtime_directories()
    _cleanup_stale_chromium_singleton_files(paths.profile_dir)

    with _playwright_browsers_path(paths):
        async with async_playwright() as playwright:
            context = await _launch_persistent_context_async(
                playwright,
                paths,
                headless=headless,
                browser_binary=browser_binary,
            )
            try:
                yield context
            finally:
                close_task = asyncio.create_task(context.close())
                try:
                    await asyncio.shield(close_task)
                except (PlaywrightError, asyncio.CancelledError):
                    with suppress(Exception):
                        await close_task

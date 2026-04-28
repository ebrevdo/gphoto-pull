"""High-level Google Photos pull orchestration.

Description:
    Coordinates config, browser probes, enumeration, state updates, and downloads
    for `doctor`, `login`, refresh, and pull commands.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import sys
import urllib.parse
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Never, cast
from urllib.parse import urlsplit, urlunsplit

import msgspec
import msgspec.json

from gphoto_pull.browser import (
    BrowserSessionError,
    BrowserSessionPaths,
    browser_profile_marked_logged_in,
    collect_browser_checks,
    default_login_start_url,
    interactive_login,
    launched_browser_context_async,
    mark_browser_profile_logged_in,
    require_browser_binaries,
)
from gphoto_pull.config import ConfigError, ProjectConfig
from gphoto_pull.detail_payloads import DetailMetadata, parse_detail_metadata
from gphoto_pull.download import (
    DownloadError,
    DownloadPlan,
    create_staging_path,
    finalize_download,
    plan_download_target,
    primary_download_path,
)
from gphoto_pull.enumeration import (
    EnumerationSummary,
    enumerate_index_candidates,
)
from gphoto_pull.interrupts import (
    add_interrupt_callback,
    interrupt_requested,
    raise_if_interrupt_requested,
    remove_interrupt_callback,
)
from gphoto_pull.models import DownloadTrace, MediaMetadata, MediaStateRecord
from gphoto_pull.photos_ui import (
    PHOTOS_APP_ORIGIN,
    RECENTLY_ADDED_URL,
    UPDATES_URL,
    GooglePhotosUi,
    PhotosSurface,
    PhotosUiError,
    classify_photos_url,
)
from gphoto_pull.progress import PullProgressDisplay
from gphoto_pull.rpc_payloads import (
    JsonValue,
    RecentPayload,
    RpcPayloadParseError,
    merge_recent_payloads,
    parse_batchexecute_frames,
    parse_recent_payload,
    parse_updates_payload,
)
from gphoto_pull.state import PullStateStore
from gphoto_pull.takeout import write_takeout_sidecar

LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from playwright.async_api import APIRequestContext
    from playwright.async_api import BrowserContext as AsyncBrowserContext
    from playwright.async_api import Download as AsyncDownload
    from playwright.async_api import Page as AsyncPage
    from playwright.async_api import Request as AsyncRequest
    from playwright.async_api import Response as AsyncResponse
    from playwright.async_api import Route as AsyncRoute

PHASE_LOG_LEVEL = logging.INFO
TIMING_LOG_LEVEL = logging.DEBUG

LOGIN_START_URL = default_login_start_url()
DETAIL_METADATA_ENRICHMENT_TIMEOUT_SECONDS = 15.0
ACCOUNT_SCOPE_HASH_LENGTH = 16
_EMAIL_PATTERN = re.compile(r"[\w.!#$%&'*+/=?^_`{|}~-]+@[\w.-]+\.[A-Za-z]{2,}")


@dataclass(slots=True)
class DoctorCheck:
    """Doctor check result.

    Description:
        Represents one prerequisite or warning line for `gphoto-pull doctor`.

    Attributes:
        name: Check name.
        ok: Whether the check passed.
        detail: Operator-facing detail text.
        warning: Whether a passing check should be displayed as a warning.
    """

    name: str
    ok: bool
    detail: str
    warning: bool = False


_DOWNLOAD_START_ATTEMPTS = 3
_DOWNLOAD_RETRY_DELAY_MS = 500


@dataclass(slots=True, frozen=True)
class PullExecutionSummary:
    """Pull execution counters.

    Description:
        Aggregates the result from one download execution pass.

    Attributes:
        queued_count: Number of queued download candidates.
        skipped_existing_count: Number skipped because final files already exist.
        downloaded_count: Number finalized successfully.
        failed_count: Number that failed.
        failure_media_ids: Failed media ids, in encounter order.
    """

    queued_count: int
    skipped_existing_count: int
    downloaded_count: int
    failed_count: int
    failure_media_ids: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class AccountScope:
    """Account-specific state namespace.

    Description:
        Carries the opaque key used in local account-scoped state paths.

    Attributes:
        key: Filesystem-safe account namespace derived from authenticated Google state.
    """

    key: str


@dataclass(slots=True, frozen=True)
class _DownloadTraceCandidate:
    """Download trace response candidate.

    Description:
        Captures one network response that may describe a Playwright download.

    Attributes:
        request_url: Request URL observed by Playwright.
        response_url: Final response URL.
        content_type: Response content type, when present.
        content_length: Parsed response body size, when present.
        content_disposition: Response disposition header, when present.
    """

    request_url: str
    response_url: str
    content_type: str | None
    content_length: int | None
    content_disposition: str | None


@dataclass(slots=True, frozen=True)
class _PendingDownload:
    """Started but unfinished download.

    Description:
        Tracks a download that has been triggered but not yet saved to the final
        path.

    Attributes:
        slot: Worker slot number that owns this pending download.
        page: Worker page that owns the Playwright download handle.
        record: State record used to start the download.
        metadata: Metadata refreshed with the suggested filename and trace details.
        plan: Final target plan for the artifact.
        download: Playwright download handle.
        download_trace: Captured download network metadata.
        detail_metadata: Detail metadata captured while opening the detail menu.
        queued_at: Monotonic timestamp when the item entered a worker slot.
        start_begin_at: Monotonic timestamp when browser download triggering began.
        download_event_at: Monotonic timestamp when Playwright produced the download handle.
    """

    slot: int
    page: AsyncPage
    record: MediaStateRecord
    metadata: MediaMetadata
    plan: DownloadPlan
    download: AsyncDownload
    download_trace: DownloadTrace
    detail_metadata: DetailMetadata | None
    queued_at: float
    start_begin_at: float
    download_event_at: float


@dataclass(slots=True, frozen=True)
class _EnrichmentJob:
    """Post-download metadata enrichment job.

    Description:
        Carries the finalized media path and metadata needed to rewrite a
        Takeout-style sidecar with Google Photos detail metadata.

    Attributes:
        media_path: Final downloaded media path.
        metadata: Final metadata written with the minimal sidecar.
        queued_at: Monotonic timestamp when enrichment was queued.
    """

    media_path: Path
    metadata: MediaMetadata
    queued_at: float


@dataclass(slots=True)
class _AsyncResponseCapture:
    """Async response capture state.

    Description:
        Tracks fire-and-forget response parsing tasks installed on async
        Playwright pages.

    Attributes:
        response_texts: Unique response bodies accepted by the parser.
        tasks: Pending response-processing tasks.
        accepted_event: Event set when a new unique response body is accepted.
        page: Page whose response listener was registered.
        response_handler: Registered response listener.
    """

    response_texts: list[str]
    tasks: set[asyncio.Task[None]]
    accepted_event: asyncio.Event
    page: AsyncPage
    response_handler: Callable[[AsyncResponse], None]


@dataclass(slots=True, frozen=True)
class _RecentPageRequest:
    """Captured recent-media request data needed for direct pagination.

    Description:
        Holds the endpoint URL and XSRF token from a browser-originated recent
        media request. The RPC id is opaque and captured from the browser rather
        than hardcoded.

    Attributes:
        rpc_id: Opaque RPC id from the browser request.
        url: Batchexecute endpoint URL.
        at_token: XSRF token from the request body.
    """

    rpc_id: str
    url: str
    at_token: str


@dataclass(slots=True, frozen=True)
class _RecentPageCursor:
    """Recently Added pagination cursor.

    Description:
        Couples a next-page cursor to the opaque RPC id whose validated payload
        produced it.

    Attributes:
        rpc_id: Opaque RPC id that yielded the cursor.
        cursor: Opaque next-page cursor.
    """

    rpc_id: str
    cursor: str


class _DownloadTraceJson(msgspec.Struct, frozen=True):
    """Download trace JSON payload.

    Description:
        Serialized network details for one download attempt.

    Attributes:
        download_url: URL reported by Playwright's download handle.
        final_url: Best matched response URL.
        content_type: Response content type.
        content_length: Response content length.
        content_disposition: Response content disposition.
    """

    download_url: str | None
    final_url: str | None
    content_type: str | None
    content_length: int | None
    content_disposition: str | None


class _DownloadTraceArtifact(msgspec.Struct, frozen=True):
    """Download trace artifact JSON payload.

    Description:
        Owned JSON artifact written under diagnostics for endpoint analysis.

    Attributes:
        media_id: Google Photos media key.
        filename: Local filename chosen for the download.
        product_url: Google Photos product/detail URL when known.
        page_url: Direct or detail page URL used to start the download.
        download_trace: Captured download network metadata.
    """

    media_id: str
    filename: str
    product_url: str | None
    page_url: str
    download_trace: _DownloadTraceJson


@dataclass(slots=True)
class _RecentPayloadStats:
    """Running recent payload statistics.

    Description:
        Tracks unique media ids and oldest upload time without reparsing all
        previous response bodies on every page.

    Attributes:
        media_ids: Unique media ids seen so far.
        oldest_upload_time: Oldest upload timestamp seen so far.
        newest_upload_time: Newest upload timestamp seen so far.
    """

    media_ids: set[str]
    oldest_upload_time: datetime | None = None
    newest_upload_time: datetime | None = None

    @property
    def item_count(self) -> int:
        """Description:
        Count unique media ids seen so far.

        Returns:
            Unique media item count.
        """

        return len(self.media_ids)


@dataclass(slots=True, frozen=True)
class _RecentPaginationStart:
    """Starting point for direct recent pagination.

    Description:
        Describes either a normal first-page continuation or a stored checkpoint
        continuation.

    Attributes:
        cursor: Cursor to request next.
        page_count: Page count to use for progress logs.
        from_checkpoint: Whether this start came from a stored checkpoint.
    """

    cursor: _RecentPageCursor
    page_count: int
    from_checkpoint: bool = False


@dataclass(slots=True, frozen=True)
class _RecentPaginationResult:
    """Direct recent pagination outcome.

    Description:
        Separates successful completion from a checkpoint that should be ignored
        for the current run.

    Attributes:
        completed: Whether pagination reached a normal stop condition.
        checkpoint_invalid: Whether a checkpoint path should be retried from
            the normal first-page cursor.
        fetched_response_texts: Raw response texts fetched by this pagination
            attempt, excluding texts that were already in the capture.
    """

    completed: bool
    checkpoint_invalid: bool = False
    fetched_response_texts: tuple[str, ...] = ()


class GooglePhotosPuller:
    """Application service for doctor, login, refresh, and pull commands.

    Description:
        Coordinates configuration, browser lifecycle, Google Photos UI actions,
        enumeration, durable state, and downloads.

    Attributes:
        config: Resolved project configuration.
        photos_ui: UI adapter for Google Photos selectors/actions.
    """

    def __init__(self, config: ProjectConfig) -> None:
        """Description:
        Create a pull service.

        Args:
            config: Resolved project configuration.
        """

        self.config = config
        self.photos_ui = GooglePhotosUi()

    def doctor(self, *, dry_run: bool = False) -> list[DoctorCheck]:
        """Description:
        Collect operator-facing prerequisite checks.

        Args:
            dry_run: Skip live browser authentication checks.

        Returns:
            Ordered `DoctorCheck` results.

        Side Effects:
            Creates runtime directories through browser checks and may inspect the
            sync database.
        """

        strategy_detail = (
            "Launch the app-owned persistent Playwright profile at "
            f"{self.config.browser_profile_dir}. Run `gphoto-pull login` once if Google "
            "requires interactive authentication; `gphoto-pull pull` reuses the same profile."
        )
        browser_checks = collect_browser_checks(
            BrowserSessionPaths(
                download_dir=self.config.download_dir,
                profile_dir=self.config.browser_profile_dir,
                diagnostics_dir=self.config.diagnostics_dir,
                browsers_path=self.config.browsers_path,
            )
        )

        checks = [
            DoctorCheck(
                name="config file",
                ok=True,
                detail=(
                    str(self.config.config_file)
                    if self.config.config_file_loaded
                    else "not found (using defaults)"
                ),
            ),
            DoctorCheck(
                name="date window",
                ok=self.config.after is not None,
                detail=(
                    _date_window_label(self.config.after, self.config.before)
                    if self.config.after
                    else "not configured; pass --after or set `after` in gphoto-pull.toml"
                ),
            ),
            DoctorCheck(
                name="login/session strategy",
                ok=True,
                detail=strategy_detail,
            ),
        ]

        checks.extend(
            DoctorCheck(
                name=f"browser {check.name}",
                ok=check.ok,
                detail=check.detail,
            )
            for check in browser_checks
        )
        checks.append(self._doctor_auth_check(dry_run=dry_run))
        checks.append(
            DoctorCheck(
                name="python",
                ok=True,
                detail=sys.version.split()[0],
            )
        )
        return checks

    def _doctor_auth_check(self, *, dry_run: bool) -> DoctorCheck:
        """Description:
        Verify the marked persistent profile can reach authenticated Photos.

        Args:
            dry_run: Skip the live browser check.

        Returns:
            Operator-facing doctor check result.
        """

        if dry_run:
            return DoctorCheck(
                name="authenticated session",
                ok=True,
                detail="not checked because doctor is running with --dry-run",
            )

        if not browser_profile_marked_logged_in(self.config.browser_profile_dir):
            return DoctorCheck(
                name="authenticated session",
                ok=True,
                detail="not checked; run `gphoto-pull login` to mark this profile as logged in",
            )

        paths = BrowserSessionPaths(
            download_dir=self.config.download_dir,
            profile_dir=self.config.browser_profile_dir,
            diagnostics_dir=self.config.diagnostics_dir,
            browsers_path=self.config.browsers_path,
        )
        try:
            require_browser_binaries(paths, browser_binary=self.config.browser_binary)
            asyncio.run(self._check_authenticated_session_async(paths))
        except BrowserSessionError as exc:
            return DoctorCheck(
                name="authenticated session",
                ok=False,
                detail=str(exc),
            )

        return DoctorCheck(
            name="authenticated session",
            ok=True,
            detail="Recently added reached with the persistent browser profile",
        )

    async def _check_authenticated_session_async(self, paths: BrowserSessionPaths) -> None:
        """Description:
        Launch a short-lived browser context and prove Photos auth is usable.

        Args:
            paths: Browser paths used for the automated check.

        Side Effects:
            Launches Chromium and writes failure artifacts if authentication is missing.
        """

        async with launched_browser_context_async(
            paths,
            headless=True,
            browser_binary=self.config.browser_binary,
        ) as context:
            await _assert_authenticated_photos_session_async(
                context,
                diagnostics_dir=self.config.diagnostics_dir,
                photos_ui=self.photos_ui,
                artifact_prefix="doctor-auth-failure",
            )

    def pull(self) -> list[str]:
        """Description:
        Execute a Google Photos pull.

        Returns:
            Operator-facing result lines.

        Side Effects:
            For real pulls, launches Chromium, captures diagnostics, updates SQLite
            state, and writes downloaded media files.
        """

        if self.config.after is None:
            raise ConfigError(
                "`after` is required before running a pull. Pass --after or set "
                "`after` in gphoto-pull.toml."
            )

        self.config.ensure_runtime_paths()

        lines = [
            f"Date window: {_date_window_label(self.config.after, self.config.before)}",
            f"Browser profile dir: {self.config.browser_profile_dir}",
            f"Download directory: {self.config.download_dir}",
            f"Diagnostics directory: {self.config.diagnostics_dir}",
            f"Sync DB template: {self.config.sync_db_path}",
            f"Download concurrency: {self.config.download_concurrency}",
            f"Metadata enrichment concurrency: {self.config.enrichment_concurrency}",
            f"Post-download metadata enrichment: {self.config.enrich_metadata}",
        ]
        paths = BrowserSessionPaths(
            download_dir=self.config.download_dir,
            profile_dir=self.config.browser_profile_dir,
            diagnostics_dir=self.config.diagnostics_dir,
            browsers_path=self.config.browsers_path,
        )
        require_browser_binaries(paths, browser_binary=self.config.browser_binary)

        try:
            execution = asyncio.run(self._pull_live_interruptible(paths, lines))
        except (asyncio.CancelledError, KeyboardInterrupt) as exc:
            raise KeyboardInterrupt from exc

        lines.append(
            "Pull execution: "
            f"queued={execution.queued_count}, "
            f"downloaded={execution.downloaded_count}, "
            f"failed={execution.failed_count}, "
            f"already-downloaded={execution.skipped_existing_count}"
        )
        if execution.failure_media_ids:
            lines.append("Failed media ids: " + ", ".join(execution.failure_media_ids[:5]))
        return lines

    def refresh(self) -> list[str]:
        """Description:
        Refresh the local media index without downloading files.

        Returns:
            Operator-facing result lines.

        Side Effects:
            Launches Chromium, captures diagnostics, and updates the SQLite
            media index.
        """

        if self.config.after is None:
            raise ConfigError(
                "`after` is required before running refresh. Pass --after or set "
                "`after` in gphoto-pull.toml."
            )

        self.config.ensure_runtime_paths()
        lines = [
            f"Date window: {_date_window_label(self.config.after, self.config.before)}",
            f"Browser profile dir: {self.config.browser_profile_dir}",
            f"Diagnostics directory: {self.config.diagnostics_dir}",
            f"Sync DB template: {self.config.sync_db_path}",
        ]
        paths = BrowserSessionPaths(
            download_dir=self.config.download_dir,
            profile_dir=self.config.browser_profile_dir,
            diagnostics_dir=self.config.diagnostics_dir,
            browsers_path=self.config.browsers_path,
        )
        require_browser_binaries(paths, browser_binary=self.config.browser_binary)

        try:
            asyncio.run(self._refresh_live(paths, lines))
        except (asyncio.CancelledError, KeyboardInterrupt) as exc:
            raise KeyboardInterrupt from exc
        return lines

    async def _pull_live_interruptible(
        self,
        paths: BrowserSessionPaths,
        lines: list[str],
    ) -> PullExecutionSummary:
        """Description:
        Run live pull work and cancel it promptly after cooperative Ctrl-C.

        Args:
            paths: Browser paths used for the automated pull context.
            lines: Mutable summary lines to append to.

        Returns:
            Pull execution counters.

        Side Effects:
            Starts one monitor task that cancels the live pull on SIGINT.
        """

        pull_task = asyncio.create_task(self._pull_live(paths, lines))
        interrupt_task = asyncio.create_task(_wait_for_interrupt_request())
        done, pending = await asyncio.wait(
            {pull_task, interrupt_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if interrupt_task in done:
            pull_task.cancel()
            with suppress(asyncio.CancelledError, BrowserSessionError):
                await pull_task
            raise KeyboardInterrupt

        interrupt_task.cancel()
        for task in pending:
            with suppress(asyncio.CancelledError):
                await task
        return pull_task.result()

    async def _pull_live(
        self,
        paths: BrowserSessionPaths,
        lines: list[str],
    ) -> PullExecutionSummary:
        """Description:
        Execute live browser probes and async download workers.

        Args:
            paths: Browser paths used for the automated pull context.
            lines: Mutable summary lines to append to.

        Returns:
            Pull execution counters.

        Side Effects:
            Launches Chromium, captures diagnostics, updates SQLite state, and
            writes downloaded media files.
        """

        LOGGER.log(PHASE_LOG_LEVEL, "Launching browser profile.")
        async with launched_browser_context_async(
            paths,
            headless=self.config.headless,
            browser_binary=self.config.browser_binary,
        ) as context:
            account_scope = await _assert_authenticated_photos_session_async(
                context,
                diagnostics_dir=self.config.diagnostics_dir,
                photos_ui=self.photos_ui,
                artifact_prefix="pull-auth-failure",
            )
            lines.append("Session check: authenticated Google Photos profile")
            state_db_path = _account_scoped_sync_db_path(
                self.config.sync_db_path,
                account_scope,
            )
            lines.append(f"Account state scope: {account_scope.key}")
            lines.append(f"Account sync DB path: {state_db_path}")
            if self.config.after is None:
                raise ConfigError("`after` is required before running a pull.")
            with PullStateStore(state_db_path) as state_store:
                if state_store.upload_window_satisfies(
                    after=self.config.after,
                    before=self.config.before,
                ):
                    LOGGER.log(
                        PHASE_LOG_LEVEL,
                        "Bounded upload window is covered by the media index; "
                        "skipping live diagnostics.",
                    )
                    lines.append("Indexed window coverage: complete; skipping live diagnostics")
                else:
                    stop_on_index_overlap = state_store.upload_coverage_satisfies(self.config.after)
                    LOGGER.log(PHASE_LOG_LEVEL, "Capturing Recently added diagnostics.")
                    recent_page = await context.new_page()
                    recent_response_count = await _capture_recent_probe_async(
                        recent_page,
                        diagnostics_dir=self.config.diagnostics_dir,
                        photos_ui=self.photos_ui,
                        after=self.config.after,
                        state_store=state_store,
                        stop_on_index_overlap=stop_on_index_overlap,
                        allow_checkpoint_resume=False,
                    )
                    await recent_page.close()

                    LOGGER.log(PHASE_LOG_LEVEL, "Capturing Updates diagnostics.")
                    updates_page = await context.new_page()
                    updates_response_count = await _capture_updates_probe_async(
                        updates_page,
                        diagnostics_dir=self.config.diagnostics_dir,
                    )
                    await updates_page.close()

                    lines.append(
                        "Captured live diagnostics: "
                        f"recent batchexecute responses={recent_response_count}, "
                        f"updates batchexecute responses={updates_response_count}"
                    )

                LOGGER.log(PHASE_LOG_LEVEL, "Querying media index.")
                records = state_store.list_media_in_upload_window(
                    after=self.config.after,
                    before=self.config.before,
                )
                summary = enumerate_index_candidates(
                    records,
                    after=self.config.after,
                    before=self.config.before,
                )
                lines.extend(
                    _enumeration_summary_lines(
                        summary,
                        state_db_path,
                        label_prefix="Indexed",
                    )
                )

                return await self._download_summary_candidates(context, state_store, summary, lines)

    async def _download_summary_candidates(
        self,
        context: AsyncBrowserContext,
        state_store: PullStateStore,
        summary: EnumerationSummary,
        lines: list[str],
    ) -> PullExecutionSummary:
        """Description:
        Build and execute a download queue from an enumeration summary.

        Args:
            context: Browser context used by download workers.
            state_store: Open media index.
            summary: Candidate summary to download.
            lines: Mutable summary lines to append to.

        Returns:
            Pull execution counters.

        Side Effects:
            Reads target-file metadata, downloads files, and appends summary
            lines.
        """

        LOGGER.log(PHASE_LOG_LEVEL, "Building download queue.")
        queue, skipped_existing_count = _build_download_queue(
            summary,
            download_dir=self.config.download_dir,
        )
        lines.append(
            "Download queue: "
            f"{len(queue)} cutoff-matched candidates, "
            f"{skipped_existing_count} already-downloaded"
        )

        if not queue:
            return PullExecutionSummary(
                queued_count=0,
                skipped_existing_count=skipped_existing_count,
                downloaded_count=0,
                failed_count=0,
                failure_media_ids=(),
            )

        download_concurrency = _bounded_download_concurrency(
            self.config.download_concurrency,
            len(queue),
        )
        lines.append(f"Download workers: {download_concurrency} async download workers")
        enrichment_concurrency = (
            _bounded_download_concurrency(self.config.enrichment_concurrency, len(queue))
            if self.config.enrich_metadata
            else 0
        )
        if enrichment_concurrency:
            lines.append(f"Metadata enrichment workers: {enrichment_concurrency} async workers")
        LOGGER.log(
            PHASE_LOG_LEVEL,
            "Starting downloads with %s worker(s), enrichment workers=%s.",
            download_concurrency,
            enrichment_concurrency,
        )
        execution = await _download_candidates_async(
            context,
            diagnostics_dir=self.config.diagnostics_dir,
            download_dir=self.config.download_dir,
            state_store=state_store,
            photos_ui=self.photos_ui,
            queued_candidates=queue,
            download_concurrency=download_concurrency,
            enrichment_concurrency=enrichment_concurrency,
            enrich_metadata=self.config.enrich_metadata,
            progress_interactive=self.config.progress_interactive,
        )
        return replace(execution, skipped_existing_count=skipped_existing_count)

    async def _refresh_live(self, paths: BrowserSessionPaths, lines: list[str]) -> None:
        """Description:
        Execute live browser probes and update the local media index.

        Args:
            paths: Browser paths used for the automated refresh context.
            lines: Mutable summary lines to append to.

        Side Effects:
            Launches Chromium, captures diagnostics, and writes metadata rows.
        """

        LOGGER.log(PHASE_LOG_LEVEL, "Launching browser profile.")
        async with launched_browser_context_async(
            paths,
            headless=self.config.headless,
            browser_binary=self.config.browser_binary,
        ) as context:
            account_scope = await _assert_authenticated_photos_session_async(
                context,
                diagnostics_dir=self.config.diagnostics_dir,
                photos_ui=self.photos_ui,
                artifact_prefix="refresh-auth-failure",
            )
            lines.append("Session check: authenticated Google Photos profile")
            state_db_path = _account_scoped_sync_db_path(
                self.config.sync_db_path,
                account_scope,
            )
            lines.append(f"Account state scope: {account_scope.key}")
            lines.append(f"Account sync DB path: {state_db_path}")
            if self.config.after is None:
                raise ConfigError("`after` is required before running refresh.")
            with PullStateStore(state_db_path) as state_store:
                LOGGER.log(PHASE_LOG_LEVEL, "Capturing Recently added diagnostics.")
                recent_page = await context.new_page()
                recent_response_count = await _capture_recent_probe_async(
                    recent_page,
                    diagnostics_dir=self.config.diagnostics_dir,
                    photos_ui=self.photos_ui,
                    after=self.config.after,
                    state_store=state_store,
                )
                await recent_page.close()

                LOGGER.log(PHASE_LOG_LEVEL, "Capturing Updates diagnostics.")
                updates_page = await context.new_page()
                updates_response_count = await _capture_updates_probe_async(
                    updates_page,
                    diagnostics_dir=self.config.diagnostics_dir,
                )
                await updates_page.close()

                lines.append(
                    "Captured live diagnostics: "
                    f"recent batchexecute responses={recent_response_count}, "
                    f"updates batchexecute responses={updates_response_count}"
                )

                LOGGER.log(PHASE_LOG_LEVEL, "Querying refreshed media index.")
                records = state_store.list_media_in_upload_window(
                    after=self.config.after,
                    before=self.config.before,
                )
                summary = enumerate_index_candidates(
                    records,
                    after=self.config.after,
                    before=self.config.before,
                )
                lines.extend(
                    _enumeration_summary_lines(
                        summary,
                        state_db_path,
                        label_prefix="Refresh",
                    )
                )

    def login(self) -> list[str]:
        """Description:
        Launch the headed browser login flow.

        Returns:
            Operator-facing completion lines.

        Side Effects:
            Opens Chromium with the persistent profile and waits for terminal input.
        """

        profile_dir = interactive_login(
            BrowserSessionPaths(
                download_dir=self.config.download_dir,
                profile_dir=self.config.browser_profile_dir,
                diagnostics_dir=self.config.diagnostics_dir,
                browsers_path=self.config.browsers_path,
            ),
            browser_binary=self.config.browser_binary,
            start_url=LOGIN_START_URL,
        )
        mark_browser_profile_logged_in(profile_dir)

        return [
            f"Opened persistent browser profile: {profile_dir}",
            "Login state is stored in that browser profile.",
            "You can now rerun `gphoto-pull doctor` or `gphoto-pull pull`.",
        ]


def _date_window_label(after: datetime | None, before: datetime | None) -> str:
    """Description:
    Render the configured upload/share timestamp window.

    Args:
        after: Inclusive lower bound.
        before: Exclusive upper bound.

    Returns:
        Operator-facing date window label.
    """

    lower = after.isoformat() if after is not None else "<not set>"
    upper = before.isoformat() if before is not None else "<none>"
    return f"{lower} <= uploaded_time < {upper}"


async def _wait_for_interrupt_request() -> None:
    """Description:
    Wait until the cooperative SIGINT handler records Ctrl-C.

    Side Effects:
        Temporarily registers a callback with the cooperative SIGINT handler.
    """

    event = asyncio.Event()

    def notify() -> None:
        event.set()

    add_interrupt_callback(notify)
    try:
        if interrupt_requested():
            return
        await event.wait()
    finally:
        remove_interrupt_callback(notify)


def _enumeration_summary_lines(
    summary: EnumerationSummary,
    sync_db_path: Path,
    *,
    label_prefix: str,
) -> list[str]:
    """Description:
    Render enumeration counts and a sample candidate.

    Args:
        summary: Enumeration result to summarize.
        sync_db_path: State database path used for persisted candidates.
        label_prefix: Prefix that identifies the enumeration source.

    Returns:
        Operator-facing summary lines.
    """

    source_details = ", ".join(f"{name}={count}" for name, count in summary.source_counts)
    lines = [
        f"{label_prefix} enumeration source counts: "
        + (source_details if source_details else "<none>"),
        f"{label_prefix} enumeration candidates persisted: {len(summary.persisted_records)} -> "
        f"{sync_db_path}",
        f"{label_prefix} exact uploaded-time candidates: {summary.exact_uploaded_time_count}",
        f"{label_prefix} unknown uploaded-time candidates: {summary.unknown_uploaded_time_count}",
        f"{label_prefix} cutoff matches with exact uploaded time: {summary.cutoff_matched_count}",
    ]

    if summary.candidates:
        sample = summary.candidates[0]
        lines.append(
            f"{label_prefix} sample candidate: "
            f"{sample.metadata.media_id} from {sample.source} "
            f"(uploaded_time={sample.metadata.uploaded_time}, "
            f"product_url={sample.metadata.product_url})"
        )
    else:
        lines.append(
            f"{label_prefix} sample candidate: no saved diagnostics candidates were found."
        )

    return lines


async def _capture_recent_probe_async(
    page: AsyncPage,
    *,
    diagnostics_dir: Path,
    photos_ui: GooglePhotosUi,
    after: datetime,
    state_store: PullStateStore | None = None,
    stop_on_index_overlap: bool = False,
    allow_checkpoint_resume: bool = True,
) -> int:
    """Description:
    Visit Recently Added and capture live batchexecute diagnostics.

    Args:
        page: Playwright page dedicated to the probe.
        diagnostics_dir: Diagnostics directory root.
        photos_ui: Google Photos UI adapter.
        after: Inclusive lower-bound timestamp for deciding scroll depth.
        state_store: Optional media index used for cursor checkpoint resume.
        stop_on_index_overlap: Whether indexed media overlap can stop the live
            refresh before reaching `after`.
        allow_checkpoint_resume: Whether stored older-page cursors can be used
            to resume deep pagination.

    Returns:
        Number of captured recent batchexecute responses.

    Side Effects:
        Navigates the page, scrolls the grid, writes HTML, screenshot, and
        response artifacts.
    """

    capture = _install_batchexecute_capture_async(page, parse_recent_payload)
    recent_page_requests = _install_recent_page_request_capture(page)
    target_dir = diagnostics_dir / "live_recent_probe"

    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    try:
        try:
            raise_if_interrupt_requested()
            await page.goto(RECENTLY_ADDED_URL, wait_until="domcontentloaded")
            if classify_photos_url(page.url).surface is not PhotosSurface.SEARCH_RESULTS:
                await _raise_recent_auth_error_async(
                    page,
                    diagnostics_dir=diagnostics_dir,
                    artifact_prefix="recent-auth-failure",
                )
            with suppress(PlaywrightTimeoutError):
                await photos_ui.wait_for_recently_added_async(page, timeout_ms=10_000)
            await _wait_for_capture_or_visible_recent(
                capture,
                photos_ui=photos_ui,
                page=page,
                previous_payload_count=0,
                previous_visible_count=0,
                timeout_seconds=2.0,
            )
        except PlaywrightError as exc:
            if interrupt_requested():
                raise KeyboardInterrupt from exc
            raise BrowserSessionError(f"Failed to open Recently added: {exc}") from exc

        location = classify_photos_url(page.url)
        if location.surface is not PhotosSurface.SEARCH_RESULTS:
            await _write_failure_artifacts_async(
                page,
                diagnostics_dir / "pull_failures",
                "recent-auth-failure",
            )
            raise BrowserSessionError(
                "Persistent browser profile did not reach the authenticated Recently added route. "
                f"Current URL: {page.url}. Rerun `gphoto-pull login`."
            )

        stable_iterations = 0
        scroll_iterations = 0
        coverage_recorded = False
        previous_payload_count = _recent_payload_item_count(capture.response_texts)
        previous_visible_count = await photos_ui.visible_recent_media_count_async(page)
        while True:
            raise_if_interrupt_requested()
            oldest_upload_time = _oldest_recent_upload_time(capture.response_texts)
            if oldest_upload_time is not None and oldest_upload_time < after:
                LOGGER.log(
                    PHASE_LOG_LEVEL,
                    "Recent probe reached requested window: items=%s oldest_upload=%s",
                    previous_payload_count,
                    oldest_upload_time.isoformat(),
                )
                break

            if stop_on_index_overlap and _recent_payloads_overlap_index(
                capture.response_texts,
                state_store,
                after=after,
            ):
                LOGGER.log(
                    PHASE_LOG_LEVEL,
                    "Recent probe reached indexed overlap: items=%s oldest_upload=%s",
                    previous_payload_count,
                    (
                        oldest_upload_time.isoformat()
                        if oldest_upload_time is not None
                        else "<unknown>"
                    ),
                )
                break

            if await _page_recent_payloads_to_window(
                page.context.request,
                capture,
                recent_page_requests,
                after=after,
                state_store=state_store,
                stop_on_index_overlap=stop_on_index_overlap,
                allow_checkpoint_resume=allow_checkpoint_resume,
            ):
                coverage_recorded = True
                break

            scrolled = await photos_ui.scroll_recently_added_container_async(page)
            scroll_iterations += 1
            await _wait_for_capture_or_visible_recent(
                capture,
                photos_ui=photos_ui,
                page=page,
                previous_payload_count=previous_payload_count,
                previous_visible_count=previous_visible_count,
                timeout_seconds=1.5,
            )
            payload_count = _recent_payload_item_count(capture.response_texts)
            visible_count = await photos_ui.visible_recent_media_count_async(page)
            oldest_upload_time = _oldest_recent_upload_time(capture.response_texts)

            if payload_count > previous_payload_count or visible_count > previous_visible_count:
                previous_payload_count = payload_count
                previous_visible_count = visible_count
                stable_iterations = 0
            elif not scrolled:
                stable_iterations += 1
            else:
                stable_iterations += 1

            if stable_iterations >= 2 and (payload_count > 0 or visible_count > 0):
                LOGGER.log(
                    PHASE_LOG_LEVEL,
                    "Recent probe stopped after feed stopped advancing: items=%s oldest_upload=%s",
                    payload_count,
                    (
                        oldest_upload_time.isoformat()
                        if oldest_upload_time is not None
                        else "<unknown>"
                    ),
                )
                break

            if scroll_iterations % 25 == 0:
                LOGGER.log(
                    PHASE_LOG_LEVEL,
                    "Recent probe scroll progress: scrolls=%s items=%s oldest_upload=%s",
                    scroll_iterations,
                    payload_count,
                    (
                        oldest_upload_time.isoformat()
                        if oldest_upload_time is not None
                        else "<unknown>"
                    ),
                )

        await _flush_response_capture(capture)
        if not coverage_recorded:
            _persist_recent_payloads_from_responses(state_store, capture.response_texts)
            _record_recent_payload_coverage(state_store, capture.response_texts)
        _reset_capture_dir(target_dir)
        await _write_probe_artifacts_async(
            page=page,
            target_dir=target_dir,
            html_name="recent.html",
            screenshot_name="recent.png",
            response_texts=capture.response_texts,
        )
        return len(capture.response_texts)
    finally:
        await _close_response_capture(capture)


async def _assert_authenticated_photos_session_async(
    context: AsyncBrowserContext,
    *,
    diagnostics_dir: Path,
    photos_ui: GooglePhotosUi,
    artifact_prefix: str,
) -> AccountScope:
    """Description:
    Prove the persistent browser profile reaches authenticated Google Photos.

    Args:
        context: Browser context using the persistent profile.
        diagnostics_dir: Diagnostics directory root.
        photos_ui: Google Photos UI adapter.
        artifact_prefix: Failure artifact filename prefix.

    Returns:
        Account state scope derived from the authenticated Photos shell.

    Side Effects:
        Opens a page and writes diagnostics when authentication is missing.
    """

    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    page = await context.new_page()
    try:
        try:
            raise_if_interrupt_requested()
            await page.goto(RECENTLY_ADDED_URL, wait_until="domcontentloaded")
            if classify_photos_url(page.url).surface is not PhotosSurface.SEARCH_RESULTS:
                await _raise_recent_auth_error_async(
                    page,
                    diagnostics_dir=diagnostics_dir,
                    artifact_prefix=artifact_prefix,
                )
            with suppress(PlaywrightTimeoutError):
                await photos_ui.wait_for_recently_added_async(page, timeout_ms=10_000)
        except PlaywrightError as exc:
            if interrupt_requested():
                raise KeyboardInterrupt from exc
            raise BrowserSessionError(
                f"Failed to open Recently added for auth check: {exc}"
            ) from exc

        location = classify_photos_url(page.url)
        if location.surface is PhotosSurface.SEARCH_RESULTS:
            try:
                return await _photos_account_scope_from_page_async(page)
            except BrowserSessionError:
                await _write_failure_artifacts_async(
                    page,
                    diagnostics_dir / "pull_failures",
                    artifact_prefix,
                )
                raise

        await _raise_recent_auth_error_async(
            page,
            diagnostics_dir=diagnostics_dir,
            artifact_prefix=artifact_prefix,
        )
    finally:
        await page.close()


async def _photos_account_scope_from_page_async(page: AsyncPage) -> AccountScope:
    """Description:
    Derive an account namespace from the authenticated Google Photos page.

    Args:
        page: Authenticated Photos page.

    Returns:
        Account scope from the signed-in Google account control.
    """

    raw_label = await page.evaluate(
        """() => {
            const accountControl = document.querySelector('[aria-label^="Google Account"]');
            return accountControl ? accountControl.getAttribute("aria-label") : null;
        }"""
    )
    identity = _account_identity_from_google_account_label(raw_label)
    return AccountScope(_account_scope_key(identity))


def _account_identity_from_google_account_label(raw_label: object) -> str:
    if not isinstance(raw_label, str):
        raise BrowserSessionError(
            "Authenticated Google Photos page is missing the Google Account aria-label. "
            "The page structure changed; update the account selector."
        )

    email_match = _EMAIL_PATTERN.search(raw_label)
    if email_match is None:
        raise BrowserSessionError(
            "Authenticated Google Photos Google Account aria-label did not contain an email "
            "address. The page structure changed; update the account selector."
        )
    return f"email:{email_match.group(0).lower()}"


def _account_scope_key(identity: str) -> str:
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:ACCOUNT_SCOPE_HASH_LENGTH]


def _account_scoped_sync_db_path(sync_db_template: Path, account_scope: AccountScope) -> Path:
    return sync_db_template.parent / "accounts" / account_scope.key / sync_db_template.name


async def _raise_recent_auth_error_async(
    page: AsyncPage,
    *,
    diagnostics_dir: Path,
    artifact_prefix: str,
) -> Never:
    await _write_failure_artifacts_async(
        page,
        diagnostics_dir / "pull_failures",
        artifact_prefix,
    )
    raise BrowserSessionError(
        "Persistent browser profile did not reach the authenticated Recently added route. "
        f"Current URL: {page.url}. Rerun `gphoto-pull login`."
    )


async def _capture_updates_probe_async(
    page: AsyncPage,
    *,
    diagnostics_dir: Path,
) -> int:
    """Description:
    Visit Updates and capture live batchexecute diagnostics.

    Args:
        page: Playwright page dedicated to the probe.
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Number of captured updates batchexecute responses.

    Side Effects:
        Navigates the page and writes HTML, screenshot, and response artifacts.
    """

    capture = _install_batchexecute_capture_async(page, parse_updates_payload)
    target_dir = diagnostics_dir / "live_updates_probe"

    from playwright.async_api import Error as PlaywrightError

    try:
        try:
            raise_if_interrupt_requested()
            await page.goto(UPDATES_URL, wait_until="domcontentloaded")
            await _wait_for_capture_count(capture, previous_count=0, timeout_seconds=2.0)
        except PlaywrightError as exc:
            if interrupt_requested():
                raise KeyboardInterrupt from exc
            raise BrowserSessionError(f"Failed to open Updates: {exc}") from exc

        location = classify_photos_url(page.url)
        if location.surface is not PhotosSurface.UPDATES:
            await _write_failure_artifacts_async(
                page,
                diagnostics_dir / "pull_failures",
                "updates-auth-failure",
            )
            raise BrowserSessionError(
                "Persistent browser profile did not reach the authenticated Updates route. "
                f"Current URL: {page.url}. Rerun `gphoto-pull login`."
            )

        await _flush_response_capture(capture)
        _reset_capture_dir(target_dir)
        await _write_probe_artifacts_async(
            page=page,
            target_dir=target_dir,
            html_name="updates.html",
            screenshot_name="updates.png",
            response_texts=capture.response_texts,
        )

        neutral_artifact = diagnostics_dir / "updates-batchexecute.txt"
        if capture.response_texts:
            neutral_artifact.write_text(capture.response_texts[-1], encoding="utf-8")
        elif neutral_artifact.exists():
            neutral_artifact.unlink()

        return len(capture.response_texts)
    finally:
        await _close_response_capture(capture)


def _install_batchexecute_capture_async(
    page: AsyncPage,
    parser: Callable[[str], object],
) -> _AsyncResponseCapture:
    """Description:
    Attach a response listener that records parseable Photos batchexecute bodies.

    Args:
        page: Page to observe.
        parser: Payload parser used to reject unrelated batchexecute frames.

    Returns:
        Response capture state populated by background tasks.

    Side Effects:
        Registers a Playwright response listener on `page`.
    """

    response_texts: list[str] = []
    tasks: set[asyncio.Task[None]] = set()
    capture_event = asyncio.Event()

    def on_response(response: AsyncResponse) -> None:
        """Description:
        Schedule capture for one response.

        Args:
            response: Playwright response event payload.

        Side Effects:
            Adds a task to the capture state.
        """

        task = asyncio.create_task(
            _capture_batchexecute_response(
                response,
                parser=parser,
                response_texts=response_texts,
                accepted_event=capture_event,
            )
        )
        tasks.add(task)

        def on_task_done(done_task: asyncio.Task[None]) -> None:
            tasks.discard(done_task)
            with suppress(asyncio.CancelledError, Exception):
                done_task.exception()

        task.add_done_callback(on_task_done)

    page.on("response", on_response)
    return _AsyncResponseCapture(
        response_texts=response_texts,
        tasks=tasks,
        accepted_event=capture_event,
        page=page,
        response_handler=on_response,
    )


def _install_recent_page_request_capture(page: AsyncPage) -> list[_RecentPageRequest]:
    """Description:
    Capture browser-originated recent-media request templates for pagination.

    Args:
        page: Recently Added page to observe.

    Returns:
        Mutable request list populated by Playwright callbacks.

    Side Effects:
        Registers a request listener.
    """

    requests: list[_RecentPageRequest] = []

    def on_request(request: AsyncRequest) -> None:
        if "/_/PhotosUi/data/batchexecute" not in request.url:
            return
        rpc_id_values = urllib.parse.parse_qs(urlsplit(request.url).query).get("rpcids", [])
        rpc_ids = tuple(
            rpc_id for rpc_id_value in rpc_id_values for rpc_id in rpc_id_value.split(",") if rpc_id
        )
        if not rpc_ids:
            return
        parsed = urllib.parse.parse_qs(request.post_data or "")
        at_values = parsed.get("at")
        if not at_values:
            return
        for rpc_id in rpc_ids:
            request_template = _RecentPageRequest(
                rpc_id=rpc_id,
                url=request.url,
                at_token=at_values[0],
            )
            if request_template not in requests:
                requests.append(request_template)

    page.on("request", on_request)
    return requests


async def _capture_batchexecute_response(
    response: AsyncResponse,
    *,
    parser: Callable[[str], object],
    response_texts: list[str],
    accepted_event: asyncio.Event,
) -> None:
    """Description:
    Capture one async response when it is a parseable Photos batchexecute body.

    Args:
        response: Async Playwright response event payload.
        parser: Payload parser used to reject unrelated batchexecute frames.
        response_texts: Mutable sink for accepted response bodies.
        accepted_event: Event notified when a unique response body is accepted.

    Side Effects:
        Appends unique response text to `response_texts`.
    """

    if "/_/PhotosUi/data/batchexecute" not in response.url:
        return

    try:
        raw_text = await response.text()
        parser(raw_text)
    except Exception:
        return

    if raw_text not in response_texts:
        response_texts.append(raw_text)
        accepted_event.set()


async def _flush_response_capture(capture: _AsyncResponseCapture) -> None:
    """Description:
    Wait for currently pending response capture tasks.

    Args:
        capture: Capture state returned by `_install_batchexecute_capture_async`.

    Side Effects:
        Lets response-processing tasks finish before payload parsing continues.
    """

    while capture.tasks:
        tasks = tuple(capture.tasks)
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            for task in tasks:
                capture.tasks.discard(task)


async def _close_response_capture(
    capture: _AsyncResponseCapture,
    *,
    drain: bool = True,
) -> None:
    """Description:
    Remove a response capture listener and optionally finish pending tasks.

    Args:
        capture: Capture state returned by `_install_batchexecute_capture_async`.
        drain: Whether to wait for pending response-body parsing tasks.

    Side Effects:
        Unregisters the page response listener. When `drain` is false, cancels
        pending capture tasks without awaiting potentially stuck response-body
        reads.
    """

    capture.page.remove_listener("response", capture.response_handler)
    if drain:
        await _flush_response_capture(capture)
        return
    for task in tuple(capture.tasks):
        task.cancel()


async def _page_recent_payloads_to_window(
    request_context: APIRequestContext,
    capture: _AsyncResponseCapture,
    recent_page_requests: list[_RecentPageRequest],
    *,
    after: datetime,
    state_store: PullStateStore | None,
    stop_on_index_overlap: bool = False,
    allow_checkpoint_resume: bool = True,
) -> bool:
    """Description:
    Fetch older Recently Added pages directly until the requested window is reached.

    Args:
        request_context: Browser request context carrying Google auth.
        capture: Response capture state whose texts are extended.
        recent_page_requests: Browser-originated recent request templates.
        after: Inclusive lower-bound timestamp.
        state_store: Optional media index used for cursor checkpoint resume.
        stop_on_index_overlap: Whether a page containing a media id already in
            the index can complete pagination.
        allow_checkpoint_resume: Whether stored older-page cursors can be used
            to resume deep pagination.

    Returns:
        `True` when direct pagination reached the requested lower bound or feed end.

    Side Effects:
        Posts captured recent-media batchexecute requests and appends response
        texts. Records upload coverage for completed contiguous page chains.
    """

    await _flush_response_capture(capture)
    if not recent_page_requests:
        return False

    normal_cursor = (
        _recent_payload_cursor(capture.response_texts[-1]) if capture.response_texts else None
    )
    if normal_cursor is None:
        return False

    initial_response_texts = tuple(capture.response_texts)
    current_rpc_ids = tuple(dict.fromkeys(request.rpc_id for request in recent_page_requests))
    checkpoint = (
        state_store.best_recent_page_checkpoint(after=after, rpc_ids=current_rpc_ids)
        if state_store is not None and allow_checkpoint_resume and not stop_on_index_overlap
        else None
    )
    if checkpoint is not None:
        LOGGER.log(
            PHASE_LOG_LEVEL,
            "Recent direct pagination resuming from checkpoint: page=%s oldest_upload=%s",
            checkpoint.page_count,
            checkpoint.oldest_upload_time.isoformat(),
        )
        checkpoint_start = _RecentPaginationStart(
            cursor=_RecentPageCursor(rpc_id=checkpoint.rpc_id, cursor=checkpoint.cursor),
            page_count=checkpoint.page_count,
            from_checkpoint=True,
        )
        try:
            checkpoint_result = await _page_recent_payloads_from_start(
                request_context,
                capture,
                recent_page_requests,
                start=checkpoint_start,
                after=after,
                state_store=state_store,
                stop_on_index_overlap=stop_on_index_overlap,
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            LOGGER.log(
                PHASE_LOG_LEVEL,
                "Recent direct checkpoint failed; retrying from first page: %s",
                exc,
            )
            checkpoint_result = _RecentPaginationResult(
                completed=False,
                checkpoint_invalid=True,
            )
        if checkpoint_result.completed:
            _persist_recent_payloads_from_responses(state_store, list(initial_response_texts))
            _record_recent_payload_coverage(state_store, initial_response_texts)
            _persist_recent_payloads_from_responses(
                state_store,
                list(checkpoint_result.fetched_response_texts),
            )
            _record_recent_payload_coverage(
                state_store,
                checkpoint_result.fetched_response_texts,
            )
            return True
        if not checkpoint_result.checkpoint_invalid:
            return False
        if checkpoint_result.fetched_response_texts:
            del capture.response_texts[-len(checkpoint_result.fetched_response_texts) :]
        LOGGER.log(PHASE_LOG_LEVEL, "Recent direct checkpoint ignored for this run.")

    normal_result = await _page_recent_payloads_from_start(
        request_context,
        capture,
        recent_page_requests,
        start=_RecentPaginationStart(cursor=normal_cursor, page_count=0),
        after=after,
        state_store=state_store,
        stop_on_index_overlap=stop_on_index_overlap,
    )
    if normal_result.completed:
        coverage_response_texts = (*initial_response_texts, *normal_result.fetched_response_texts)
        _persist_recent_payloads_from_responses(state_store, list(coverage_response_texts))
        _record_recent_payload_coverage(
            state_store,
            coverage_response_texts,
        )
    return normal_result.completed


async def _page_recent_payloads_from_start(
    request_context: APIRequestContext,
    capture: _AsyncResponseCapture,
    recent_page_requests: list[_RecentPageRequest],
    *,
    start: _RecentPaginationStart,
    after: datetime,
    state_store: PullStateStore | None,
    stop_on_index_overlap: bool,
) -> _RecentPaginationResult:
    """Description:
    Fetch direct recent pages from one starting cursor.

    Args:
        request_context: Browser request context carrying Google auth.
        capture: Response capture state whose texts are extended.
        recent_page_requests: Browser-originated recent request templates.
        start: Cursor and page count to start from.
        after: Inclusive lower-bound timestamp.
        state_store: Optional media index used for cursor checkpoints.
        stop_on_index_overlap: Whether indexed media overlap can complete
            pagination before reaching `after`.

    Returns:
        Pagination result distinguishing normal completion from bad checkpoints.

    Side Effects:
        Posts recent-media requests, appends response text, and writes index
        checkpoints when pages contain media.
    """

    cursor = start.cursor
    page_count = start.page_count

    request_template = _recent_page_request_for_rpc_id(
        recent_page_requests,
        rpc_id=cursor.rpc_id,
    )
    if request_template is None:
        return _RecentPaginationResult(
            completed=False,
            checkpoint_invalid=start.from_checkpoint,
        )

    no_progress_pages = 0
    stats = _recent_payload_stats(capture.response_texts)
    previous_item_count = stats.item_count
    seen_cursors: set[str] = set()
    fetched_response_texts: list[str] = []
    while cursor is not None:
        raise_if_interrupt_requested()
        if cursor.cursor in seen_cursors:
            LOGGER.log(
                PHASE_LOG_LEVEL,
                "Recent direct pagination stopped after repeated cursor: pages=%s items=%s",
                page_count,
                stats.item_count,
            )
            return _RecentPaginationResult(
                completed=not start.from_checkpoint,
                checkpoint_invalid=start.from_checkpoint,
                fetched_response_texts=tuple(fetched_response_texts),
            )
        seen_cursors.add(cursor.cursor)
        if request_template.rpc_id != cursor.rpc_id:
            next_template = _recent_page_request_for_rpc_id(
                recent_page_requests,
                rpc_id=cursor.rpc_id,
            )
            if next_template is None:
                return _RecentPaginationResult(
                    completed=False,
                    checkpoint_invalid=start.from_checkpoint,
                    fetched_response_texts=tuple(fetched_response_texts),
                )
            request_template = next_template
        raw_text = await _fetch_recent_page(
            request_context,
            request_template=request_template,
            cursor=cursor.cursor,
        )
        overlaps_index = stop_on_index_overlap and _recent_payload_overlaps_index(
            raw_text,
            state_store,
            after=after,
        )
        capture.response_texts.append(raw_text)
        fetched_response_texts.append(raw_text)
        page_count += 1

        _update_recent_payload_stats(stats, raw_text)
        if stats.item_count > previous_item_count:
            previous_item_count = stats.item_count
            no_progress_pages = 0
        else:
            no_progress_pages += 1
        LOGGER.log(
            PHASE_LOG_LEVEL,
            "Recent direct page progress: pages=%s items=%s oldest_upload=%s",
            page_count,
            stats.item_count,
            (
                stats.oldest_upload_time.isoformat()
                if stats.oldest_upload_time is not None
                else "<unknown>"
            ),
        )
        cursor = _recent_payload_cursor(raw_text)
        _store_recent_page_checkpoint(
            state_store,
            raw_text=raw_text,
            cursor=cursor,
            page_count=page_count,
        )
        if overlaps_index:
            LOGGER.log(
                PHASE_LOG_LEVEL,
                "Recent direct pagination stopped after indexed overlap: pages=%s items=%s",
                page_count,
                stats.item_count,
            )
            return _RecentPaginationResult(
                completed=True,
                fetched_response_texts=tuple(fetched_response_texts),
            )
        if stats.oldest_upload_time is not None and stats.oldest_upload_time < after:
            return _RecentPaginationResult(
                completed=True,
                fetched_response_texts=tuple(fetched_response_texts),
            )
        if no_progress_pages >= 5:
            LOGGER.log(
                PHASE_LOG_LEVEL,
                "Recent direct pagination stopped after %s no-progress page(s): "
                "pages=%s items=%s oldest_upload=%s",
                no_progress_pages,
                page_count,
                stats.item_count,
                (
                    stats.oldest_upload_time.isoformat()
                    if stats.oldest_upload_time is not None
                    else "<unknown>"
                ),
            )
            return _RecentPaginationResult(
                completed=not start.from_checkpoint,
                checkpoint_invalid=start.from_checkpoint,
                fetched_response_texts=tuple(fetched_response_texts),
            )

    return _RecentPaginationResult(
        completed=True,
        fetched_response_texts=tuple(fetched_response_texts),
    )


def _store_recent_page_checkpoint(
    state_store: PullStateStore | None,
    *,
    raw_text: str,
    cursor: _RecentPageCursor | None,
    page_count: int,
) -> None:
    """Description:
    Store a recent page cursor checkpoint when the page has media items.

    Args:
        state_store: Optional media index.
        raw_text: Raw recent-media page response.
        cursor: Next-page cursor parsed from the response.
        page_count: One-based direct page count.

    Side Effects:
        Writes a cursor checkpoint when possible.
    """

    if state_store is None or cursor is None:
        return
    try:
        payload = parse_recent_payload(raw_text)
    except RpcPayloadParseError:
        return
    _persist_recent_payload_page(state_store, payload)
    upload_times = [
        datetime.fromtimestamp(item.upload_timestamp_ms / 1000, tz=UTC)
        for item in payload.items
        if item.upload_timestamp_ms is not None
    ]
    if not upload_times:
        return
    oldest_upload_time = min(upload_times)
    state_store.upsert_recent_page_checkpoint(
        rpc_id=cursor.rpc_id,
        cursor=cursor.cursor,
        oldest_upload_time=oldest_upload_time,
        item_count=len(payload.items),
        page_count=page_count,
    )


def _persist_recent_payloads_from_responses(
    state_store: PullStateStore | None,
    response_texts: list[str],
) -> None:
    """Description:
    Persist media rows from captured recent-media response bodies.

    Args:
        state_store: Optional media index.
        response_texts: Raw recent-media response bodies.

    Side Effects:
        Upserts media rows.
    """

    if state_store is None:
        return

    for raw_text in response_texts:
        try:
            payload = parse_recent_payload(raw_text)
        except RpcPayloadParseError:
            continue
        _persist_recent_payload_page(state_store, payload)


def _record_recent_payload_coverage(
    state_store: PullStateStore | None,
    response_texts: tuple[str, ...] | list[str],
) -> None:
    """Description:
    Record coverage from one contiguous Recent feed response chain.

    Args:
        state_store: Optional media index.
        response_texts: Raw response bodies from one contiguous traversal.

    Side Effects:
        Writes or merges one trusted upload coverage range when timestamps exist.
    """

    if state_store is None:
        return
    stats = _recent_payload_stats(list(response_texts))
    if stats.oldest_upload_time is None or stats.newest_upload_time is None:
        return
    state_store.record_upload_coverage(
        oldest_upload_time=stats.oldest_upload_time,
        newest_upload_time=stats.newest_upload_time,
    )


def _persist_recent_payload_page(state_store: PullStateStore, payload: RecentPayload) -> None:
    """Description:
    Persist media rows from one recent-media payload page.

    Args:
        state_store: Open media index.
        payload: Parsed recent-media payload.

    Side Effects:
        Upserts media metadata rows.
    """

    for item in payload.items:
        capture_time = (
            None
            if item.capture_timestamp_ms is None
            else datetime.fromtimestamp(item.capture_timestamp_ms / 1000, tz=UTC)
        )
        uploaded_time = (
            None
            if item.upload_timestamp_ms is None
            else datetime.fromtimestamp(item.upload_timestamp_ms / 1000, tz=UTC)
        )
        state_store.upsert_media(
            MediaMetadata(
                media_id=item.media_id,
                filename=f"unresolved-{item.media_id}",
                capture_time=capture_time,
                uploaded_time=uploaded_time,
                product_url=(
                    f"{PHOTOS_APP_ORIGIN}/photo/{urllib.parse.quote(item.media_id, safe='')}"
                ),
                preview_url=item.preview_url,
                width=item.width,
                height=item.height,
            )
        )


def _recent_payloads_overlap_index(
    response_texts: list[str],
    state_store: PullStateStore | None,
    *,
    after: datetime,
) -> bool:
    """Description:
    Check whether captured recent payloads reached trusted indexed coverage.

    Args:
        response_texts: Raw batchexecute response bodies.
        state_store: Optional media index to check.
        after: Requested lower-bound timestamp.

    Returns:
        `True` when any captured upload timestamp is inside a trusted coverage
        range that extends through `after`.
    """

    return any(
        _recent_payload_overlaps_index(raw_text, state_store, after=after)
        for raw_text in response_texts
    )


def _recent_payload_overlaps_index(
    raw_text: str,
    state_store: PullStateStore | None,
    *,
    after: datetime,
) -> bool:
    """Description:
    Check whether one recent payload reached trusted indexed coverage.

    Args:
        raw_text: Raw batchexecute response body.
        state_store: Optional media index to check.
        after: Requested lower-bound timestamp.

    Returns:
        `True` when the payload contains an upload timestamp inside a trusted
        coverage range that extends through `after`.
    """

    if state_store is None:
        return False
    try:
        payload = parse_recent_payload(raw_text)
    except RpcPayloadParseError:
        return False
    for item in payload.items:
        if item.upload_timestamp_ms is None:
            continue
        uploaded_time = datetime.fromtimestamp(item.upload_timestamp_ms / 1000, tz=UTC)
        if state_store.upload_time_has_covering_range(uploaded_time=uploaded_time, after=after):
            return True
    return False


def _recent_payload_stats(response_texts: list[str]) -> _RecentPayloadStats:
    """Description:
    Build running statistics from existing recent payload responses.

    Args:
        response_texts: Raw batchexecute response bodies.

    Returns:
        Initial recent payload statistics.
    """

    stats = _RecentPayloadStats(media_ids=set())
    for raw_text in response_texts:
        _update_recent_payload_stats(stats, raw_text)
    return stats


def _update_recent_payload_stats(stats: _RecentPayloadStats, raw_text: str) -> None:
    """Description:
    Update running recent payload statistics from one response body.

    Args:
        stats: Mutable running stats.
        raw_text: Raw batchexecute response body.

    Side Effects:
        Mutates `stats`.
    """

    try:
        payload = parse_recent_payload(raw_text)
    except RpcPayloadParseError:
        return
    for item in payload.items:
        stats.media_ids.add(item.media_id)
        if item.upload_timestamp_ms is None:
            continue
        upload_time = datetime.fromtimestamp(item.upload_timestamp_ms / 1000, tz=UTC)
        if stats.oldest_upload_time is None or upload_time < stats.oldest_upload_time:
            stats.oldest_upload_time = upload_time
        if stats.newest_upload_time is None or upload_time > stats.newest_upload_time:
            stats.newest_upload_time = upload_time


def _recent_page_request_for_rpc_id(
    recent_page_requests: list[_RecentPageRequest],
    *,
    rpc_id: str,
) -> _RecentPageRequest | None:
    """Description:
    Find the most recent browser-captured request template for an opaque RPC id.

    Args:
        recent_page_requests: Captured browser request templates.
        rpc_id: Opaque RPC id from a shape-validated recent payload.

    Returns:
        Matching request template, or `None`.
    """

    for request in reversed(recent_page_requests):
        if request.rpc_id == rpc_id:
            return request
    return None


async def _fetch_recent_page(
    request_context: APIRequestContext,
    *,
    request_template: _RecentPageRequest,
    cursor: str,
) -> str:
    """Description:
    Fetch one Recently Added metadata page by cursor.

    Args:
        request_context: Browser request context carrying Google auth.
        request_template: Captured endpoint and XSRF token.
        cursor: Opaque pagination cursor.

    Returns:
        Raw batchexecute response text.
    """

    response = await request_context.post(
        request_template.url,
        form={
            "f.req": _recent_page_form_request(cursor, rpc_id=request_template.rpc_id),
            "at": request_template.at_token,
        },
        headers={"content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
    )
    return await response.text()


def _recent_page_form_request(cursor: str, *, rpc_id: str) -> str:
    """Description:
    Build a recent-media batchexecute `f.req` body for one cursor.

    Args:
        cursor: Opaque pagination cursor.
        rpc_id: Opaque browser-captured RPC id.

    Returns:
        JSON text for the `f.req` form field.
    """

    filter_shape = cast(
        JsonValue,
        msgspec.json.decode(
            b'["",[[[null,null,null,null,null,null,null,null,null,null,null,null,null,[[[]]]]]]]'
        ),
    )
    inner: list[JsonValue] = [None, None, cursor, None, None, None, 1, filter_shape]
    outer: list[JsonValue] = [[[rpc_id, msgspec.json.encode(inner).decode(), None, "generic"]]]
    return msgspec.json.encode(outer).decode()


def _recent_payload_cursor(response_text: str) -> _RecentPageCursor | None:
    """Description:
    Extract the next-page cursor from a recent-media response.

    Args:
        response_text: Raw batchexecute response body.

    Returns:
        Opaque cursor plus its RPC id, or `None` when no next page is present.
    """

    recent_rpc_ids: set[str] = set()
    try:
        recent_payload = parse_recent_payload(response_text)
        recent_rpc_ids = set(recent_payload.rpc_ids)
    except RpcPayloadParseError:
        pass

    for frame in parse_batchexecute_frames(response_text):
        if frame.rpc_id is None:
            continue
        if recent_rpc_ids and frame.rpc_id not in recent_rpc_ids:
            continue
        payload = frame.decoded_payload()
        if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], str):
            return _RecentPageCursor(rpc_id=frame.rpc_id, cursor=payload[1])
    return None


async def _wait_for_capture_count(
    capture: _AsyncResponseCapture,
    *,
    previous_count: int,
    timeout_seconds: float,
) -> None:
    """Description:
    Wait until accepted Photos RPC payload count increases or timeout expires.

    Args:
        capture: Active response capture state.
        previous_count: Accepted payload count already observed.
        timeout_seconds: Maximum wait before continuing.

    Side Effects:
        Polls in-process capture tasks without sleeping for a fixed duration.
    """

    await _flush_response_capture(capture)
    if len(capture.response_texts) > previous_count:
        return

    capture.accepted_event.clear()
    with suppress(TimeoutError):
        await asyncio.wait_for(capture.accepted_event.wait(), timeout=timeout_seconds)
    await _flush_response_capture(capture)


async def _wait_for_capture_or_visible_recent(
    capture: _AsyncResponseCapture,
    *,
    photos_ui: GooglePhotosUi,
    page: AsyncPage,
    previous_payload_count: int,
    previous_visible_count: int,
    timeout_seconds: float,
) -> None:
    """Description:
    Wait for recent RPC or rendered-tile progress after navigation/scroll.

    Args:
        capture: Active response capture state.
        photos_ui: Google Photos UI adapter.
        page: Async Playwright page on Recently Added.
        previous_payload_count: Parsed media item count already observed.
        previous_visible_count: Rendered tile count already observed.
        timeout_seconds: Maximum wait before continuing.

    Side Effects:
        Polls response capture and visible tile count with a bounded timeout.
    """

    await _flush_response_capture(capture)
    if _recent_payload_item_count(capture.response_texts) > previous_payload_count:
        return
    if await photos_ui.visible_recent_media_count_async(page) > previous_visible_count:
        return

    capture_task = asyncio.create_task(
        _wait_for_capture_count(
            capture,
            previous_count=len(capture.response_texts),
            timeout_seconds=timeout_seconds,
        )
    )
    visible_task = asyncio.create_task(
        photos_ui.wait_for_recent_media_count_above_async(
            page,
            previous_count=previous_visible_count,
            timeout_ms=int(timeout_seconds * 1_000),
        )
    )
    done, pending = await asyncio.wait(
        {capture_task, visible_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in done:
        with suppress(Exception):
            task.result()
    await _flush_response_capture(capture)


def _recent_payload_item_count(response_texts: list[str]) -> int:
    """Description:
    Count unique media items across captured recent payload texts.

    Args:
        response_texts: Raw batchexecute response bodies.

    Returns:
        Deduped media item count, or zero when no payload parses.
    """

    payloads: list[RecentPayload] = []
    for raw_text in response_texts:
        try:
            payloads.append(parse_recent_payload(raw_text))
        except RpcPayloadParseError:
            continue

    if not payloads:
        return 0
    return len(merge_recent_payloads(payloads).items)


def _oldest_recent_upload_time(response_texts: list[str]) -> datetime | None:
    """Description:
    Find the oldest upload timestamp in captured Recently Added payloads.

    Args:
        response_texts: Raw batchexecute response bodies.

    Returns:
        Oldest upload timestamp, or `None` when no timestamp parses.
    """

    payloads: list[RecentPayload] = []
    for raw_text in response_texts:
        try:
            payloads.append(parse_recent_payload(raw_text))
        except RpcPayloadParseError:
            continue
    if not payloads:
        return None

    upload_times = [
        datetime.fromtimestamp(item.upload_timestamp_ms / 1000, tz=UTC)
        for item in merge_recent_payloads(payloads).items
        if item.upload_timestamp_ms is not None
    ]
    return min(upload_times) if upload_times else None


def _reset_capture_dir(directory: Path) -> None:
    """Description:
    Empty file artifacts from a probe directory while preserving subdirectories.

    Args:
        directory: Probe directory to create/reset.

    Side Effects:
        Creates the directory and removes direct child files.
    """

    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.iterdir():
        if path.is_file():
            path.unlink()


async def _write_probe_artifacts_async(
    *,
    page: AsyncPage,
    target_dir: Path,
    html_name: str,
    screenshot_name: str,
    response_texts: list[str],
) -> None:
    """Description:
    Persist page and response artifacts from a probe.

    Args:
        page: Async Playwright page to snapshot.
        target_dir: Directory to write.
        html_name: HTML artifact filename.
        screenshot_name: Screenshot artifact filename.
        response_texts: Batchexecute response bodies to write.

    Side Effects:
        Writes HTML, PNG, and text artifacts.
    """

    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / html_name).write_text(await page.content(), encoding="utf-8")
    await page.screenshot(path=str(target_dir / screenshot_name))
    for index, raw_text in enumerate(response_texts, start=1):
        (target_dir / f"resp_{index:02d}.txt").write_text(raw_text, encoding="utf-8")


async def _write_failure_artifacts_async(
    page: AsyncPage,
    directory: Path,
    slug: str,
) -> None:
    """Description:
    Persist HTML and screenshot artifacts for a failed browser action.

    Args:
        page: Async Playwright page to snapshot.
        directory: Directory to write.
        slug: Filename stem for the artifacts.

    Side Effects:
        Writes HTML and PNG diagnostics.
    """

    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{slug}.html").write_text(await page.content(), encoding="utf-8")
    await page.screenshot(path=str(directory / f"{slug}.png"))


def _build_download_queue(
    summary: EnumerationSummary,
    *,
    download_dir: Path,
) -> tuple[list[tuple[MediaMetadata, MediaStateRecord]], int]:
    """Description:
    Select cutoff-matched candidates that still need local files.

    Args:
        summary: Enumeration output with candidates and persisted state rows.
        download_dir: Final download directory.

    Returns:
        Queue of metadata/state records and count skipped because files exist.

    Side Effects:
        Reads target-file metadata.
    """

    queue: list[tuple[MediaMetadata, MediaStateRecord]] = []
    skipped_existing_count = 0

    for candidate, record in zip(summary.candidates, summary.persisted_records, strict=True):
        if candidate.cutoff_match is not True:
            continue
        if _target_path_exists(record, download_dir=download_dir):
            skipped_existing_count += 1
            continue
        queue.append((candidate.metadata, record))

    return queue, skipped_existing_count


def _target_path_exists(record: MediaStateRecord, *, download_dir: Path) -> bool:
    """Description:
    Check whether the current run's deterministic target file already exists.

    Args:
        record: Persisted media index row.
        download_dir: Final download directory.

    Returns:
        `True` when the final planned path exists.
    """

    primary_path = primary_download_path(download_dir, record)
    if not primary_path.exists():
        return False
    sidecar_path = primary_path.with_name(f"{primary_path.name}.supplemental-metadata.json")
    if not sidecar_path.exists():
        return False
    try:
        return record.metadata.media_id in sidecar_path.read_text(encoding="utf-8")
    except OSError:
        return False


async def _download_candidates_async(
    context: AsyncBrowserContext,
    *,
    diagnostics_dir: Path,
    download_dir: Path,
    state_store: PullStateStore,
    photos_ui: GooglePhotosUi,
    queued_candidates: list[tuple[MediaMetadata, MediaStateRecord]],
    download_concurrency: int,
    enrichment_concurrency: int,
    enrich_metadata: bool,
    progress_interactive: bool | None = None,
) -> PullExecutionSummary:
    """Description:
    Download queued candidates with one async task per worker page.

    Args:
        context: Browser context that owns worker pages.
        diagnostics_dir: Diagnostics directory root.
        download_dir: Final download directory.
        state_store: SQLite media index.
        photos_ui: Google Photos UI adapter.
        queued_candidates: Metadata/state pairs selected for download.
        download_concurrency: Number of parallel worker pages.
        enrichment_concurrency: Number of parallel metadata enrichment pages.
        enrich_metadata: Whether to fetch post-download detail metadata.
        progress_interactive: Optional override for Rich live progress rendering.

    Returns:
        Aggregate execution counts and failed media ids.

    Side Effects:
        Opens pages, triggers downloads, writes files, updates state, and renders
        terminal progress.
    """

    progress = PullProgressDisplay(
        total_items=len(queued_candidates),
        interactive=progress_interactive,
        reserved_active_rows=download_concurrency
        + (enrichment_concurrency if enrich_metadata else 0),
    )
    queue: asyncio.Queue[tuple[MediaMetadata, MediaStateRecord]] = asyncio.Queue()
    for candidate in queued_candidates:
        queue.put_nowait(candidate)
    enrichment_queue: asyncio.Queue[_EnrichmentJob] = asyncio.Queue()
    enrichment_enabled = enrich_metadata and enrichment_concurrency > 0

    async def worker(slot: int, page: AsyncPage) -> PullExecutionSummary:
        """Description:
        Process queued downloads on one async Playwright page.

        Args:
            slot: Worker slot number.
            page: Dedicated download worker page.

        Returns:
            Partial worker execution counters.

        Side Effects:
            Opens and closes one page, downloads files, updates progress/state.
        """

        downloaded_count = 0
        failed_count = 0
        failure_media_ids: list[str] = []

        def report_stage(
            stage: str,
            staged_metadata: MediaMetadata,
            trace: DownloadTrace | None,
        ) -> None:
            """Description:
            Update the active row for one worker substage.

            Args:
                stage: Display status label.
                staged_metadata: Current media metadata.
                trace: Optional live response metadata.

            Side Effects:
                Refreshes terminal progress output.
            """

            rendered_metadata = (
                _metadata_with_download_trace(staged_metadata, trace)
                if trace is not None
                else staged_metadata
            )
            progress.update_item(
                slot,
                stage,
                _download_item_log_line(
                    rendered_metadata,
                    expected_bytes=(
                        trace.content_length if trace is not None else staged_metadata.bytes_size
                    ),
                ),
            )

        try:
            while True:
                raise_if_interrupt_requested()
                try:
                    metadata, record = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                queued_at = perf_counter()
                try:
                    progress.update_item(
                        slot,
                        "queue",
                        _download_item_log_line(metadata, expected_bytes=metadata.bytes_size),
                    )
                    LOGGER.log(
                        TIMING_LOG_LEVEL,
                        "timing queue slot=%s name=%s",
                        slot + 1,
                        _display_name(metadata),
                    )
                    start_begin_at = perf_counter()
                    progress.update_item(
                        slot,
                        "probe",
                        _download_item_log_line(metadata, expected_bytes=metadata.bytes_size),
                    )
                    pending_download = await _start_download_candidate_async(
                        page,
                        slot=slot,
                        queued_at=queued_at,
                        start_begin_at=start_begin_at,
                        diagnostics_dir=diagnostics_dir,
                        download_dir=download_dir,
                        state_store=state_store,
                        photos_ui=photos_ui,
                        metadata=metadata,
                        record=record,
                        on_stage=report_stage,
                    )
                    LOGGER.log(
                        TIMING_LOG_LEVEL,
                        "timing start slot=%s name=%s trigger=%.2fs queued=%.2fs",
                        slot + 1,
                        _display_name(pending_download.metadata),
                        pending_download.download_event_at - start_begin_at,
                        pending_download.download_event_at - queued_at,
                    )
                    progress.mark_started(
                        expected_bytes=pending_download.download_trace.content_length
                    )
                    downloaded_count, failed_count = await _finalize_pending_download_async(
                        pending_download,
                        diagnostics_dir=diagnostics_dir,
                        state_store=state_store,
                        downloaded_count=downloaded_count,
                        failed_count=failed_count,
                        failure_media_ids=failure_media_ids,
                        progress=progress,
                        enrichment_queue=enrichment_queue if enrichment_enabled else None,
                    )
                except BrowserSessionError:
                    raise
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    if interrupt_requested():
                        raise KeyboardInterrupt from exc
                    LOGGER.log(
                        TIMING_LOG_LEVEL,
                        "timing failed slot=%s name=%s phase=start queued=%.2fs error=%s",
                        slot + 1,
                        _display_name(metadata),
                        perf_counter() - queued_at,
                        exc,
                    )
                    failed_count += 1
                    failure_media_ids.append(metadata.media_id)
                    progress.mark_failure(expected_bytes=None, started=False)
                    progress.complete_item(
                        slot,
                        "failed",
                        _download_item_log_line(metadata, expected_bytes=metadata.bytes_size),
                    )
                finally:
                    queue.task_done()
        finally:
            from playwright.async_api import Error as PlaywrightError

            with suppress(PlaywrightError):
                await page.close()

        return PullExecutionSummary(
            queued_count=0,
            skipped_existing_count=0,
            downloaded_count=downloaded_count,
            failed_count=failed_count,
            failure_media_ids=tuple(failure_media_ids),
        )

    async def enrichment_worker(slot: int, page: AsyncPage) -> None:
        """Description:
        Process post-download metadata sidecar enrichment jobs.

        Args:
            slot: Progress slot number reserved for this enrichment worker.
            page: Dedicated enrichment page.

        Side Effects:
            Rewrites sidecars when detail metadata is captured and updates
            progress rows.
        """

        try:
            while True:
                job = await enrichment_queue.get()
                try:
                    progress.update_item(
                        slot,
                        "enrich",
                        _download_item_log_line(
                            job.metadata,
                            expected_bytes=job.metadata.bytes_size,
                        ),
                    )
                    begin_at = perf_counter()
                    detail_metadata = await _enrich_detail_metadata_after_download_async(
                        page,
                        metadata=job.metadata,
                    )
                    if detail_metadata is not None:
                        write_takeout_sidecar(job.media_path, job.metadata, detail_metadata)
                    LOGGER.log(
                        TIMING_LOG_LEVEL,
                        "timing enriched slot=%s name=%s enrichment=%.2fs detail=%s queued=%.2fs",
                        slot + 1,
                        _display_name(job.metadata),
                        perf_counter() - begin_at,
                        detail_metadata is not None,
                        perf_counter() - job.queued_at,
                    )
                    progress.complete_item(
                        slot,
                        "enriched" if detail_metadata is not None else "enrich-miss",
                        _download_item_log_line(
                            job.metadata,
                            expected_bytes=job.metadata.bytes_size,
                        ),
                    )
                except Exception as exc:
                    LOGGER.log(
                        TIMING_LOG_LEVEL,
                        "timing failed slot=%s name=%s phase=enrich queued=%.2fs error=%s",
                        slot + 1,
                        _display_name(job.metadata),
                        perf_counter() - job.queued_at,
                        exc,
                    )
                    progress.complete_item(
                        slot,
                        "enrich-failed",
                        _download_item_log_line(
                            job.metadata,
                            expected_bytes=job.metadata.bytes_size,
                        ),
                    )
                finally:
                    enrichment_queue.task_done()
        finally:
            from playwright.async_api import Error as PlaywrightError

            with suppress(PlaywrightError):
                await page.close()

    async def open_download_pages() -> list[AsyncPage]:
        """Description:
        Open required download worker pages before optional enrichment pages.

        Returns:
            Download pages ready for worker tasks.

        Side Effects:
            Opens browser pages.
        """

        pages: list[AsyncPage] = []
        try:
            for _ in range(download_concurrency):
                pages.append(await context.new_page())
        except Exception:
            await _close_pages(pages)
            raise
        return pages

    async def open_enrichment_pages() -> list[AsyncPage]:
        """Description:
        Open the dedicated enrichment pages that actually started successfully.

        Returns:
            Enrichment pages ready for worker tasks.

        Side Effects:
            Opens browser pages and logs any optional enrichment startup failure.
        """

        if not enrichment_enabled:
            return []

        pages: list[AsyncPage] = []
        for slot in range(enrichment_concurrency):
            try:
                pages.append(await context.new_page())
            except Exception as exc:
                LOGGER.log(
                    TIMING_LOG_LEVEL,
                    "timing failed slot=%s phase=enrich-start error=%s",
                    download_concurrency + slot + 1,
                    exc,
                )
                break
        if not pages:
            LOGGER.log(
                PHASE_LOG_LEVEL,
                "Metadata enrichment disabled because no enrichment workers could start.",
            )
        return pages

    tasks: list[asyncio.Task[PullExecutionSummary]] = []
    enrichment_tasks: list[asyncio.Task[None]] = []
    results: list[PullExecutionSummary] = []
    try:
        download_pages = await open_download_pages()
        enrichment_pages = await open_enrichment_pages()
        enrichment_enabled = bool(enrichment_pages)
        tasks = [
            asyncio.create_task(worker(slot, page)) for slot, page in enumerate(download_pages)
        ]
        enrichment_tasks = [
            asyncio.create_task(enrichment_worker(download_concurrency + slot, page))
            for slot, page in enumerate(enrichment_pages)
        ]
        results = await asyncio.gather(*tasks)
        await enrichment_queue.join()
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        for task in enrichment_tasks:
            task.cancel()
        await asyncio.gather(*tasks, *enrichment_tasks, return_exceptions=True)
        raise
    finally:
        for task in enrichment_tasks:
            task.cancel()
        await asyncio.gather(*enrichment_tasks, return_exceptions=True)
        progress.close()

    return PullExecutionSummary(
        queued_count=len(queued_candidates),
        skipped_existing_count=0,
        downloaded_count=sum(result.downloaded_count for result in results),
        failed_count=sum(result.failed_count for result in results),
        failure_media_ids=tuple(
            media_id for result in results for media_id in result.failure_media_ids
        ),
    )


async def _close_pages(pages: list[AsyncPage]) -> None:
    """Description:
    Close a list of Playwright pages, ignoring browser shutdown errors.

    Args:
        pages: Pages to close.

    Side Effects:
        Attempts to close each page.
    """

    from playwright.async_api import Error as PlaywrightError

    for page in pages:
        with suppress(PlaywrightError):
            await page.close()


async def _start_download_candidate_async(
    page: AsyncPage,
    *,
    slot: int,
    queued_at: float,
    start_begin_at: float,
    diagnostics_dir: Path,
    download_dir: Path,
    state_store: PullStateStore,
    photos_ui: GooglePhotosUi,
    metadata: MediaMetadata,
    record: MediaStateRecord,
    on_stage: Callable[[str, MediaMetadata, DownloadTrace | None], None] | None = None,
) -> _PendingDownload:
    """Description:
    Start one media download with bounded retries.

    Args:
        page: Worker page used to trigger the download.
        slot: Worker slot number.
        queued_at: Monotonic timestamp when the item entered a worker slot.
        start_begin_at: Monotonic timestamp when browser download triggering began.
        diagnostics_dir: Diagnostics directory root.
        download_dir: Final download directory.
        state_store: SQLite state repository.
        photos_ui: Google Photos UI adapter.
        metadata: Candidate metadata.
        record: Candidate state record.
        on_stage: Optional callback when a download substage changes.

    Returns:
        Pending download to finalize later.

    Side Effects:
        May navigate the worker page, start a browser download, and update state
        with trace metadata.
    """

    last_error: DownloadError | None = None

    for attempt in range(1, _DOWNLOAD_START_ATTEMPTS + 1):
        raise_if_interrupt_requested()
        if attempt > 1:
            await _reset_download_page_async(page)
            await page.wait_for_timeout(_DOWNLOAD_RETRY_DELAY_MS)
            raise_if_interrupt_requested()

        try:
            return await _start_download_candidate_once_async(
                page,
                slot=slot,
                queued_at=queued_at,
                start_begin_at=start_begin_at,
                diagnostics_dir=diagnostics_dir,
                download_dir=download_dir,
                state_store=state_store,
                photos_ui=photos_ui,
                metadata=metadata,
                record=record,
                on_stage=on_stage,
            )
        except BrowserSessionError:
            raise
        except KeyboardInterrupt:
            raise
        except DownloadError as exc:
            if interrupt_requested():
                raise KeyboardInterrupt from exc
            last_error = exc
            if "does not have a direct download URL or product URL" in str(exc):
                raise

    if last_error is None:
        raise DownloadError(f"Failed to start download for {metadata.media_id}.")
    raise DownloadError(f"{last_error} (after {_DOWNLOAD_START_ATTEMPTS} attempts)")


async def _start_download_candidate_once_async(
    page: AsyncPage,
    *,
    slot: int,
    queued_at: float,
    start_begin_at: float,
    diagnostics_dir: Path,
    download_dir: Path,
    state_store: PullStateStore,
    photos_ui: GooglePhotosUi,
    metadata: MediaMetadata,
    record: MediaStateRecord,
    on_stage: Callable[[str, MediaMetadata, DownloadTrace | None], None] | None = None,
) -> _PendingDownload:
    """Description:
    Try the direct URL path first, then fall back to the detail-page UI.

    Args:
        page: Worker page used to trigger the download.
        slot: Worker slot number.
        queued_at: Monotonic timestamp when the item entered a worker slot.
        start_begin_at: Monotonic timestamp when browser download triggering began.
        diagnostics_dir: Diagnostics directory root.
        download_dir: Final download directory.
        state_store: SQLite state repository.
        photos_ui: Google Photos UI adapter.
        metadata: Candidate metadata.
        record: Candidate state record.
        on_stage: Optional callback when a download substage changes.

    Returns:
        Pending download to finalize later.

    Side Effects:
        Navigates the worker page, blocks preview images on fallback, starts a
        browser download, and writes failure diagnostics when needed.
    """

    from playwright.async_api import Error as PlaywrightError

    try:
        direct_urls = _direct_download_urls_for_metadata(metadata)
        if direct_urls:
            try:
                raise_if_interrupt_requested()
                download, download_trace = await _trigger_direct_download_async(
                    page,
                    slot=slot,
                    direct_urls=direct_urls,
                    metadata=metadata,
                    on_stage=on_stage,
                )
                return _pending_download_from_started_download_async(
                    page=page,
                    slot=slot,
                    queued_at=queued_at,
                    start_begin_at=start_begin_at,
                    download_event_at=perf_counter(),
                    diagnostics_dir=diagnostics_dir,
                    download_dir=download_dir,
                    state_store=state_store,
                    metadata=metadata,
                    record=record,
                    download=download,
                    download_trace=download_trace,
                    detail_metadata=None,
                    page_url=download_trace.download_url or direct_urls[0],
                )
            except PlaywrightError as exc:
                if interrupt_requested():
                    raise KeyboardInterrupt from exc
                if _is_browser_shutdown_error(exc):
                    raise BrowserSessionError("Browser session ended during pull.") from exc
            except DownloadError as exc:
                if interrupt_requested():
                    raise KeyboardInterrupt from exc
                pass

        if metadata.product_url is None:
            raise DownloadError(
                f"{metadata.media_id} does not have a direct download URL or product URL."
            )

        preview_media_blocker = await _install_preview_media_blocker_async(page)
        try:
            raise_if_interrupt_requested()
            if on_stage is not None:
                on_stage("detail", metadata, None)
            await page.goto(metadata.product_url, wait_until="domcontentloaded")
            await photos_ui.wait_for_detail_actions_async(page)
        except PlaywrightError as exc:
            if interrupt_requested():
                raise KeyboardInterrupt from exc
            await _write_failure_artifacts_async(
                page,
                diagnostics_dir / "pull_failures",
                metadata.media_id,
            )
            raise DownloadError(f"Failed to open {metadata.product_url}: {exc}") from exc
        finally:
            await _remove_preview_media_blocker_async(page, preview_media_blocker)

        location = classify_photos_url(page.url)
        if location.media_id is None:
            await _write_failure_artifacts_async(
                page,
                diagnostics_dir / "pull_failures",
                metadata.media_id,
            )
            raise BrowserSessionError(
                "Persistent browser profile stopped reaching Google Photos media routes "
                "during pull. "
                f"Current URL: {page.url}. Rerun `gphoto-pull login`."
            )

        if on_stage is not None:
            on_stage("request", metadata, None)
        detail_capture = _install_detail_response_capture(
            page,
            expected_media_id=metadata.media_id,
        )
        try:
            download, download_trace = await _trigger_detail_download_async(
                page,
                slot=slot,
                metadata=metadata,
                photos_ui=photos_ui,
            )
            await _flush_response_capture(detail_capture)
            detail_metadata = parse_detail_metadata(
                detail_capture.response_texts,
                expected_media_id=metadata.media_id,
            )
        finally:
            await _close_response_capture(detail_capture)
        return _pending_download_from_started_download_async(
            page=page,
            slot=slot,
            queued_at=queued_at,
            start_begin_at=start_begin_at,
            download_event_at=perf_counter(),
            diagnostics_dir=diagnostics_dir,
            download_dir=download_dir,
            state_store=state_store,
            metadata=metadata,
            record=record,
            download=download,
            download_trace=download_trace,
            detail_metadata=detail_metadata,
            page_url=page.url,
        )
    except PhotosUiError as exc:
        if interrupt_requested():
            raise KeyboardInterrupt from exc
        await _write_failure_artifacts_async(
            page,
            diagnostics_dir / "pull_failures",
            metadata.media_id,
        )
        raise DownloadError(str(exc)) from exc
    except PlaywrightError as exc:
        if interrupt_requested():
            raise KeyboardInterrupt from exc
        if _is_browser_shutdown_error(exc):
            raise BrowserSessionError("Browser session ended during pull.") from exc
        await _write_failure_artifacts_async(
            page,
            diagnostics_dir / "pull_failures",
            metadata.media_id,
        )
        raise DownloadError(f"Playwright download failed for {metadata.media_id}: {exc}") from exc


async def _reset_download_page_async(page: AsyncPage) -> None:
    """Description:
    Move a worker page back to a neutral document before retrying.

    Args:
        page: Worker page to reset.

    Side Effects:
        Navigates the page to `about:blank` when possible.
    """

    with suppress(Exception):
        await page.goto("about:blank", wait_until="load")


def _pending_download_from_started_download_async(
    *,
    page: AsyncPage,
    slot: int,
    queued_at: float,
    start_begin_at: float,
    download_event_at: float,
    diagnostics_dir: Path,
    download_dir: Path,
    state_store: PullStateStore,
    metadata: MediaMetadata,
    record: MediaStateRecord,
    download: AsyncDownload,
    download_trace: DownloadTrace,
    detail_metadata: DetailMetadata | None,
    page_url: str,
) -> _PendingDownload:
    """Description:
    Convert a started Playwright download into a finalized-download plan.

    Args:
        page: Worker page that owns the download.
        slot: Worker slot number.
        queued_at: Monotonic timestamp when the item entered a worker slot.
        start_begin_at: Monotonic timestamp when browser download triggering began.
        download_event_at: Monotonic timestamp when Playwright produced the download handle.
        diagnostics_dir: Diagnostics directory root.
        download_dir: Final download directory.
        state_store: SQLite state repository.
        metadata: Candidate metadata before filename refresh.
        record: Candidate state record.
        download: Started Playwright download.
        download_trace: Observed download network metadata.
        detail_metadata: Optional parsed detail metadata.
        page_url: Direct or detail URL used to start the download.

    Returns:
        Pending download with refreshed metadata and a target plan.

    Side Effects:
        Upserts refreshed metadata and writes a trace artifact.
    """

    suggested_filename = download.suggested_filename
    updated_metadata = replace(
        metadata,
        filename=suggested_filename,
        media_type=metadata.media_type or _media_type_from_filename(suggested_filename),
        mime_type=metadata.mime_type or download_trace.content_type,
    )
    refreshed_record = state_store.upsert_media(updated_metadata)
    plan = plan_download_target(download_dir, refreshed_record)

    _write_download_trace_artifact(
        diagnostics_dir / "download_traces",
        media_id=record.metadata.media_id,
        filename=updated_metadata.filename,
        product_url=metadata.product_url,
        page_url=page_url,
        trace=download_trace,
    )
    return _PendingDownload(
        slot=slot,
        page=page,
        record=record,
        metadata=updated_metadata,
        plan=plan,
        download=download,
        download_trace=download_trace,
        detail_metadata=detail_metadata,
        queued_at=queued_at,
        start_begin_at=start_begin_at,
        download_event_at=download_event_at,
    )


async def _finalize_pending_download_async(
    pending_download: _PendingDownload,
    *,
    diagnostics_dir: Path,
    state_store: PullStateStore,
    downloaded_count: int,
    failed_count: int,
    failure_media_ids: list[str],
    progress: PullProgressDisplay,
    enrichment_queue: asyncio.Queue[_EnrichmentJob] | None,
) -> tuple[int, int]:
    """Description:
    Save one pending browser download and update state/progress.

    Args:
        pending_download: Download that was already triggered.
        diagnostics_dir: Diagnostics directory root.
        state_store: SQLite media index.
        downloaded_count: Current success count.
        failed_count: Current failure count.
        failure_media_ids: Mutable failed id accumulator.
        progress: Live progress display.
        enrichment_queue: Optional queue for post-download detail metadata jobs.

    Returns:
        Updated success and failure counts.

    Side Effects:
        Saves the browser artifact, finalizes a local file, refreshes indexed
        metadata, writes diagnostics for failures, and deletes Playwright's temporary
        download file.
    """

    from playwright.async_api import Error as PlaywrightError

    plan = _resolve_pending_download_plan(pending_download)
    operation_begin_at = perf_counter()
    phase = "download"
    phase_begin_at = operation_begin_at
    item_detail = _download_item_log_line(
        pending_download.metadata,
        expected_bytes=pending_download.download_trace.content_length,
    )
    try:
        raise_if_interrupt_requested()
        staging_path = create_staging_path(plan)
        progress.update_item(pending_download.slot, "download", item_detail)
        await pending_download.download.save_as(str(staging_path))
        download_elapsed = perf_counter() - phase_begin_at
        phase = "finalize"
        phase_begin_at = perf_counter()
        progress.update_item(pending_download.slot, "finalize", item_detail)
        final_path = finalize_download(
            staging_path,
            plan,
            staging_path=staging_path,
        )
        finalize_elapsed = perf_counter() - phase_begin_at
    except PlaywrightError as exc:
        if interrupt_requested():
            raise KeyboardInterrupt from exc
        if _is_browser_shutdown_error(exc):
            raise BrowserSessionError("Browser session ended during pull.") from exc
        LOGGER.log(
            TIMING_LOG_LEVEL,
            "timing failed slot=%s name=%s phase=%s elapsed=%.2fs total=%.2fs error=%s",
            pending_download.slot + 1,
            _display_name(pending_download.metadata),
            phase,
            perf_counter() - phase_begin_at,
            perf_counter() - pending_download.queued_at,
            exc,
        )
        await _write_failure_artifacts_async(
            pending_download.page,
            diagnostics_dir / "pull_failures",
            pending_download.metadata.media_id,
        )
        failed_count += 1
        failure_media_ids.append(pending_download.metadata.media_id)
        progress.mark_failure(
            expected_bytes=pending_download.download_trace.content_length,
            started=True,
        )
        return downloaded_count, failed_count
    except Exception as exc:
        if interrupt_requested():
            raise KeyboardInterrupt from exc
        LOGGER.log(
            TIMING_LOG_LEVEL,
            "timing failed slot=%s name=%s phase=%s elapsed=%.2fs total=%.2fs error=%s",
            pending_download.slot + 1,
            _display_name(pending_download.metadata),
            phase,
            perf_counter() - phase_begin_at,
            perf_counter() - pending_download.queued_at,
            exc,
        )
        await _write_failure_artifacts_async(
            pending_download.page,
            diagnostics_dir / "pull_failures",
            pending_download.metadata.media_id,
        )
        failed_count += 1
        failure_media_ids.append(pending_download.metadata.media_id)
        progress.mark_failure(
            expected_bytes=pending_download.download_trace.content_length,
            started=True,
        )
        return downloaded_count, failed_count
    finally:
        await _cleanup_playwright_download_async(pending_download.download)

    final_metadata = replace(
        pending_download.metadata,
        filename=final_path.name,
        product_url=(
            pending_download.metadata.product_url
            or (
                f"{PHOTOS_APP_ORIGIN}/photo/"
                f"{urllib.parse.quote(pending_download.metadata.media_id, safe='')}"
            )
        ),
        bytes_size=final_path.stat().st_size,
    )
    LOGGER.log(
        TIMING_LOG_LEVEL,
        ("timing finalized slot=%s name=%s download=%.2fs finalize=%.2fs total=%.2fs bytes=%s"),
        pending_download.slot + 1,
        _display_name(final_metadata),
        download_elapsed,
        finalize_elapsed,
        perf_counter() - pending_download.queued_at,
        final_metadata.bytes_size or 0,
    )

    write_takeout_sidecar(
        final_path,
        final_metadata,
        pending_download.detail_metadata,
    )
    detail_metadata = pending_download.detail_metadata
    if enrichment_queue is not None and detail_metadata is None:
        enrichment_queue.put_nowait(
            _EnrichmentJob(
                media_path=final_path,
                metadata=final_metadata,
                queued_at=perf_counter(),
            )
        )
    LOGGER.log(
        TIMING_LOG_LEVEL,
        (
            "timing done slot=%s name=%s download=%.2fs finalize=%.2fs "
            "enrichment_queued=%s total=%.2fs bytes=%s"
        ),
        pending_download.slot + 1,
        _display_name(final_metadata),
        download_elapsed,
        finalize_elapsed,
        enrichment_queue is not None and detail_metadata is None,
        perf_counter() - pending_download.queued_at,
        final_metadata.bytes_size or 0,
    )
    progress.mark_success(
        expected_bytes=pending_download.download_trace.content_length,
        actual_bytes=final_metadata.bytes_size or 0,
    )
    progress.complete_item(
        pending_download.slot,
        "done",
        _download_item_log_line(final_metadata, expected_bytes=final_metadata.bytes_size),
    )
    state_store.upsert_media(final_metadata)
    return downloaded_count + 1, failed_count


async def _enrich_detail_metadata_after_download_async(
    page: AsyncPage,
    *,
    metadata: MediaMetadata,
) -> DetailMetadata | None:
    """Description:
    Best-effort detail metadata enrichment after media finalization.

    Args:
        page: Worker page to use for the detail/info panel.
        metadata: Final media metadata.

    Returns:
        Parsed detail metadata, or `None` when enrichment fails.

    Side Effects:
        May navigate the worker page and open the Google Photos info panel.
    """

    if metadata.product_url is None:
        return None

    from playwright.async_api import Error as PlaywrightError

    capture: _AsyncResponseCapture | None = None
    try:
        async with asyncio.timeout(DETAIL_METADATA_ENRICHMENT_TIMEOUT_SECONDS):
            capture = _install_detail_response_capture(page, expected_media_id=metadata.media_id)
            await page.goto(metadata.product_url, wait_until="domcontentloaded")
            detail = await _parse_detail_capture(capture, expected_media_id=metadata.media_id)
            if detail is not None:
                return detail

            info_button = page.get_by_label("Info").first
            await info_button.wait_for(state="visible")
            detail = await _parse_detail_capture(capture, expected_media_id=metadata.media_id)
            if detail is not None:
                return detail

            await info_button.click()
            return await _wait_for_detail_metadata_response(
                capture,
                expected_media_id=metadata.media_id,
                timeout_seconds=DETAIL_METADATA_ENRICHMENT_TIMEOUT_SECONDS,
            )
    except (PlaywrightError, TimeoutError):
        return None
    finally:
        if capture is not None:
            await _close_response_capture(capture, drain=False)


async def _parse_detail_capture(
    capture: _AsyncResponseCapture,
    *,
    expected_media_id: str,
) -> DetailMetadata | None:
    """Description:
    Parse any detail metadata already captured for one media item.

    Args:
        capture: Active detail response capture.
        expected_media_id: Media id required by the detail parser.

    Returns:
        Parsed detail metadata, or `None`.

    Side Effects:
        Waits for currently scheduled response capture tasks.
    """

    await _flush_response_capture(capture)
    return parse_detail_metadata(capture.response_texts, expected_media_id=expected_media_id)


async def _wait_for_detail_metadata_response(
    capture: _AsyncResponseCapture,
    *,
    expected_media_id: str,
    timeout_seconds: float,
) -> DetailMetadata | None:
    """Description:
    Wait for accepted response events until detail metadata is captured.

    Args:
        capture: Active detail response capture.
        expected_media_id: Media id required by the detail parser.
        timeout_seconds: Total maximum wait.

    Returns:
        Parsed detail metadata, or `None` when the timeout expires.

    Side Effects:
        Waits on accepted response events and parses captured payloads.
    """

    try:
        async with asyncio.timeout(timeout_seconds):
            while True:
                detail = await _parse_detail_capture(capture, expected_media_id=expected_media_id)
                if detail is not None:
                    return detail
                capture.accepted_event.clear()
                detail = await _parse_detail_capture(capture, expected_media_id=expected_media_id)
                if detail is not None:
                    return detail
                await capture.accepted_event.wait()
    except TimeoutError:
        return None


def _install_detail_response_capture(
    page: AsyncPage,
    *,
    expected_media_id: str,
) -> _AsyncResponseCapture:
    """Description:
    Capture item-specific detail/info batchexecute responses.

    Args:
        page: Page to observe.
        expected_media_id: Media id required by the detail parser.

    Returns:
        Response capture state.

    Side Effects:
        Registers a Playwright response listener.
    """

    def parse_or_raise(raw_text: str) -> DetailMetadata:
        detail = parse_detail_metadata([raw_text], expected_media_id=expected_media_id)
        if detail is None:
            raise RpcPayloadParseError("No matching detail metadata.")
        return detail

    return _install_batchexecute_capture_async(page, parse_or_raise)


def _download_item_log_line(metadata: MediaMetadata, *, expected_bytes: int | None) -> str:
    """Description:
    Render one concise per-item download log line.

    Args:
        metadata: Media item metadata.
        expected_bytes: Known or expected byte size.

    Returns:
        Human-readable item detail string.
    """

    uploaded = _format_datetime(metadata.uploaded_time) if metadata.uploaded_time else "upload ?"
    captured = _format_datetime(metadata.capture_time) if metadata.capture_time else "capture ?"
    media_type = metadata.media_type or "type ?"
    name = _display_name(metadata)
    size = _format_bytes(expected_bytes)
    return (
        f"[bold]{name}[/]|[magenta]{uploaded}[/]|[dim]{captured}[/]|"
        f"[cyan]{media_type}[/]|[green]{size}[/]"
    )


def _metadata_with_download_trace(
    metadata: MediaMetadata,
    trace: DownloadTrace,
) -> MediaMetadata:
    """Description:
    Add live response metadata to a media row for active progress display.

    Args:
        metadata: Candidate metadata.
        trace: Download response metadata from a live request.

    Returns:
        Metadata enriched with filename, MIME/type, and byte size when present.
    """

    filename = _filename_from_content_disposition(trace.content_disposition) or metadata.filename
    return replace(
        metadata,
        filename=filename,
        mime_type=metadata.mime_type or trace.content_type,
        media_type=(
            metadata.media_type
            or _media_type_from_mime_type(trace.content_type)
            or _media_type_from_filename(filename)
        ),
        bytes_size=metadata.bytes_size or trace.content_length,
    )


def _filename_from_content_disposition(value: str | None) -> str | None:
    """Description:
    Extract a filename from a Content-Disposition header.

    Args:
        value: Header value to parse.

    Returns:
        Filename from the header, or `None`.
    """

    if value is None:
        return None
    message = Message()
    message["content-disposition"] = value
    filename = message.get_filename()
    if filename is None or filename.strip() == "":
        return None
    return filename


def _media_type_from_mime_type(value: str | None) -> str | None:
    """Description:
    Infer coarse media type from a MIME type.

    Args:
        value: MIME type string.

    Returns:
        `photo`, `video`, or `None`.
    """

    if value is None:
        return None
    if value.startswith("image/"):
        return "photo"
    if value.startswith("video/"):
        return "video"
    return None


def _display_name(metadata: MediaMetadata) -> str:
    """Description:
    Build a compact name for progress rows.

    Args:
        metadata: Media item metadata.

    Returns:
        Filename or shortened unresolved media id.
    """

    unresolved_prefix = "unresolved-"
    if not metadata.filename.startswith(unresolved_prefix):
        return metadata.filename
    media_id = metadata.filename.removeprefix(unresolved_prefix)
    if len(media_id) <= 18:
        return media_id
    return f"{media_id[:10]}…{media_id[-6:]}"


def _format_datetime(value: datetime) -> str:
    """Description:
    Format datetimes compactly for progress rows.

    Args:
        value: Datetime to format.

    Returns:
        Compact local-style date/time string.
    """

    return value.astimezone().strftime("%b %d %H:%M")


def _format_bytes(value: int | None) -> str:
    """Description:
    Format byte counts for operator-facing logs.

    Args:
        value: Optional byte count.

    Returns:
        Human-readable size string.
    """

    if value is None:
        return "unknown"
    return f"{value / 1_000_000:.1f} MB"


def _resolve_pending_download_plan(pending_download: _PendingDownload) -> DownloadPlan:
    """Description:
    Re-plan a pending target if another worker claimed the same filename first.

    Args:
        pending_download: Pending download with the original target plan.

    Returns:
        A collision-free final target plan.
    """

    plan = pending_download.plan
    download_dir = plan.final_path.parent
    while plan.final_path.exists():
        plan = plan_download_target(download_dir, pending_download.metadata)
    return plan


async def _trigger_detail_download_async(
    page: AsyncPage,
    *,
    slot: int,
    metadata: MediaMetadata,
    photos_ui: GooglePhotosUi,
) -> tuple[AsyncDownload, DownloadTrace]:
    """Description:
    Trigger the menu-based Google Photos download action on a detail page.

    Args:
        page: Detail page for the current media item.
        slot: Worker slot number for timing logs.
        metadata: Candidate metadata for timing logs.
        photos_ui: Google Photos UI adapter.

    Returns:
        Started Playwright download and captured network trace.

    Side Effects:
        Opens the overflow menu, clicks Download, observes response events, and
        may press Escape between retries.
    """

    from playwright.async_api import Error as PlaywrightError

    last_error: Exception | None = None
    for _ in range(3):
        raise_if_interrupt_requested()
        observed_responses: list[AsyncResponse] = []

        def on_response(
            response: AsyncResponse,
            sink: list[AsyncResponse] = observed_responses,
        ) -> None:
            """Description:
            Collect responses emitted while the menu download action runs.

            Args:
                response: Playwright response event payload.
                sink: Mutable response list for the current attempt.

            Side Effects:
                Appends `response` to `sink`.
            """

            sink.append(response)

        page.on("response", on_response)
        try:
            request_begin = perf_counter()
            LOGGER.log(
                TIMING_LOG_LEVEL,
                "timing request slot=%s name=%s mode=detail step=menu-open",
                slot + 1,
                _display_name(metadata),
            )
            await photos_ui.wait_for_detail_actions_async(page, timeout_ms=10_000)
            await photos_ui.open_download_menu_async(page)
            await photos_ui.wait_for_download_action_async(page, timeout_ms=10_000)
            LOGGER.log(
                TIMING_LOG_LEVEL,
                "timing request slot=%s name=%s mode=detail step=menu-ready elapsed=%.2fs",
                slot + 1,
                _display_name(metadata),
                perf_counter() - request_begin,
            )
            async with page.expect_download(timeout=30_000) as download_info:
                await photos_ui.click_download_action_async(page)
                LOGGER.log(
                    TIMING_LOG_LEVEL,
                    "timing request slot=%s name=%s mode=detail step=click-return elapsed=%.2fs",
                    slot + 1,
                    _display_name(metadata),
                    perf_counter() - request_begin,
                )
            download = await download_info.value
            LOGGER.log(
                TIMING_LOG_LEVEL,
                "timing request slot=%s name=%s mode=detail step=download-event elapsed=%.2fs",
                slot + 1,
                _display_name(metadata),
                perf_counter() - request_begin,
            )
            return download, await _enrich_download_trace_async(
                page,
                await _build_download_trace_async(download, observed_responses),
            )
        except (PhotosUiError, PlaywrightError) as exc:
            if interrupt_requested():
                raise KeyboardInterrupt from exc
            if _is_browser_shutdown_error(exc):
                raise BrowserSessionError("Browser session ended during pull.") from exc
            last_error = exc
            with suppress(PlaywrightError):
                await page.keyboard.press("Escape")
            await _wait_for_download_menu_closed(page)
        finally:
            page.remove_listener("response", on_response)

    raise DownloadError(f"Failed to trigger the Google Photos download action: {last_error}")


async def _trigger_direct_download_async(
    page: AsyncPage,
    *,
    slot: int,
    direct_urls: tuple[str, ...],
    metadata: MediaMetadata,
    on_stage: Callable[[str, MediaMetadata, DownloadTrace | None], None] | None,
) -> tuple[AsyncDownload, DownloadTrace]:
    """Description:
    Start a download by navigating directly to a derived media URL.

    Args:
        page: Worker page used to initiate the download.
        slot: Worker slot number for timing logs.
        direct_urls: Candidate Googleusercontent download URLs.
        metadata: Candidate metadata used for progress callbacks.
        on_stage: Optional callback when a download substage changes.

    Returns:
        Started Playwright download and captured network trace.

    Side Effects:
        Navigates the worker page and performs a HEAD request for trace metadata.
    """

    from playwright.async_api import Error as PlaywrightError

    raise_if_interrupt_requested()
    probe_begin = perf_counter()
    LOGGER.log(
        TIMING_LOG_LEVEL,
        "timing probe slot=%s name=%s mode=direct step=head-start",
        slot + 1,
        _display_name(metadata),
    )
    if on_stage is not None:
        on_stage("probe", metadata, None)
    direct_url, download_trace = await _select_direct_download_async(
        page,
        direct_urls=direct_urls,
        metadata=metadata,
    )
    content_length = download_trace.content_length
    logged_size = "?" if content_length is None else str(content_length)
    LOGGER.log(
        TIMING_LOG_LEVEL,
        "timing probe slot=%s name=%s mode=direct step=head-done elapsed=%.2fs type=%s size=%s",
        slot + 1,
        _display_name(metadata),
        perf_counter() - probe_begin,
        download_trace.content_type or "?",
        logged_size,
    )
    if on_stage is not None:
        on_stage("request", metadata, download_trace)

    request_begin = perf_counter()
    LOGGER.log(
        TIMING_LOG_LEVEL,
        "timing request slot=%s name=%s mode=direct step=goto-start",
        slot + 1,
        _display_name(metadata),
    )
    async with page.expect_download(timeout=30_000) as download_info:
        with suppress(PlaywrightError):
            await page.goto(direct_url, wait_until="commit")
            LOGGER.log(
                TIMING_LOG_LEVEL,
                "timing request slot=%s name=%s mode=direct step=goto-return elapsed=%.2fs",
                slot + 1,
                _display_name(metadata),
                perf_counter() - request_begin,
            )
    download = await download_info.value
    LOGGER.log(
        TIMING_LOG_LEVEL,
        "timing request slot=%s name=%s mode=direct step=download-event elapsed=%.2fs",
        slot + 1,
        _display_name(metadata),
        perf_counter() - request_begin,
    )
    return download, replace(
        download_trace,
        download_url=download_trace.download_url or direct_url,
        final_url=download_trace.final_url or download.url,
    )


async def _select_direct_download_async(
    page: AsyncPage,
    *,
    direct_urls: tuple[str, ...],
    metadata: MediaMetadata,
) -> tuple[str, DownloadTrace]:
    """Description:
    Pick the best preview-derived direct download URL.

    Args:
        page: Worker page whose request context can make HEAD requests.
        direct_urls: Candidate direct URLs.
        metadata: Candidate metadata used for validation.

    Returns:
        Selected URL and its response trace.
    """

    traces: list[tuple[str, DownloadTrace]] = []
    for direct_url in direct_urls:
        trace = await _probe_direct_download_trace_async(page, direct_url=direct_url)
        traces.append((direct_url, trace))
        if _direct_trace_matches_metadata(trace, metadata):
            return direct_url, trace

    for direct_url, trace in traces:
        if _direct_trace_is_download(trace):
            return direct_url, trace

    raise DownloadError(f"No direct download candidate worked for {metadata.media_id}.")


def _direct_trace_matches_metadata(trace: DownloadTrace, metadata: MediaMetadata) -> bool:
    """Description:
    Check whether a direct trace matches known media type.

    Args:
        trace: Probe result.
        metadata: Candidate metadata.

    Returns:
        `True` when the response looks like the intended original artifact.
    """

    if not _direct_trace_is_download(trace):
        return False
    media_type = _media_type_for_direct_download(metadata)
    if media_type is None:
        return True
    if media_type == "video":
        return _media_type_from_mime_type(trace.content_type) == "video"
    return _media_type_from_mime_type(trace.content_type) == "photo"


def _direct_trace_is_download(trace: DownloadTrace) -> bool:
    """Description:
    Check whether a HEAD response looks like a downloadable media artifact.

    Args:
        trace: Probe result.

    Returns:
        `True` when content type or disposition indicates media.
    """

    disposition = trace.content_disposition or ""
    return "attachment" in disposition.lower()


def _bounded_download_concurrency(requested: int, queue_size: int) -> int:
    """Description:
    Clamp requested worker count to a valid value for the queue.

    Args:
        requested: Configured worker count.
        queue_size: Number of queued downloads.

    Returns:
        Worker count between one and the queue size.
    """

    if queue_size <= 0:
        return 1
    return max(1, min(requested, queue_size))


async def _wait_for_download_menu_closed(page: AsyncPage) -> None:
    """Description:
    Wait briefly for the detail overflow menu to close after Escape.

    Args:
        page: Async Playwright page.

    Side Effects:
        Waits on a selector state with a bounded timeout.
    """

    from playwright.async_api import TimeoutError as PlaywrightTimeoutError

    with suppress(PlaywrightTimeoutError):
        await page.locator('[role="menu"]').first.wait_for(state="hidden", timeout=1_000)


async def _build_download_trace_async(
    download: AsyncDownload,
    responses: list[AsyncResponse],
) -> DownloadTrace:
    """Description:
    Build trace metadata from response events around a menu-based download.

    Args:
        download: Started Playwright download.
        responses: Responses observed during the trigger window.

    Returns:
        Download trace with best-effort headers and final URL.
    """

    download_url = download.url
    candidates = [
        candidate
        for response in responses
        if (candidate := await _extract_download_trace_candidate_async(response, download_url))
        is not None
    ]
    selected = _select_download_trace_candidate(download_url, candidates)
    if selected is None:
        return DownloadTrace(download_url=download_url)
    return DownloadTrace(
        download_url=download_url,
        final_url=selected.response_url,
        content_type=selected.content_type,
        content_length=selected.content_length,
        content_disposition=selected.content_disposition,
    )


async def _probe_direct_download_trace_async(
    page: AsyncPage,
    *,
    direct_url: str,
) -> DownloadTrace:
    """Description:
    Probe trace metadata for a direct-URL download before triggering it.

    Args:
        page: Worker page whose request context can make HEAD requests.
        direct_url: Derived direct download URL.

    Returns:
        Download trace with best-effort headers and final URL.

    Side Effects:
        Performs a HEAD request through the browser context.
    """

    from playwright.async_api import Error as PlaywrightError

    try:
        response = await page.context.request.head(
            direct_url,
            fail_on_status_code=False,
            max_redirects=5,
            timeout=15_000,
        )
    except PlaywrightError:
        return DownloadTrace(
            download_url=direct_url,
        )

    headers = dict(response.headers)
    return DownloadTrace(
        download_url=direct_url,
        final_url=response.url,
        content_type=headers.get("content-type"),
        content_length=_parse_content_length(headers.get("content-length")),
        content_disposition=headers.get("content-disposition"),
    )


async def _enrich_download_trace_async(page: AsyncPage, trace: DownloadTrace) -> DownloadTrace:
    """Description:
    Fill missing trace fields with a browser-context HEAD request.

    Args:
        page: Page whose request context can make HEAD requests.
        trace: Existing trace to enrich.

    Returns:
        Enriched trace, or the original trace when enrichment fails.

    Side Effects:
        Performs a HEAD request when trace fields are incomplete.
    """

    if (
        trace.final_url is not None
        and trace.content_type is not None
        and trace.content_length is not None
        and trace.content_disposition is not None
    ):
        return trace

    from playwright.async_api import Error as PlaywrightError

    if trace.download_url is None:
        return trace

    try:
        response = await page.context.request.head(
            trace.download_url,
            fail_on_status_code=False,
            max_redirects=5,
            timeout=15_000,
        )
    except PlaywrightError:
        return trace

    headers = dict(response.headers)
    return DownloadTrace(
        download_url=trace.download_url,
        final_url=trace.final_url or response.url,
        content_type=trace.content_type or headers.get("content-type"),
        content_length=(
            trace.content_length
            if trace.content_length is not None
            else _parse_content_length(headers.get("content-length"))
        ),
        content_disposition=trace.content_disposition or headers.get("content-disposition"),
    )


def _direct_download_urls_for_metadata(metadata: MediaMetadata) -> tuple[str, ...]:
    """Description:
    Derive candidate direct download URLs from preview metadata.

    Args:
        metadata: Candidate metadata carrying preview and optional type hints.

    Returns:
        Candidate URLs in preference order.
    """

    if metadata.preview_url is None:
        return ()

    media_type = _media_type_for_direct_download(metadata)
    preview_base = _preview_base_url(metadata.preview_url)
    if media_type == "video":
        return (f"{preview_base}=dv",)
    photo_url = f"{preview_base}=s0-d-I?authuser=0"
    if media_type == "photo":
        return (photo_url,)
    return (f"{preview_base}=dv", photo_url)


def _media_type_for_direct_download(metadata: MediaMetadata) -> str | None:
    """Description:
    Infer the media type needed for direct download URL construction.

    Args:
        metadata: Candidate metadata.

    Returns:
        `photo`, `video`, or `None`.
    """

    if metadata.media_type in {"photo", "video"}:
        return metadata.media_type
    if metadata.mime_type is not None:
        if metadata.mime_type.startswith("image/"):
            return "photo"
        if metadata.mime_type.startswith("video/"):
            return "video"
    return _media_type_from_filename(metadata.filename)


def _preview_base_url(preview_url: str) -> str:
    """Description:
    Strip Googleusercontent transform parameters from a preview URL.

    Args:
        preview_url: Google Photos preview URL.

    Returns:
        Preview URL base without query, fragment, or trailing transform suffix.
    """

    parsed = urlsplit(preview_url)
    path = parsed.path
    tail = path.rsplit("/", maxsplit=1)[-1]
    if "=" in tail:
        path = path.rsplit("=", maxsplit=1)[0]
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


async def _extract_download_trace_candidate_async(
    response: AsyncResponse,
    download_url: str,
) -> _DownloadTraceCandidate | None:
    """Description:
    Extract trace-relevant headers from one observed response.

    Args:
        response: Playwright response event payload.
        download_url: Playwright download URL to match against.

    Returns:
        Candidate trace response, or `None` when unrelated.
    """

    request = response.request
    request_url = request.url
    response_url = response.url
    content_type = await _response_header_value_async(response, "content-type")
    content_disposition = await _response_header_value_async(response, "content-disposition")
    content_length = _parse_content_length(
        await _response_header_value_async(response, "content-length")
    )
    matches_download = request_url == download_url or response_url == download_url

    if (
        not matches_download
        and content_disposition is None
        and "googleusercontent.com" not in response_url
    ):
        return None

    return _DownloadTraceCandidate(
        request_url=request_url,
        response_url=response_url,
        content_type=content_type,
        content_length=content_length,
        content_disposition=content_disposition,
    )


def _select_download_trace_candidate(
    download_url: str,
    candidates: list[_DownloadTraceCandidate],
) -> _DownloadTraceCandidate | None:
    """Description:
    Pick the response most likely to describe the final download artifact.

    Args:
        download_url: Playwright download URL to prefer for exact matches.
        candidates: Candidate responses extracted from observed traffic.

    Returns:
        Best candidate, or `None` when no candidates exist.
    """

    if not candidates:
        return None

    def score(candidate: _DownloadTraceCandidate) -> tuple[int, int, int, int]:
        """Description:
        Rank candidates by attachment headers, URL match, size, and host signal.

        Args:
            candidate: Candidate to rank.

        Returns:
            Sort key where larger is better.
        """

        disposition = candidate.content_disposition or ""
        has_attachment = int("attachment" in disposition.lower())
        exact_match = int(
            candidate.request_url == download_url or candidate.response_url == download_url
        )
        has_length = int(candidate.content_length is not None)
        usercontent = int("googleusercontent.com" in candidate.response_url)
        return (has_attachment, exact_match, has_length, usercontent)

    return max(candidates, key=score)


async def _response_header_value_async(
    response: AsyncResponse,
    header_name: str,
) -> str | None:
    """Description:
    Read and normalize one response header.

    Args:
        response: Playwright response event payload.
        header_name: Case-insensitive header name.

    Returns:
        Non-empty stripped header value, or `None`.
    """

    value = await response.header_value(header_name)
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _parse_content_length(value: str | None) -> int | None:
    """Description:
    Parse a non-negative Content-Length header.

    Args:
        value: Header value to parse.

    Returns:
        Parsed byte count, or `None` for missing/invalid values.
    """

    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    if parsed < 0:
        return None
    return parsed


def _write_download_trace_artifact(
    directory: Path,
    *,
    media_id: str,
    filename: str,
    product_url: str | None,
    page_url: str,
    trace: DownloadTrace,
) -> None:
    """Description:
    Persist structured diagnostics for a triggered download.

    Args:
        directory: Directory for trace JSON files.
        media_id: Google Photos media key.
        filename: Local filename chosen for the download.
        product_url: Google Photos product/detail URL when known.
        page_url: Direct or detail page URL used to start the download.
        trace: Captured download trace.

    Side Effects:
        Writes a JSON trace artifact.
    """

    directory.mkdir(parents=True, exist_ok=True)
    payload = _DownloadTraceArtifact(
        media_id=media_id,
        filename=filename,
        product_url=product_url,
        page_url=page_url,
        download_trace=_DownloadTraceJson(
            download_url=trace.download_url,
            final_url=trace.final_url,
            content_type=trace.content_type,
            content_length=trace.content_length,
            content_disposition=trace.content_disposition,
        ),
    )
    encoded = msgspec.json.encode(payload)
    (directory / f"{media_id}.json").write_bytes(msgspec.json.format(encoded, indent=2) + b"\n")


async def _install_preview_media_blocker_async(
    page: AsyncPage,
) -> Callable[[AsyncRoute], Awaitable[None]]:
    """Description:
    Block image previews while opening a detail page fallback.

    Args:
        page: Worker page to route.

    Returns:
        Route handler that must be passed to `_remove_preview_media_blocker`.

    Side Effects:
        Registers a route handler that aborts image requests.
    """

    async def handle_route(route: AsyncRoute) -> None:
        """Description:
        Abort preview image requests and pass everything else through.

        Args:
            route: Playwright route handle.

        Side Effects:
            Aborts or continues the intercepted request.
        """

        request = route.request
        if request.resource_type == "image":
            await route.abort()
            return
        await route.continue_()

    await page.route("**/*", handle_route)
    return handle_route


async def _remove_preview_media_blocker_async(
    page: AsyncPage,
    route_handler: Callable[[AsyncRoute], Awaitable[None]],
) -> None:
    """Description:
    Remove the preview media route handler.

    Args:
        page: Worker page that owns the route.
        route_handler: Handler returned by `_install_preview_media_blocker`.

    Side Effects:
        Removes the route handler when Playwright allows it.
    """

    from playwright.async_api import Error as PlaywrightError

    with suppress(PlaywrightError):
        await page.unroute("**/*", route_handler)


async def _cleanup_playwright_download_async(download: AsyncDownload) -> None:
    """Description:
    Delete Playwright's temporary download artifact after finalization.

    Args:
        download: Playwright download handle to clean up.

    Side Effects:
        Requests deletion of the temporary browser artifact.
    """

    from playwright.async_api import Error as PlaywrightError

    with suppress(PlaywrightError):
        await download.delete()


def _is_browser_shutdown_error(exc: Exception) -> bool:
    """Description:
    Classify Playwright exceptions caused by browser/context/page shutdown.

    Args:
        exc: Exception to inspect.

    Returns:
        `True` when the message indicates closed browser state.
    """

    message = str(exc).lower()
    return any(
        needle in message
        for needle in (
            "target page, context or browser has been closed",
            "browser has been closed",
            "context has been closed",
            "page has been closed",
            "connection closed",
        )
    )


def _media_type_from_filename(filename: str) -> str | None:
    """Description:
    Infer media type from a filename extension.

    Args:
        filename: Filename to inspect.

    Returns:
        `photo`, `video`, or `None`.
    """

    suffix = Path(filename).suffix.lower()
    if suffix in {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".heic",
        ".heif",
        ".webp",
        ".bmp",
        ".tif",
        ".tiff",
    }:
        return "photo"
    if suffix in {".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm", ".3gp"}:
        return "video"
    return None

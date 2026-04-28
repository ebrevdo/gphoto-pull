# pyright: reportPrivateUsage=false

import asyncio
import unittest
from collections.abc import Callable
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

import msgspec.json

from gphoto_pull.automation import (
    AccountScope,
    GooglePhotosPuller,
    PullExecutionSummary,
    _account_identity_from_google_account_label,
    _account_scope_key,
    _account_scoped_sync_db_path,
    _AsyncResponseCapture,
    _build_download_trace_async,
    _close_response_capture,
    _direct_download_urls_for_metadata,
    _download_candidates_async,
    _EnrichmentJob,
    _finalize_pending_download_async,
    _page_recent_payloads_to_window,
    _PendingDownload,
    _persist_recent_payloads_from_responses,
    _recent_payload_cursor,
    _recent_payload_stats,
    _RecentPageRequest,
    _start_download_candidate_async,
    _store_recent_page_checkpoint,
    _target_path_exists,
    _update_recent_payload_stats,
    _wait_for_detail_metadata_response,
)
from gphoto_pull.browser import BrowserSessionError, BrowserSessionPaths
from gphoto_pull.config import ConfigOverrides, ProjectConfig
from gphoto_pull.download import DownloadError, DownloadPlan
from gphoto_pull.models import DownloadTrace, MediaMetadata, MediaStateRecord
from gphoto_pull.photos_ui import GooglePhotosUi
from gphoto_pull.progress import PullProgressDisplay
from gphoto_pull.rpc_payloads import JsonValue
from gphoto_pull.state import PullStateStore
from gphoto_pull.takeout import TakeoutSidecar

if TYPE_CHECKING:
    from playwright.async_api import APIRequestContext, BrowserContext, Download, Page, Response


class _FakeDownload:
    def __init__(self, url: str) -> None:
        self.url = url


class _FakeRequest:
    def __init__(self, url: str) -> None:
        self.url = url


class _FakeResponse:
    def __init__(
        self,
        *,
        request_url: str,
        response_url: str,
        content_type: str | None = None,
        content_length: str | None = None,
        content_disposition: str | None = None,
    ) -> None:
        self.request = _FakeRequest(request_url)
        self.url = response_url
        self._headers = {
            "content-type": content_type,
            "content-length": content_length,
            "content-disposition": content_disposition,
        }

    async def header_value(self, name: str) -> str | None:
        return self._headers.get(name)


class _FakePage:
    def __init__(self) -> None:
        self.goto_calls: list[tuple[str, str]] = []
        self.wait_calls: list[int] = []

    async def goto(self, url: str, *, wait_until: str) -> None:
        self.goto_calls.append((url, wait_until))

    async def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_calls.append(timeout_ms)

    async def close(self) -> None:
        return None


class _FakeContext:
    async def new_page(self) -> _FakePage:
        return _FakePage()


class _FakeLimitedContext:
    def __init__(self, *, limit: int) -> None:
        self.limit = limit
        self.calls = 0
        self.pages: list[_FakePage] = []

    async def new_page(self) -> _FakePage:
        self.calls += 1
        if len(self.pages) >= self.limit:
            raise RuntimeError("page limit reached")
        page = _FakePage()
        self.pages.append(page)
        return page


class _FakeCapturePage:
    def __init__(self) -> None:
        self.removed = False

    def remove_listener(self, event: str, handler: object) -> None:
        del event, handler
        self.removed = True


class _FakePlaywrightDownload:
    def __init__(self, content: bytes) -> None:
        self._content = content
        self.deleted = False

    async def save_as(self, path: str) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self._content)

    async def delete(self) -> None:
        self.deleted = True


class _FakeApiResponse:
    def __init__(self, raw_text: str) -> None:
        self._raw_text = raw_text

    async def text(self) -> str:
        return self._raw_text


class _FakeApiRequestContext:
    def __init__(self, responses: list[str]) -> None:
        self._responses = responses
        self.requested_cursors: list[str] = []

    async def post(
        self,
        url: str,
        *,
        form: dict[str, str],
        headers: dict[str, str],
    ) -> _FakeApiResponse:
        del url, headers
        request = cast(JsonValue, msgspec.json.decode(form["f.req"].encode()))
        if not isinstance(request, list) or not request or not isinstance(request[0], list):
            raise AssertionError("unexpected f.req shape")
        group = request[0]
        if not group or not isinstance(group[0], list) or len(group[0]) <= 1:
            raise AssertionError("unexpected f.req group shape")
        inner_text = group[0][1]
        if not isinstance(inner_text, str):
            raise AssertionError("unexpected f.req inner payload")
        inner = cast(JsonValue, msgspec.json.decode(inner_text.encode()))
        if not isinstance(inner, list) or len(inner) <= 2:
            raise AssertionError("unexpected f.req inner shape")
        self.requested_cursors.append(str(inner[2]))
        return _FakeApiResponse(self._responses.pop(0))


def _recent_raw(media_id: str, upload_ms: int, cursor: str) -> str:
    payload = [
        [
            [
                media_id,
                ["https://example.invalid/pw", 100, 100],
                upload_ms - 1000,
                None,
                0,
                upload_ms,
            ]
        ],
        cursor,
    ]
    payload_text = msgspec.json.encode(payload).decode()
    return (
        """)]}'\n\n258\n"""
        + msgspec.json.encode(
            [["wrb.fr", "opaqueRecentRpc", payload_text, None, None, None]]
        ).decode()
        + "\n"
    )


def _cursor_only_raw(cursor: str) -> str:
    payload = [None, cursor]
    payload_text = msgspec.json.encode(payload).decode()
    return (
        """)]}'\n\n258\n"""
        + msgspec.json.encode(
            [["wrb.fr", "opaqueRecentRpc", payload_text, None, None, None]]
        ).decode()
        + "\n"
    )


def _detail_raw(media_id: str) -> str:
    item: list[JsonValue] = [
        media_id,
        "caption",
        "IMG_0001.JPG",
        1467939770000,
        -25200000,
        12345,
        100,
        200,
        None,
        None,
        None,
        None,
        None,
        None,
    ]
    payload_text = msgspec.json.encode([item]).decode()
    return (
        """)]}'\n\n258\n"""
        + msgspec.json.encode([["wrb.fr", "detailRpc", payload_text, None, None, None]]).decode()
        + "\n"
    )


def _noop_response_handler(response: object) -> None:
    del response


class DownloadTraceTests(unittest.TestCase):
    def test_pull_live_checks_auth_before_refreshing_covered_index(self) -> None:
        class FakePage:
            async def close(self) -> None:
                events.append("close-page")

        class FakeContextManager:
            async def __aenter__(self) -> "FakeContextManager":
                return self

            async def __aexit__(
                self,
                _exc_type: object,
                _exc: object,
                _tb: object,
            ) -> bool:
                return False

            async def new_page(self) -> FakePage:
                events.append("new-page")
                return FakePage()

        class FakeStore:
            def __enter__(self) -> "FakeStore":
                events.append("store")
                return self

            def __exit__(
                self,
                _exc_type: object,
                _exc: object,
                _tb: object,
            ) -> bool:
                return False

            def upload_coverage_satisfies(self, _after: datetime) -> bool:
                return True

            def upload_window_satisfies(
                self,
                *,
                after: datetime,
                before: datetime | None,
            ) -> bool:
                _ = after, before
                return False

            def list_media_in_upload_window(
                self,
                *,
                after: datetime,
                before: datetime | None,
            ) -> list[MediaStateRecord]:
                _ = after, before
                return []

        async def fake_auth_check(*_args: object, **_kwargs: object) -> AccountScope:
            events.append("auth")
            return AccountScope("account-key")

        async def fake_recent_probe(*_args: object, **kwargs: object) -> int:
            events.append("recent")
            self.assertIs(kwargs.get("stop_on_index_overlap"), True)
            self.assertIs(kwargs.get("allow_checkpoint_resume"), False)
            return 1

        async def fake_updates_probe(*_args: object, **_kwargs: object) -> int:
            events.append("updates")
            return 1

        async def fake_download_summary(*_args: object, **_kwargs: object) -> PullExecutionSummary:
            events.append("download")
            return PullExecutionSummary(
                queued_count=0,
                skipped_existing_count=0,
                downloaded_count=0,
                failed_count=0,
                failure_media_ids=(),
            )

        events: list[str] = []
        config = ProjectConfig.from_sources(
            config_path=Path("/missing-gphoto-pull.toml"),
            overrides=ConfigOverrides(after="2026-01-02T03:04:05-08:00"),
        )
        service = GooglePhotosPuller(config)
        paths = BrowserSessionPaths(
            download_dir=config.download_dir,
            profile_dir=config.browser_profile_dir,
            diagnostics_dir=config.diagnostics_dir,
            browsers_path=config.browsers_path,
        )

        with (
            patch(
                "gphoto_pull.automation.launched_browser_context_async",
                return_value=FakeContextManager(),
            ),
            patch(
                "gphoto_pull.automation._assert_authenticated_photos_session_async",
                fake_auth_check,
            ),
            patch("gphoto_pull.automation._capture_recent_probe_async", fake_recent_probe),
            patch("gphoto_pull.automation._capture_updates_probe_async", fake_updates_probe),
            patch("gphoto_pull.automation.PullStateStore", return_value=FakeStore()),
            patch.object(service, "_download_summary_candidates", fake_download_summary),
        ):
            asyncio.run(service._pull_live(paths, []))

        self.assertEqual(
            events,
            [
                "auth",
                "store",
                "new-page",
                "recent",
                "close-page",
                "new-page",
                "updates",
                "close-page",
                "download",
            ],
        )

    def test_pull_live_skips_refresh_for_covered_bounded_index_window(self) -> None:
        test_case = self

        class FakeContextManager:
            async def __aenter__(self) -> "FakeContextManager":
                return self

            async def __aexit__(
                self,
                _exc_type: object,
                _exc: object,
                _tb: object,
            ) -> bool:
                return False

            async def new_page(self) -> object:
                raise AssertionError("covered bounded windows should not refresh live pages")

        class FakeStore:
            def __enter__(self) -> "FakeStore":
                events.append("store")
                return self

            def __exit__(
                self,
                _exc_type: object,
                _exc: object,
                _tb: object,
            ) -> bool:
                return False

            def upload_window_satisfies(
                self,
                *,
                after: datetime,
                before: datetime | None,
            ) -> bool:
                events.append("coverage")
                test_case.assertEqual(
                    after,
                    datetime.fromisoformat("2026-01-02T03:04:05-08:00"),
                )
                test_case.assertEqual(
                    before,
                    datetime.fromisoformat("2026-01-03T03:04:05-08:00"),
                )
                return True

            def upload_coverage_satisfies(self, _after: datetime) -> bool:
                raise AssertionError("covered bounded windows should not probe overlap")

            def list_media_in_upload_window(
                self,
                *,
                after: datetime,
                before: datetime | None,
            ) -> list[MediaStateRecord]:
                events.append("list")
                test_case.assertEqual(
                    after,
                    datetime.fromisoformat("2026-01-02T03:04:05-08:00"),
                )
                test_case.assertEqual(
                    before,
                    datetime.fromisoformat("2026-01-03T03:04:05-08:00"),
                )
                return []

        async def fake_auth_check(*_args: object, **_kwargs: object) -> AccountScope:
            events.append("auth")
            return AccountScope("account-key")

        async def fail_recent_probe(*_args: object, **_kwargs: object) -> int:
            raise AssertionError("covered bounded windows should not capture Recently added")

        async def fail_updates_probe(*_args: object, **_kwargs: object) -> int:
            raise AssertionError("covered bounded windows should not capture Updates")

        async def fake_download_summary(*_args: object, **_kwargs: object) -> PullExecutionSummary:
            events.append("download")
            return PullExecutionSummary(
                queued_count=0,
                skipped_existing_count=0,
                downloaded_count=0,
                failed_count=0,
                failure_media_ids=(),
            )

        events: list[str] = []
        config = ProjectConfig.from_sources(
            config_path=Path("/missing-gphoto-pull.toml"),
            overrides=ConfigOverrides(
                after="2026-01-02T03:04:05-08:00",
                before="2026-01-03T03:04:05-08:00",
            ),
        )
        service = GooglePhotosPuller(config)
        paths = BrowserSessionPaths(
            download_dir=config.download_dir,
            profile_dir=config.browser_profile_dir,
            diagnostics_dir=config.diagnostics_dir,
            browsers_path=config.browsers_path,
        )
        lines: list[str] = []

        with (
            patch(
                "gphoto_pull.automation.launched_browser_context_async",
                return_value=FakeContextManager(),
            ),
            patch(
                "gphoto_pull.automation._assert_authenticated_photos_session_async",
                fake_auth_check,
            ),
            patch("gphoto_pull.automation._capture_recent_probe_async", fail_recent_probe),
            patch("gphoto_pull.automation._capture_updates_probe_async", fail_updates_probe),
            patch("gphoto_pull.automation.PullStateStore", return_value=FakeStore()),
            patch.object(service, "_download_summary_candidates", fake_download_summary),
        ):
            asyncio.run(service._pull_live(paths, lines))

        self.assertEqual(events, ["auth", "store", "coverage", "list", "download"])
        self.assertIn("Indexed window coverage: complete; skipping live diagnostics", lines)

    def test_account_scoped_sync_db_path_uses_template_parent_and_name(self) -> None:
        template = Path("/tmp/gphoto/state/pull-state.sqlite3")

        path = _account_scoped_sync_db_path(template, AccountScope("account-key"))

        self.assertEqual(
            path,
            Path("/tmp/gphoto/state/accounts/account-key/pull-state.sqlite3"),
        )

    def test_account_identity_uses_google_account_aria_label_email(self) -> None:
        identity = _account_identity_from_google_account_label(
            "Google Account: Test.User@example.COM"
        )

        self.assertEqual(identity, "email:test.user@example.com")

    def test_account_identity_rejects_missing_email(self) -> None:
        with self.assertRaisesRegex(BrowserSessionError, "did not contain an email"):
            _account_identity_from_google_account_label("Google Account")

    def test_account_scope_key_hashes_identity(self) -> None:
        key = _account_scope_key("email:test.user@example.com")

        self.assertEqual(len(key), 16)
        self.assertNotIn("@", key)

    def test_recent_payload_cursor_handles_cursor_only_response(self) -> None:
        raw_text = (
            """)]}'\n\n258\n"""
            """[["wrb.fr","opaqueRecentRpc","[null,\\"next-cursor\\"]",null,null,null,"generic"]]\n"""
        )

        cursor = _recent_payload_cursor(raw_text)

        self.assertIsNotNone(cursor)
        assert cursor is not None
        self.assertEqual(cursor.rpc_id, "opaqueRecentRpc")
        self.assertEqual(cursor.cursor, "next-cursor")

    def test_store_recent_page_checkpoint_records_media_page_cursor(self) -> None:
        raw_text = (
            """)]}'\n\n258\n"""
            """[["wrb.fr","opaqueRecentRpc","[[[\\"AF1QipCheckpoint\\",[\\"https://example.invalid/pw\\",100,100],1770000000000,null,0,1770000100000]],\\"next-cursor\\"]",null,null,null,"generic"]]\n"""
        )
        cursor = _recent_payload_cursor(raw_text)

        with (
            TemporaryDirectory() as tmp_dir,
            PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
        ):
            _store_recent_page_checkpoint(
                store,
                raw_text=raw_text,
                cursor=cursor,
                page_count=4,
            )
            checkpoint = store.best_recent_page_checkpoint(
                after=datetime(2026, 1, 1, tzinfo=UTC),
                rpc_ids=("opaqueRecentRpc",),
            )
            indexed = store.get_media("AF1QipCheckpoint")

        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        self.assertEqual(checkpoint.cursor, "next-cursor")
        self.assertEqual(checkpoint.page_count, 4)
        self.assertIsNotNone(indexed)
        assert indexed is not None
        self.assertEqual(
            indexed.metadata.uploaded_time,
            datetime(2026, 2, 2, 2, 41, 40, tzinfo=UTC),
        )
        self.assertEqual(
            indexed.metadata.product_url,
            "https://photos.google.com/photo/AF1QipCheckpoint",
        )

    def test_recent_payload_stats_incrementally_dedupes_media(self) -> None:
        first = (
            """)]}'\n\n258\n"""
            """[["wrb.fr","opaqueRecentRpc","[[[\\"AF1QipStats\\",[\\"https://example.invalid/pw\\",100,100],1770000000000,null,0,1770000100000]],\\"cursor-1\\"]",null,null,null,"generic"]]\n"""
        )
        duplicate_older = (
            """)]}'\n\n258\n"""
            """[["wrb.fr","opaqueRecentRpc","[[[\\"AF1QipStats\\",[\\"https://example.invalid/pw\\",100,100],1760000000000,null,0,1760000100000]],\\"cursor-2\\"]",null,null,null,"generic"]]\n"""
        )
        stats = _recent_payload_stats([first])

        _update_recent_payload_stats(stats, duplicate_older)

        self.assertEqual(stats.item_count, 1)
        self.assertEqual(
            stats.oldest_upload_time,
            datetime.fromtimestamp(1760000100000 / 1000, tz=UTC),
        )

    def test_recent_pagination_ignores_bad_checkpoint_for_current_run(self) -> None:
        initial = _recent_raw("AF1QipInitial", 1770000100000, "first-page-cursor")
        bad_pages = [_cursor_only_raw(f"bad-cursor-{index}") for index in range(1, 6)]
        fallback_page = _recent_raw("AF1QipFallback", 1262304000000, "done-cursor")
        request_context = _FakeApiRequestContext([*bad_pages, fallback_page])

        async def run() -> bool:
            with (
                TemporaryDirectory() as tmp_dir,
                PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
            ):
                store.upsert_recent_page_checkpoint(
                    rpc_id="opaqueRecentRpc",
                    cursor="bad-cursor-0",
                    oldest_upload_time=datetime(2026, 1, 1, tzinfo=UTC),
                    item_count=500,
                    page_count=10,
                )
                capture = _AsyncResponseCapture(
                    response_texts=[initial],
                    tasks=set(),
                    accepted_event=asyncio.Event(),
                    page=cast("Page", SimpleNamespace()),
                    response_handler=cast("Callable[[Response], None]", _noop_response_handler),
                )
                return await _page_recent_payloads_to_window(
                    cast("APIRequestContext", request_context),
                    capture,
                    [
                        _RecentPageRequest(
                            rpc_id="opaqueRecentRpc",
                            url="https://photos.google.com/_/PhotosUi/data/batchexecute",
                            at_token="token",
                        )
                    ],
                    after=datetime(2011, 1, 1, tzinfo=UTC),
                    state_store=store,
                )

        completed = asyncio.run(run())

        self.assertTrue(completed)
        self.assertEqual(
            request_context.requested_cursors,
            [
                "bad-cursor-0",
                "bad-cursor-1",
                "bad-cursor-2",
                "bad-cursor-3",
                "bad-cursor-4",
                "first-page-cursor",
            ],
        )

    def test_recent_pagination_records_checkpoint_resume_as_disjoint_coverage(self) -> None:
        initial = _recent_raw(
            "AF1QipInitial",
            int(datetime(2026, 4, 27, tzinfo=UTC).timestamp() * 1000),
            "first-page-cursor",
        )
        checkpoint_page = _recent_raw(
            "AF1QipCheckpointOlder",
            int(datetime(2025, 12, 31, tzinfo=UTC).timestamp() * 1000),
            "done-cursor",
        )
        request_context = _FakeApiRequestContext([checkpoint_page])

        async def run() -> tuple[bool, bool, bool]:
            with (
                TemporaryDirectory() as tmp_dir,
                PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
            ):
                store.upsert_recent_page_checkpoint(
                    rpc_id="opaqueRecentRpc",
                    cursor="checkpoint-cursor",
                    oldest_upload_time=datetime(2026, 1, 15, tzinfo=UTC),
                    item_count=500,
                    page_count=10,
                )
                capture = _AsyncResponseCapture(
                    response_texts=[initial],
                    tasks=set(),
                    accepted_event=asyncio.Event(),
                    page=cast("Page", SimpleNamespace()),
                    response_handler=cast("Callable[[Response], None]", _noop_response_handler),
                )

                completed = await _page_recent_payloads_to_window(
                    cast("APIRequestContext", request_context),
                    capture,
                    [
                        _RecentPageRequest(
                            rpc_id="opaqueRecentRpc",
                            url="https://photos.google.com/_/PhotosUi/data/batchexecute",
                            at_token="token",
                        )
                    ],
                    after=datetime(2026, 1, 1, tzinfo=UTC),
                    state_store=store,
                    allow_checkpoint_resume=True,
                )

                self.assertTrue(completed)
                self.assertFalse(
                    store.upload_window_satisfies(
                        after=datetime(2026, 3, 1, tzinfo=UTC),
                        before=datetime(2026, 3, 2, tzinfo=UTC),
                    )
                )
                return (
                    store.upload_time_has_covering_range(
                        uploaded_time=datetime(2026, 4, 27, tzinfo=UTC),
                        after=datetime(2026, 4, 27, tzinfo=UTC),
                    ),
                    store.upload_time_has_covering_range(
                        uploaded_time=datetime(2025, 12, 31, tzinfo=UTC),
                        after=datetime(2025, 12, 31, tzinfo=UTC),
                    ),
                    store.upload_time_has_covering_range(
                        uploaded_time=datetime(2026, 4, 27, tzinfo=UTC),
                        after=datetime(2026, 3, 1, tzinfo=UTC),
                    ),
                )

        head_covered, older_covered, gap_covered = asyncio.run(run())

        self.assertEqual(request_context.requested_cursors, ["checkpoint-cursor"])
        self.assertTrue(head_covered)
        self.assertTrue(older_covered)
        self.assertFalse(gap_covered)

    def test_recent_pagination_stops_at_trusted_index_overlap(self) -> None:
        initial = _recent_raw("AF1QipNew", 1770000300000, "first-page-cursor")
        overlap = _recent_raw("AF1QipKnown", 1770000200000, "known-cursor")
        older = _recent_raw("AF1QipOlder", 1262304000000, "done-cursor")
        request_context = _FakeApiRequestContext([overlap, older])

        async def run() -> bool:
            with (
                TemporaryDirectory() as tmp_dir,
                PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
            ):
                store.upsert_media(
                    MediaMetadata(
                        media_id="AF1QipKnown",
                        filename="known.jpg",
                        uploaded_time=datetime.fromtimestamp(1770000200000 / 1000, tz=UTC),
                    )
                )
                store.record_upload_coverage(
                    oldest_upload_time=datetime(2026, 1, 1, tzinfo=UTC),
                    newest_upload_time=datetime.fromtimestamp(1770000200000 / 1000, tz=UTC),
                )
                store.upsert_recent_page_checkpoint(
                    rpc_id="opaqueRecentRpc",
                    cursor="checkpoint-cursor",
                    oldest_upload_time=datetime(2026, 1, 1, tzinfo=UTC),
                    item_count=500,
                    page_count=10,
                )
                capture = _AsyncResponseCapture(
                    response_texts=[initial],
                    tasks=set(),
                    accepted_event=asyncio.Event(),
                    page=cast("Page", SimpleNamespace()),
                    response_handler=cast("Callable[[Response], None]", _noop_response_handler),
                )
                return await _page_recent_payloads_to_window(
                    cast("APIRequestContext", request_context),
                    capture,
                    [
                        _RecentPageRequest(
                            rpc_id="opaqueRecentRpc",
                            url="https://photos.google.com/_/PhotosUi/data/batchexecute",
                            at_token="token",
                        )
                    ],
                    after=datetime(2026, 1, 1, tzinfo=UTC),
                    state_store=store,
                    stop_on_index_overlap=True,
                    allow_checkpoint_resume=False,
                )

        completed = asyncio.run(run())

        self.assertTrue(completed)
        self.assertEqual(request_context.requested_cursors, ["first-page-cursor"])

    def test_recent_pagination_ignores_overlap_when_index_coverage_is_not_trusted(self) -> None:
        initial = _recent_raw("AF1QipNew", 1770000300000, "first-page-cursor")
        overlap = _recent_raw("AF1QipKnown", 1770000200000, "known-cursor")
        older = _recent_raw("AF1QipOlder", 1262304000000, "done-cursor")
        request_context = _FakeApiRequestContext([overlap, older])

        async def run() -> bool:
            with (
                TemporaryDirectory() as tmp_dir,
                PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
            ):
                store.upsert_media(
                    MediaMetadata(
                        media_id="AF1QipKnown",
                        filename="known.jpg",
                        uploaded_time=datetime.fromtimestamp(1770000200000 / 1000, tz=UTC),
                    )
                )
                store.upsert_recent_page_checkpoint(
                    rpc_id="opaqueRecentRpc",
                    cursor="checkpoint-cursor",
                    oldest_upload_time=datetime(2026, 1, 1, tzinfo=UTC),
                    item_count=500,
                    page_count=10,
                )
                capture = _AsyncResponseCapture(
                    response_texts=[initial],
                    tasks=set(),
                    accepted_event=asyncio.Event(),
                    page=cast("Page", SimpleNamespace()),
                    response_handler=cast("Callable[[Response], None]", _noop_response_handler),
                )
                return await _page_recent_payloads_to_window(
                    cast("APIRequestContext", request_context),
                    capture,
                    [
                        _RecentPageRequest(
                            rpc_id="opaqueRecentRpc",
                            url="https://photos.google.com/_/PhotosUi/data/batchexecute",
                            at_token="token",
                        )
                    ],
                    after=datetime(2026, 1, 1, tzinfo=UTC),
                    state_store=store,
                    stop_on_index_overlap=False,
                    allow_checkpoint_resume=False,
                )

        completed = asyncio.run(run())

        self.assertTrue(completed)
        self.assertEqual(request_context.requested_cursors, ["first-page-cursor", "known-cursor"])

    def test_persist_recent_payloads_from_captured_responses_updates_index(self) -> None:
        raw_text = _recent_raw("AF1QipCaptured", 1770000100000, "next-cursor")

        with (
            TemporaryDirectory() as tmp_dir,
            PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
        ):
            _persist_recent_payloads_from_responses(store, [raw_text])
            record = store.get_media("AF1QipCaptured")

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(
            record.metadata.product_url, "https://photos.google.com/photo/AF1QipCaptured"
        )

    def test_wait_for_detail_metadata_response_returns_none_after_timeout(self) -> None:
        capture = _AsyncResponseCapture(
            response_texts=[],
            tasks=set(),
            accepted_event=asyncio.Event(),
            page=cast("Page", SimpleNamespace()),
            response_handler=cast("Callable[[Response], None]", _noop_response_handler),
        )

        detail = asyncio.run(
            _wait_for_detail_metadata_response(
                capture,
                expected_media_id="AF1QipMissing",
                timeout_seconds=0.01,
            )
        )

        self.assertIsNone(detail)

    def test_wait_for_detail_metadata_response_wakes_on_matching_capture(self) -> None:
        async def run() -> str | None:
            capture = _AsyncResponseCapture(
                response_texts=[],
                tasks=set(),
                accepted_event=asyncio.Event(),
                page=cast("Page", SimpleNamespace()),
                response_handler=cast("Callable[[Response], None]", _noop_response_handler),
            )
            task = asyncio.create_task(
                _wait_for_detail_metadata_response(
                    capture,
                    expected_media_id="AF1QipDetail",
                    timeout_seconds=1.0,
                )
            )
            await asyncio.sleep(0)
            capture.response_texts.append(_detail_raw("AF1QipDetail"))
            capture.accepted_event.set()
            detail = await task
            return None if detail is None else detail.media_id

        self.assertEqual(asyncio.run(run()), "AF1QipDetail")

    def test_close_response_capture_can_cancel_without_draining(self) -> None:
        async def run() -> tuple[bool, bool]:
            event = asyncio.Event()

            async def wait_forever() -> None:
                await event.wait()

            task = asyncio.create_task(wait_forever())
            page = _FakeCapturePage()
            capture = _AsyncResponseCapture(
                response_texts=[],
                tasks={task},
                accepted_event=asyncio.Event(),
                page=cast("Page", page),
                response_handler=cast("Callable[[Response], None]", _noop_response_handler),
            )

            await asyncio.wait_for(_close_response_capture(capture, drain=False), timeout=0.1)
            await asyncio.sleep(0)
            return page.removed, task.cancelled()

        removed, cancelled = asyncio.run(run())

        self.assertTrue(removed)
        self.assertTrue(cancelled)

    def test_build_download_trace_prefers_attachment_response(self) -> None:
        async def run() -> DownloadTrace:
            return await _build_download_trace_async(
                cast(
                    "Download",
                    _FakeDownload("https://photos.fife.usercontent.google.com/download/start"),
                ),
                [
                    cast(
                        "Response",
                        _FakeResponse(
                            request_url="https://photos.fife.usercontent.google.com/download/start",
                            response_url="https://photos.fife.usercontent.google.com/download/final",
                            content_type="image/jpeg",
                            content_length="2400000",
                            content_disposition='attachment; filename="IMG_0001.JPG"',
                        ),
                    ),
                ],
            )

        trace = asyncio.run(run())

        self.assertEqual(
            trace,
            DownloadTrace(
                download_url="https://photos.fife.usercontent.google.com/download/start",
                final_url="https://photos.fife.usercontent.google.com/download/final",
                content_type="image/jpeg",
                content_length=2_400_000,
                content_disposition='attachment; filename="IMG_0001.JPG"',
            ),
        )

    def test_build_download_trace_falls_back_to_download_url_when_no_match_exists(self) -> None:
        async def run() -> DownloadTrace:
            return await _build_download_trace_async(
                cast(
                    "Download",
                    _FakeDownload("https://photos.fife.usercontent.google.com/download/start"),
                ),
                [
                    cast(
                        "Response",
                        _FakeResponse(
                            request_url="https://example.invalid/other",
                            response_url="https://example.invalid/other",
                            content_type="text/html",
                        ),
                    ),
                ],
            )

        trace = asyncio.run(run())

        self.assertEqual(
            trace,
            DownloadTrace(
                download_url="https://photos.fife.usercontent.google.com/download/start",
            ),
        )

    def test_direct_download_url_for_photo_uses_attachment_suffix(self) -> None:
        metadata = MediaMetadata(
            media_id="media-photo",
            filename="unresolved-media-photo",
            media_type="photo",
            preview_url="https://photos.fife.usercontent.google.com/pw/preview-photo-token",
        )

        self.assertEqual(
            _direct_download_urls_for_metadata(metadata),
            (
                "https://photos.fife.usercontent.google.com/pw/"
                "preview-photo-token=s0-d-I?authuser=0",
            ),
        )

    def test_direct_download_url_for_video_uses_dv_suffix(self) -> None:
        metadata = MediaMetadata(
            media_id="media-video",
            filename="unresolved-media-video",
            media_type="video",
            preview_url="https://photos.fife.usercontent.google.com/pw/preview-video-token",
        )

        self.assertEqual(
            _direct_download_urls_for_metadata(metadata),
            ("https://photos.fife.usercontent.google.com/pw/preview-video-token=dv",),
        )

    def test_direct_download_urls_for_unknown_type_probe_video_then_photo(self) -> None:
        metadata = MediaMetadata(
            media_id="media-unknown",
            filename="unresolved-media-unknown",
            preview_url="https://photos.fife.usercontent.google.com/pw/preview-token",
        )

        self.assertEqual(
            _direct_download_urls_for_metadata(metadata),
            (
                "https://photos.fife.usercontent.google.com/pw/preview-token=dv",
                "https://photos.fife.usercontent.google.com/pw/preview-token=s0-d-I?authuser=0",
            ),
        )

    def test_target_path_exists_checks_primary_path_before_collision_suffixing(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            primary = download_dir / "uploaded" / "2026" / "04" / "19" / "IMG_0001.JPG"
            primary.parent.mkdir(parents=True)
            primary.write_bytes(b"image")
            primary.with_name("IMG_0001.JPG.supplemental-metadata.json").write_text(
                '"url": "https://photos.google.com/photo/AF1QipSame"',
                encoding="utf-8",
            )
            record = MediaStateRecord(
                metadata=MediaMetadata(
                    media_id="AF1QipSame",
                    filename="IMG_0001.JPG",
                    uploaded_time=datetime(2026, 4, 19, tzinfo=UTC),
                )
            )

            self.assertTrue(_target_path_exists(record, download_dir=download_dir))

    def test_target_path_exists_recovers_reset_index_download_from_sidecar(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            existing = download_dir / "uploaded" / "2026" / "04" / "19" / "IMG_0001.JPG"
            existing.parent.mkdir(parents=True)
            existing.write_bytes(b"image")
            existing.with_name("IMG_0001.JPG.supplemental-metadata.json").write_bytes(
                msgspec.json.encode(
                    {"url": "https://photos.google.com/search/_tra_/photo/AF1QipSame"}
                )
            )
            record = MediaStateRecord(
                metadata=MediaMetadata(
                    media_id="AF1QipSame",
                    filename="unresolved-AF1QipSame",
                    uploaded_time=datetime(2026, 4, 19, tzinfo=UTC),
                )
            )

            self.assertTrue(_target_path_exists(record, download_dir=download_dir))

    def test_target_path_exists_ignores_matching_sidecar_without_media_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            sidecar_dir = download_dir / "uploaded" / "2026" / "04" / "19"
            sidecar_dir.mkdir(parents=True)
            (sidecar_dir / "IMG_0001.JPG.supplemental-metadata.json").write_bytes(
                msgspec.json.encode({"url": "https://photos.google.com/photo/AF1QipSame"})
            )
            record = MediaStateRecord(
                metadata=MediaMetadata(
                    media_id="AF1QipSame",
                    filename="unresolved-AF1QipSame",
                    uploaded_time=datetime(2026, 4, 19, tzinfo=UTC),
                )
            )

            self.assertFalse(_target_path_exists(record, download_dir=download_dir))

    def test_target_path_exists_does_not_skip_different_media_collision(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            primary = download_dir / "uploaded" / "2026" / "04" / "19" / "IMG_0001.JPG"
            primary.parent.mkdir(parents=True)
            primary.write_bytes(b"image")
            primary.with_name("IMG_0001.JPG.supplemental-metadata.json").write_text(
                '"url": "https://photos.google.com/photo/AF1QipOther"',
                encoding="utf-8",
            )
            record = MediaStateRecord(
                metadata=MediaMetadata(
                    media_id="AF1QipSame",
                    filename="IMG_0001.JPG",
                    uploaded_time=datetime(2026, 4, 19, tzinfo=UTC),
                )
            )

            self.assertFalse(_target_path_exists(record, download_dir=download_dir))

    def test_target_path_exists_does_not_skip_without_sidecar(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            primary = download_dir / "uploaded" / "2026" / "04" / "19" / "IMG_0001.JPG"
            primary.parent.mkdir(parents=True)
            primary.write_bytes(b"manual image")
            record = MediaStateRecord(
                metadata=MediaMetadata(
                    media_id="AF1QipSame",
                    filename="IMG_0001.JPG",
                    uploaded_time=datetime(2026, 4, 19, tzinfo=UTC),
                )
            )

            self.assertFalse(_target_path_exists(record, download_dir=download_dir))

    def test_download_candidates_finalizes_trailing_pending_downloads(self) -> None:
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        pending_download = SimpleNamespace(
            slot=0,
            metadata=metadata,
            download_trace=DownloadTrace(content_length=1),
            download_event_at=0.0,
        )
        finalized: list[SimpleNamespace] = []

        async def fake_finalize(
            current_pending: SimpleNamespace,
            *,
            diagnostics_dir: Path,
            state_store: PullStateStore,
            downloaded_count: int,
            failed_count: int,
            failure_media_ids: list[str],
            progress: PullProgressDisplay,
            enrichment_queue: asyncio.Queue[object] | None,
        ) -> tuple[int, int]:
            del diagnostics_dir, state_store, failure_media_ids, progress
            self.assertIsNotNone(enrichment_queue)
            finalized.append(current_pending)
            return downloaded_count + 1, failed_count

        with (
            patch(
                "gphoto_pull.automation._start_download_candidate_async",
                return_value=pending_download,
            ),
            patch(
                "gphoto_pull.automation._finalize_pending_download_async",
                side_effect=fake_finalize,
            ),
        ):
            summary = asyncio.run(
                _download_candidates_async(
                    cast("BrowserContext", _FakeContext()),
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    queued_candidates=[
                        (
                            metadata,
                            MediaStateRecord(metadata=metadata),
                        )
                    ],
                    download_concurrency=2,
                    enrichment_concurrency=2,
                    enrich_metadata=True,
                )
            )

        self.assertEqual(finalized, [pending_download])
        self.assertEqual(summary.downloaded_count, 1)
        self.assertEqual(summary.failed_count, 0)

    def test_download_candidates_disables_enrichment_when_workers_cannot_start(self) -> None:
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        pending_download = SimpleNamespace(
            slot=0,
            metadata=metadata,
            download_trace=DownloadTrace(content_length=1),
            download_event_at=0.0,
        )
        observed_enrichment_queues: list[asyncio.Queue[object] | None] = []

        async def fake_finalize(
            current_pending: SimpleNamespace,
            *,
            diagnostics_dir: Path,
            state_store: PullStateStore,
            downloaded_count: int,
            failed_count: int,
            failure_media_ids: list[str],
            progress: PullProgressDisplay,
            enrichment_queue: asyncio.Queue[object] | None,
        ) -> tuple[int, int]:
            del current_pending, diagnostics_dir, state_store, failure_media_ids, progress
            observed_enrichment_queues.append(enrichment_queue)
            return downloaded_count + 1, failed_count

        context = _FakeLimitedContext(limit=1)
        with (
            patch(
                "gphoto_pull.automation._start_download_candidate_async",
                return_value=pending_download,
            ),
            patch(
                "gphoto_pull.automation._finalize_pending_download_async",
                side_effect=fake_finalize,
            ),
        ):
            summary = asyncio.run(
                _download_candidates_async(
                    cast("BrowserContext", context),
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    queued_candidates=[(metadata, MediaStateRecord(metadata=metadata))],
                    download_concurrency=1,
                    enrichment_concurrency=2,
                    enrich_metadata=True,
                )
            )

        self.assertEqual(summary.downloaded_count, 1)
        self.assertEqual(summary.failed_count, 0)
        self.assertEqual(observed_enrichment_queues, [None])
        self.assertEqual(context.calls, 2)

    def test_download_candidates_reserves_download_pages_before_enrichment_pages(self) -> None:
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        pending_download = SimpleNamespace(
            slot=0,
            metadata=metadata,
            download_trace=DownloadTrace(content_length=1),
            download_event_at=0.0,
        )
        observed_enrichment_queues: list[asyncio.Queue[object] | None] = []

        async def fake_finalize(
            current_pending: SimpleNamespace,
            *,
            diagnostics_dir: Path,
            state_store: PullStateStore,
            downloaded_count: int,
            failed_count: int,
            failure_media_ids: list[str],
            progress: PullProgressDisplay,
            enrichment_queue: asyncio.Queue[object] | None,
        ) -> tuple[int, int]:
            del current_pending, diagnostics_dir, state_store, failure_media_ids, progress
            observed_enrichment_queues.append(enrichment_queue)
            return downloaded_count + 1, failed_count

        context = _FakeLimitedContext(limit=2)
        other_metadata = MediaMetadata(media_id="media-2", filename="file-2")
        with (
            patch(
                "gphoto_pull.automation._start_download_candidate_async",
                return_value=pending_download,
            ),
            patch(
                "gphoto_pull.automation._finalize_pending_download_async",
                side_effect=fake_finalize,
            ),
        ):
            summary = asyncio.run(
                _download_candidates_async(
                    cast("BrowserContext", context),
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    queued_candidates=[
                        (metadata, MediaStateRecord(metadata=metadata)),
                        (other_metadata, MediaStateRecord(metadata=other_metadata)),
                    ],
                    download_concurrency=2,
                    enrichment_concurrency=5,
                    enrich_metadata=True,
                )
            )

        self.assertEqual(summary.downloaded_count, 2)
        self.assertEqual(summary.failed_count, 0)
        self.assertEqual(observed_enrichment_queues, [None, None])
        self.assertEqual(context.calls, 3)

    def test_download_candidates_closes_progress_when_page_startup_fails(self) -> None:
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        context = _FakeLimitedContext(limit=0)

        with (
            patch("gphoto_pull.automation.PullProgressDisplay.close") as close_progress,
            self.assertRaises(RuntimeError),
        ):
            asyncio.run(
                _download_candidates_async(
                    cast("BrowserContext", context),
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    queued_candidates=[(metadata, MediaStateRecord(metadata=metadata))],
                    download_concurrency=1,
                    enrichment_concurrency=1,
                    enrich_metadata=True,
                    progress_interactive=False,
                )
            )

        close_progress.assert_called_once_with()
        self.assertEqual(context.calls, 1)

    def test_finalize_writes_canonical_product_url_for_index_only_download(self) -> None:
        metadata = MediaMetadata(media_id="AF1QipNoUrl", filename="original.mp4")

        with (
            TemporaryDirectory() as tmp_dir,
            PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
            patch(
                "gphoto_pull.automation._enrich_detail_metadata_after_download_async",
                return_value=None,
            ),
        ):
            download_root = Path(tmp_dir) / "downloads"
            final_path = download_root / "uploaded" / "unknown-date" / "original.mp4"
            pending = _PendingDownload(
                slot=0,
                page=cast("Page", SimpleNamespace()),
                record=MediaStateRecord(metadata=metadata),
                metadata=metadata,
                plan=DownloadPlan(
                    media_id=metadata.media_id,
                    original_filename=metadata.filename,
                    final_filename=metadata.filename,
                    final_path=final_path,
                    relative_path=final_path.relative_to(download_root),
                ),
                download=cast("Download", _FakePlaywrightDownload(b"video-bytes")),
                download_trace=DownloadTrace(content_length=11),
                detail_metadata=None,
                queued_at=0.0,
                start_begin_at=0.0,
                download_event_at=0.0,
            )
            progress = PullProgressDisplay(
                total_items=1,
                stream=StringIO(),
                interactive=False,
            )
            enrichment_queue: asyncio.Queue[_EnrichmentJob] = asyncio.Queue()

            downloaded_count, failed_count = asyncio.run(
                _finalize_pending_download_async(
                    pending,
                    diagnostics_dir=Path(tmp_dir) / "diagnostics",
                    state_store=store,
                    downloaded_count=0,
                    failed_count=0,
                    failure_media_ids=[],
                    progress=progress,
                    enrichment_queue=enrichment_queue,
                )
            )
            indexed = store.get_media(metadata.media_id)
            sidecar = msgspec.json.decode(
                final_path.with_name("original.mp4.supplemental-metadata.json").read_bytes(),
                type=TakeoutSidecar,
            )
            enrichment_job = enrichment_queue.get_nowait()

        self.assertEqual(downloaded_count, 1)
        self.assertEqual(failed_count, 0)
        self.assertEqual(enrichment_job.metadata.media_id, metadata.media_id)
        self.assertEqual(sidecar.url, "https://photos.google.com/photo/AF1QipNoUrl")
        self.assertIsNotNone(indexed)
        assert indexed is not None
        self.assertEqual(
            indexed.metadata.product_url, "https://photos.google.com/photo/AF1QipNoUrl"
        )

    def test_finalize_can_skip_post_download_metadata_enrichment(self) -> None:
        metadata = MediaMetadata(media_id="AF1QipNoEnrich", filename="original.mp4")

        with (
            TemporaryDirectory() as tmp_dir,
            PullStateStore(Path(tmp_dir) / "index.sqlite3") as store,
            patch("gphoto_pull.automation._enrich_detail_metadata_after_download_async") as enrich,
        ):
            download_root = Path(tmp_dir) / "downloads"
            final_path = download_root / "uploaded" / "unknown-date" / "original.mp4"
            pending = _PendingDownload(
                slot=0,
                page=cast("Page", SimpleNamespace()),
                record=MediaStateRecord(metadata=metadata),
                metadata=metadata,
                plan=DownloadPlan(
                    media_id=metadata.media_id,
                    original_filename=metadata.filename,
                    final_filename=metadata.filename,
                    final_path=final_path,
                    relative_path=final_path.relative_to(download_root),
                ),
                download=cast("Download", _FakePlaywrightDownload(b"video-bytes")),
                download_trace=DownloadTrace(content_length=11),
                detail_metadata=None,
                queued_at=0.0,
                start_begin_at=0.0,
                download_event_at=0.0,
            )
            progress = PullProgressDisplay(
                total_items=1,
                stream=StringIO(),
                interactive=False,
            )
            progress_stream = cast(StringIO, progress.stream)

            downloaded_count, failed_count = asyncio.run(
                _finalize_pending_download_async(
                    pending,
                    diagnostics_dir=Path(tmp_dir) / "diagnostics",
                    state_store=store,
                    downloaded_count=0,
                    failed_count=0,
                    failure_media_ids=[],
                    progress=progress,
                    enrichment_queue=None,
                )
            )
            sidecar_path = final_path.with_name("original.mp4.supplemental-metadata.json")
            sidecar_exists = sidecar_path.exists()
            progress_output = progress_stream.getvalue()

        self.assertEqual(downloaded_count, 1)
        self.assertEqual(failed_count, 0)
        enrich.assert_not_called()
        self.assertTrue(sidecar_exists)
        self.assertLess(progress_output.index("download:"), progress_output.index("finalize:"))
        self.assertLess(progress_output.index("finalize:"), progress_output.index("done:"))
        self.assertNotIn("enrich: original.mp4", progress_output)

    def test_start_download_candidate_retries_after_transient_download_error(self) -> None:
        page = _FakePage()
        pending_download = SimpleNamespace()
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        record = MediaStateRecord(metadata=metadata)

        with patch(
            "gphoto_pull.automation._start_download_candidate_once_async",
            side_effect=[
                DownloadError("Failed to trigger the Google Photos download action: no menu"),
                pending_download,
            ],
        ) as start_once:
            actual = asyncio.run(
                _start_download_candidate_async(
                    cast("Page", page),
                    slot=0,
                    queued_at=0.0,
                    start_begin_at=0.0,
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    metadata=metadata,
                    record=record,
                )
            )

        self.assertIs(actual, pending_download)
        self.assertEqual(start_once.call_count, 2)
        self.assertEqual(page.goto_calls, [("about:blank", "load")])
        self.assertEqual(page.wait_calls, [500])

    def test_start_download_candidate_does_not_retry_non_retryable_error(self) -> None:
        page = _FakePage()
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        record = MediaStateRecord(metadata=metadata)

        with (
            patch(
                "gphoto_pull.automation._start_download_candidate_once_async",
                side_effect=DownloadError(
                    "media-1 does not have a direct download URL or product URL."
                ),
            ) as start_once,
            self.assertRaisesRegex(
                DownloadError,
                "does not have a direct download URL or product URL",
            ),
        ):
            asyncio.run(
                _start_download_candidate_async(
                    cast("Page", page),
                    slot=0,
                    queued_at=0.0,
                    start_begin_at=0.0,
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    metadata=metadata,
                    record=record,
                )
            )

        self.assertEqual(start_once.call_count, 1)
        self.assertEqual(page.goto_calls, [])
        self.assertEqual(page.wait_calls, [])

    def test_start_download_candidate_raises_keyboard_interrupt_when_cancel_requested(self) -> None:
        page = _FakePage()
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        record = MediaStateRecord(metadata=metadata)

        with (
            patch(
                "gphoto_pull.automation._start_download_candidate_once_async",
                side_effect=DownloadError("transient download failure"),
            ) as start_once,
            patch("gphoto_pull.automation.interrupt_requested", return_value=True),
            self.assertRaises(KeyboardInterrupt),
        ):
            asyncio.run(
                _start_download_candidate_async(
                    cast("Page", page),
                    slot=0,
                    queued_at=0.0,
                    start_begin_at=0.0,
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    metadata=metadata,
                    record=record,
                )
            )

        self.assertEqual(start_once.call_count, 1)

    def test_download_candidates_raises_keyboard_interrupt_instead_of_marking_failure(self) -> None:
        metadata = MediaMetadata(media_id="media-1", filename="file-1")
        record = MediaStateRecord(metadata=metadata)

        with (
            patch(
                "gphoto_pull.automation._start_download_candidate_async",
                side_effect=RuntimeError("interrupted side-effect"),
            ),
            patch("gphoto_pull.automation.interrupt_requested", return_value=True),
            self.assertRaises(KeyboardInterrupt),
        ):
            asyncio.run(
                _download_candidates_async(
                    cast("BrowserContext", _FakeContext()),
                    diagnostics_dir=Path("tmp-diagnostics"),
                    download_dir=Path("tmp-downloads"),
                    state_store=cast(PullStateStore, SimpleNamespace()),
                    photos_ui=cast(GooglePhotosUi, SimpleNamespace()),
                    queued_candidates=[(metadata, record)],
                    download_concurrency=1,
                    enrichment_concurrency=1,
                    enrich_metadata=True,
                )
            )

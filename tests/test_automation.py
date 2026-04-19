# pyright: reportPrivateUsage=false

import asyncio
import json
import unittest
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from gphoto_pull.automation import (
    _AsyncResponseCapture,
    _build_download_trace_async,
    _direct_download_urls_for_metadata,
    _download_candidates_async,
    _finalize_pending_download_async,
    _page_recent_payloads_to_window,
    _PendingDownload,
    _recent_payload_cursor,
    _recent_payload_stats,
    _RecentPageRequest,
    _start_download_candidate_async,
    _store_recent_page_checkpoint,
    _update_recent_payload_stats,
)
from gphoto_pull.download import DownloadError, DownloadPlan
from gphoto_pull.models import DownloadTrace, MediaMetadata, MediaStateRecord
from gphoto_pull.photos_ui import GooglePhotosUi
from gphoto_pull.progress import PullProgressDisplay
from gphoto_pull.state import PullStateStore

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
        request = json.loads(form["f.req"])
        inner = json.loads(request[0][0][1])
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
    return (
        """)]}'\n\n258\n"""
        + json.dumps([["wrb.fr", "opaqueRecentRpc", json.dumps(payload), None, None, None]])
        + "\n"
    )


def _cursor_only_raw(cursor: str) -> str:
    payload = [None, cursor]
    return (
        """)]}'\n\n258\n"""
        + json.dumps([["wrb.fr", "opaqueRecentRpc", json.dumps(payload), None, None, None]])
        + "\n"
    )


class DownloadTraceTests(unittest.TestCase):
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
            covered = store.upload_coverage_satisfies(datetime(2026, 2, 3, tzinfo=UTC))

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
        self.assertTrue(covered)

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
        ) -> tuple[int, int]:
            del diagnostics_dir, state_store, failure_media_ids, progress
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
                )
            )

        self.assertEqual(finalized, [pending_download])
        self.assertEqual(summary.downloaded_count, 1)
        self.assertEqual(summary.failed_count, 0)

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

            downloaded_count, failed_count = asyncio.run(
                _finalize_pending_download_async(
                    pending,
                    diagnostics_dir=Path(tmp_dir) / "diagnostics",
                    state_store=store,
                    downloaded_count=0,
                    failed_count=0,
                    failure_media_ids=[],
                    progress=progress,
                )
            )
            indexed = store.get_media(metadata.media_id)
            sidecar = json.loads(
                final_path.with_name("original.mp4.supplemental-metadata.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(downloaded_count, 1)
        self.assertEqual(failed_count, 0)
        self.assertEqual(sidecar["url"], "https://photos.google.com/photo/AF1QipNoUrl")
        self.assertIsNotNone(indexed)
        assert indexed is not None
        self.assertEqual(indexed.metadata.product_url, "https://photos.google.com/photo/AF1QipNoUrl")

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
                )
            )

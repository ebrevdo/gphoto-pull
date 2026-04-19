# pyright: reportPrivateUsage=false

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from gphoto_pull.automation import (
    _build_download_trace_async,
    _direct_download_urls_for_metadata,
    _download_candidates_async,
    _start_download_candidate_async,
)
from gphoto_pull.download import DownloadError
from gphoto_pull.models import DownloadTrace, MediaMetadata, MediaStateRecord
from gphoto_pull.photos_ui import GooglePhotosUi
from gphoto_pull.progress import PullProgressDisplay
from gphoto_pull.state import PullStateStore

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, Download, Page, Response


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


class DownloadTraceTests(unittest.TestCase):
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

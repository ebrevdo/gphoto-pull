"""Core typed records shared by enumeration, state, and downloads.

Description:
    Defines validated dataclasses for media metadata, download traces, sync
    state, and checkpoint records.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


def _require_aware_datetime(value: datetime, *, field_name: str) -> datetime:
    """Description:
    Validate that a datetime includes usable timezone information.

    Args:
        value: Datetime to validate.
        field_name: Field name for error messages.

    Returns:
        The validated datetime.
    """

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone information.")
    return value


def _require_non_empty(value: str, *, field_name: str) -> str:
    """Description:
    Validate that text contains non-whitespace content.

    Args:
        value: Text to validate.
        field_name: Field name for error messages.

    Returns:
        The validated text.
    """

    if value.strip() == "":
        raise ValueError(f"{field_name} must not be empty.")
    return value


def _require_positive_int(value: int, *, field_name: str) -> int:
    """Description:
    Validate that an integer is strictly positive.

    Args:
        value: Integer to validate.
        field_name: Field name for error messages.

    Returns:
        The validated integer.
    """

    if value <= 0:
        raise ValueError(f"{field_name} must be greater than zero.")
    return value


@dataclass(slots=True, frozen=True)
class DownloadTrace:
    """Network details observed while downloading one media item.

    Description:
        Captures enough response metadata to debug whether Google returned an
        original file and to compare direct-download and toolbar-download paths.

    Attributes:
        download_url: URL reported by Playwright's download handle.
        final_url: Best matched response URL, after redirects when observed.
        content_type: Response `content-type` header.
        content_length: Response `content-length` header as bytes.
        content_disposition: Response `content-disposition` header.
    """

    download_url: str | None = None
    final_url: str | None = None
    content_type: str | None = None
    content_length: int | None = None
    content_disposition: str | None = None

    def __post_init__(self) -> None:
        if self.download_url is not None:
            _require_non_empty(self.download_url, field_name="download_url")
        if self.final_url is not None:
            _require_non_empty(self.final_url, field_name="final_url")
        if self.content_type is not None:
            _require_non_empty(self.content_type, field_name="content_type")
        if self.content_disposition is not None:
            _require_non_empty(
                self.content_disposition,
                field_name="content_disposition",
            )
        if self.content_length is not None and self.content_length < 0:
            raise ValueError("content_length must be zero or greater.")


@dataclass(slots=True, frozen=True)
class MediaMetadata:
    """Metadata known for one Google Photos media item.

    Description:
        Stores the stable media key plus optional fields discovered from visible
        UI, hidden RPC payloads, and download responses.

    Attributes:
        media_id: Google Photos media key.
        filename: Preferred original filename.
        capture_time: Time the photo/video was captured, if known.
        uploaded_time: Time the item was added/uploaded/shared, if known.
        mime_type: MIME type, when known.
        media_type: Coarse type such as `photo` or `video`.
        product_url: Google Photos product/detail URL.
        preview_url: Preview URL used to derive direct downloads when safe.
        width: Pixel width, if known.
        height: Pixel height, if known.
        bytes_size: Expected download size, if known.
    """

    media_id: str
    filename: str
    capture_time: datetime | None = None
    uploaded_time: datetime | None = None
    mime_type: str | None = None
    media_type: str | None = None
    product_url: str | None = None
    preview_url: str | None = None
    width: int | None = None
    height: int | None = None
    bytes_size: int | None = None

    def __post_init__(self) -> None:
        _require_non_empty(self.media_id, field_name="media_id")
        _require_non_empty(self.filename, field_name="filename")
        if self.product_url is not None:
            _require_non_empty(self.product_url, field_name="product_url")
        if self.preview_url is not None:
            _require_non_empty(self.preview_url, field_name="preview_url")

        if self.capture_time is not None:
            _require_aware_datetime(self.capture_time, field_name="capture_time")
        if self.uploaded_time is not None:
            _require_aware_datetime(self.uploaded_time, field_name="uploaded_time")
        if self.width is not None:
            _require_positive_int(self.width, field_name="width")
        if self.height is not None:
            _require_positive_int(self.height, field_name="height")
        if self.bytes_size is not None and self.bytes_size < 0:
            raise ValueError("bytes_size must be zero or greater.")


@dataclass(slots=True, frozen=True)
class MediaStateRecord:
    """Persisted sync record for one media item.

    Description:
        Combines media metadata with first/last-seen timestamps for the local
        Google Photos index.

    Attributes:
        metadata: Google Photos metadata for the item.
        first_seen_at: First time this media key was discovered.
        last_seen_at: Most recent time this media key was discovered.
    """

    metadata: MediaMetadata
    first_seen_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_seen_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        first_seen_at = _require_aware_datetime(self.first_seen_at, field_name="first_seen_at")
        last_seen_at = _require_aware_datetime(self.last_seen_at, field_name="last_seen_at")

        if last_seen_at < first_seen_at:
            raise ValueError("last_seen_at must be on or after first_seen_at.")


@dataclass(slots=True, frozen=True)
class SyncCheckpoint:
    """Named checkpoint stored in the sync database.

    Description:
        Supports future resumable cursors or pagination tokens independent of
        individual media records.

    Attributes:
        name: Checkpoint name.
        value: Optional checkpoint payload.
        updated_at: Time this checkpoint was written.
    """

    name: str
    value: str | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require_non_empty(self.name, field_name="name")
        _require_aware_datetime(self.updated_at, field_name="updated_at")

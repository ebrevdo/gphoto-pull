"""Offline and live-diagnostic media enumeration for Google Photos.

Description:
    Converts captured Google Photos HTML/RPC diagnostics into persisted media
    candidates with cutoff-match metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from gphoto_pull.models import MediaMetadata, MediaStateRecord
from gphoto_pull.photos_ui import (
    PHOTOS_APP_ORIGIN,
    PhotosSurface,
    extract_photo_locations_from_html,
    infer_media_kind,
)
from gphoto_pull.rpc_payloads import (
    RecentPayload,
    RpcPayloadParseError,
    UpdatesPayload,
    find_updates_payload_artifact,
    merge_recent_payloads,
    merge_updates_payloads,
    parse_recent_payload,
    parse_recently_added_bootstrap,
    parse_updates_payload,
)
from gphoto_pull.state import PullStateStore

LOCAL_TIMEZONE = ZoneInfo("America/Los_Angeles")


@dataclass(slots=True, frozen=True)
class EnumeratedCandidate:
    """Media candidate discovered from Google Photos diagnostics.

    Description:
        Pairs media metadata with source and cutoff evaluation details.

    Attributes:
        metadata: Candidate metadata to persist.
        source: Enumeration source label such as `recently-added`.
        uploaded_time_exact: Whether the cutoff timestamp came from exact payload data.
        cutoff_match: `True` when it meets the cutoff, `False` when it does not,
            or `None` when the timestamp is unknown.
    """

    metadata: MediaMetadata
    source: str
    uploaded_time_exact: bool
    cutoff_match: bool | None


@dataclass(slots=True, frozen=True)
class EnumerationSummary:
    """Result of one saved-diagnostics enumeration pass.

    Description:
        Contains the deduped candidate set, persisted state records, and aggregate
        counts for CLI reporting.

    Attributes:
        candidates: Deduped media candidates.
        persisted_records: State records written/read for those candidates.
        source_counts: Per-source candidate counts.
        exact_uploaded_time_count: Candidates with exact upload/share time.
        unknown_uploaded_time_count: Candidates with unknown upload/share time.
        cutoff_matched_count: Candidates that exactly matched the cutoff.
    """

    candidates: tuple[EnumeratedCandidate, ...]
    persisted_records: tuple[MediaStateRecord, ...]
    source_counts: tuple[tuple[str, int], ...]
    exact_uploaded_time_count: int
    unknown_uploaded_time_count: int
    cutoff_matched_count: int


def enumerate_saved_candidates(
    *,
    diagnostics_dir: Path,
    after: datetime,
    before: datetime | None = None,
    state_store: PullStateStore,
) -> EnumerationSummary:
    """Description:
    Parse saved diagnostics and persist discovered media candidates.

    Args:
        diagnostics_dir: Directory containing recent/updates diagnostics.
        after: Inclusive lower-bound timestamp.
        before: Exclusive upper-bound timestamp.
        state_store: Open sync-state store used for upserts.

    Returns:
        An `EnumerationSummary` describing candidates and persisted records.

    Side Effects:
        Reads diagnostic files and writes/updates media rows in SQLite.
    """

    candidates: list[EnumeratedCandidate] = []

    recent_path = _preferred_recent_html_path(diagnostics_dir)
    if recent_path.exists():
        recent_html = recent_path.read_text(encoding="utf-8")
        recent_payload = _load_recent_payload(diagnostics_dir)
        candidates.extend(
            _enumerate_recent_candidates(
                recent_html,
                recent_payload,
                after=after,
                before=before,
            )
        )

    updates_path = _preferred_updates_html_path(diagnostics_dir)
    updates_payload = _load_updates_payload(diagnostics_dir)
    if updates_path.exists() and updates_payload is not None:
        updates_html = updates_path.read_text(encoding="utf-8")
        candidates.extend(
            _enumerate_updates_candidates(
                updates_html,
                updates_payload,
                after=after,
                before=before,
            )
        )

    deduped = _dedupe_candidates(candidates)
    persisted_records = tuple(state_store.upsert_media(candidate.metadata) for candidate in deduped)
    source_counts = _count_sources(deduped)
    exact_uploaded_time_count = sum(candidate.uploaded_time_exact for candidate in deduped)
    unknown_uploaded_time_count = sum(candidate.cutoff_match is None for candidate in deduped)
    cutoff_matched_count = sum(candidate.cutoff_match is True for candidate in deduped)

    return EnumerationSummary(
        candidates=tuple(deduped),
        persisted_records=persisted_records,
        source_counts=source_counts,
        exact_uploaded_time_count=exact_uploaded_time_count,
        unknown_uploaded_time_count=unknown_uploaded_time_count,
        cutoff_matched_count=cutoff_matched_count,
    )


def enumerate_index_candidates(
    records: list[MediaStateRecord],
    *,
    after: datetime,
    before: datetime | None,
) -> EnumerationSummary:
    """Description:
    Build an enumeration summary from already-indexed media records.

    Args:
        records: Media index records to summarize.
        after: Inclusive lower-bound timestamp.
        before: Optional exclusive upper-bound timestamp.

    Returns:
        Enumeration summary suitable for queue construction.
    """

    candidates = tuple(
        EnumeratedCandidate(
            metadata=record.metadata,
            source="media-index",
            uploaded_time_exact=record.metadata.uploaded_time is not None,
            cutoff_match=_timestamp_in_window(
                record.metadata.uploaded_time,
                after=after,
                before=before,
            ),
        )
        for record in records
    )
    return EnumerationSummary(
        candidates=candidates,
        persisted_records=tuple(records),
        source_counts=_count_sources(list(candidates)),
        exact_uploaded_time_count=sum(candidate.uploaded_time_exact for candidate in candidates),
        unknown_uploaded_time_count=sum(candidate.cutoff_match is None for candidate in candidates),
        cutoff_matched_count=sum(candidate.cutoff_match is True for candidate in candidates),
    )


def _enumerate_recent_candidates(
    html: str,
    payload: RecentPayload | None,
    *,
    after: datetime,
    before: datetime | None,
) -> list[EnumeratedCandidate]:
    """Description:
    Enumerate candidates from Recently Added HTML and payload diagnostics.

    Args:
        html: Recently Added HTML snapshot.
        payload: Merged recent payload when available.
        after: Inclusive lower-bound timestamp.
        before: Exclusive upper-bound timestamp.

    Returns:
        Candidate media items from visible links and payload-only items.
    """

    candidates: list[EnumeratedCandidate] = []
    recent_items = {item.media_id: item for item in payload.items} if payload is not None else {}
    seen_media_ids: set[str] = set()
    search_token = _recent_search_token_from_html(html)

    for location in extract_photo_locations_from_html(html):
        if location.surface is not PhotosSurface.SEARCH_MEDIA_DETAIL:
            continue
        label = _label_for_route(html, location.relative_path)
        capture_time = _capture_time_from_label(label)
        media_kind = infer_media_kind(label)
        recent_item = recent_items.get(location.media_id or "")

        if recent_item is not None:
            payload_capture_time = _datetime_from_epoch_millis(recent_item.capture_timestamp_ms)
            capture_time = payload_capture_time or capture_time
            uploaded_time = _datetime_from_epoch_millis(recent_item.upload_timestamp_ms)
        else:
            uploaded_time = None

        metadata = MediaMetadata(
            media_id=location.media_id or location.relative_path,
            filename=f"unresolved-{location.media_id or 'media'}",
            capture_time=capture_time,
            uploaded_time=uploaded_time,
            media_type=None if media_kind == "unknown" else media_kind,
            mime_type=_mime_type_for_media_kind(media_kind),
            product_url=location.absolute_url,
            preview_url=recent_item.preview_url if recent_item is not None else None,
            width=recent_item.width if recent_item is not None else None,
            height=recent_item.height if recent_item is not None else None,
        )
        cutoff_match = _timestamp_in_window(uploaded_time, after=after, before=before)
        candidates.append(
            EnumeratedCandidate(
                metadata=metadata,
                source="recently-added",
                uploaded_time_exact=uploaded_time is not None,
                cutoff_match=cutoff_match,
            )
        )
        seen_media_ids.add(metadata.media_id)

    if payload is not None and search_token is not None:
        for recent_item in payload.items:
            if recent_item.media_id in seen_media_ids:
                continue

            capture_time = _datetime_from_epoch_millis(recent_item.capture_timestamp_ms)
            uploaded_time = _datetime_from_epoch_millis(recent_item.upload_timestamp_ms)
            metadata = MediaMetadata(
                media_id=recent_item.media_id,
                filename=f"unresolved-{recent_item.media_id}",
                capture_time=capture_time,
                uploaded_time=uploaded_time,
                product_url=(
                    f"{PHOTOS_APP_ORIGIN}/search/{search_token}/photo/{recent_item.media_id}"
                ),
                preview_url=recent_item.preview_url,
                width=recent_item.width,
                height=recent_item.height,
            )
            cutoff_match = _timestamp_in_window(uploaded_time, after=after, before=before)
            candidates.append(
                EnumeratedCandidate(
                    metadata=metadata,
                    source="recently-added",
                    uploaded_time_exact=uploaded_time is not None,
                    cutoff_match=cutoff_match,
                )
            )

    return candidates


def _enumerate_updates_candidates(
    html: str,
    payload: UpdatesPayload,
    *,
    after: datetime,
    before: datetime | None,
) -> list[EnumeratedCandidate]:
    """Description:
    Enumerate shared candidates from Updates HTML and activity payloads.

    Args:
        html: Updates HTML snapshot.
        payload: Merged Updates activity payload.
        after: Inclusive lower-bound timestamp.
        before: Exclusive upper-bound timestamp.

    Returns:
        Candidate shared media items.
    """

    candidates: list[EnumeratedCandidate] = []
    event_time_by_container: dict[str, datetime] = {}

    for activity in payload.activities:
        if activity.event_timestamp_ms is None:
            continue
        event_time_by_container[activity.container_id] = datetime.fromtimestamp(
            activity.event_timestamp_ms / 1000,
            tz=UTC,
        )

    for location in extract_photo_locations_from_html(html):
        if location.surface is not PhotosSurface.SHARED_ALBUM_MEDIA_DETAIL:
            continue
        uploaded_time = event_time_by_container.get(location.album_id or "")
        metadata = MediaMetadata(
            media_id=location.media_id or location.relative_path,
            filename=f"unresolved-{location.media_id or 'media'}",
            uploaded_time=uploaded_time,
            product_url=location.absolute_url,
        )
        cutoff_match = _timestamp_in_window(uploaded_time, after=after, before=before)
        candidates.append(
            EnumeratedCandidate(
                metadata=metadata,
                source="updates-shared",
                uploaded_time_exact=uploaded_time is not None,
                cutoff_match=cutoff_match,
            )
        )

    return candidates


def _count_sources(candidates: list[EnumeratedCandidate]) -> tuple[tuple[str, int], ...]:
    """Description:
    Count candidates by enumeration source.

    Args:
        candidates: Candidate list to aggregate.

    Returns:
        Sorted source/count pairs.
    """

    counts: dict[str, int] = {}
    for candidate in candidates:
        counts[candidate.source] = counts.get(candidate.source, 0) + 1
    return tuple(sorted(counts.items()))


def _timestamp_in_window(
    value: datetime | None,
    *,
    after: datetime,
    before: datetime | None,
) -> bool | None:
    """Description:
    Check whether an optional timestamp is inside the configured window.

    Args:
        value: Timestamp to check.
        after: Inclusive lower bound.
        before: Exclusive upper bound.

    Returns:
        `True` for in-window, `False` for outside, or `None` when unknown.
    """

    if value is None:
        return None
    if value < after:
        return False
    return not (before is not None and value >= before)


def _dedupe_candidates(candidates: list[EnumeratedCandidate]) -> list[EnumeratedCandidate]:
    """Description:
    Deduplicate candidates by media id, preferring richer metadata.

    Args:
        candidates: Candidate list to dedupe.

    Returns:
        Deduped candidates in first-seen order.
    """

    deduped: dict[str, EnumeratedCandidate] = {}

    for candidate in candidates:
        media_id = candidate.metadata.media_id
        existing = deduped.get(media_id)
        if existing is None:
            deduped[media_id] = candidate
            continue

        prefer_new = (
            (
                existing.metadata.uploaded_time is None
                and candidate.metadata.uploaded_time is not None
            )
            or (existing.metadata.media_type is None and candidate.metadata.media_type is not None)
            or (
                existing.metadata.capture_time is None
                and candidate.metadata.capture_time is not None
            )
        )

        if prefer_new:
            deduped[media_id] = candidate

    return list(deduped.values())


def _label_for_route(html: str, relative_path: str) -> str:
    """Description:
    Find the aria label associated with a media route in saved HTML.

    Args:
        html: Saved Google Photos HTML.
        relative_path: Route path to find.

    Returns:
        Aria label text, or an empty string.
    """

    marker = f'href=".{relative_path}"'
    index = html.find(marker)
    if index == -1:
        marker = f'href="{relative_path}"'
        index = html.find(marker)
    if index == -1:
        return ""

    aria_marker = 'aria-label="'
    aria_index = html.find(aria_marker, index)
    if aria_index == -1:
        return ""
    value_start = aria_index + len(aria_marker)
    value_end = html.find('"', value_start)
    if value_end == -1:
        return ""
    return html[value_start:value_end]


def _capture_time_from_label(label: str) -> datetime | None:
    """Description:
    Parse a capture timestamp from a Google Photos tile label.

    Args:
        label: Tile aria label.

    Returns:
        Localized capture datetime, or `None`.
    """

    if label.strip() == "":
        return None

    timestamp_text = label.rsplit(" - ", maxsplit=1)[-1]
    normalized = timestamp_text.replace("\u202f", " ").replace("\u00a0", " ").strip()
    try:
        parsed = datetime.strptime(normalized, "%b %d, %Y, %I:%M:%S %p")
    except ValueError:
        return None
    return parsed.replace(tzinfo=LOCAL_TIMEZONE)


def _mime_type_for_media_kind(media_kind: str) -> str | None:
    """Description:
    Map coarse media kind to a MIME wildcard.

    Args:
        media_kind: `photo`, `video`, or `unknown`.

    Returns:
        MIME wildcard, or `None`.
    """

    if media_kind == "photo":
        return "image/*"
    if media_kind == "video":
        return "video/*"
    return None


def _preferred_recent_html_path(diagnostics_dir: Path) -> Path:
    """Description:
    Choose the preferred Recently Added HTML artifact path.

    Args:
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Live probe path when present, otherwise older probe path.
    """

    live_path = diagnostics_dir / "live_recent_probe" / "recent.html"
    if live_path.exists():
        return live_path
    return diagnostics_dir / "recent_probe" / "recent.html"


def _preferred_updates_html_path(diagnostics_dir: Path) -> Path:
    """Description:
    Choose the preferred Updates HTML artifact path.

    Args:
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Live probe path when present, otherwise older snapshot path.
    """

    live_path = diagnostics_dir / "live_updates_probe" / "updates.html"
    if live_path.exists():
        return live_path
    return diagnostics_dir / "updates-page.html"


def _recent_search_token_from_html(html: str) -> str | None:
    """Description:
    Extract the Recently Added search token from bootstrap data or links.

    Args:
        html: Recently Added HTML snapshot.

    Returns:
        Search token, or `None`.
    """

    try:
        bootstrap = parse_recently_added_bootstrap(html)
    except RpcPayloadParseError:
        bootstrap = None

    if bootstrap is not None and bootstrap.canonical_search_token is not None:
        return bootstrap.canonical_search_token

    for location in extract_photo_locations_from_html(html):
        if location.surface in {PhotosSurface.SEARCH_RESULTS, PhotosSurface.SEARCH_MEDIA_DETAIL}:
            return location.search_token

    return None


def _load_recent_payload(diagnostics_dir: Path) -> RecentPayload | None:
    """Description:
    Load and merge live Recently Added response artifacts.

    Args:
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Merged Recent payload, or `None`.

    Side Effects:
        Reads response artifacts from disk.
    """

    payloads: list[RecentPayload] = []
    for path in sorted((diagnostics_dir / "live_recent_probe").glob("resp_*.txt")):
        try:
            payloads.append(parse_recent_payload(path.read_text(encoding="utf-8")))
        except RpcPayloadParseError:
            continue

    if payloads:
        return merge_recent_payloads(payloads)

    return None


def _load_updates_payload(diagnostics_dir: Path) -> UpdatesPayload | None:
    """Description:
    Load and merge saved Updates artifacts.

    Args:
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Merged Updates payload, or `None`.

    Side Effects:
        Reads response artifacts from disk.
    """

    payloads: list[UpdatesPayload] = []
    for path in sorted((diagnostics_dir / "live_updates_probe").glob("resp_*.txt")):
        try:
            payloads.append(parse_updates_payload(path.read_text(encoding="utf-8")))
        except RpcPayloadParseError:
            continue

    if payloads:
        return merge_updates_payloads(payloads)

    fallback_path = find_updates_payload_artifact(diagnostics_dir)
    if fallback_path is not None:
        return parse_updates_payload(fallback_path.read_text(encoding="utf-8"))

    return None


def _datetime_from_epoch_millis(value: int | None) -> datetime | None:
    """Description:
    Convert epoch milliseconds into an aware UTC datetime.

    Args:
        value: Epoch milliseconds.

    Returns:
        UTC datetime, or `None`.
    """

    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=UTC)

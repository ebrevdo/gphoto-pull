"""Shape-based parsers for Google Photos batchexecute payloads.

Description:
    Parses Google Photos bootstrap HTML and batchexecute response shapes without
    treating obfuscated RPC ids as stable contracts.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

INIT_REQUEST_PATTERN = re.compile(r"'(?P<ds_key>ds:\d+)'\s*:\s*\{id:'(?P<rpcid>[^']+)'")
RECENT_CANONICAL_RE = re.compile(r'https://photos\.google\.com/search/([^"<]+)')
RECENT_LINK_RE = re.compile(r'href="(\./search/[^"]+)"[^>]*aria-label="Recently added"')
MEDIA_ID_RE = re.compile(r"AF1Qip[A-Za-z0-9_-]+")
type JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


class RpcPayloadParseError(ValueError):
    """Google Photos payload parse failure.

    Description:
        Raised when a saved or live Google Photos payload cannot be decoded into
        a recognized shape.
    """


@dataclass(slots=True, frozen=True)
class InitDataRequest:
    """Bootstrap RPC declaration.

    Description:
        Captures one RPC entry advertised in Google Photos bootstrap HTML.

    Attributes:
        ds_key: Data-service key such as `ds:5`.
        rpcid: Opaque Google RPC id.
    """

    ds_key: str
    rpcid: str


@dataclass(slots=True, frozen=True)
class RecentlyAddedBootstrap:
    """Recently Added bootstrap metadata.

    Description:
        Captures route and RPC hints from saved Recently Added HTML.

    Attributes:
        canonical_search_token: Opaque canonical search token when present.
        recent_link_href: Sidebar/link href for Recently Added when present.
        bootstrap_rpc_ids: RPC ids advertised in bootstrap data.
    """

    canonical_search_token: str | None
    recent_link_href: str | None
    bootstrap_rpc_ids: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class RecentMediaItem:
    """Recently Added payload media item.

    Description:
        Represents one media item decoded from a Recently Added batchexecute
        payload.

    Attributes:
        media_id: Google Photos media key.
        preview_url: Preview URL carried in the payload.
        width: Pixel width when present.
        height: Pixel height when present.
        capture_timestamp_ms: Capture timestamp in epoch milliseconds.
        upload_timestamp_ms: Upload/add timestamp in epoch milliseconds.
        timezone_offset_ms: Offset value carried by the payload.
        opaque_token: Extra opaque token associated with the item.
    """

    media_id: str
    preview_url: str | None
    width: int | None
    height: int | None
    capture_timestamp_ms: int | None
    upload_timestamp_ms: int | None
    timezone_offset_ms: int | None
    opaque_token: str | None = None


@dataclass(slots=True, frozen=True)
class RecentPayload:
    """Decoded Recently Added payload.

    Description:
        Contains media items recovered from one or more Recently Added
        batchexecute frames.

    Attributes:
        rpc_ids: RPC ids that yielded decoded recent items.
        items: Deduped media items.
    """

    rpc_ids: tuple[str, ...]
    items: tuple[RecentMediaItem, ...]


@dataclass(slots=True, frozen=True)
class BatchedRpcFrame:
    """Google batchexecute frame.

    Description:
        Wraps one decoded frame from a Google `batchexecute` response.

    Attributes:
        rpc_id: Opaque RPC id when present.
        payload_text: JSON payload string when present.
        frame: Raw decoded frame tuple.
    """

    rpc_id: str | None
    payload_text: str | None
    frame: tuple[JsonValue, ...]

    def decoded_payload(self) -> JsonValue | None:
        """Description:
        Decode the frame payload JSON when possible.

        Returns:
            Decoded JSON payload, or `None` when missing/invalid.
        """

        if self.payload_text is None:
            return None
        try:
            return cast(JsonValue, json.loads(self.payload_text))
        except json.JSONDecodeError:
            return None


@dataclass(slots=True, frozen=True)
class UpdatesActivity:
    """Updates feed activity.

    Description:
        Represents one activity decoded from the Google Photos Updates feed
        payload.

    Attributes:
        activity_kind: Activity token prefix such as `ai` or `h`.
        container_id: Shared album or direct-thread container id.
        actor_id: Actor identifier when present.
        event_timestamp_ms: Exact activity/share timestamp in epoch milliseconds.
        item_timestamp_ms: Media/item timestamp in epoch milliseconds.
    """

    activity_kind: str
    container_id: str
    actor_id: str | None = None
    event_timestamp_ms: int | None = None
    item_timestamp_ms: int | None = None


@dataclass(slots=True, frozen=True)
class UpdatesPayload:
    """Decoded Updates feed payload.

    Description:
        Contains activity items recovered from one or more Updates batchexecute
        frames.

    Attributes:
        rpc_ids: RPC ids that yielded decoded activities.
        activities: Deduped update activities.
    """

    rpc_ids: tuple[str, ...]
    activities: tuple[UpdatesActivity, ...]


def find_updates_payload_artifact(diagnostics_dir: Path) -> Path | None:
    """Description:
    Find the preferred saved Updates batchexecute artifact.

    Args:
        diagnostics_dir: Diagnostics directory to inspect.

    Returns:
        First existing Updates payload path, or `None`.
    """

    for path in (
        diagnostics_dir / "updates-batchexecute.txt",
        diagnostics_dir / "updates-frGlJf.txt",
    ):
        if path.exists():
            return path
    return None


def extract_init_data_requests(html: str) -> tuple[InitDataRequest, ...]:
    """Description:
    Extract bootstrap RPC declarations from saved Google Photos HTML.

    Args:
        html: HTML snapshot.

    Returns:
        RPC declarations in document order.
    """

    requests = tuple(
        InitDataRequest(ds_key=match.group("ds_key"), rpcid=match.group("rpcid"))
        for match in INIT_REQUEST_PATTERN.finditer(html)
    )
    if not requests:
        raise RpcPayloadParseError("Could not find AF_dataServiceRequests entries in HTML fixture.")
    return requests


def parse_recently_added_bootstrap(html: str) -> RecentlyAddedBootstrap:
    """Description:
    Parse Recently Added route hints from saved HTML.

    Args:
        html: HTML snapshot.

    Returns:
        Recently Added bootstrap metadata.
    """

    requests = extract_init_data_requests(html)
    canonical_match = RECENT_CANONICAL_RE.search(html)
    recent_link_match = RECENT_LINK_RE.search(html)
    return RecentlyAddedBootstrap(
        canonical_search_token=canonical_match.group(1) if canonical_match else None,
        recent_link_href=recent_link_match.group(1) if recent_link_match else None,
        bootstrap_rpc_ids=tuple(dict.fromkeys(request.rpcid for request in requests)),
    )


def parse_batchexecute_frames(raw_text: str) -> tuple[BatchedRpcFrame, ...]:
    """Description:
    Parse newline-delimited Google `batchexecute` frames.

    Args:
        raw_text: Raw response text.

    Returns:
        Decoded frame wrappers.
    """

    frames: list[BatchedRpcFrame] = []

    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("["):
            continue

        payload = cast(JsonValue, json.loads(stripped))
        if not isinstance(payload, list):
            continue

        for entry in payload:
            if not isinstance(entry, list) or not entry:
                continue
            frames.append(
                BatchedRpcFrame(
                    rpc_id=entry[1] if len(entry) > 1 and isinstance(entry[1], str) else None,
                    payload_text=entry[2] if len(entry) > 2 and isinstance(entry[2], str) else None,
                    frame=tuple(entry),
                )
            )

    if not frames:
        raise RpcPayloadParseError("Could not identify any batchexecute frames.")

    return tuple(frames)


def parse_recent_payload(raw_text: str) -> RecentPayload:
    """Description:
    Decode recent media items from a batchexecute response.

    Args:
        raw_text: Raw response text.

    Returns:
        Deduped recent payload.
    """

    frames = parse_batchexecute_frames(raw_text)
    rpc_ids: list[str] = []
    items: list[RecentMediaItem] = []

    for frame in frames:
        payload = frame.decoded_payload()
        parsed_items = _parse_recent_items_from_payload(payload)
        if not parsed_items:
            continue

        if frame.rpc_id is not None:
            rpc_ids.append(frame.rpc_id)
        items.extend(parsed_items)

    deduped_items = _dedupe_recent_items(items)
    if not deduped_items:
        raise RpcPayloadParseError("Could not decode any recent media items from payload.")

    return RecentPayload(
        rpc_ids=tuple(dict.fromkeys(rpc_ids)),
        items=tuple(deduped_items),
    )


def merge_recent_payloads(payloads: list[RecentPayload]) -> RecentPayload:
    """Description:
    Merge multiple Recent payloads from one probe directory.

    Args:
        payloads: Parsed recent payloads.

    Returns:
        Deduped merged payload.
    """

    rpc_ids: list[str] = []
    items: list[RecentMediaItem] = []

    for payload in payloads:
        rpc_ids.extend(payload.rpc_ids)
        items.extend(payload.items)

    deduped_items = _dedupe_recent_items(items)
    if not deduped_items:
        raise RpcPayloadParseError("Could not merge any recent media items from payloads.")

    return RecentPayload(
        rpc_ids=tuple(dict.fromkeys(rpc_ids)),
        items=tuple(deduped_items),
    )


def parse_updates_payload(raw_text: str) -> UpdatesPayload:
    """Description:
    Decode shared/direct activity from an Updates batchexecute response.

    Args:
        raw_text: Raw response text.

    Returns:
        Deduped Updates payload.
    """

    frames = parse_batchexecute_frames(raw_text)
    rpc_ids = tuple(dict.fromkeys(frame.rpc_id for frame in frames if frame.rpc_id is not None))

    activities: list[UpdatesActivity] = []
    for frame in frames:
        payload = frame.decoded_payload()
        if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
            continue

        for raw_activity in payload[1]:
            parsed = _parse_updates_activity(raw_activity)
            if parsed is not None:
                activities.append(parsed)

    deduped = _dedupe_updates_activities(activities)
    if not deduped:
        raise RpcPayloadParseError("Could not decode any updates activities from payload.")

    return UpdatesPayload(rpc_ids=rpc_ids, activities=tuple(deduped))


def merge_updates_payloads(payloads: list[UpdatesPayload]) -> UpdatesPayload:
    """Description:
    Merge multiple Updates payloads from one probe directory.

    Args:
        payloads: Parsed Updates payloads.

    Returns:
        Deduped merged payload.
    """

    rpc_ids: list[str] = []
    activities: list[UpdatesActivity] = []

    for payload in payloads:
        rpc_ids.extend(payload.rpc_ids)
        activities.extend(payload.activities)

    deduped = _dedupe_updates_activities(activities)
    if not deduped:
        raise RpcPayloadParseError("Could not merge any updates activities from payloads.")

    return UpdatesPayload(
        rpc_ids=tuple(dict.fromkeys(rpc_ids)),
        activities=tuple(deduped),
    )


def _parse_recent_items_from_payload(payload: JsonValue | None) -> list[RecentMediaItem]:
    """Description:
    Decode recent media items from the payload section of a batchexecute frame.

    Args:
        payload: Decoded frame payload.

    Returns:
        Parsed media items.
    """

    if not isinstance(payload, list) or not payload or not isinstance(payload[0], list):
        return []

    items: list[RecentMediaItem] = []
    for raw_item in payload[0]:
        parsed = _parse_recent_item(raw_item)
        if parsed is not None:
            items.append(parsed)

    return items


def _parse_recent_item(raw_item: JsonValue) -> RecentMediaItem | None:
    """Description:
    Decode one shape-matched Recently Added media item.

    Args:
        raw_item: Decoded JSON value for one item.

    Returns:
        Parsed item, or `None` when the shape is not recognized.
    """

    if not isinstance(raw_item, list) or len(raw_item) < 6:
        return None

    media_id = raw_item[0]
    if not isinstance(media_id, str) or MEDIA_ID_RE.fullmatch(media_id) is None:
        return None

    preview_info = raw_item[1] if isinstance(raw_item[1], list) else None
    preview_url = preview_info[0] if preview_info and isinstance(preview_info[0], str) else None
    width = preview_info[1] if preview_info and isinstance(preview_info[1], int) else None
    height = preview_info[2] if preview_info and isinstance(preview_info[2], int) else None

    capture_timestamp_ms = (
        raw_item[2] if isinstance(raw_item[2], int) and 10**12 <= raw_item[2] < 10**13 else None
    )
    timezone_offset_ms = raw_item[4] if isinstance(raw_item[4], int) else None
    upload_timestamp_ms = (
        raw_item[5] if isinstance(raw_item[5], int) and 10**12 <= raw_item[5] < 10**13 else None
    )
    opaque_token = raw_item[3] if isinstance(raw_item[3], str) else None

    if capture_timestamp_ms is None and upload_timestamp_ms is None:
        return None

    return RecentMediaItem(
        media_id=media_id,
        preview_url=preview_url,
        width=width,
        height=height,
        capture_timestamp_ms=capture_timestamp_ms,
        upload_timestamp_ms=upload_timestamp_ms,
        timezone_offset_ms=timezone_offset_ms,
        opaque_token=opaque_token,
    )


def _parse_updates_activity(raw_activity: JsonValue) -> UpdatesActivity | None:
    """Description:
    Decode one shape-matched Updates activity.

    Args:
        raw_activity: Decoded JSON value for one activity.

    Returns:
        Parsed activity, or `None` when the shape is not recognized.
    """

    if not isinstance(raw_activity, list) or not raw_activity:
        return None

    token = raw_activity[0]
    if not isinstance(token, str):
        return None

    kind = token.split(":", 1)[0]
    container_id = _container_id_from_token(token)
    if container_id is None:
        return None

    actor_id = _actor_id_from_token(token)
    event_timestamp_ms = _event_timestamp_from_token(token)
    item_timestamp_ms = _first_epoch_millis(raw_activity)

    return UpdatesActivity(
        activity_kind=kind,
        container_id=container_id,
        actor_id=actor_id,
        event_timestamp_ms=event_timestamp_ms,
        item_timestamp_ms=item_timestamp_ms,
    )


def _dedupe_updates_activities(activities: list[UpdatesActivity]) -> list[UpdatesActivity]:
    """Description:
    Deduplicate Updates activities by kind and container.

    Args:
        activities: Activities to dedupe.

    Returns:
        Deduped activities in first-seen order.
    """

    deduped: list[UpdatesActivity] = []
    seen: set[tuple[str, str]] = set()

    for activity in activities:
        key = (activity.activity_kind, activity.container_id)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(activity)

    return deduped


def _dedupe_recent_items(items: list[RecentMediaItem]) -> list[RecentMediaItem]:
    """Description:
    Deduplicate recent media items, preferring richer metadata.

    Args:
        items: Items to dedupe.

    Returns:
        Deduped items keyed by media id.
    """

    deduped: dict[str, RecentMediaItem] = {}

    for item in items:
        existing = deduped.get(item.media_id)
        if existing is None:
            deduped[item.media_id] = item
            continue

        prefer_new = (
            (existing.upload_timestamp_ms is None and item.upload_timestamp_ms is not None)
            or (existing.capture_timestamp_ms is None and item.capture_timestamp_ms is not None)
            or (existing.preview_url is None and item.preview_url is not None)
            or (existing.width is None and item.width is not None)
            or (existing.height is None and item.height is not None)
        )
        if prefer_new:
            deduped[item.media_id] = item

    return list(deduped.values())


def _container_id_from_token(token: str) -> str | None:
    """Description:
    Extract the shared container/media id from an Updates token.

    Args:
        token: Colon-delimited Updates activity token.

    Returns:
        Container media id, or `None`.
    """

    parts = token.split(":")
    for part in parts[2:]:
        if MEDIA_ID_RE.fullmatch(part):
            return part
    return None


def _actor_id_from_token(token: str) -> str | None:
    """Description:
    Extract the actor id from an Updates token.

    Args:
        token: Colon-delimited Updates activity token.

    Returns:
        Actor media id, or `None`.
    """

    parts = token.split(":")
    for part in parts[3:]:
        if MEDIA_ID_RE.fullmatch(part):
            return part
    return None


def _event_timestamp_from_token(token: str) -> int | None:
    """Description:
    Extract an event timestamp from an Updates token.

    Args:
        token: Colon-delimited Updates activity token.

    Returns:
        Epoch milliseconds, or `None`.
    """

    parts = token.split(":")
    if len(parts) > 1 and parts[1].isdigit():
        return int(parts[1]) * 1000
    return None


def _first_epoch_millis(value: JsonValue) -> int | None:
    """Description:
    Find the earliest epoch-millisecond integer nested in a JSON value.

    Args:
        value: JSON value to traverse.

    Returns:
        Earliest epoch-millisecond value, or `None`.
    """

    found: list[int] = []

    def walk(node: JsonValue) -> None:
        """Description:
        Traverse nested JSON values collecting epoch-millisecond integers.

        Args:
            node: JSON node to inspect.

        Side Effects:
            Appends matches to `found`.
        """

        if isinstance(node, int) and 10**12 <= node < 10**13:
            found.append(node)
            return
        if isinstance(node, list):
            for child in node:
                walk(child)
        elif isinstance(node, dict):
            for child in node.values():
                walk(child)

    walk(value)
    return min(found) if found else None

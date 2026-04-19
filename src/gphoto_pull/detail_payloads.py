"""Shape-validated Google Photos detail metadata parsing.

Description:
    Extracts a narrow set of item-specific metadata from private Google Photos
    detail/info `batchexecute` payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from gphoto_pull.rpc_payloads import JsonValue, RpcPayloadParseError, parse_batchexecute_frames


@dataclass(slots=True, frozen=True)
class GeoData:
    """Google Photos geodata.

    Description:
        Represents cloud-side latitude/longitude metadata suitable for Takeout's
        `geoData` object.

    Attributes:
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.
        altitude: Altitude in meters.
    """

    latitude: float
    longitude: float
    altitude: float = 0.0


@dataclass(slots=True, frozen=True)
class DetailMetadata:
    """Item-specific Google Photos detail metadata.

    Description:
        Contains the proven subset of detail/info metadata used to enrich
        Takeout-style sidecars.

    Attributes:
        media_id: Google Photos media key.
        title: Detail title/filename.
        description: Detail description.
        photo_taken_time: Capture time from the detail payload.
        timezone_offset_ms: Timezone offset carried by the detail payload.
        bytes_size: Media byte size.
        width: Pixel width.
        height: Pixel height.
        geo_data: Cloud-side location coordinates.
        people: Person names visible in the detail payload.
    """

    media_id: str
    title: str | None = None
    description: str | None = None
    photo_taken_time: datetime | None = None
    timezone_offset_ms: int | None = None
    bytes_size: int | None = None
    width: int | None = None
    height: int | None = None
    geo_data: GeoData | None = None
    people: tuple[str, ...] = ()


def parse_detail_metadata(
    response_texts: list[str],
    *,
    expected_media_id: str,
) -> DetailMetadata | None:
    """Description:
    Parse item-specific detail metadata from captured batchexecute responses.

    Args:
        response_texts: Raw batchexecute response bodies.
        expected_media_id: Media id that must match the parsed detail item.

    Returns:
        Parsed detail metadata, or `None` when no matching detail payload exists.
    """

    for raw_text in response_texts:
        try:
            frames = parse_batchexecute_frames(raw_text)
        except RpcPayloadParseError:
            continue
        for frame in frames:
            payload = frame.decoded_payload()
            detail = _parse_detail_payload(payload, expected_media_id=expected_media_id)
            if detail is not None:
                return detail
    return None


def _parse_detail_payload(
    payload: JsonValue | None,
    *,
    expected_media_id: str,
) -> DetailMetadata | None:
    """Description:
    Parse the observed detail tuple from one decoded RPC payload.

    Args:
        payload: Decoded RPC payload.
        expected_media_id: Media id that must match the tuple.

    Returns:
        Parsed detail metadata, or `None`.
    """

    if not isinstance(payload, list) or not payload or not isinstance(payload[0], list):
        return None
    item = payload[0]
    if len(item) < 14 or item[0] != expected_media_id:
        return None

    return DetailMetadata(
        media_id=expected_media_id,
        title=item[2] if isinstance(item[2], str) else None,
        description=item[1] if isinstance(item[1], str) else None,
        photo_taken_time=_datetime_from_epoch_millis(item[3]),
        timezone_offset_ms=item[4] if isinstance(item[4], int) else None,
        bytes_size=item[5] if isinstance(item[5], int) and item[5] >= 0 else None,
        width=item[6] if isinstance(item[6], int) and item[6] > 0 else None,
        height=item[7] if isinstance(item[7], int) and item[7] > 0 else None,
        geo_data=_geo_data_from_detail_item(item),
        people=_people_from_detail_item(item),
    )


def _datetime_from_epoch_millis(value: JsonValue) -> datetime | None:
    """Description:
    Convert an epoch-millisecond JSON value into UTC datetime.

    Args:
        value: JSON value to convert.

    Returns:
        Aware UTC datetime, or `None`.
    """

    if not isinstance(value, int) or not 10**12 <= value < 10**13:
        return None
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _geo_data_from_detail_item(item: list[JsonValue]) -> GeoData | None:
    """Description:
    Extract cloud-side location coordinates from a detail tuple.

    Args:
        item: Detail tuple.

    Returns:
        Parsed geodata, or `None`.
    """

    for index in (13, 9):
        if len(item) <= index:
            continue
        coordinates = _coordinates_from_value(item[index])
        if coordinates is not None:
            return coordinates
    return None


def _coordinates_from_value(value: JsonValue) -> GeoData | None:
    """Description:
    Extract scaled integer coordinates from a nested detail value.

    Args:
        value: JSON value that may contain a coordinate pair.

    Returns:
        Parsed geodata, or `None`.
    """

    if (
        isinstance(value, list)
        and value
        and isinstance(value[0], list)
        and len(value[0]) >= 2
        and isinstance(value[0][0], int)
        and isinstance(value[0][1], int)
    ):
        latitude = value[0][0] / 10_000_000
        longitude = value[0][1] / 10_000_000
        if -90 <= latitude <= 90 and -180 <= longitude <= 180:
            return GeoData(latitude=latitude, longitude=longitude)
    return None


def _people_from_detail_item(item: list[JsonValue]) -> tuple[str, ...]:
    """Description:
    Extract person names from the observed people block.

    Args:
        item: Detail tuple.

    Returns:
        Deduped person names.
    """

    if len(item) <= 18:
        return ()
    raw_people = item[18]
    if not isinstance(raw_people, list):
        return ()

    names: list[str] = []
    for entry in raw_people:
        if not isinstance(entry, list) or len(entry) <= 1 or not isinstance(entry[1], list):
            continue
        candidate = entry[1]
        if len(candidate) > 5 and isinstance(candidate[5], str):
            names.append(candidate[5])
    return tuple(dict.fromkeys(names))

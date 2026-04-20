"""Google Takeout-style metadata sidecars.

Description:
    Writes `*.supplemental-metadata.json` files using the field names and basic
    shapes emitted by Google Takeout for Google Photos exports.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import msgspec
import msgspec.json

from gphoto_pull.detail_payloads import DetailMetadata
from gphoto_pull.models import MediaMetadata


class TakeoutTime(msgspec.Struct, frozen=True):
    """Takeout timestamp object.

    Description:
        Matches Google Takeout's `creationTime` and `photoTakenTime` object
        shape.

    Attributes:
        timestamp: Epoch seconds encoded as text.
        formatted: Human-readable UTC timestamp.
    """

    timestamp: str
    formatted: str


class TakeoutGeoData(msgspec.Struct, frozen=True):
    """Takeout geodata object.

    Description:
        Matches Google Takeout's geodata object shape.

    Attributes:
        latitude: Latitude in decimal degrees.
        longitude: Longitude in decimal degrees.
        altitude: Altitude in meters.
        latitude_span: Viewport latitude span, encoded as `latitudeSpan`.
        longitude_span: Viewport longitude span, encoded as `longitudeSpan`.
    """

    latitude: float
    longitude: float
    altitude: float
    latitude_span: float = msgspec.field(name="latitudeSpan")
    longitude_span: float = msgspec.field(name="longitudeSpan")


class TakeoutPerson(msgspec.Struct, frozen=True):
    """Takeout person object.

    Description:
        Matches Google Takeout's person name object shape.

    Attributes:
        name: Person display name.
    """

    name: str


class TakeoutSidecar(msgspec.Struct, frozen=True, omit_defaults=True):
    """Takeout supplemental metadata object.

    Description:
        Matches the JSON shape written next to downloaded media files.

    Attributes:
        title: Final media filename.
        description: Google Photos description or an empty string.
        image_views: Google Takeout image-views field, encoded as `imageViews`.
        creation_time: Upload/add timestamp, encoded as `creationTime`.
        photo_taken_time: Capture timestamp, encoded as `photoTakenTime`.
        geo_data: Cloud-side geodata when known, encoded as `geoData`.
        people: People labels when known.
        url: Google Photos detail URL when known.
    """

    title: str
    description: str
    image_views: str = msgspec.field(name="imageViews")
    creation_time: TakeoutTime = msgspec.field(name="creationTime")
    photo_taken_time: TakeoutTime = msgspec.field(name="photoTakenTime")
    geo_data: TakeoutGeoData | None = msgspec.field(default=None, name="geoData")
    people: tuple[TakeoutPerson, ...] = ()
    url: str | None = None


def write_takeout_sidecar(
    media_path: Path,
    metadata: MediaMetadata,
    detail: DetailMetadata | None = None,
) -> Path:
    """Description:
    Write a Google Takeout-style supplemental metadata sidecar.

    Args:
        media_path: Final downloaded media path.
        metadata: Final metadata known for the media item.
        detail: Optional item-specific detail metadata.

    Returns:
        Written sidecar path.

    Side Effects:
        Writes JSON next to `media_path`.
    """

    sidecar_path = media_path.with_name(f"{media_path.name}.supplemental-metadata.json")
    encoded = msgspec.json.encode(_takeout_metadata(metadata, detail))
    sidecar_path.write_bytes(msgspec.json.format(encoded, indent=2) + b"\n")
    return sidecar_path


def _takeout_metadata(
    metadata: MediaMetadata,
    detail: DetailMetadata | None,
) -> TakeoutSidecar:
    """Description:
    Build Takeout-style JSON metadata from known media metadata.

    Args:
        metadata: Final metadata known for the media item.
        detail: Optional item-specific detail metadata.

    Returns:
        Typed sidecar payload.
    """

    geo_data: TakeoutGeoData | None = None
    if detail is not None and detail.geo_data is not None:
        geo_data = TakeoutGeoData(
            latitude=detail.geo_data.latitude,
            longitude=detail.geo_data.longitude,
            altitude=detail.geo_data.altitude,
            latitude_span=0.0,
            longitude_span=0.0,
        )
    return TakeoutSidecar(
        title=metadata.filename,
        description="" if detail is None or detail.description is None else detail.description,
        image_views="0",
        creation_time=_takeout_time(metadata.uploaded_time),
        photo_taken_time=_takeout_time(
            detail.photo_taken_time if detail is not None else metadata.capture_time
        ),
        geo_data=geo_data,
        people=() if detail is None else tuple(TakeoutPerson(name=name) for name in detail.people),
        url=metadata.product_url,
    )


def _takeout_time(value: datetime | None) -> TakeoutTime:
    """Description:
    Render a datetime using Google Takeout's timestamp object shape.

    Args:
        value: Aware datetime to render. Missing values are represented as Unix
            epoch zero.

    Returns:
        Takeout timestamp object.
    """

    normalized = datetime.fromtimestamp(0, tz=UTC) if value is None else value.astimezone(UTC)
    timestamp = int(normalized.timestamp())
    return TakeoutTime(
        timestamp=str(timestamp),
        formatted=normalized.strftime("%b %-d, %Y, %-I:%M:%S %p UTC"),
    )

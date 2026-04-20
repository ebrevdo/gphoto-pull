"""Download target planning and atomic file finalization.

Description:
    Chooses safe local filenames, creates staging paths, and finalizes browser
    download artifacts into the download directory.
"""

from __future__ import annotations

import errno
import os
import re
import secrets
import shutil
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path

from gphoto_pull.models import MediaMetadata, MediaStateRecord

_INVALID_FILENAME_CHARS = str.maketrans({character: "_" for character in '<>:"/\\|?*'})
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f]")
_MEDIA_ID_SLUG = re.compile(r"[^A-Za-z0-9]+")
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


class DownloadError(RuntimeError):
    """Download planning or finalization failure.

    Description:
        Raised when a download target cannot be planned, staged, or finalized
        safely.
    """


@dataclass(slots=True, frozen=True)
class DownloadPlan:
    """Filesystem target chosen for one media download.

    Description:
        Describes how a Playwright download artifact should be staged and named
        before it is committed into the final download directory.

    Attributes:
        media_id: Google Photos media key.
        original_filename: Filename reported by Google Photos.
        final_filename: Sanitized unique local filename.
        final_path: Absolute or root-relative final path.
        relative_path: Path stored in sync state.
        collision_index: Numeric suffix index used for collisions.
        used_media_id_suffix: Whether the media id was added to avoid collision.
    """

    media_id: str
    original_filename: str
    final_filename: str
    final_path: Path
    relative_path: Path
    collision_index: int = 0
    used_media_id_suffix: bool = False


def plan_download_target(
    download_dir: Path | str,
    media: MediaMetadata | MediaStateRecord,
) -> DownloadPlan:
    """Description:
    Choose a safe final filename for a media item.

    Args:
        download_dir: Root directory for finalized downloads.
        media: Metadata or a persisted state record to plan for.

    Returns:
        A `DownloadPlan` with a unique target path.

    Side Effects:
        Creates `download_dir` if it does not exist.
    """

    download_root = Path(download_dir)
    download_root.mkdir(parents=True, exist_ok=True)

    metadata = media.metadata if isinstance(media, MediaStateRecord) else media
    original_filename = metadata.filename

    primary_path = primary_download_path(download_root, metadata)
    sanitized_name = primary_path.name
    target_dir = primary_path.parent
    if not primary_path.exists():
        relative_path = primary_path.relative_to(download_root)
        return DownloadPlan(
            media_id=metadata.media_id,
            original_filename=original_filename,
            final_filename=sanitized_name,
            final_path=primary_path,
            relative_path=relative_path,
        )

    stem, suffix = _split_filename(sanitized_name)
    media_id_suffix = _slugify_media_id(metadata.media_id)

    candidate_name = f"{stem}--{media_id_suffix}{suffix}"
    candidate_path = target_dir / candidate_name
    if not candidate_path.exists():
        relative_path = candidate_path.relative_to(download_root)
        return DownloadPlan(
            media_id=metadata.media_id,
            original_filename=original_filename,
            final_filename=candidate_name,
            final_path=candidate_path,
            relative_path=relative_path,
            collision_index=1,
            used_media_id_suffix=True,
        )

    probe = 2
    while True:
        candidate_name = f"{stem}--{media_id_suffix}-{probe}{suffix}"
        candidate_path = target_dir / candidate_name
        if not candidate_path.exists():
            relative_path = candidate_path.relative_to(download_root)
            return DownloadPlan(
                media_id=metadata.media_id,
                original_filename=original_filename,
                final_filename=candidate_name,
                final_path=candidate_path,
                relative_path=relative_path,
                collision_index=probe,
                used_media_id_suffix=True,
            )
        probe += 1


def primary_download_path(
    download_dir: Path | str,
    media: MediaMetadata | MediaStateRecord,
) -> Path:
    """Description:
    Build the primary non-collision target path for a media item.

    Args:
        download_dir: Root directory for finalized downloads.
        media: Metadata or a persisted state record to plan for.

    Returns:
        Primary target path before collision suffixing.
    """

    download_root = Path(download_dir)
    metadata = media.metadata if isinstance(media, MediaStateRecord) else media
    return (
        download_root
        / _uploaded_date_directory(metadata)
        / _sanitize_filename(metadata.filename)
    )


def create_staging_path(plan: DownloadPlan, *, suffix: str = ".part") -> Path:
    """Description:
    Create a collision-resistant temporary path next to the final target.

    Args:
        plan: Download plan whose final directory and filename are used.
        suffix: Temporary filename suffix.

    Returns:
        A hidden staging path in the final target directory.

    Side Effects:
        Generates random bytes for the staging token.
    """

    token = secrets.token_hex(8)
    return plan.final_path.with_name(f".{plan.final_filename}.{token}{suffix}")


def finalize_download(
    source_path: Path | str,
    plan: DownloadPlan,
    *,
    staging_path: Path | str | None = None,
    preserve_source: bool = False,
) -> Path:
    """Description:
    Move or copy a Playwright download artifact into its final location atomically.

    Args:
        source_path: Existing file produced by Playwright.
        plan: Final target plan.
        staging_path: Optional caller-provided staging path.
        preserve_source: Copy instead of move when `True`.

    Returns:
        The finalized file path.

    Side Effects:
        Creates parent directories, moves/copies the source artifact, atomically
        replaces the staging file into the final path, and cleans failed staging
        files when possible.
    """

    source = Path(source_path)
    if not source.exists():
        raise DownloadError(f"Download artifact does not exist: {source}")
    if not source.is_file():
        raise DownloadError(f"Download artifact is not a file: {source}")

    final_path = plan.final_path
    final_path.parent.mkdir(parents=True, exist_ok=True)
    staging = create_staging_path(plan) if staging_path is None else Path(staging_path)

    if staging == final_path:
        raise DownloadError("staging_path must be different from the final target path.")
    if staging.parent != final_path.parent:
        raise DownloadError("staging_path must be created in the same directory as the final path.")

    if final_path.exists():
        raise DownloadError(f"Final download target already exists: {final_path}")

    try:
        if source != staging:
            _move_or_copy_file(source, staging, preserve_source=preserve_source)

        if final_path.exists():
            raise DownloadError(f"Final download target already exists: {final_path}")

        os.replace(staging, final_path)
        return final_path
    except OSError as exc:
        _cleanup_staging_file(staging)
        raise DownloadError(str(exc)) from exc
    except DownloadError:
        _cleanup_staging_file(staging)
        raise


def _move_or_copy_file(
    source: Path,
    destination: Path,
    *,
    preserve_source: bool,
) -> None:
    """Description:
    Move a file, falling back to copy/unlink across filesystems.

    Args:
        source: Existing source file.
        destination: Destination staging path.
        preserve_source: Copy instead of move when true.

    Side Effects:
        Creates the destination parent and moves/copies file data.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)

    if preserve_source:
        shutil.copy2(source, destination)
        return

    try:
        os.replace(source, destination)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.copy2(source, destination)
        source.unlink()


def _cleanup_staging_file(path: Path) -> None:
    """Description:
    Best-effort cleanup for a failed staging artifact.

    Args:
        path: Staging path to remove.

    Side Effects:
        Deletes `path` when it exists.
    """

    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _uploaded_date_directory(metadata: MediaMetadata) -> Path:
    """Description:
    Build the upload-date directory for one media item.

    Args:
        metadata: Media item metadata.

    Returns:
        Relative directory such as `uploaded/2026/04/18`.
    """

    upload_time = metadata.uploaded_time
    if upload_time is None:
        return Path("uploaded") / "unknown"
    normalized = upload_time.astimezone(UTC)
    return (
        Path("uploaded")
        / f"{normalized.year:04d}"
        / f"{normalized.month:02d}"
        / f"{normalized.day:02d}"
    )


def _sanitize_filename(filename: str) -> str:
    """Description:
    Convert an arbitrary Google-provided filename into a safe local filename.

    Args:
        filename: Suggested filename from Google Photos.

    Returns:
        Sanitized filename.
    """

    leaf_name = filename.replace("\\", "/").split("/")[-1]
    sanitized = _CONTROL_CHARACTERS.sub("", leaf_name).translate(_INVALID_FILENAME_CHARS)
    sanitized = re.sub(r"\s+", " ", sanitized).strip(" .")

    if sanitized == "":
        sanitized = "download"

    stem, suffix = _split_filename(sanitized)
    if stem.upper() in _WINDOWS_RESERVED_NAMES:
        stem = f"_{stem}"

    return f"{stem}{suffix}"


def _slugify_media_id(media_id: str) -> str:
    """Description:
    Convert a media id into a filename-safe collision suffix.

    Args:
        media_id: Google Photos media key.

    Returns:
        Lowercase slug.
    """

    slug = _MEDIA_ID_SLUG.sub("-", media_id).strip("-").lower()
    return slug or "media"


def _split_filename(filename: str) -> tuple[str, str]:
    """Description:
    Split a filename into non-empty stem and suffix.

    Args:
        filename: Filename to split.

    Returns:
        Stem and suffix.
    """

    path = Path(filename)
    suffix = path.suffix
    stem = path.name[: -len(suffix)] if suffix else path.name
    if stem == "":
        stem = "download"
    return stem, suffix

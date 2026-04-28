"""SQLite-backed local Google Photos index.

Description:
    Persists media metadata discovered during Google Photos enumeration plus
    coarse checkpoints. Download lifecycle state is intentionally not persisted.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

from gphoto_pull.models import MediaMetadata, MediaStateRecord, SyncCheckpoint

DEFAULT_STATE_DB_PATH = Path(".state/pull-state.sqlite3")
STATE_SCHEMA_VERSION = 2
RESET_INDEX_COMMAND = "gphoto-pull reset --target index --yes"


class StateSchemaError(RuntimeError):
    """Incompatible media index schema that needs an explicit user reset."""


def _require_datetime(value: datetime | None, *, field_name: str) -> datetime:
    """Description:
    Require a non-null datetime from persisted state.

    Args:
        value: Parsed datetime.
        field_name: Field name for error messages.

    Returns:
        Non-null datetime.
    """

    if value is None:
        raise ValueError(f"{field_name} is missing from persisted state.")
    return value


def _reset_required_message(db_path: Path) -> str:
    """Description:
    Build the user-facing reset instruction for incompatible state databases.

    Args:
        db_path: Incompatible database path.

    Returns:
        Error message with a concrete reset command.
    """

    return (
        f"{db_path} uses an older media index schema that cannot be upgraded safely. "
        "Stop any running gphoto-pull process, run "
        f"`{RESET_INDEX_COMMAND}`, then rerun your pull or refresh command."
    )


def _merge_media_metadata(existing: MediaMetadata, incoming: MediaMetadata) -> MediaMetadata:
    """Description:
    Merge a new discovery row into existing media metadata without losing richer fields.

    Args:
        existing: Current persisted metadata.
        incoming: Newly discovered metadata for the same media id.

    Returns:
        Merged metadata preserving known filenames and optional fields unless the
        incoming row provides better information.
    """

    return MediaMetadata(
        media_id=existing.media_id,
        filename=_merge_filename(existing.filename, incoming.filename, media_id=existing.media_id),
        capture_time=incoming.capture_time or existing.capture_time,
        uploaded_time=incoming.uploaded_time or existing.uploaded_time,
        mime_type=incoming.mime_type or existing.mime_type,
        media_type=incoming.media_type or existing.media_type,
        product_url=incoming.product_url or existing.product_url,
        preview_url=incoming.preview_url or existing.preview_url,
        width=incoming.width or existing.width,
        height=incoming.height or existing.height,
        bytes_size=incoming.bytes_size if incoming.bytes_size is not None else existing.bytes_size,
    )


def _merge_filename(existing: str, incoming: str, *, media_id: str) -> str:
    """Description:
    Prefer resolved filenames over unresolved placeholder filenames.

    Args:
        existing: Current persisted filename.
        incoming: Newly discovered filename.
        media_id: Media id used to recognize unresolved placeholders.

    Returns:
        Filename to persist.
    """

    unresolved = f"unresolved-{media_id}"
    if existing == unresolved and incoming != unresolved:
        return incoming
    if incoming == unresolved:
        return existing
    return incoming


@dataclass(slots=True, frozen=True)
class RecentPageCheckpoint:
    """Stored recent-media cursor checkpoint.

    Description:
        Represents a safe place to resume older upload-time pagination.

    Attributes:
        rpc_id: Opaque browser RPC id that produced the cursor.
        cursor: Opaque cursor for the next older page.
        oldest_upload_time: Oldest upload timestamp seen before this cursor.
        item_count: Number of media items in the page that produced this cursor.
        page_count: One-based direct page count observed in the refresh run.
        created_at: Time this checkpoint was stored.
    """

    rpc_id: str
    cursor: str
    oldest_upload_time: datetime
    item_count: int
    page_count: int
    created_at: datetime


class PullStateStore:
    """Repository for persisted media index records and sync checkpoints.

    Description:
        Owns a SQLite connection and exposes explicit operations for media
        metadata upserts and checkpoint storage.

    Attributes:
        db_path: SQLite database path.
    """

    def __init__(self, db_path: Path | str = DEFAULT_STATE_DB_PATH) -> None:
        """Description:
        Open or create a pull-state database.

        Args:
            db_path: SQLite database path.

        Side Effects:
            Creates the database parent directory, opens SQLite, enables foreign
            keys, and initializes the current schema.
        """

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.db_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        try:
            self.initialize()
        except Exception:
            self._connection.close()
            raise

    def __enter__(self) -> PullStateStore:
        """Description:
        Enter the store context manager.

        Returns:
            This open store.
        """

        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Description:
        Exit the store context manager.

        Args:
            _exc_type: Exception type from the context, if any.
            _exc: Exception instance from the context, if any.
            _exc_tb: Traceback from the context, if any.

        Side Effects:
            Closes the SQLite connection.
        """

        self.close()

    def close(self) -> None:
        """Description:
        Close the SQLite connection.

        Side Effects:
            Releases the database connection.
        """

        self._connection.close()

    def initialize(self) -> None:
        """Description:
        Create or validate tables used by the local index.

        Side Effects:
            Writes the current schema for new databases.
        """

        table_names = self._table_names()
        if not table_names:
            self._create_current_schema()
            return

        schema_version = self._schema_version()
        if schema_version != STATE_SCHEMA_VERSION:
            raise StateSchemaError(_reset_required_message(self.db_path))

        required_tables = {
            "media_state",
            "recent_page_checkpoints",
            "sync_checkpoints",
            "upload_coverage_ranges",
        }
        if not required_tables.issubset(table_names):
            raise StateSchemaError(_reset_required_message(self.db_path))

        self._ensure_current_indexes()

    def _create_current_schema(self) -> None:
        """Description:
        Create the current media index schema.

        Side Effects:
            Writes SQLite tables, indexes, and schema version metadata.
        """

        with self._connection:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS media_state (
                    media_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    capture_time TEXT,
                    uploaded_time TEXT,
                    mime_type TEXT,
                    media_type TEXT,
                    product_url TEXT,
                    preview_url TEXT,
                    width INTEGER,
                    height INTEGER,
                    bytes_size INTEGER,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS sync_checkpoints (
                    name TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS recent_page_checkpoints (
                    cursor TEXT PRIMARY KEY,
                    rpc_id TEXT NOT NULL,
                    oldest_upload_time TEXT NOT NULL,
                    item_count INTEGER NOT NULL,
                    page_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS upload_coverage_ranges (
                    oldest_upload_time TEXT NOT NULL,
                    newest_upload_time TEXT NOT NULL,
                    CHECK (oldest_upload_time <= newest_upload_time)
                );
                """
            )
            self._ensure_current_indexes()
            self._connection.execute(f"PRAGMA user_version = {STATE_SCHEMA_VERSION}")

    def _schema_version(self) -> int:
        """Description:
        Read the SQLite schema version.

        Returns:
            Current `PRAGMA user_version` value.

        Side Effects:
            Reads SQLite metadata.
        """

        row = self._connection.execute("PRAGMA user_version").fetchone()
        return int(row[0])

    def _table_names(self) -> set[str]:
        """Description:
        List application table names present in the database.

        Returns:
            Table names excluding SQLite internal tables.

        Side Effects:
            Reads SQLite schema metadata.
        """

        rows = self._connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
            """
        ).fetchall()
        return {str(row["name"]) for row in rows}

    def _ensure_current_indexes(self) -> None:
        """Description:
        Ensure indexes for the current schema exist.

        Side Effects:
            Writes SQLite indexes when missing.
        """

        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_media_state_uploaded_time
                ON media_state (uploaded_time DESC, media_id)
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_recent_page_checkpoints_oldest
                ON recent_page_checkpoints (oldest_upload_time ASC, rpc_id)
            """
        )
        self._connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_upload_coverage_ranges_bounds
                ON upload_coverage_ranges (oldest_upload_time ASC, newest_upload_time DESC)
            """
        )

    def upsert_media(self, metadata: MediaMetadata) -> MediaStateRecord:
        """Description:
        Insert or refresh a discovered media item.

        Args:
            metadata: Latest metadata for the media item.

        Returns:
            The persisted index record.

        Side Effects:
            Writes to `media_state` and updates `last_seen_at` for existing rows.
        """

        existing = self.get_media(metadata.media_id)
        now = datetime.now(UTC)

        if existing is None:
            record = MediaStateRecord(
                metadata=metadata,
                first_seen_at=now,
                last_seen_at=now,
            )
        else:
            record = replace(
                existing,
                metadata=_merge_media_metadata(existing.metadata, metadata),
                last_seen_at=now,
            )

        self._write_media_record(record)
        return record

    def get_media(self, media_id: str) -> MediaStateRecord | None:
        """Description:
        Load one media index record by id.

        Args:
            media_id: Google Photos media key.

        Returns:
            The record when present, otherwise `None`.
        """

        row = self._connection.execute(
            """
            SELECT
                media_id,
                filename,
                capture_time,
                uploaded_time,
                mime_type,
                media_type,
                product_url,
                preview_url,
                width,
                height,
                bytes_size,
                first_seen_at,
                last_seen_at
            FROM media_state
            WHERE media_id = ?
            """,
            (media_id,),
        ).fetchone()
        return None if row is None else self._row_to_media_record(row)

    def list_media(self, *, limit: int | None = None) -> list[MediaStateRecord]:
        """Description:
        List media index records with an optional row-count filter.

        Args:
            limit: Optional maximum row count.

        Returns:
            Matching media records ordered by most recent upload/capture/seen time.
        """

        query = """
            SELECT
                media_id,
                filename,
                capture_time,
                uploaded_time,
                mime_type,
                media_type,
                product_url,
                preview_url,
                width,
                height,
                bytes_size,
                first_seen_at,
                last_seen_at
            FROM media_state
            ORDER BY
                COALESCE(uploaded_time, capture_time, last_seen_at) DESC,
                media_id ASC
        """
        parameters: list[int] = []

        if limit is not None:
            if limit <= 0:
                raise ValueError("limit must be greater than zero.")
            query += " LIMIT ?"
            parameters.append(limit)

        rows = self._connection.execute(query, parameters).fetchall()
        return [self._row_to_media_record(row) for row in rows]

    def list_media_in_upload_window(
        self,
        *,
        after: datetime,
        before: datetime | None,
    ) -> list[MediaStateRecord]:
        """Description:
        List indexed media whose upload timestamp is inside a requested window.

        Args:
            after: Inclusive lower-bound timestamp.
            before: Optional exclusive upper-bound timestamp.

        Returns:
            Matching media records ordered by upload time descending.
        """

        records = self.list_media()
        return [
            record
            for record in records
            if record.metadata.uploaded_time is not None
            and record.metadata.uploaded_time >= after
            and not (before is not None and record.metadata.uploaded_time >= before)
        ]

    def upload_coverage_satisfies(self, after: datetime) -> bool:
        """Description:
        Check whether any trusted range covers the requested lower bound.

        Args:
            after: Inclusive lower-bound timestamp requested by the caller.

        Returns:
            `True` when a trusted coverage range extends through `after`.
        """

        normalized_after = after.astimezone(UTC).isoformat()
        row = self._connection.execute(
            """
            SELECT 1
            FROM upload_coverage_ranges
            WHERE oldest_upload_time <= ?
            LIMIT 1
            """,
            (normalized_after,),
        ).fetchone()
        return row is not None

    def upload_window_satisfies(self, *, after: datetime, before: datetime | None) -> bool:
        """Description:
        Check whether a bounded upload window is fully covered by the local index.

        Args:
            after: Inclusive lower-bound timestamp requested by the caller.
            before: Optional exclusive upper-bound timestamp requested by the caller.

        Returns:
            `True` when the index can answer `[after, before)` without a live
            refresh. Open-ended windows are never considered fully covered.
        """

        if before is None:
            return False
        normalized_after = after.astimezone(UTC).isoformat()
        normalized_before = before.astimezone(UTC).isoformat()
        row = self._connection.execute(
            """
            SELECT 1
            FROM upload_coverage_ranges
            WHERE oldest_upload_time <= ?
              AND newest_upload_time >= ?
            LIMIT 1
            """,
            (normalized_after, normalized_before),
        ).fetchone()
        return row is not None

    def upload_time_has_covering_range(self, *, uploaded_time: datetime, after: datetime) -> bool:
        """Description:
        Check whether an upload timestamp is inside a range that reaches `after`.

        Args:
            uploaded_time: Upload timestamp from a live Recent payload item.
            after: Requested inclusive lower bound.

        Returns:
            `True` when the timestamp is inside a trusted range whose lower
            bound already covers the requested `after` timestamp.
        """

        normalized_upload = uploaded_time.astimezone(UTC).isoformat()
        normalized_after = after.astimezone(UTC).isoformat()
        row = self._connection.execute(
            """
            SELECT 1
            FROM upload_coverage_ranges
            WHERE oldest_upload_time <= ?
              AND oldest_upload_time <= ?
              AND newest_upload_time >= ?
            LIMIT 1
            """,
            (normalized_after, normalized_upload, normalized_upload),
        ).fetchone()
        return row is not None

    def record_upload_coverage(
        self,
        *,
        oldest_upload_time: datetime,
        newest_upload_time: datetime,
    ) -> None:
        """Description:
        Record a trusted contiguous upload-time coverage range.

        Args:
            oldest_upload_time: Inclusive lower bound of the contiguous traversal.
            newest_upload_time: Inclusive upper bound of the contiguous traversal.

        Side Effects:
            Inserts or merges rows in `upload_coverage_ranges`.
        """

        normalized_oldest = oldest_upload_time.astimezone(UTC)
        normalized_newest = newest_upload_time.astimezone(UTC)
        if normalized_oldest > normalized_newest:
            raise ValueError("oldest_upload_time must be on or before newest_upload_time.")

        with self._connection:
            rows = self._connection.execute(
                """
                SELECT
                    oldest_upload_time,
                    newest_upload_time
                FROM upload_coverage_ranges
                WHERE oldest_upload_time <= ?
                  AND newest_upload_time >= ?
                """,
                (normalized_newest.isoformat(), normalized_oldest.isoformat()),
            ).fetchall()

            merged_oldest = normalized_oldest
            merged_newest = normalized_newest
            for row in rows:
                row_oldest = datetime.fromisoformat(str(row["oldest_upload_time"]))
                row_newest = datetime.fromisoformat(str(row["newest_upload_time"]))
                if row_oldest < merged_oldest:
                    merged_oldest = row_oldest
                if row_newest > merged_newest:
                    merged_newest = row_newest

            if rows:
                self._connection.execute(
                    """
                    DELETE FROM upload_coverage_ranges
                    WHERE oldest_upload_time <= ?
                      AND newest_upload_time >= ?
                    """,
                    (normalized_newest.isoformat(), normalized_oldest.isoformat()),
                )

            self._connection.execute(
                """
                INSERT INTO upload_coverage_ranges (
                    oldest_upload_time,
                    newest_upload_time
                )
                VALUES (?, ?)
                """,
                (
                    merged_oldest.isoformat(),
                    merged_newest.isoformat(),
                ),
            )

    def upsert_recent_page_checkpoint(
        self,
        *,
        rpc_id: str,
        cursor: str,
        oldest_upload_time: datetime,
        item_count: int,
        page_count: int,
    ) -> RecentPageCheckpoint:
        """Description:
        Store a recent-media cursor checkpoint.

        Args:
            rpc_id: Opaque RPC id used for the page request.
            cursor: Opaque cursor for the next older page.
            oldest_upload_time: Oldest upload timestamp in the page that yielded
                this cursor.
            item_count: Number of media items in that page.
            page_count: One-based direct page count observed in the refresh run.

        Returns:
            Persisted checkpoint.

        Side Effects:
            Writes one checkpoint row.
        """

        if item_count <= 0:
            raise ValueError("item_count must be greater than zero.")
        if page_count <= 0:
            raise ValueError("page_count must be greater than zero.")

        checkpoint = RecentPageCheckpoint(
            rpc_id=rpc_id,
            cursor=cursor,
            oldest_upload_time=oldest_upload_time.astimezone(UTC),
            item_count=item_count,
            page_count=page_count,
            created_at=datetime.now(UTC),
        )
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO recent_page_checkpoints (
                    cursor,
                    rpc_id,
                    oldest_upload_time,
                    item_count,
                    page_count,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cursor) DO UPDATE SET
                    rpc_id = excluded.rpc_id,
                    oldest_upload_time = excluded.oldest_upload_time,
                    item_count = excluded.item_count,
                    page_count = excluded.page_count,
                    created_at = excluded.created_at
                """,
                (
                    checkpoint.cursor,
                    checkpoint.rpc_id,
                    checkpoint.oldest_upload_time.isoformat(),
                    checkpoint.item_count,
                    checkpoint.page_count,
                    checkpoint.created_at.isoformat(),
                ),
            )
        return checkpoint

    def best_recent_page_checkpoint(
        self,
        *,
        after: datetime,
        rpc_ids: tuple[str, ...],
    ) -> RecentPageCheckpoint | None:
        """Description:
        Find the nearest stored cursor that can extend coverage to `after`.

        Args:
            after: Requested lower-bound timestamp.
            rpc_ids: Current browser-captured RPC ids that are safe to replay.

        Returns:
            Checkpoint closest to `after`, or `None`.
        """

        if not rpc_ids:
            return None
        placeholders = ", ".join("?" for _ in rpc_ids)
        rows = self._connection.execute(
            f"""
            SELECT
                cursor,
                rpc_id,
                oldest_upload_time,
                item_count,
                page_count,
                created_at
            FROM recent_page_checkpoints
            WHERE oldest_upload_time >= ?
              AND rpc_id IN ({placeholders})
            ORDER BY oldest_upload_time ASC
            LIMIT 1
            """,
            (after.astimezone(UTC).isoformat(), *rpc_ids),
        ).fetchall()
        if not rows:
            return None
        return _row_to_recent_page_checkpoint(rows[0])

    def set_checkpoint(self, name: str, value: str | None) -> SyncCheckpoint:
        """Description:
        Store a named sync checkpoint.

        Args:
            name: Checkpoint name.
            value: Optional checkpoint payload.

        Returns:
            Persisted checkpoint record.

        Side Effects:
            Inserts or updates one SQLite checkpoint row.
        """

        checkpoint = SyncCheckpoint(name=name, value=value, updated_at=datetime.now(UTC))
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO sync_checkpoints (name, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (
                    checkpoint.name,
                    checkpoint.value,
                    checkpoint.updated_at.isoformat(),
                ),
            )
        return checkpoint

    def get_checkpoint(self, name: str) -> SyncCheckpoint | None:
        """Description:
        Load a named sync checkpoint.

        Args:
            name: Checkpoint name.

        Returns:
            The checkpoint when present, otherwise `None`.
        """

        row = self._connection.execute(
            """
            SELECT name, value, updated_at
            FROM sync_checkpoints
            WHERE name = ?
            """,
            (name,),
        ).fetchone()
        if row is None:
            return None
        updated_at_value = row["updated_at"]
        updated_at = datetime.fromisoformat(updated_at_value)
        return SyncCheckpoint(
            name=str(row["name"]),
            value=row["value"],
            updated_at=_require_datetime(updated_at, field_name="updated_at"),
        )

    def _write_media_record(self, record: MediaStateRecord) -> None:
        """Description:
        Insert or replace a complete media index record.

        Args:
            record: Record to write.

        Side Effects:
            Writes one row to SQLite.
        """

        with self._connection:
            self._connection.execute(
                """
                INSERT INTO media_state (
                    media_id,
                    filename,
                    capture_time,
                    uploaded_time,
                    mime_type,
                    media_type,
                    product_url,
                    preview_url,
                    width,
                    height,
                    bytes_size,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_id) DO UPDATE SET
                    filename = excluded.filename,
                    capture_time = excluded.capture_time,
                    uploaded_time = excluded.uploaded_time,
                    mime_type = excluded.mime_type,
                    media_type = excluded.media_type,
                    product_url = excluded.product_url,
                    preview_url = excluded.preview_url,
                    width = excluded.width,
                    height = excluded.height,
                    bytes_size = excluded.bytes_size,
                    first_seen_at = excluded.first_seen_at,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    record.metadata.media_id,
                    record.metadata.filename,
                    (
                        None
                        if record.metadata.capture_time is None
                        else record.metadata.capture_time.isoformat()
                    ),
                    (
                        None
                        if record.metadata.uploaded_time is None
                        else record.metadata.uploaded_time.isoformat()
                    ),
                    record.metadata.mime_type,
                    record.metadata.media_type,
                    record.metadata.product_url,
                    record.metadata.preview_url,
                    record.metadata.width,
                    record.metadata.height,
                    record.metadata.bytes_size,
                    record.first_seen_at.isoformat(),
                    record.last_seen_at.isoformat(),
                ),
            )

    def _row_to_media_record(self, row: sqlite3.Row) -> MediaStateRecord:
        """Description:
        Convert a SQLite row into a media index record.

        Args:
            row: SQLite row from `media_state`.

        Returns:
            Decoded media index record.
        """

        return MediaStateRecord(
            metadata=MediaMetadata(
                media_id=str(row["media_id"]),
                filename=str(row["filename"]),
                capture_time=(
                    None
                    if row["capture_time"] is None
                    else datetime.fromisoformat(row["capture_time"])
                ),
                uploaded_time=(
                    None
                    if row["uploaded_time"] is None
                    else datetime.fromisoformat(row["uploaded_time"])
                ),
                mime_type=row["mime_type"],
                media_type=row["media_type"],
                product_url=row["product_url"],
                preview_url=row["preview_url"],
                width=row["width"],
                height=row["height"],
                bytes_size=row["bytes_size"],
            ),
            first_seen_at=_require_datetime(
                (
                    None
                    if row["first_seen_at"] is None
                    else datetime.fromisoformat(row["first_seen_at"])
                ),
                field_name="first_seen_at",
            ),
            last_seen_at=_require_datetime(
                (
                    None
                    if row["last_seen_at"] is None
                    else datetime.fromisoformat(row["last_seen_at"])
                ),
                field_name="last_seen_at",
            ),
        )


def _row_to_recent_page_checkpoint(row: sqlite3.Row) -> RecentPageCheckpoint:
    """Description:
    Convert a SQLite row into a recent page checkpoint.

    Args:
        row: SQLite row from `recent_page_checkpoints`.

    Returns:
        Decoded checkpoint.
    """

    return RecentPageCheckpoint(
        rpc_id=str(row["rpc_id"]),
        cursor=str(row["cursor"]),
        oldest_upload_time=datetime.fromisoformat(str(row["oldest_upload_time"])),
        item_count=int(row["item_count"]),
        page_count=int(row["page_count"]),
        created_at=datetime.fromisoformat(str(row["created_at"])),
    )

"""SQLite-backed local Google Photos index.

Description:
    Persists media metadata discovered during Google Photos enumeration plus
    coarse checkpoints. Download lifecycle state is intentionally not persisted.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType

from gphoto_pull.models import MediaMetadata, MediaStateRecord, SyncCheckpoint

DEFAULT_STATE_DB_PATH = Path(".state/pull-state.sqlite3")


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
            keys, and initializes/migrates schema.
        """

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.db_path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self.initialize()

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
        Create or migrate tables used by the local index.

        Side Effects:
            Writes SQLite schema changes when required.
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
                """
            )
            self._ensure_media_state_schema()
            self._connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_media_state_uploaded_time
                    ON media_state (uploaded_time DESC, media_id)
                """
            )

    def _ensure_media_state_schema(self) -> None:
        """Description:
        Add columns required by newer versions of the media index schema.

        Side Effects:
            Executes `ALTER TABLE` statements for missing columns.
        """

        expected_columns = (
            ("uploaded_time", "TEXT"),
            ("preview_url", "TEXT"),
        )
        for column_name, column_type in expected_columns:
            if self._media_state_has_column(column_name):
                continue
            self._connection.execute(
                f"""
                ALTER TABLE media_state
                ADD COLUMN {column_name} {column_type}
                """
            )

    def _media_state_has_column(self, column_name: str) -> bool:
        """Description:
        Check whether `media_state` contains a column.

        Args:
            column_name: Column name to check.

        Returns:
            `True` when the column exists.

        Side Effects:
            Reads SQLite schema metadata.
        """

        columns = self._connection.execute("PRAGMA table_info(media_state)").fetchall()
        return any(row["name"] == column_name for row in columns)

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
                metadata=metadata,
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

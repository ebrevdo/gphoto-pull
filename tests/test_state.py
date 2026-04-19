import sqlite3
import unittest
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from gphoto_pull.models import MediaMetadata
from gphoto_pull.state import PullStateStore


class PullStateStoreTests(unittest.TestCase):
    def test_initialize_creates_database_and_schema(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / ".state" / "pull-state.sqlite3"

            with PullStateStore(db_path) as store:
                self.assertEqual(store.db_path, db_path)
                self.assertTrue(db_path.exists())

            with closing(sqlite3.connect(db_path)) as connection:
                rows = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                    ORDER BY name
                    """
                ).fetchall()

            self.assertEqual(
                [row[0] for row in rows],
                ["media_state", "recent_page_checkpoints", "sync_checkpoints"],
            )

    def test_upsert_get_and_list_media_records(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            metadata = MediaMetadata(
                media_id="media-001",
                filename="IMG_0001.JPG",
                capture_time=datetime(2026, 3, 15, 10, 30, tzinfo=UTC),
                uploaded_time=datetime(2026, 3, 17, 14, 5, tzinfo=UTC),
                mime_type="image/jpeg",
                media_type="image",
                product_url="https://photos.google.com/lr/photo/example-1",
                preview_url="https://photos.fife.usercontent.google.com/pw/example-1",
                width=4032,
                height=3024,
                bytes_size=2_400_000,
            )
            updated_metadata = MediaMetadata(
                media_id="media-001",
                filename="Vacation-IMG_0001.JPG",
                capture_time=metadata.capture_time,
                uploaded_time=metadata.uploaded_time,
                mime_type=metadata.mime_type,
                media_type=metadata.media_type,
                product_url=metadata.product_url,
                preview_url=metadata.preview_url,
                width=metadata.width,
                height=metadata.height,
                bytes_size=metadata.bytes_size,
            )

            with PullStateStore(Path(tmp_dir) / "pull-state.sqlite3") as store:
                inserted = store.upsert_media(metadata)
                refreshed = store.upsert_media(updated_metadata)
                loaded = store.get_media(metadata.media_id)
                all_records = store.list_media()

            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.metadata.filename, "Vacation-IMG_0001.JPG")
            self.assertEqual(loaded.metadata.uploaded_time, metadata.uploaded_time)
            self.assertEqual(loaded.metadata.preview_url, metadata.preview_url)
            self.assertEqual(refreshed.first_seen_at, inserted.first_seen_at)
            self.assertGreaterEqual(refreshed.last_seen_at, inserted.last_seen_at)
            self.assertEqual(len(all_records), 1)
            self.assertEqual(all_records[0].metadata.media_id, "media-001")

    def test_manage_checkpoints(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            with PullStateStore(Path(tmp_dir) / "pull-state.sqlite3") as store:
                checkpoint = store.set_checkpoint(
                    "photos-library-last-upload-time",
                    "2026-03-16T08:45:00+00:00",
                )
                saved_checkpoint = store.get_checkpoint(checkpoint.name)

            self.assertIsNotNone(saved_checkpoint)
            assert saved_checkpoint is not None
            self.assertEqual(saved_checkpoint.value, "2026-03-16T08:45:00+00:00")

    def test_upload_coverage_and_window_query(self) -> None:
        with (
            TemporaryDirectory() as tmp_dir,
            PullStateStore(Path(tmp_dir) / "pull-state.sqlite3") as store,
        ):
            store.upsert_media(
                MediaMetadata(
                    media_id="in-window",
                    filename="IMG_0001.JPG",
                    uploaded_time=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
                )
            )
            store.upsert_media(
                MediaMetadata(
                    media_id="too-old",
                    filename="IMG_0002.JPG",
                    uploaded_time=datetime(2026, 4, 17, 12, 0, tzinfo=UTC),
                )
            )
            after = datetime(2026, 4, 18, 0, 0, tzinfo=UTC)
            before = datetime(2026, 4, 19, 0, 0, tzinfo=UTC)

            self.assertFalse(store.upload_coverage_satisfies(after))
            store.advance_upload_coverage(after)
            records = store.list_media_in_upload_window(after=after, before=before)

            self.assertTrue(store.upload_coverage_satisfies(after))
            self.assertEqual([record.metadata.media_id for record in records], ["in-window"])

    def test_recent_page_checkpoint_selects_nearest_resume_cursor(self) -> None:
        with (
            TemporaryDirectory() as tmp_dir,
            PullStateStore(Path(tmp_dir) / "pull-state.sqlite3") as store,
        ):
            store.upsert_recent_page_checkpoint(
                rpc_id="recentRpc",
                cursor="cursor-march",
                oldest_upload_time=datetime(2026, 3, 1, tzinfo=UTC),
                item_count=500,
                page_count=10,
            )
            store.upsert_recent_page_checkpoint(
                rpc_id="recentRpc",
                cursor="cursor-feb",
                oldest_upload_time=datetime(2026, 2, 1, tzinfo=UTC),
                item_count=500,
                page_count=20,
            )

            checkpoint = store.best_recent_page_checkpoint(
                after=datetime(2026, 1, 15, tzinfo=UTC),
                rpc_ids=("recentRpc",),
            )
            no_matching_rpc = store.best_recent_page_checkpoint(
                after=datetime(2026, 1, 15, tzinfo=UTC),
                rpc_ids=("otherRpc",),
            )

            self.assertIsNotNone(checkpoint)
            assert checkpoint is not None
            self.assertEqual(checkpoint.cursor, "cursor-feb")
            self.assertIsNone(no_matching_rpc)

    def test_initialize_adds_index_columns_to_existing_index_database(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "pull-state.sqlite3"

            with closing(sqlite3.connect(db_path)) as connection:
                connection.executescript(
                    """
                    CREATE TABLE media_state (
                        media_id TEXT PRIMARY KEY,
                        filename TEXT NOT NULL,
                        capture_time TEXT,
                        mime_type TEXT,
                        media_type TEXT,
                        product_url TEXT,
                        width INTEGER,
                        height INTEGER,
                        bytes_size INTEGER,
                        first_seen_at TEXT NOT NULL,
                        last_seen_at TEXT NOT NULL
                    );

                    CREATE TABLE sync_checkpoints (
                        name TEXT PRIMARY KEY,
                        value TEXT,
                        updated_at TEXT NOT NULL
                    );

                    INSERT INTO media_state (
                        media_id,
                        filename,
                        capture_time,
                        mime_type,
                        media_type,
                        product_url,
                        width,
                        height,
                        bytes_size,
                        first_seen_at,
                        last_seen_at
                    ) VALUES (
                        'media-existing',
                        'IMG_existing.JPG',
                        '2026-03-01T12:00:00+00:00',
                        'image/jpeg',
                        'image',
                        NULL,
                        1024,
                        768,
                        123456,
                        '2026-03-02T00:00:00+00:00',
                        '2026-03-02T00:00:00+00:00'
                    );
                    """
                )
                connection.commit()

            with PullStateStore(db_path) as store:
                loaded = store.get_media("media-existing")
                inserted = store.upsert_media(
                    MediaMetadata(
                        media_id="media-new",
                        filename="IMG_new.JPG",
                        uploaded_time=datetime(2026, 4, 18, 12, 0, tzinfo=UTC),
                    )
                )

            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertIsNone(loaded.metadata.uploaded_time)
            self.assertIsNone(loaded.metadata.preview_url)
            self.assertEqual(inserted.metadata.media_id, "media-new")

            with closing(sqlite3.connect(db_path)) as connection:
                columns = connection.execute("PRAGMA table_info(media_state)").fetchall()

            column_names = [column[1] for column in columns]
            self.assertIn("uploaded_time", column_names)
            self.assertIn("preview_url", column_names)


if __name__ == "__main__":
    unittest.main()

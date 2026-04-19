import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from gphoto_pull.download import (
    create_staging_path,
    finalize_download,
    plan_download_target,
)
from gphoto_pull.models import MediaMetadata, MediaStateRecord


class DownloadPlanningTests(unittest.TestCase):
    def test_plan_download_target_sanitizes_filename(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            metadata = MediaMetadata(
                media_id="media-001",
                filename="albums/Trip:IMG?.JPG",
                capture_time=datetime(2026, 3, 15, 10, 30, tzinfo=UTC),
                uploaded_time=datetime(2026, 4, 18, 1, 30, tzinfo=UTC),
            )

            plan = plan_download_target(download_dir, metadata)

            self.assertEqual(plan.final_filename, "Trip_IMG_.JPG")
            self.assertEqual(
                plan.relative_path,
                Path("uploaded") / "2026" / "04" / "18" / "Trip_IMG_.JPG",
            )
            self.assertEqual(
                plan.final_path,
                download_dir / "uploaded" / "2026" / "04" / "18" / "Trip_IMG_.JPG",
            )
            self.assertEqual(plan.collision_index, 0)
            self.assertFalse(plan.used_media_id_suffix)

    def test_plan_download_target_ignores_existing_state_path(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            record = MediaStateRecord(
                metadata=MediaMetadata(
                    media_id="media-002",
                    filename="IMG_0002.JPG",
                    capture_time=datetime(2026, 3, 16, 8, 45, tzinfo=UTC),
                    uploaded_time=datetime(2026, 4, 19, 2, 0, tzinfo=UTC),
                ),
            )

            plan = plan_download_target(download_dir, record)

            self.assertEqual(plan.final_filename, "IMG_0002.JPG")
            self.assertEqual(
                plan.relative_path,
                Path("uploaded") / "2026" / "04" / "19" / "IMG_0002.JPG",
            )
            self.assertEqual(
                plan.final_path,
                download_dir / "uploaded" / "2026" / "04" / "19" / "IMG_0002.JPG",
            )

    def test_plan_download_target_uses_media_id_suffix_for_collisions(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            dated_dir = download_dir / "uploaded" / "2026" / "04" / "20"
            dated_dir.mkdir(parents=True)
            (dated_dir / "IMG_0001.JPG").write_text("first", encoding="utf-8")
            (dated_dir / "IMG_0001--media-003.JPG").write_text("second", encoding="utf-8")

            metadata = MediaMetadata(
                media_id="media-003",
                filename="IMG_0001.JPG",
                capture_time=datetime(2026, 3, 17, 9, 0, tzinfo=UTC),
                uploaded_time=datetime(2026, 4, 20, 3, 0, tzinfo=UTC),
            )

            plan = plan_download_target(download_dir, metadata)

            self.assertEqual(plan.final_filename, "IMG_0001--media-003-2.JPG")
            self.assertEqual(
                plan.relative_path,
                Path("uploaded") / "2026" / "04" / "20" / "IMG_0001--media-003-2.JPG",
            )
            self.assertEqual(plan.collision_index, 2)
            self.assertTrue(plan.used_media_id_suffix)

    def test_plan_download_target_uses_unknown_upload_directory_when_needed(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            metadata = MediaMetadata(
                media_id="media-unknown",
                filename="IMG_UNKNOWN.JPG",
                capture_time=datetime(2026, 3, 17, 9, 0, tzinfo=UTC),
            )

            plan = plan_download_target(download_dir, metadata)

            self.assertEqual(
                plan.relative_path,
                Path("uploaded") / "unknown" / "IMG_UNKNOWN.JPG",
            )


class DownloadFinalizationTests(unittest.TestCase):
    def test_finalize_download_promotes_staged_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            download_dir = Path(tmp_dir) / "downloads"
            metadata = MediaMetadata(
                media_id="media-004",
                filename="IMG_0004.JPG",
                capture_time=datetime(2026, 3, 18, 11, 15, tzinfo=UTC),
                uploaded_time=datetime(2026, 4, 21, 4, 0, tzinfo=UTC),
            )
            plan = plan_download_target(download_dir, metadata)
            staging_path = create_staging_path(plan)
            staging_path.parent.mkdir(parents=True)
            staging_path.write_bytes(b"binary-photo-data")

            final_path = finalize_download(staging_path, plan, staging_path=staging_path)

            self.assertEqual(
                final_path,
                download_dir / "uploaded" / "2026" / "04" / "21" / "IMG_0004.JPG",
            )
            self.assertEqual(final_path.read_bytes(), b"binary-photo-data")
            self.assertFalse(staging_path.exists())

    def test_finalize_download_moves_external_temp_file(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            download_dir = root / "downloads"
            source_path = root / "playwright-artifact.bin"
            source_path.write_bytes(b"external-download")

            metadata = MediaMetadata(
                media_id="media-005",
                filename="IMG_0005.JPG",
                capture_time=datetime(2026, 3, 19, 7, 5, tzinfo=UTC),
                uploaded_time=datetime(2026, 4, 22, 5, 0, tzinfo=UTC),
            )
            plan = plan_download_target(download_dir, metadata)

            final_path = finalize_download(source_path, plan)

            self.assertEqual(final_path.read_bytes(), b"external-download")
            self.assertFalse(source_path.exists())
            self.assertEqual(
                sorted(
                    path.name
                    for path in (download_dir / "uploaded" / "2026" / "04" / "22").iterdir()
                ),
                ["IMG_0005.JPG"],
            )


if __name__ == "__main__":
    unittest.main()

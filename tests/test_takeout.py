import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import msgspec.json
from gphoto_pull.detail_payloads import DetailMetadata, GeoData
from gphoto_pull.models import MediaMetadata
from gphoto_pull.takeout import TakeoutSidecar, write_takeout_sidecar


class TakeoutSidecarTests(unittest.TestCase):
    def test_write_takeout_sidecar_uses_takeout_field_shape(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            media_path = Path(tmp_dir) / "IMG_0001.JPG"
            media_path.write_bytes(b"image")
            metadata = MediaMetadata(
                media_id="media-1",
                filename="IMG_0001.JPG",
                capture_time=datetime(2026, 4, 18, 12, 34, 56, tzinfo=UTC),
                uploaded_time=datetime(2026, 4, 19, 1, 2, 3, tzinfo=UTC),
                product_url="https://photos.google.com/photo/media-1",
            )

            sidecar_path = write_takeout_sidecar(media_path, metadata)
            payload = msgspec.json.decode(sidecar_path.read_bytes(), type=TakeoutSidecar)

            self.assertEqual(sidecar_path.name, "IMG_0001.JPG.supplemental-metadata.json")
            self.assertEqual(payload.title, "IMG_0001.JPG")
            self.assertEqual(payload.description, "")
            self.assertEqual(payload.imageViews, "0")
            self.assertEqual(payload.creationTime.timestamp, "1776560523")
            self.assertEqual(payload.photoTakenTime.timestamp, "1776515696")
            self.assertIsNone(payload.geoData)
            self.assertEqual(payload.url, "https://photos.google.com/photo/media-1")

    def test_write_takeout_sidecar_matches_final_collision_filename(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            media_path = Path(tmp_dir) / "IMG_0001--media-1.JPG"
            media_path.write_bytes(b"image")
            metadata = MediaMetadata(
                media_id="media-1",
                filename=media_path.name,
                uploaded_time=datetime(2026, 4, 19, 1, 2, 3, tzinfo=UTC),
            )

            sidecar_path = write_takeout_sidecar(media_path, metadata)
            payload = msgspec.json.decode(sidecar_path.read_bytes(), type=TakeoutSidecar)

            self.assertEqual(
                sidecar_path.name,
                "IMG_0001--media-1.JPG.supplemental-metadata.json",
            )
            self.assertEqual(payload.title, "IMG_0001--media-1.JPG")

    def test_write_takeout_sidecar_includes_real_detail_metadata(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            media_path = Path(tmp_dir) / "VID_0001.mp4"
            media_path.write_bytes(b"video")
            metadata = MediaMetadata(
                media_id="media-1",
                filename=media_path.name,
                uploaded_time=datetime(2026, 4, 19, 1, 2, 3, tzinfo=UTC),
            )
            detail = DetailMetadata(
                media_id="media-1",
                description="caption",
                photo_taken_time=datetime(2026, 4, 18, 12, 34, 56, tzinfo=UTC),
                geo_data=GeoData(latitude=37.6313, longitude=-122.4491),
                people=("Gideon",),
            )

            sidecar_path = write_takeout_sidecar(media_path, metadata, detail)
            payload = msgspec.json.decode(sidecar_path.read_bytes(), type=TakeoutSidecar)

            self.assertEqual(payload.description, "caption")
            self.assertIsNotNone(payload.geoData)
            assert payload.geoData is not None
            self.assertEqual(payload.geoData.latitude, 37.6313)
            self.assertEqual(payload.geoData.longitude, -122.4491)
            self.assertEqual(tuple(person.name for person in payload.people), ("Gideon",))


if __name__ == "__main__":
    unittest.main()

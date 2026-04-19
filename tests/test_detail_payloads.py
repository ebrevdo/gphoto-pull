import json
import unittest
from datetime import UTC, datetime

from gphoto_pull.detail_payloads import parse_detail_metadata


def _detail_response() -> str:
    item = [
        "AF1QipDetail",
        "caption",
        "VID_20160707_180250.mp4",
        1467939770000,
        -25200000,
        18875368,
        1080,
        1920,
        None,
        [[376313000, -1224491000], False, None, 2],
        None,
        "opaque",
        [[1], [2]],
        [[376313000, -1224491000], False, [[None, [["San Bruno", None, 1, False, True]]]]],
        2,
        None,
        None,
        None,
        [
            [
                ["face-media", ["owner", "face-id"]],
                ["https://example.invalid/face", None, 0, 0, 0, "Gideon"],
            ]
        ],
    ]
    return (
        """)]}'\n\n258\n"""
        + json.dumps([["wrb.fr", "detailRpc", json.dumps([item]), None, None, None]])
        + "\n"
    )


class DetailPayloadTests(unittest.TestCase):
    def test_parse_detail_metadata_extracts_item_fields(self) -> None:
        detail = parse_detail_metadata([_detail_response()], expected_media_id="AF1QipDetail")

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail.title, "VID_20160707_180250.mp4")
        self.assertEqual(detail.description, "caption")
        self.assertEqual(detail.photo_taken_time, datetime(2016, 7, 8, 1, 2, 50, tzinfo=UTC))
        self.assertEqual(detail.timezone_offset_ms, -25200000)
        self.assertEqual(detail.bytes_size, 18875368)
        self.assertEqual(detail.width, 1080)
        self.assertEqual(detail.height, 1920)
        self.assertIsNotNone(detail.geo_data)
        assert detail.geo_data is not None
        self.assertAlmostEqual(detail.geo_data.latitude, 37.6313)
        self.assertAlmostEqual(detail.geo_data.longitude, -122.4491)
        self.assertEqual(detail.people, ("Gideon",))

    def test_parse_detail_metadata_rejects_wrong_media_id(self) -> None:
        self.assertIsNone(
            parse_detail_metadata([_detail_response()], expected_media_id="AF1QipOther")
        )


if __name__ == "__main__":
    unittest.main()

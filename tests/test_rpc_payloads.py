import unittest

import msgspec.json

from gphoto_pull.rpc_payloads import (
    JsonValue,
    extract_init_data_requests,
    parse_recent_payload,
    parse_recently_added_bootstrap,
    parse_updates_payload,
)

RECENT_SEARCH_TOKEN = "synthetic-recent-token"
SHARED_CONTAINER_ID = "AF1QipSharedContainer"
ACTOR_ID = "AF1QipSyntheticActor"

RECENT_HTML = f"""
<html>
  <script>'ds:1':{{id:'eNG3nf'}}</script>
  <a href="https://photos.google.com/search/{RECENT_SEARCH_TOKEN}">Canonical recent route</a>
  <a href="./search/{RECENT_SEARCH_TOKEN}" aria-label="Recently added">Recently added</a>
</html>
"""


def _recent_batchexecute_frame(items: list[list[JsonValue]]) -> str:
    payload_text = msgspec.json.encode([items]).decode()
    return msgspec.json.encode(
        [["wrb.fr", "opaqueRecentRpc", payload_text, None, None, None]]
    ).decode()


def _updates_batchexecute_frame(activities: list[list[JsonValue]]) -> str:
    payload_text = msgspec.json.encode([None, activities]).decode()
    return msgspec.json.encode(
        [["wrb.fr", "opaqueUpdatesRpc", payload_text, None, None, None]]
    ).decode()


class RpcPayloadFixtureTests(unittest.TestCase):
    def test_parse_recently_added_bootstrap_from_saved_html(self) -> None:
        bootstrap = parse_recently_added_bootstrap(RECENT_HTML)
        requests = extract_init_data_requests(RECENT_HTML)

        self.assertIsNotNone(bootstrap.canonical_search_token)
        self.assertEqual(
            bootstrap.recent_link_href,
            f"./search/{RECENT_SEARCH_TOKEN}",
        )
        self.assertIn("eNG3nf", bootstrap.bootstrap_rpc_ids)
        self.assertGreaterEqual(len(requests), 1)

    def test_parse_updates_payload_from_saved_response(self) -> None:
        raw_text = _updates_batchexecute_frame(
            [
                [f"ai:1774100000:opaque:{SHARED_CONTAINER_ID}:{ACTOR_ID}", 1774000000000],
                [f"h:1774200000:opaque:{SHARED_CONTAINER_ID}:{ACTOR_ID}", 1774100000000],
            ]
        )

        payload = parse_updates_payload(raw_text)

        self.assertEqual(len(payload.rpc_ids), 1)
        self.assertEqual(len(payload.activities), 2)
        self.assertEqual(payload.activities[0].activity_kind, "ai")
        self.assertEqual(payload.activities[1].activity_kind, "h")
        self.assertTrue(payload.activities[0].container_id.startswith("AF1Qip"))
        self.assertIsNotNone(payload.activities[0].event_timestamp_ms)
        self.assertIsNotNone(payload.activities[0].item_timestamp_ms)

    def test_parse_recent_payload_from_live_recent_response(self) -> None:
        raw_text = _recent_batchexecute_frame(
            [
                [
                    "AF1QipRecentMediaOne",
                    ["https://example.invalid/one", 4032, 3024],
                    1775858509344,
                    "opaque-one",
                    -25200000,
                    1775934657629,
                ]
            ]
        )

        payload = parse_recent_payload(raw_text)

        self.assertIn("opaqueRecentRpc", payload.rpc_ids)
        self.assertGreaterEqual(len(payload.items), 1)
        first = payload.items[0]
        self.assertTrue(first.media_id.startswith("AF1Qip"))
        self.assertIsNotNone(first.capture_timestamp_ms)
        self.assertIsNotNone(first.upload_timestamp_ms)
        assert first.capture_timestamp_ms is not None
        assert first.upload_timestamp_ms is not None
        self.assertLess(first.capture_timestamp_ms, first.upload_timestamp_ms)

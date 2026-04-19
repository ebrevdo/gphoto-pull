import json
import unittest
from pathlib import Path

from gphoto_pull.rpc_payloads import (
    JsonValue,
    extract_init_data_requests,
    parse_recent_payload,
    parse_recently_added_bootstrap,
    parse_updates_payload,
)

DIAGNOSTICS_DIR = Path(".state/diagnostics")
LEGACY_UPDATES_PATH = DIAGNOSTICS_DIR / "updates-frGlJf.txt"


def _recent_batchexecute_frame(items: list[list[JsonValue]]) -> str:
    return json.dumps([["wrb.fr", "opaqueRecentRpc", json.dumps([items]), None, None, None]])


class RpcPayloadFixtureTests(unittest.TestCase):
    def test_parse_recently_added_bootstrap_from_saved_html(self) -> None:
        html = (DIAGNOSTICS_DIR / "recent_probe" / "recent.html").read_text(encoding="utf-8")

        bootstrap = parse_recently_added_bootstrap(html)
        requests = extract_init_data_requests(html)

        self.assertIsNotNone(bootstrap.canonical_search_token)
        self.assertEqual(
            bootstrap.recent_link_href,
            "./search/Cg5SZWNlbnRseSBhZGRlZCIIEgYKBHICCgAogsisgdcz",
        )
        self.assertIn("eNG3nf", bootstrap.bootstrap_rpc_ids)
        self.assertGreaterEqual(len(requests), 1)

    def test_parse_updates_payload_from_saved_response(self) -> None:
        raw_text = LEGACY_UPDATES_PATH.read_text(encoding="utf-8")

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

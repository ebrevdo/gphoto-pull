import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import msgspec.json
from gphoto_pull.enumeration import enumerate_saved_candidates
from gphoto_pull.photos_ui import PhotosSurface, extract_photo_locations_from_html
from gphoto_pull.rpc_payloads import JsonValue
from gphoto_pull.state import PullStateStore

DIAGNOSTICS_DIR = Path(".state/diagnostics")


def _recent_batchexecute_frame(items: list[list[JsonValue]]) -> str:
    payload_text = msgspec.json.encode([items]).decode()
    return msgspec.json.encode(
        [["wrb.fr", "opaqueRecentRpc", payload_text, None, None, None]]
    ).decode()


class SavedEnumerationTests(unittest.TestCase):
    def _write_test_diagnostics(self, root: Path) -> tuple[Path, int]:
        diagnostics_dir = root / "diagnostics"
        (diagnostics_dir / "recent_probe").mkdir(parents=True)
        (diagnostics_dir / "live_recent_probe").mkdir(parents=True)
        (diagnostics_dir / "live_updates_probe").mkdir(parents=True)

        recent_html = (DIAGNOSTICS_DIR / "recent_probe" / "recent.html").read_text(encoding="utf-8")
        updates_html = (DIAGNOSTICS_DIR / "updates-page.html").read_text(encoding="utf-8")
        updates_payload = (DIAGNOSTICS_DIR / "updates-frGlJf.txt").read_text(encoding="utf-8")

        recent_locations = [
            location
            for location in extract_photo_locations_from_html(recent_html)
            if (
                location.surface is PhotosSurface.SEARCH_MEDIA_DETAIL
                and location.media_id is not None
            )
        ]
        first_media_id = recent_locations[0].media_id
        assert first_media_id is not None

        (diagnostics_dir / "recent_probe" / "recent.html").write_text(recent_html, encoding="utf-8")
        (diagnostics_dir / "live_recent_probe" / "recent.html").write_text(
            recent_html,
            encoding="utf-8",
        )
        (diagnostics_dir / "live_recent_probe" / "resp_01.txt").write_text(
            _recent_batchexecute_frame(
                [
                    [
                        first_media_id,
                        ["https://example.invalid/visible", 4032, 3024],
                        1775858509344,
                        "opaque-visible",
                        -25200000,
                        1775934657629,
                    ],
                    [
                        "AF1QipPayloadOnlyItem",
                        ["https://example.invalid/payload-only", 4000, 3000],
                        1774000000000,
                        "opaque-payload-only",
                        -25200000,
                        1774100000000,
                    ],
                ]
            ),
            encoding="utf-8",
        )
        (diagnostics_dir / "live_updates_probe" / "updates.html").write_text(
            updates_html,
            encoding="utf-8",
        )
        (diagnostics_dir / "live_updates_probe" / "resp_01.txt").write_text(
            updates_payload,
            encoding="utf-8",
        )

        visible_recent_count = len(recent_locations)
        return diagnostics_dir, visible_recent_count

    def test_enumerate_saved_candidates_persists_recent_and_updates_candidates(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            diagnostics_dir, _visible_recent_count = self._write_test_diagnostics(root)
            db_path = root / "pull-state.sqlite3"

            with PullStateStore(db_path) as state_store:
                summary = enumerate_saved_candidates(
                    diagnostics_dir=diagnostics_dir,
                    after=datetime.fromisoformat("2026-01-01T00:00:00-08:00"),
                    state_store=state_store,
                )
                persisted = state_store.list_media()

        source_counts = dict(summary.source_counts)

        self.assertGreater(len(summary.candidates), 0)
        self.assertEqual(len(summary.persisted_records), len(summary.candidates))
        self.assertEqual(len(persisted), len(summary.candidates))
        self.assertIn("recently-added", source_counts)
        self.assertIn("updates-shared", source_counts)
        self.assertGreater(source_counts["recently-added"], 0)
        self.assertGreater(source_counts["updates-shared"], 0)
        self.assertGreaterEqual(summary.exact_uploaded_time_count, 1)
        self.assertTrue(
            any(
                candidate.source == "recently-added" and candidate.uploaded_time_exact
                for candidate in summary.candidates
            )
        )
        self.assertTrue(
            any(
                candidate.source == "updates-shared" and candidate.uploaded_time_exact
                for candidate in summary.candidates
            )
        )
        self.assertGreaterEqual(summary.unknown_uploaded_time_count, 0)
        self.assertGreaterEqual(summary.cutoff_matched_count, 0)
        self.assertTrue(any(record.metadata.capture_time is not None for record in persisted))
        self.assertTrue(any(record.metadata.uploaded_time is not None for record in persisted))

    def test_enumerate_saved_candidates_uses_recent_payload_items_beyond_visible_html(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            diagnostics_dir, visible_recent_count = self._write_test_diagnostics(root)
            db_path = root / "pull-state.sqlite3"

            with PullStateStore(db_path) as state_store:
                summary = enumerate_saved_candidates(
                    diagnostics_dir=diagnostics_dir,
                    after=datetime.fromisoformat("2026-01-01T00:00:00-08:00"),
                    state_store=state_store,
                )

        source_counts = dict(summary.source_counts)

        self.assertGreater(source_counts["recently-added"], visible_recent_count)

    def test_enumerate_saved_candidates_applies_exclusive_before_bound(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            diagnostics_dir, _visible_recent_count = self._write_test_diagnostics(root)
            db_path = root / "pull-state.sqlite3"

            with PullStateStore(db_path) as state_store:
                summary = enumerate_saved_candidates(
                    diagnostics_dir=diagnostics_dir,
                    after=datetime.fromisoformat("2026-03-21T00:00:00-07:00"),
                    before=datetime.fromisoformat("2026-03-22T00:00:00-07:00"),
                    state_store=state_store,
                )

        self.assertTrue(
            any(
                candidate.metadata.media_id == "AF1QipPayloadOnlyItem"
                and candidate.cutoff_match is True
                for candidate in summary.candidates
            )
        )
        self.assertFalse(
            any(
                candidate.cutoff_match is True
                and candidate.metadata.uploaded_time is not None
                and candidate.metadata.uploaded_time
                >= datetime.fromisoformat("2026-03-22T00:00:00-07:00")
                for candidate in summary.candidates
            )
        )

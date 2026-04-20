import io
import unittest

from gphoto_pull.progress import (
    PullProgress,
    PullProgressDisplay,
    recent_row_limit,
    render_pull_progress,
)


class PullProgressTests(unittest.TestCase):
    def test_render_line_tracks_item_counts(self) -> None:
        progress = PullProgress(total_items=4)
        progress.mark_started(expected_bytes=5_000_000)
        progress.mark_success(expected_bytes=5_000_000, actual_bytes=5_000_000)

        rendered = render_pull_progress(progress, terminal_columns=120)

        self.assertIn("Count", rendered)
        self.assertIn("1/4 ok=1 fail=0", rendered)

    def test_failure_tracks_item_counts(self) -> None:
        progress = PullProgress(total_items=1)
        progress.mark_started(expected_bytes=7_000_000)
        progress.mark_failure(expected_bytes=7_000_000, started=True)

        rendered = render_pull_progress(progress, terminal_columns=120)

        self.assertIn("1/1 ok=0 fail=1", rendered)

    def test_recent_row_limit_uses_available_terminal_height(self) -> None:
        self.assertEqual(recent_row_limit(console_height=20, active_rows=4), 2)
        self.assertEqual(recent_row_limit(console_height=80, active_rows=4), 50)
        self.assertEqual(recent_row_limit(console_height=12, active_rows=8), 0)


class PullProgressDisplayTests(unittest.TestCase):
    def test_display_renders_to_stream_and_ends_with_newline_on_close(self) -> None:
        stream = io.StringIO()
        display = PullProgressDisplay(total_items=1, stream=stream, interactive=True)

        display.mark_started(expected_bytes=2_000_000)
        display.mark_success(expected_bytes=2_000_000, actual_bytes=2_000_000)
        display.close()

        rendered = stream.getvalue()
        self.assertIn("\x1b[?1049h", rendered)
        self.assertIn("\x1b[?1049l", rendered)

    def test_display_writes_item_status_when_not_interactive(self) -> None:
        stream = io.StringIO()
        display = PullProgressDisplay(total_items=1, stream=stream, interactive=False)

        display.update_item(0, "queue", "[bold]IMG_0001.JPG[/]|Apr 17 10:00|capture ?|photo|2.0 MB")
        display.close()

        self.assertEqual(
            stream.getvalue(),
            "queue: IMG_0001.JPG Apr 17 10:00 capture ? photo 2.0 MB\n",
        )

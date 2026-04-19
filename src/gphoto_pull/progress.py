"""Progress accounting and terminal rendering for pull downloads.

Description:
    Tracks item counters and renders a compact Rich live display for pull
    downloads.
"""

from __future__ import annotations

import sys
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TextIO

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.progress import BarColumn, Progress, TaskID, TextColumn
from rich.table import Table
from rich.text import Text


@dataclass(slots=True)
class PullProgress:
    """Mutable progress counters for a pull run.

    Description:
        Tracks item counts so callers can render deterministic summaries or live
        progress bars.

    Attributes:
        total_items: Number of queued items.
        processed_items: Items that finished successfully or failed.
        downloaded_items: Items finalized successfully.
        failed_items: Items that failed.
        active_items: Items with in-flight downloads.
    """

    total_items: int
    processed_items: int = 0
    downloaded_items: int = 0
    failed_items: int = 0
    active_items: int = 0

    def mark_started(self, *, expected_bytes: int | None) -> None:
        """Description:
        Record that an item started downloading.

        Args:
            expected_bytes: Expected byte size if known.

        Side Effects:
            Mutates progress counters.
        """

        del expected_bytes
        self.active_items += 1

    def mark_success(self, *, expected_bytes: int | None, actual_bytes: int) -> None:
        """Description:
        Record a successful item download.

        Args:
            expected_bytes: Expected byte size if it was known at start time.
            actual_bytes: Final file size written to disk.

        Side Effects:
            Mutates progress counters.
        """

        self.processed_items += 1
        self.downloaded_items += 1
        self.active_items -= 1
        del expected_bytes, actual_bytes

    def mark_failure(self, *, expected_bytes: int | None, started: bool) -> None:
        """Description:
        Record a failed item.

        Args:
            expected_bytes: Expected byte size if the download had started.
            started: Whether the failure happened after download start.

        Side Effects:
            Mutates progress counters.
        """

        self.processed_items += 1
        self.failed_items += 1
        del expected_bytes
        if started:
            self.active_items -= 1


@dataclass(slots=True)
class PullItemStatus:
    """Display state for one active or recently completed item.

    Description:
        Stores the text shown in the live active/recent tables.

    Attributes:
        slot: Worker slot number.
        status: Short status label.
        name: Display filename or media label.
        uploaded: Compact uploaded timestamp.
        captured: Compact captured timestamp.
        media_type: Photo/video type label.
        size: File size label.
    """

    slot: int
    status: str
    name: str
    uploaded: str
    captured: str
    media_type: str
    size: str


def render_pull_progress(progress: PullProgress, *, terminal_columns: int) -> str:
    """Description:
    Render deterministic one-line progress text.

    Args:
        progress: Current progress counters.
        terminal_columns: Reserved for future width-aware rendering.

    Returns:
        A concise count and data progress line.
    """

    del terminal_columns
    return (
        f"Count {progress.processed_items}/{progress.total_items} "
        f"ok={progress.downloaded_items} fail={progress.failed_items}"
    )


class PullProgressDisplay:
    """Rich-backed progress display for live pull runs.

    Description:
        Owns a `PullProgress` model and renders active item state to stderr only
        when the output stream is interactive.

    Attributes:
        progress: Mutable progress counter model.
        stream: Text stream used for output.
        interactive: Whether Rich live rendering is enabled.
    """

    def __init__(
        self,
        *,
        total_items: int,
        stream: TextIO | None = None,
        interactive: bool | None = None,
    ) -> None:
        """Description:
        Create a live progress display.

        Args:
            total_items: Number of queued items.
            stream: Optional output stream. Defaults to `sys.stderr`.
            interactive: Optional override for TTY detection.

        Side Effects:
            Starts a Rich live renderer when interactive.
        """

        self.progress = PullProgress(total_items=total_items)
        self.stream = sys.stderr if stream is None else stream
        self.interactive = self.stream.isatty() if interactive is None else interactive
        self._console = Console(file=self.stream, force_terminal=self.interactive)
        self._rich_progress = Progress(
            TextColumn("[bold cyan]{task.description}[/]"),
            BarColumn(),
            TextColumn("[bold]{task.fields[detail]}[/]"),
            console=self._console,
            disable=not self.interactive,
            expand=True,
        )
        self._count_task: TaskID = self._rich_progress.add_task(
            "Items",
            total=total_items,
            detail="0/0 ok=0 fail=0",
        )
        self._active: dict[int, PullItemStatus] = {}
        self._recent: deque[PullItemStatus] = deque(maxlen=20)
        self._live = Live(
            self._renderable(),
            console=self._console,
            refresh_per_second=2,
            screen=True,
            transient=True,
            vertical_overflow="crop",
        )
        if self.interactive:
            self._live.start()
        self._render()

    def mark_started(self, *, expected_bytes: int | None) -> None:
        """Description:
        Record and render a started item.

        Args:
            expected_bytes: Expected byte size if known.

        Side Effects:
            Mutates progress and refreshes terminal output.
        """

        self.progress.mark_started(expected_bytes=expected_bytes)
        self._render()

    def mark_success(self, *, expected_bytes: int | None, actual_bytes: int) -> None:
        """Description:
        Record and render a successful item.

        Args:
            expected_bytes: Expected byte size if known.
            actual_bytes: Final file size written to disk.

        Side Effects:
            Mutates progress and refreshes terminal output.
        """

        self.progress.mark_success(expected_bytes=expected_bytes, actual_bytes=actual_bytes)
        self._render()

    def mark_failure(self, *, expected_bytes: int | None, started: bool) -> None:
        """Description:
        Record and render a failed item.

        Args:
            expected_bytes: Expected byte size if known.
            started: Whether the failure happened after download start.

        Side Effects:
            Mutates progress and refreshes terminal output.
        """

        self.progress.mark_failure(expected_bytes=expected_bytes, started=started)
        self._render()

    def update_item(self, slot: int, status: str, detail: str) -> None:
        """Description:
        Update one active worker row.

        Args:
            slot: Worker slot number.
            status: Short status label.
            detail: Media metadata summary.

        Side Effects:
            Updates the live table or writes a plain log line.
        """

        if self.interactive:
            self._active[slot] = _parse_item_status(slot=slot, status=status, detail=detail)
            self._render()
        else:
            self.stream.write(f"{status}: {_plain_item_detail(detail)}\n")
            self.stream.flush()

    def complete_item(self, slot: int, status: str, detail: str) -> None:
        """Description:
        Move one worker row into recent completions.

        Args:
            slot: Worker slot number.
            status: Short completion label.
            detail: Media metadata summary.

        Side Effects:
            Updates the live table or writes a plain log line.
        """

        if self.interactive:
            self._active.pop(slot, None)
            self._recent.appendleft(_parse_item_status(slot=slot, status=status, detail=detail))
            self._render()
        else:
            self.stream.write(f"{status}: {_plain_item_detail(detail)}\n")
            self.stream.flush()

    def close(self) -> None:
        """Description:
        Stop the Rich renderer.

        Side Effects:
            Flushes/stops terminal progress output.
        """

        if self.interactive:
            self._live.stop()

    def _render(self) -> None:
        """Description:
        Refresh Rich task fields from the current progress counters.

        Side Effects:
            Writes progress updates to the configured stream when interactive.
        """

        if not self.interactive:
            return

        self._rich_progress.update(
            self._count_task,
            completed=self.progress.processed_items,
            total=self.progress.total_items,
            detail=(
                f"{self.progress.processed_items}/{self.progress.total_items} "
                f"ok={self.progress.downloaded_items} fail={self.progress.failed_items}"
            ),
        )
        self._live.update(self._renderable(), refresh=True)

    def _renderable(self) -> Group:
        """Description:
        Build the Rich renderable for the current live state.

        Returns:
            Group containing progress bars and item tables.
        """

        active_rows = max(1, len(self._active))
        recent_limit = recent_row_limit(
            console_height=self._console.size.height,
            active_rows=active_rows,
        )
        recent_rows = list(self._recent)[:recent_limit]
        sections: list[RenderableType] = []
        if recent_rows:
            sections.append(_item_table("Recent completions", recent_rows, show_slot=False))
        sections.extend(
            [
                _item_table("Active downloads", self._active.values(), show_slot=True),
                self._rich_progress,
            ]
        )
        return Group(*sections)


def _item_table(title: str, items: Iterable[PullItemStatus], *, show_slot: bool) -> Table:
    """Description:
    Build a compact Rich table for item statuses.

    Args:
        title: Table title.
        items: Rows to render.
        show_slot: Whether to include the worker slot column.

    Returns:
        Rich table.
    """

    table = Table(title=title, expand=True, title_style="bold cyan")
    if show_slot:
        table.add_column("Slot", justify="right", no_wrap=True, style="dim")
    table.add_column("Status", no_wrap=True)
    table.add_column("Name", overflow="fold", ratio=4)
    table.add_column("Uploaded", no_wrap=True, ratio=1)
    table.add_column("Captured", no_wrap=True, ratio=1)
    table.add_column("Type", no_wrap=True, ratio=1)
    table.add_column("Size", no_wrap=True, justify="right", ratio=1)
    for item in items:
        row = [
            _styled_status(item.status),
            item.name,
            item.uploaded,
            item.captured,
            item.media_type,
            item.size,
        ]
        if show_slot:
            row.insert(0, str(item.slot + 1))
        table.add_row(*row)
    return table


def recent_row_limit(*, console_height: int, active_rows: int) -> int:
    """Description:
    Estimate how many recent rows fit above active downloads and progress.

    Args:
        console_height: Current terminal height reported by Rich.
        active_rows: Number of active rows to reserve.

    Returns:
        Recent-completion row budget.
    """

    fixed_rows = 12
    return max(3, min(50, console_height - active_rows - fixed_rows))


def _parse_item_status(*, slot: int, status: str, detail: str) -> PullItemStatus:
    """Description:
    Convert the automation detail payload into a display row.

    Args:
        slot: Worker slot number.
        status: Status label.
        detail: Pipe-delimited detail payload.

    Returns:
        Structured item status.
    """

    parts = detail.split("|")
    while len(parts) < 5:
        parts.append("")
    return PullItemStatus(
        slot=slot,
        status=status,
        name=parts[0],
        uploaded=parts[1],
        captured=parts[2],
        media_type=parts[3],
        size=parts[4],
    )


def _plain_item_detail(detail: str) -> str:
    """Description:
    Render a pipe-delimited detail payload without Rich markup.

    Args:
        detail: Pipe-delimited detail payload.

    Returns:
        Plain text detail.
    """

    return " ".join(Text.from_markup(part).plain for part in detail.split("|") if part)


def _styled_status(status: str) -> Text:
    """Description:
    Style status labels for the live tables.

    Args:
        status: Status text.

    Returns:
        Rich text with a status-specific color.
    """

    styles = {
        "queue": "yellow",
        "probe": "bright_magenta",
        "detail": "magenta",
        "request": "cyan",
        "download": "bright_blue",
        "finalize": "blue",
        "done": "green",
        "failed": "red",
    }
    return Text(status, style=styles.get(status, "white"))

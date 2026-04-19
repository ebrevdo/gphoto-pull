import unittest
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from gphoto_pull.photos_ui import GooglePhotosUi, PhotosUiError

if TYPE_CHECKING:
    from playwright.sync_api import Page


@dataclass(slots=True)
class _FakeNthLocator:
    page: "_FakeMenuPage"
    index: int

    def click(self, *, timeout: int) -> None:
        self.page.clicked_indexes.append(self.index)
        self.page.click_timeouts.append(timeout)


@dataclass(slots=True)
class _FakeLocator:
    page: "_FakeMenuPage"

    @property
    def first(self) -> "_FakeLocator":
        return self

    def count(self) -> int:
        return self.page.visible_count

    def nth(self, index: int) -> _FakeNthLocator:
        return _FakeNthLocator(self.page, index)

    def wait_for(self, *, state: str, timeout: int) -> None:
        self.page.wait_states.append(state)
        self.page.wait_timeouts.append(timeout)


class _FakeKeyboard:
    def __init__(self) -> None:
        self.presses: list[str] = []

    def press(self, key: str) -> None:
        self.presses.append(key)


class _FakeMenuPage:
    def __init__(self, *, visible_count: int) -> None:
        self.visible_count = visible_count
        self.clicked_indexes: list[int] = []
        self.click_timeouts: list[int] = []
        self.wait_calls: list[int] = []
        self.wait_states: list[str] = []
        self.wait_timeouts: list[int] = []
        self.locator_calls: list[str] = []
        self.keyboard = _FakeKeyboard()

    def locator(self, selector: str) -> _FakeLocator:
        self.locator_calls.append(selector)
        return _FakeLocator(self)

    def wait_for_timeout(self, timeout_ms: int) -> None:
        self.wait_calls.append(timeout_ms)


class GooglePhotosUiTests(unittest.TestCase):
    def test_open_download_menu_tries_next_visible_candidate(self) -> None:
        ui = GooglePhotosUi()
        page = _FakeMenuPage(visible_count=2)

        with patch(
            "gphoto_pull.photos_ui.GooglePhotosUi.wait_for_download_action",
            autospec=True,
            side_effect=[PhotosUiError("not yet"), None],
        ) as wait_for_download_action:
            ui.open_download_menu(cast("Page", page))

        self.assertEqual(
            page.locator_calls,
            ['[aria-label="More options"]:visible', '[role="menu"]'],
        )
        self.assertEqual(page.clicked_indexes, [0, 1])
        self.assertEqual(page.click_timeouts, [5000, 5000])
        self.assertEqual(page.keyboard.presses, ["Escape"])
        self.assertEqual(page.wait_calls, [])
        self.assertEqual(page.wait_states, ["hidden"])
        self.assertEqual(page.wait_timeouts, [1000])
        self.assertEqual(wait_for_download_action.call_count, 2)

    def test_open_download_menu_raises_when_no_visible_more_options_exist(self) -> None:
        ui = GooglePhotosUi()
        page = _FakeMenuPage(visible_count=0)

        with self.assertRaisesRegex(PhotosUiError, "More options"):
            ui.open_download_menu(cast("Page", page))

if __name__ == "__main__":
    unittest.main()

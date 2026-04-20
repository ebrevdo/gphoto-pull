"""Google Photos URL classification, selectors, and UI actions.

Description:
    Isolates volatile Google Photos routes, selectors, and page actions from the
    higher-level pull orchestration.
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from html.parser import HTMLParser
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

if TYPE_CHECKING:
    from playwright.async_api import Page as AsyncPage
    from playwright.sync_api import Page

PHOTOS_APP_ORIGIN = "https://photos.google.com"
PHOTOS_BASE_URL = f"{PHOTOS_APP_ORIGIN}/"
RECENTLY_ADDED_URL = f"{PHOTOS_APP_ORIGIN}/search/_tra_"
UPDATES_URL = f"{PHOTOS_APP_ORIGIN}/updates"

_RECENT_GROUP_PATTERN = re.compile(r'<h2 class="ZEmz6b">(Added [^<]+)</h2>')
_RECENT_ITEM_PATTERN = re.compile(r'href="(\./search/[^"]+/photo/[^"]+)"[^>]*aria-label="([^"]+)"')
_RECENT_MEDIA_LINK_SELECTOR = (
    'a[href*="/photo/"][aria-label^="Photo - "], a[href*="/photo/"][aria-label^="Video - "]'
)
_MORE_OPTIONS_SELECTORS = (
    'button[aria-label="More options"]',
    '[role="button"][aria-label="More options"]',
    '[aria-label="More options"]',
)
_DOWNLOAD_ACTION_SELECTORS = (
    '[role="menuitem"][aria-label="Download"]',
    '[role="menuitem"]:has-text("Download")',
    'button[aria-label="Download"]',
    'button:has-text("Download")',
    '[aria-label="Download"]',
)
_MORE_OPTIONS_VISIBLE_SELECTOR = '[aria-label="More options"]:visible'


class PhotosUiError(RuntimeError):
    """Google Photos UI automation failure.

    Description:
        Raised when a live Google Photos page does not expose the expected
        controls or route state.
    """


class PhotosSurface(StrEnum):
    """Known Google Photos route families.

    Description:
        Route classification keeps library, search, album, shared, direct, and
        updates surfaces separate without relying on volatile DOM structure.
    """

    LIBRARY = "library"
    SEARCH_RESULTS = "search-results"
    SEARCH_MEDIA_DETAIL = "search-media-detail"
    MEDIA_DETAIL = "media-detail"
    ALBUMS_LIST = "albums-list"
    ALBUM_DETAIL = "album-detail"
    ALBUM_MEDIA_DETAIL = "album-media-detail"
    SHARED_ALBUM_DETAIL = "shared-album-detail"
    SHARED_ALBUM_MEDIA_DETAIL = "shared-album-media-detail"
    DIRECT_THREAD = "direct-thread"
    UPDATES = "updates"
    UNKNOWN = "unknown"


@dataclass(slots=True, frozen=True)
class PhotosLocation:
    """Normalized Google Photos location.

    Description:
        Captures route-family metadata from a Google Photos URL.

    Attributes:
        surface: Classified route family.
        absolute_url: Fully resolved URL.
        relative_path: Path component from the URL.
        search_token: Opaque search route token, when present.
        album_id: Album/share container id, when present.
        media_id: Media key, when present.
        direct_thread_id: Direct-share thread id, when present.
    """

    surface: PhotosSurface
    absolute_url: str
    relative_path: str
    search_token: str | None = None
    album_id: str | None = None
    media_id: str | None = None
    direct_thread_id: str | None = None


class _HrefCollector(HTMLParser):
    """Anchor href collector.

    Description:
        HTML parser that records anchor href values from saved page snapshots.

    Attributes:
        hrefs: Anchor href values in encounter order.
    """

    def __init__(self) -> None:
        """Description:
        Create an empty href collector.
        """

        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Description:
        Collect href values from anchor start tags.

        Args:
            tag: HTML tag name.
            attrs: Start-tag attributes.

        Side Effects:
            Appends hrefs to `hrefs`.
        """

        if tag != "a":
            return
        for key, value in attrs:
            if key == "href" and value:
                self.hrefs.append(value)


def normalize_photos_href(href: str, *, base_url: str = PHOTOS_BASE_URL) -> str:
    """Description:
    Resolve a Google Photos href into an absolute URL.

    Args:
        href: Relative or absolute href.
        base_url: URL used for relative href resolution.

    Returns:
        Absolute URL.
    """

    value = href.strip()
    if value == "":
        raise ValueError("href must not be empty.")
    return urljoin(base_url, value)


def classify_photos_url(url: str) -> PhotosLocation:
    """Description:
    Classify a Google Photos URL into a route family.

    Args:
        url: Relative or absolute Google Photos URL.

    Returns:
        A normalized `PhotosLocation`.
    """

    absolute_url = normalize_photos_href(url)
    parsed = urlparse(absolute_url)
    parts = [part for part in parsed.path.split("/") if part]

    surface = PhotosSurface.UNKNOWN
    search_token: str | None = None
    album_id: str | None = None
    media_id: str | None = None
    direct_thread_id: str | None = None

    if not parts:
        surface = PhotosSurface.LIBRARY
    elif parts == ["albums"]:
        surface = PhotosSurface.ALBUMS_LIST
    elif parts == ["updates"]:
        surface = PhotosSurface.UPDATES
    elif len(parts) == 2 and parts[0] == "search":
        surface = PhotosSurface.SEARCH_RESULTS
        search_token = parts[1]
    elif len(parts) == 4 and parts[0] == "search" and parts[2] == "photo":
        surface = PhotosSurface.SEARCH_MEDIA_DETAIL
        search_token = parts[1]
        media_id = parts[3]
    elif len(parts) == 2 and parts[0] == "photo":
        surface = PhotosSurface.MEDIA_DETAIL
        media_id = parts[1]
    elif len(parts) == 2 and parts[0] == "album":
        surface = PhotosSurface.ALBUM_DETAIL
        album_id = parts[1]
    elif len(parts) == 4 and parts[0] == "album" and parts[2] == "photo":
        surface = PhotosSurface.ALBUM_MEDIA_DETAIL
        album_id = parts[1]
        media_id = parts[3]
    elif len(parts) == 2 and parts[0] == "share":
        surface = PhotosSurface.SHARED_ALBUM_DETAIL
        album_id = parts[1]
    elif len(parts) == 4 and parts[0] == "share" and parts[2] == "photo":
        surface = PhotosSurface.SHARED_ALBUM_MEDIA_DETAIL
        album_id = parts[1]
        media_id = parts[3]
    elif len(parts) == 2 and parts[0] == "direct":
        surface = PhotosSurface.DIRECT_THREAD
        direct_thread_id = parts[1]

    return PhotosLocation(
        surface=surface,
        absolute_url=absolute_url,
        relative_path=parsed.path,
        search_token=search_token,
        album_id=album_id,
        media_id=media_id,
        direct_thread_id=direct_thread_id,
    )


def extract_hrefs_from_html(html: str) -> tuple[str, ...]:
    """Description:
    Extract anchor hrefs from saved Google Photos HTML.

    Args:
        html: HTML snapshot.

    Returns:
        Hrefs in document order.
    """

    parser = _HrefCollector()
    parser.feed(html)
    return tuple(parser.hrefs)


def extract_photo_locations_from_html(html: str) -> tuple[PhotosLocation, ...]:
    """Description:
    Extract and classify Google Photos links from saved HTML.

    Args:
        html: HTML snapshot.

    Returns:
        Deduped, known Google Photos locations in document order.
    """

    locations: list[PhotosLocation] = []
    seen: set[str] = set()

    for href in extract_hrefs_from_html(html):
        location = classify_photos_url(href)
        if location.surface is PhotosSurface.UNKNOWN:
            continue
        if location.absolute_url in seen:
            continue
        seen.add(location.absolute_url)
        locations.append(location)

    return tuple(locations)


def infer_media_kind(label: str) -> str:
    """Description:
    Infer media kind from a visible tile label.

    Args:
        label: Google Photos aria label such as `Photo - ...` or `Video - ...`.

    Returns:
        `photo`, `video`, or `unknown`.
    """

    prefix = label.partition(" - ")[0].strip().lower()
    if prefix == "photo":
        return "photo"
    if prefix == "video":
        return "video"
    return "unknown"


def _summarize_recently_added(html: str) -> list[str]:
    """Description:
    Summarize saved Recently Added HTML diagnostics.

    Args:
        html: Recently Added HTML snapshot.

    Returns:
        Operator-facing summary lines.
    """

    headings = list(_RECENT_GROUP_PATTERN.finditer(html))
    if not headings:
        return ["Saved diagnostics recently added: no saved recent capture found."]

    media_locations = tuple(
        location
        for location in extract_photo_locations_from_html(html)
        if location.media_id is not None
    )
    first_group_label = headings[0].group(1)
    first_group_start = headings[0].end()
    first_group_end = headings[1].start() if len(headings) > 1 else len(html)
    first_group_count = len(_RECENT_ITEM_PATTERN.findall(html[first_group_start:first_group_end]))

    return [
        "Saved diagnostics recently added: "
        f"{len(media_locations)} media tiles across {len(headings)} groups.",
        f"Saved diagnostics recent sample: {first_group_label} ({first_group_count} items).",
    ]


def _summarize_updates(html: str) -> list[str]:
    """Description:
    Summarize saved Updates HTML diagnostics.

    Args:
        html: Updates HTML snapshot.

    Returns:
        Operator-facing summary lines.
    """

    locations = extract_photo_locations_from_html(html)
    shared_container_count = sum(
        location.surface is PhotosSurface.SHARED_ALBUM_DETAIL for location in locations
    )
    direct_count = sum(location.surface is PhotosSurface.DIRECT_THREAD for location in locations)
    preview_count = sum(
        location.surface is PhotosSurface.SHARED_ALBUM_MEDIA_DETAIL for location in locations
    )
    activity_card_count = shared_container_count + direct_count

    return [
        "Saved updates HTML routes: "
        f"{shared_container_count + preview_count + direct_count} shared/direct links across "
        f"{len(locations)} normalized Photos links.",
        f"Saved diagnostics update previews: {preview_count} preview media ids.",
        "Saved diagnostics updates: "
        f"{activity_card_count} activity cards "
        f"({shared_container_count} shared, {direct_count} direct).",
    ]


class GooglePhotosUi:
    """Live Google Photos UI adapter.

    Description:
        Centralizes selectors and browser actions so orchestration code does not
        depend directly on volatile Google Photos markup.
    """

    def wait_for_recently_added(self, page: Page, *, timeout_ms: int = 15_000) -> None:
        """Description:
        Wait until the Recently Added page exposes media tiles.

        Args:
            page: Playwright page.
            timeout_ms: Maximum wait time in milliseconds.

        Side Effects:
            Blocks on the browser page until a selector appears or times out.
        """

        page.wait_for_selector(_RECENT_MEDIA_LINK_SELECTOR, timeout=timeout_ms)

    async def wait_for_recently_added_async(
        self,
        page: AsyncPage,
        *,
        timeout_ms: int = 15_000,
    ) -> None:
        """Description:
        Wait until the Recently Added page exposes media tiles.

        Args:
            page: Async Playwright page.
            timeout_ms: Maximum wait time in milliseconds.

        Side Effects:
            Waits on the browser page until a selector appears or times out.
        """

        await page.wait_for_selector(_RECENT_MEDIA_LINK_SELECTOR, timeout=timeout_ms)

    def wait_for_detail_actions(self, page: Page, *, timeout_ms: int = 15_000) -> None:
        """Description:
        Wait for detail-page actions needed to open the download menu.

        Args:
            page: Playwright page.
            timeout_ms: Maximum wait time in milliseconds.

        Side Effects:
            Blocks on the browser page until a selector appears or raises
            `PhotosUiError`.
        """

        _wait_for_any_visible_selector(
            page,
            selectors=_MORE_OPTIONS_SELECTORS,
            timeout_ms=timeout_ms,
            error_message="Could not find a visible 'More options' control.",
        )

    async def wait_for_detail_actions_async(
        self,
        page: AsyncPage,
        *,
        timeout_ms: int = 15_000,
    ) -> None:
        """Description:
        Wait for async detail-page actions needed to open the download menu.

        Args:
            page: Async Playwright page.
            timeout_ms: Maximum wait time in milliseconds.

        Side Effects:
            Waits on the browser page until a selector appears or raises
            `PhotosUiError`.
        """

        await _wait_for_any_visible_selector_async(
            page,
            selectors=_MORE_OPTIONS_SELECTORS,
            timeout_ms=timeout_ms,
            error_message="Could not find a visible 'More options' control.",
        )

    def wait_for_download_action(self, page: Page, *, timeout_ms: int = 10_000) -> None:
        """Description:
        Wait for a visible Download action in the open menu.

        Args:
            page: Playwright page.
            timeout_ms: Maximum wait time in milliseconds.

        Side Effects:
            Blocks on the browser page until a selector appears or raises
            `PhotosUiError`.
        """

        _wait_for_any_visible_selector(
            page,
            selectors=_DOWNLOAD_ACTION_SELECTORS,
            timeout_ms=timeout_ms,
            error_message="Could not find a visible 'Download' action.",
        )

    async def wait_for_download_action_async(
        self,
        page: AsyncPage,
        *,
        timeout_ms: int = 10_000,
    ) -> None:
        """Description:
        Wait for a visible Download action in the open menu.

        Args:
            page: Async Playwright page.
            timeout_ms: Maximum wait time in milliseconds.

        Side Effects:
            Waits on the browser page until a selector appears or raises
            `PhotosUiError`.
        """

        await _wait_for_any_visible_selector_async(
            page,
            selectors=_DOWNLOAD_ACTION_SELECTORS,
            timeout_ms=timeout_ms,
            error_message="Could not find a visible 'Download' action.",
        )

    def visible_recent_media_count(self, page: Page) -> int:
        """Description:
        Count currently rendered Recently Added media tiles.

        Args:
            page: Playwright page.

        Returns:
            Number of matching rendered media links.
        """

        count = page.locator(_RECENT_MEDIA_LINK_SELECTOR).count()
        return count

    async def visible_recent_media_count_async(self, page: AsyncPage) -> int:
        """Description:
        Count currently rendered Recently Added media tiles.

        Args:
            page: Async Playwright page.

        Returns:
            Number of matching rendered media links.
        """

        return await page.locator(_RECENT_MEDIA_LINK_SELECTOR).count()

    async def wait_for_recent_media_count_above_async(
        self,
        page: AsyncPage,
        *,
        previous_count: int,
        timeout_ms: int,
    ) -> bool:
        """Description:
        Wait until the rendered Recently Added tile count increases.

        Args:
            page: Async Playwright page.
            previous_count: Previously observed rendered tile count.
            timeout_ms: Maximum wait time in milliseconds.

        Returns:
            `True` when the count increased before timeout.

        Side Effects:
            Evaluates a browser-side selector predicate until it changes.
        """

        from playwright.async_api import TimeoutError as PlaywrightTimeoutError

        with suppress(PlaywrightTimeoutError):
            await page.wait_for_function(
                """
                ([selector, previousCount]) =>
                  document.querySelectorAll(selector).length > previousCount
                """,
                arg=[_RECENT_MEDIA_LINK_SELECTOR, previous_count],
                timeout=timeout_ms,
            )
            return True
        return False

    def scroll_recently_added_container(self, page: Page) -> bool:
        """Description:
        Scroll the virtualized Recently Added container.

        Args:
            page: Playwright page.

        Returns:
            `True` when the container scroll position advanced.

        Side Effects:
            Executes JavaScript in the page and scrolls the largest lazy container.
        """

        scrolled = bool(
            page.evaluate(
                """
                () => {
                  const candidates = [...document.querySelectorAll('c-wiz, div, main')].filter(
                    (element) => {
                      if (!(element instanceof HTMLElement)) {
                        return false;
                      }
                      const style = window.getComputedStyle(element);
                      return (
                        (style.overflowY === 'auto' || style.overflowY === 'scroll') &&
                        element.scrollHeight > element.clientHeight + 200
                      );
                    }
                  );
                  candidates.sort(
                    (left, right) =>
                      right.scrollHeight * right.clientHeight -
                      left.scrollHeight * left.clientHeight
                  );
                  const target = candidates[0];
                  if (!(target instanceof HTMLElement)) {
                    return false;
                  }
                  const before = target.scrollTop;
                  target.scrollBy(0, Math.max(900, target.clientHeight - 120));
                  return target.scrollTop > before;
                }
                """
            )
        )
        return scrolled

    async def scroll_recently_added_container_async(self, page: AsyncPage) -> bool:
        """Description:
        Scroll the virtualized Recently Added container.

        Args:
            page: Async Playwright page.

        Returns:
            `True` when the container scroll position advanced.

        Side Effects:
            Executes JavaScript in the page and scrolls the largest lazy container.
        """

        return bool(
            await page.evaluate(
                """
                () => {
                  const candidates = [...document.querySelectorAll('c-wiz, div, main')].filter(
                    (element) => {
                      if (!(element instanceof HTMLElement)) {
                        return false;
                      }
                      const style = window.getComputedStyle(element);
                      return (
                        (style.overflowY === 'auto' || style.overflowY === 'scroll') &&
                        element.scrollHeight > element.clientHeight + 200
                      );
                    }
                  );
                  candidates.sort(
                    (left, right) =>
                      right.scrollHeight * right.clientHeight -
                      left.scrollHeight * left.clientHeight
                  );
                  const target = candidates[0];
                  if (!(target instanceof HTMLElement)) {
                    return false;
                  }
                  const before = target.scrollTop;
                  target.scrollBy(0, Math.max(900, target.clientHeight - 120));
                  return target.scrollTop > before;
                }
                """
            )
        )

    def open_download_menu(self, page: Page) -> None:
        """Description:
        Open the detail-page overflow menu that contains Download.

        Args:
            page: Playwright page on a media detail route.

        Side Effects:
            Clicks visible overflow controls and may press Escape between retries.
        """

        locator = page.locator(_MORE_OPTIONS_VISIBLE_SELECTOR)
        try:
            visible_count = locator.count()
        except Exception as exc:
            raise PhotosUiError(
                f"Could not find a visible 'More options' control. Last count error: {exc}"
            ) from exc

        if visible_count == 0:
            raise PhotosUiError("Could not find a visible 'More options' control.")

        last_error: Exception | None = None
        for index in range(visible_count):
            try:
                locator.nth(index).click(timeout=10_000)
                self.wait_for_download_action(page, timeout_ms=1_000)
                return
            except Exception as exc:
                last_error = exc
                with suppress(Exception):
                    page.keyboard.press("Escape")
                if index + 1 < visible_count:
                    _wait_for_menu_closed(page)

        if last_error is not None:
            raise PhotosUiError(
                f"Could not find a visible 'Download' action. Last menu error: {last_error}"
            ) from last_error
        raise PhotosUiError("Could not find a visible 'Download' action.")

    async def open_download_menu_async(self, page: AsyncPage) -> None:
        """Description:
        Open the detail-page overflow menu that contains Download.

        Args:
            page: Async Playwright page on a media detail route.

        Side Effects:
            Clicks visible overflow controls and may press Escape between retries.
        """

        locator = page.locator(_MORE_OPTIONS_VISIBLE_SELECTOR)
        try:
            visible_count = await locator.count()
        except Exception as exc:
            raise PhotosUiError(
                f"Could not find a visible 'More options' control. Last count error: {exc}"
            ) from exc

        if visible_count == 0:
            raise PhotosUiError("Could not find a visible 'More options' control.")

        last_error: Exception | None = None
        for index in range(visible_count):
            try:
                await locator.nth(index).click(timeout=10_000)
                await self.wait_for_download_action_async(page, timeout_ms=1_000)
                return
            except Exception as exc:
                last_error = exc
                with suppress(Exception):
                    await page.keyboard.press("Escape")
                if index + 1 < visible_count:
                    await _wait_for_menu_closed_async(page)

        if last_error is not None:
            raise PhotosUiError(
                f"Could not find a visible 'Download' action. Last menu error: {last_error}"
            ) from last_error
        raise PhotosUiError("Could not find a visible 'Download' action.")

    def click_download_action(self, page: Page) -> None:
        """Description:
        Click the visible Download menu item.

        Args:
            page: Playwright page with the download menu open.

        Side Effects:
            Clicks the page and may trigger a browser download.
        """

        self.wait_for_download_action(page, timeout_ms=1_500)
        _click_first_visible_selector(
            page,
            selectors=_DOWNLOAD_ACTION_SELECTORS,
            error_message="Could not find a visible 'Download' action.",
        )

    async def click_download_action_async(self, page: AsyncPage) -> None:
        """Description:
        Click the visible Download menu item.

        Args:
            page: Async Playwright page with the download menu open.

        Side Effects:
            Clicks the page and may trigger a browser download.
        """

        await self.wait_for_download_action_async(page, timeout_ms=1_500)
        await _click_first_visible_selector_async(
            page,
            selectors=_DOWNLOAD_ACTION_SELECTORS,
            error_message="Could not find a visible 'Download' action.",
        )

    def dry_run_notes(self, *, diagnostics_dir: Path) -> list[str]:
        """Description:
        Summarize saved Google Photos diagnostics for `pull --dry-run`.

        Args:
            diagnostics_dir: Directory containing saved HTML/RPC artifacts.

        Returns:
            Operator-facing summary lines.

        Side Effects:
            Reads saved diagnostics from disk.
        """

        lines = [
            "Photos UI scaffold is active; saved-diagnostics helpers are wired.",
            "Known surface: Recently added -> search /search/<token>",
            "Known surface: Photo detail -> media /photo/<media_id>",
            "Known surface: Albums -> album /album/<album_id>",
            "Known surface: Shared albums -> share /share/<album_id>?key=<opaque-key>",
            "Known surface: Direct threads -> direct /direct/<thread_id>",
            "Known surface: Updates -> updates /updates",
        ]

        recent_path = _preferred_recent_html_path(diagnostics_dir)
        if recent_path.exists():
            recent_html = recent_path.read_text(encoding="utf-8")
            lines.extend(_summarize_recently_added(recent_html))
        else:
            lines.append("Saved diagnostics recently added: no saved recent capture found.")

        updates_path = _preferred_updates_html_path(diagnostics_dir)
        if updates_path.exists():
            lines.extend(_summarize_updates(updates_path.read_text(encoding="utf-8")))
        else:
            lines.append("Saved updates HTML routes: no saved updates capture found.")
            lines.append("Saved diagnostics updates: no saved updates capture found.")
            lines.append("Saved diagnostics update previews: 0 preview media ids.")

        return lines


def _wait_for_any_visible_selector(
    page: Page,
    *,
    selectors: tuple[str, ...],
    timeout_ms: int,
    error_message: str,
) -> None:
    """Description:
    Wait until any selector in a set has a visible match.

    Args:
        page: Playwright page.
        selectors: Selectors to test.
        timeout_ms: Per-selector timeout in milliseconds.
        error_message: Message for `PhotosUiError` when none match.

    Side Effects:
        Blocks on browser selector waits.
    """

    try:
        page.locator(_combined_visible_selector(selectors)).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
    except Exception as exc:
        raise PhotosUiError(f"{error_message} Last wait error: {exc}") from exc


def _wait_for_menu_closed(page: Page) -> None:
    """Description:
    Wait for an open menu overlay to close after Escape.

    Args:
        page: Playwright page.

    Side Effects:
        Waits on the menu selector with a bounded timeout.
    """

    with suppress(Exception):
        page.locator('[role="menu"]').first.wait_for(state="hidden", timeout=1_000)


async def _wait_for_any_visible_selector_async(
    page: AsyncPage,
    *,
    selectors: tuple[str, ...],
    timeout_ms: int,
    error_message: str,
) -> None:
    """Description:
    Wait until any selector in a set has a visible match on an async page.

    Args:
        page: Async Playwright page.
        selectors: Selectors to test.
        timeout_ms: Per-selector timeout in milliseconds.
        error_message: Message for `PhotosUiError` when none match.

    Side Effects:
        Waits on browser selector checks.
    """

    try:
        await page.locator(_combined_visible_selector(selectors)).first.wait_for(
            state="visible",
            timeout=timeout_ms,
        )
    except Exception as exc:
        raise PhotosUiError(f"{error_message} Last wait error: {exc}") from exc


async def _wait_for_menu_closed_async(page: AsyncPage) -> None:
    """Description:
    Wait for an open menu overlay to close after Escape.

    Args:
        page: Async Playwright page.

    Side Effects:
        Waits on the menu selector with a bounded timeout.
    """

    with suppress(Exception):
        await page.locator('[role="menu"]').first.wait_for(state="hidden", timeout=1_000)


def _click_first_visible_selector(
    page: Page,
    *,
    selectors: tuple[str, ...],
    error_message: str,
    timeout_ms: int = 10_000,
) -> None:
    """Description:
    Click the first visible selector from a candidate set.

    Args:
        page: Playwright page.
        selectors: Selectors to test.
        error_message: Message for `PhotosUiError` when none click.
        timeout_ms: Click timeout in milliseconds.

    Side Effects:
        Clicks a browser element.
    """

    try:
        page.locator(_combined_visible_selector(selectors)).first.click(timeout=timeout_ms)
    except Exception as exc:
        raise PhotosUiError(f"{error_message} Last click error: {exc}") from exc


async def _click_first_visible_selector_async(
    page: AsyncPage,
    *,
    selectors: tuple[str, ...],
    error_message: str,
    timeout_ms: int = 10_000,
) -> None:
    """Description:
    Click the first visible selector from a candidate set on an async page.

    Args:
        page: Async Playwright page.
        selectors: Selectors to test.
        error_message: Message for `PhotosUiError` when none click.
        timeout_ms: Click timeout in milliseconds.

    Side Effects:
        Clicks a browser element.
    """

    try:
        await page.locator(_combined_visible_selector(selectors)).first.click(timeout=timeout_ms)
    except Exception as exc:
        raise PhotosUiError(f"{error_message} Last click error: {exc}") from exc


def _combined_visible_selector(selectors: tuple[str, ...]) -> str:
    """Description:
    Combine alternative selectors so Playwright waits for any match once.

    Args:
        selectors: Candidate selectors.

    Returns:
        Comma-separated visible selector.
    """

    return ", ".join(f"{selector}:visible" for selector in selectors)


def _preferred_recent_html_path(diagnostics_dir: Path) -> Path:
    """Description:
    Choose the preferred Recently Added HTML artifact path.

    Args:
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Live probe path when present, otherwise older probe path.
    """

    live_path = diagnostics_dir / "live_recent_probe" / "recent.html"
    if live_path.exists():
        return live_path
    return diagnostics_dir / "recent_probe" / "recent.html"


def _preferred_updates_html_path(diagnostics_dir: Path) -> Path:
    """Description:
    Choose the preferred Updates HTML artifact path.

    Args:
        diagnostics_dir: Diagnostics directory root.

    Returns:
        Live probe path when present, otherwise older snapshot path.
    """

    live_path = diagnostics_dir / "live_updates_probe" / "updates.html"
    if live_path.exists():
        return live_path
    return diagnostics_dir / "updates-page.html"

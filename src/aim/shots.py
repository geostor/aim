"""The team's camera: photographs a mission's page whenever it's saved, so agents can see it.

Agents read code, which can't tell them whether a page looks awful. This renders the
page in a real browser at desktop and phone sizes, writes the pictures and a report
of problems into the mission's folder, and says so in the room like any other member.

The page is served through the viewer, so its security policy applies, and the browser
is refused anything that isn't this local server.
"""

from __future__ import annotations

import asyncio
import signal
import sys
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path

from fastmcp import Client
from playwright.async_api import async_playwright

from .view import make_handler, version

NAME = "camera"
SIZES = {"desktop": (1280, 800, False), "phone": (390, 844, True)}
MAX_PICTURE_HEIGHT = 4000
CHECK_SECONDS = 1.0
SETTLE_SECONDS = 1.0
LATER_SECONDS = 2.0
GIVE_UP_SECONDS = 2 * 3600  # stop if the page hasn't changed in this long


async def photograph(browser, url: str, folder: Path) -> list[str]:
    """Take every picture, and return the problems noticed along the way."""
    problems: list[str] = []
    for label, (width, height, phone) in SIZES.items():
        context = await browser.new_context(
            viewport={"width": width, "height": height},
            is_mobile=phone, has_touch=phone, service_workers="block",
        )
        page = await context.new_page()

        async def allow_only_local(route):
            if route.request.url.startswith(url):
                await route.continue_()
            else:
                problems.append(f"{label}: blocked request to {route.request.url[:100]}")
                await route.abort()

        await page.route("**/*", allow_only_local)
        page.on("pageerror", lambda e, l=label: problems.append(f"{l}: script error: {e}"))
        def note_console(message, l=label):
            # Browsers ask for a site icon by themselves, which isn't the page's doing.
            if message.type == "error" and "favicon" not in f"{message.text} {message.location}":
                problems.append(f"{l}: console error: {message.text}")

        page.on("console", note_console)
        await page.goto(url, wait_until="load", timeout=20000)
        await asyncio.sleep(SETTLE_SECONDS)
        await page.screenshot(path=folder / f"{label}.png")
        await asyncio.sleep(LATER_SECONDS)  # a second look, to show anything that moves
        await page.screenshot(path=folder / f"{label}-later.png")
        scroll_width, scroll_height = await page.evaluate(
            "[document.documentElement.scrollWidth, document.documentElement.scrollHeight]"
        )
        if scroll_width > width:
            problems.append(f"{label}: page is {scroll_width}px wide in a {width}px window, so it scrolls sideways")
        if scroll_height > height:
            await page.screenshot(
                path=folder / f"{label}-full.png", full_page=True,
                clip={"x": 0, "y": 0, "width": width, "height": min(scroll_height, MAX_PICTURE_HEIGHT)},
            )
        else:
            (folder / f"{label}-full.png").unlink(missing_ok=True)
        await context.close()
    return problems


class Member:
    """The camera's presence in the room, signing on again if it ever gets dropped."""

    def __init__(self, url: str, room: str):
        self.url, self.room = url, room
        self.token: str | None = None

    async def say(self, text: str) -> None:
        async with Client(self.url) as client:
            for attempt in (1, 2):
                try:
                    if self.token is None:
                        await self.sign_on(client)
                    await client.call_tool(
                        "post_message", {"token": self.token, "room": self.room, "text": text}
                    )
                    return
                except Exception:
                    self.token = None
                    if attempt == 2:
                        raise

    async def sign_on(self, client: Client) -> None:
        result = await client.call_tool(
            "sign_on", {"screen_name": NAME, "profile": "Takes screenshots of the team's page."}
        )
        self.token = result.data["token"]
        await client.call_tool("join_room", {"token": self.token, "room": self.room})


async def watch(page: Path, folder: Path, aim_url: str, room: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(page))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    member = Member(aim_url, room)
    seen = "none"
    last_change = time.monotonic()
    # Stop through the normal path, so the browser is closed rather than orphaned.
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        loop.add_signal_handler(number, stopping.set)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch()
        while not stopping.is_set() and time.monotonic() - last_change < GIVE_UP_SECONDS:
            current = version(page)
            if current == seen or current == "none":
                await asyncio.sleep(CHECK_SECONDS)
                continue
            await asyncio.sleep(CHECK_SECONDS)  # let a save finish
            if version(page) != current:
                continue
            seen, last_change = current, time.monotonic()
            try:
                problems = await photograph(browser, url, folder)
                (folder / "report.txt").write_text("\n".join(problems) + "\n" if problems else "No problems noticed.\n")
                summary = f" {len(problems)} problem(s) in report.txt." if problems else ""
                message = (
                    "New screenshots of index.html in the screenshots folder: desktop.png and "
                    "phone.png, -later versions taken 2 seconds on to show motion, and -full "
                    f"versions if the page scrolls.{summary}"
                )
            except Exception as error:
                message = f"Couldn't photograph the page: {str(error)[:300]}"
            try:
                await member.say(message)
            except Exception as error:
                print(f"couldn't post to the room: {error}", file=sys.stderr)
        await browser.close()
    server.shutdown()


def main() -> None:
    page, aim_url, room = sys.argv[1:4]
    asyncio.run(watch(Path(page), Path(page).parent / "screenshots", aim_url, room))


if __name__ == "__main__":
    main()

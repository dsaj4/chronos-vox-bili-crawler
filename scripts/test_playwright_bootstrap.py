from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
MEDIA_CRAWLER_ROOT = ROOT_DIR / "MediaCrawler"

if str(MEDIA_CRAWLER_ROOT) not in sys.path:
    sys.path.insert(0, str(MEDIA_CRAWLER_ROOT))

import config  # noqa: E402
from playwright.async_api import async_playwright  # noqa: E402
from tools.cdp_browser import CDPBrowserManager  # noqa: E402


def configure_console_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal Playwright bootstrap test for chronos-vox-bili-crawler.")
    parser.add_argument("--cdp", action="store_true", help="Use the project's CDP browser path instead of standard Chromium.")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode.")
    parser.add_argument("--url", default="https://www.bilibili.com", help="URL to open after browser startup.")
    parser.add_argument("--timeout-ms", type=int, default=30000, help="Page navigation timeout in milliseconds.")
    return parser.parse_args()


def print_event(event: str, **payload: Any) -> None:
    message = {"event": event, **payload}
    print(json.dumps(message, ensure_ascii=False), flush=True)


async def run_standard_mode(args: argparse.Namespace) -> int:
    print_event("bootstrap_mode", mode="standard", headless=args.headless, url=args.url)
    async with async_playwright() as playwright:
        print_event("playwright_started")
        browser = await playwright.chromium.launch(headless=args.headless)
        print_event("browser_started", browser_type="chromium")
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(args.timeout_ms)
        response = await page.goto(args.url, wait_until="domcontentloaded")
        print_event(
            "navigation_ok",
            final_url=page.url,
            status=response.status if response else None,
            title=await page.title(),
        )
        await context.close()
        await browser.close()
    print_event("completed", mode="standard")
    return 0


async def run_cdp_mode(args: argparse.Namespace) -> int:
    print_event("bootstrap_mode", mode="cdp", headless=args.headless, url=args.url)
    manager = CDPBrowserManager()
    async with async_playwright() as playwright:
        print_event("playwright_started")
        context = await manager.launch_and_connect(
            playwright=playwright,
            playwright_proxy=None,
            user_agent=None,
            headless=args.headless,
        )
        print_event("browser_started", browser_type="cdp")
        page = await context.new_page()
        page.set_default_timeout(args.timeout_ms)
        response = await page.goto(args.url, wait_until="domcontentloaded")
        print_event(
            "navigation_ok",
            final_url=page.url,
            status=response.status if response else None,
            title=await page.title(),
        )
        await manager.cleanup(force=True)
    print_event("completed", mode="cdp")
    return 0


async def main_async() -> int:
    configure_console_output()
    os.chdir(MEDIA_CRAWLER_ROOT)
    args = parse_args()
    config.SAVE_LOGIN_STATE = False

    try:
        if args.cdp:
            return await run_cdp_mode(args)
        return await run_standard_mode(args)
    except Exception as exc:
        print_event("failed", error_type=exc.__class__.__name__, error=str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))

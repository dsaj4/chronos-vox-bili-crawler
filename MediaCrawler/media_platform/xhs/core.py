# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/media_platform/xhs/core.py
# GitHub: https://github.com/NanmiCoder
# Licensed under NON-COMMERCIAL LEARNING LICENSE 1.1
#

# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。

import asyncio
import csv
import json
import os
import random
from asyncio import Task
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set
from urllib.parse import quote

from playwright.async_api import (
    BrowserContext,
    BrowserType,
    Page,
    Playwright,
    async_playwright,
)
from tenacity import RetryError
from sqlalchemy import select

import config
from base.base_crawler import AbstractCrawler
from database.db_session import get_session
from database.models import XhsNote as XhsNoteRecord
from model.m_xiaohongshu import NoteUrlInfo, CreatorUrlInfo
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import xhs as xhs_store
from tools import utils
from tools.browser_launcher import BrowserLauncher
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import XiaoHongShuClient
from .exception import DataFetchError, NoteNotFoundError
from .field import SearchSortType
from .help import parse_note_info_from_note_url, parse_creator_info_from_url, get_search_id
from .login import XiaoHongShuLogin


class XiaoHongShuCrawler(AbstractCrawler):
    context_page: Optional[Page]
    xhs_client: Optional[XiaoHongShuClient]
    browser_context: Optional[BrowserContext]
    cdp_manager: Optional[CDPBrowserManager]

    def __init__(self) -> None:
        self.index_url = "https://www.xiaohongshu.com"
        # self.user_agent = utils.get_user_agent()
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        self.cdp_manager = None
        self.ip_proxy_pool = None  # Proxy IP pool for automatic proxy refresh
        self.context_page = None
        self.xhs_client = None
        self.browser_context = None
        self._playwright_manager = None
        self._playwright = None
        self.existing_note_ids: Set[str] = set()

    @staticmethod
    def is_playwright_startup_access_error(exc: Exception) -> bool:
        if not isinstance(exc, PermissionError):
            return False
        error_text = str(exc).lower()
        return "winerror 5" in error_text or "access is denied" in error_text

    @staticmethod
    def format_exception_details(exc: Exception) -> str:
        message = str(exc).strip()
        return f"{exc.__class__.__name__}: {message}" if message else exc.__class__.__name__

    @staticmethod
    def normalize_note_id(note_id: object) -> str:
        return str(note_id or "").strip()

    def should_skip_note_id(self, note_id: object) -> bool:
        normalized_note_id = self.normalize_note_id(note_id)
        return bool(normalized_note_id) and normalized_note_id in self.existing_note_ids

    def remember_note_id(self, note_id: object) -> None:
        normalized_note_id = self.normalize_note_id(note_id)
        if normalized_note_id:
            self.existing_note_ids.add(normalized_note_id)

    def get_data_base_path(self, file_type: str) -> Path:
        if config.SAVE_DATA_PATH:
            return Path(config.SAVE_DATA_PATH) / "xhs" / file_type
        return Path("data") / "xhs" / file_type

    async def load_existing_note_ids(self) -> Set[str]:
        try:
            if config.SAVE_DATA_OPTION in {"db", "sqlite", "postgres"}:
                note_ids = await self.load_existing_note_ids_from_db()
            elif config.SAVE_DATA_OPTION == "mongodb":
                note_ids = await self.load_existing_note_ids_from_mongodb()
            elif config.SAVE_DATA_OPTION in {"json", "jsonl", "csv", "excel"}:
                note_ids = self.load_existing_note_ids_from_files(config.SAVE_DATA_OPTION)
            else:
                note_ids = set()
            utils.logger.info(
                f"[XiaoHongShuCrawler.load_existing_note_ids] Loaded {len(note_ids)} existing note ids for resume mode"
            )
            return note_ids
        except Exception as exc:
            utils.logger.warning(
                f"[XiaoHongShuCrawler.load_existing_note_ids] Failed to load existing note ids: {exc}"
            )
            return set()

    async def load_existing_note_ids_from_db(self) -> Set[str]:
        async with get_session() as session:
            if session is None:
                return set()
            result = await session.execute(select(XhsNoteRecord.note_id))
            return {
                self.normalize_note_id(note_id)
                for (note_id,) in result.all()
                if self.normalize_note_id(note_id)
            }

    async def load_existing_note_ids_from_mongodb(self) -> Set[str]:
        from database.mongodb_store_base import MongoDBStoreBase

        mongo_store = MongoDBStoreBase(collection_prefix="xhs")
        rows = await mongo_store.find_many("contents", {})
        return {
            normalized
            for row in rows
            if isinstance(row, dict) and (normalized := self.normalize_note_id(row.get("note_id")))
        }

    def load_existing_note_ids_from_files(self, file_type: str) -> Set[str]:
        base_path = self.get_data_base_path(file_type)
        if not base_path.exists():
            return set()

        patterns = {
            "json": ["*_contents_*.json"],
            "jsonl": ["*_contents_*.jsonl"],
            "csv": ["*_contents_*.csv"],
            "excel": ["*_contents_*.xlsx"],
        }
        note_ids: Set[str] = set()
        for pattern in patterns.get(file_type, []):
            for file_path in base_path.glob(pattern):
                note_ids.update(self.extract_note_ids_from_file(file_path, file_type))
        return note_ids

    def extract_note_ids_from_file(self, file_path: Path, file_type: str) -> Set[str]:
        note_ids: Set[str] = set()
        try:
            if file_type == "json":
                with file_path.open("r", encoding="utf-8") as file_obj:
                    data = json.load(file_obj)
                if isinstance(data, dict):
                    data = [data]
                for item in data:
                    if isinstance(item, dict):
                        normalized = self.normalize_note_id(item.get("note_id"))
                        if normalized:
                            note_ids.add(normalized)
            elif file_type == "jsonl":
                with file_path.open("r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        line = line.strip()
                        if not line:
                            continue
                        item = json.loads(line)
                        if isinstance(item, dict):
                            normalized = self.normalize_note_id(item.get("note_id"))
                            if normalized:
                                note_ids.add(normalized)
            elif file_type == "csv":
                with file_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        normalized = self.normalize_note_id((row or {}).get("note_id"))
                        if normalized:
                            note_ids.add(normalized)
            elif file_type == "excel":
                import pandas as pd

                dataframe = pd.read_excel(file_path)
                if "note_id" in dataframe.columns:
                    for note_id in dataframe["note_id"].tolist():
                        normalized = self.normalize_note_id(str(note_id) if pd.notna(note_id) else "")
                        if normalized:
                            note_ids.add(normalized)
        except Exception as exc:
            utils.logger.warning(
                f"[XiaoHongShuCrawler.extract_note_ids_from_file] Failed to read existing note ids from {file_path}: {exc}"
            )
        return note_ids

    async def process_note_item(self, note_item: Dict) -> Optional[Dict]:
        note_id = note_item.get("note_id")
        if self.should_skip_note_id(note_id):
            utils.logger.info(
                f"[XiaoHongShuCrawler.process_note_item] Skip duplicated note id: {note_id}"
            )
            return None
        self.remember_note_id(note_id)
        await xhs_store.update_xhs_note(note_item)
        return note_item

    def build_search_result_url(self, keyword: str) -> str:
        return f"{self.index_url}/search_result?keyword={quote(keyword)}&source=web_explore_feed"

    async def page_requires_login(self) -> bool:
        if not self.context_page:
            return True

        body_text = ""
        try:
            body_text = await self.context_page.locator("body").inner_text()
        except Exception:
            pass

        login_markers = (
            "登录后推荐更懂你的笔记",
            "马上登录即可",
            "输入手机号",
            "获取验证码",
            "小红书或微信扫码",
        )
        if any(marker in body_text for marker in login_markers):
            return True

        try:
            placeholder = await self.context_page.locator("input.search-input").first.get_attribute("placeholder")
            if placeholder and "登录" in placeholder:
                return True
        except Exception:
            pass

        try:
            if await self.context_page.locator("img.qrcode-img").count() > 0:
                return True
        except Exception:
            pass

        return False

    async def warm_up_search_context(self, keyword: str = "python") -> None:
        if not self.context_page or not self.xhs_client:
            return

        search_url = self.build_search_result_url(keyword)
        utils.logger.info(
            f"[XiaoHongShuCrawler.warm_up_search_context] Navigating to search page to warm up search context: {search_url}"
        )
        await self.context_page.goto(search_url, wait_until="commit", timeout=15000)
        await asyncio.sleep(5)
        await self.xhs_client.update_cookies(browser_context=self.browser_context)
        self.xhs_client.headers["referer"] = self.context_page.url

    async def prepare_session(self) -> None:
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            self.ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await self.ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = utils.format_proxy_info(ip_proxy_info)

        self._playwright_manager = async_playwright()
        playwright = await self._playwright_manager.__aenter__()
        self._playwright = playwright

        if config.ENABLE_CDP_MODE:
            utils.logger.info("[XiaoHongShuCrawler] Launching browser using CDP mode")
            self.browser_context = await self.launch_browser_with_cdp(
                playwright,
                playwright_proxy_format,
                self.user_agent,
                headless=config.CDP_HEADLESS,
            )
        else:
            utils.logger.info("[XiaoHongShuCrawler] Launching browser using standard mode")
            self.browser_context = await self.launch_browser(
                playwright.chromium,
                playwright_proxy_format,
                self.user_agent,
                headless=config.HEADLESS,
            )
            await self.browser_context.add_init_script(path="libs/stealth.min.js")

        self.context_page = await self.browser_context.new_page()
        await self.context_page.goto(self.index_url, wait_until="domcontentloaded")
        await asyncio.sleep(2)

        self.xhs_client = await self.create_xhs_client(httpx_proxy_format)
        self.existing_note_ids = await self.load_existing_note_ids()
        if await self.page_requires_login():
            login_obj = XiaoHongShuLogin(
                login_type=config.LOGIN_TYPE,
                login_phone="",  # input your phone number
                browser_context=self.browser_context,
                context_page=self.context_page,
                cookie_str=config.COOKIES,
            )
            await login_obj.begin()
            await self.context_page.goto(self.index_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            await self.xhs_client.update_cookies(browser_context=self.browser_context)

            if await self.page_requires_login():
                raise RuntimeError(
                    "[XiaoHongShuCrawler.prepare_session] Xiaohongshu page is still in logged-out state after login flow."
                )

        await self.warm_up_search_context((config.KEYWORDS.split(",")[0] or "python").strip())
        if await self.page_requires_login():
            raise RuntimeError(
                "[XiaoHongShuCrawler.prepare_session] Xiaohongshu page is still asking for login after search warm-up."
            )

    async def prepare_session_with_retries(self, max_attempts: int = 3, retry_delay_sec: float | None = None) -> None:
        retry_delay = retry_delay_sec if retry_delay_sec is not None else max(config.CRAWLER_MAX_SLEEP_SEC, 2.0)
        for attempt in range(1, max_attempts + 1):
            try:
                await self.prepare_session()
                return
            except Exception as exc:
                try:
                    await self.close_session()
                except Exception:
                    pass

                if not self.is_playwright_startup_access_error(exc) or attempt == max_attempts:
                    raise

                utils.logger.warning(
                    f"[XiaoHongShuCrawler.prepare_session_with_retries] Playwright startup failed with "
                    f"{self.format_exception_details(exc)}; retrying {attempt}/{max_attempts} after {retry_delay:.1f}s"
                )
                await asyncio.sleep(retry_delay)

    async def run_current_config(self) -> None:
        crawler_type_var.set(config.CRAWLER_TYPE)
        if config.CRAWLER_TYPE == "search":
            await self.search()
        elif config.CRAWLER_TYPE == "detail":
            await self.get_specified_notes()
        elif config.CRAWLER_TYPE == "creator":
            await self.get_creators_and_notes()

        utils.logger.info("[XiaoHongShuCrawler.run_current_config] Xhs crawler finished ...")

    async def start(self) -> None:
        await self.prepare_session_with_retries()
        try:
            await self.run_current_config()
        finally:
            await self.close_session()

    def get_time_range_resume_state(self, keyword: str) -> Optional[Dict]:
        return None

    def on_time_range_page_completed(
        self,
        *,
        keyword: str,
        day: str,
        next_page: int,
        notes_count_this_day: int,
        total_notes_crawled_for_keyword: int,
    ) -> None:
        return None

    def on_time_range_day_completed(
        self,
        *,
        keyword: str,
        day: str,
        next_day: Optional[str],
        total_notes_crawled_for_keyword: int,
        reason: str,
    ) -> None:
        return None

    def on_time_range_keyword_completed(
        self,
        *,
        keyword: str,
        total_notes_crawled_for_keyword: int,
    ) -> None:
        return None

    def on_time_range_day_failed(
        self,
        *,
        keyword: str,
        day: str,
        next_day: Optional[str],
        total_notes_crawled_for_keyword: int,
        reason: str,
    ) -> None:
        return None

    def on_progress_heartbeat(
        self,
        *,
        kind: str,
        cursor: str,
        day: Optional[str] = None,
        page: Optional[int] = None,
        notes_count_this_day: Optional[int] = None,
        total_notes_crawled_for_keyword: Optional[int] = None,
    ) -> None:
        return None

    @staticmethod
    def iter_days(start_day: date, end_day: date) -> Iterable[date]:
        current_day = start_day
        while current_day <= end_day:
            yield current_day
            current_day += timedelta(days=1)

    @staticmethod
    def get_note_created_date(note_detail: Dict) -> Optional[date]:
        created_time = int(note_detail.get("time") or 0)
        if created_time <= 0:
            return None
        if created_time > 1_000_000_000_000:
            created_time //= 1000
        try:
            return datetime.fromtimestamp(created_time).date()
        except (OSError, OverflowError, ValueError):
            return None

    def get_search_sort(self, *, latest: bool = False) -> SearchSortType:
        if latest:
            return SearchSortType.LATEST
        if not config.SORT_TYPE:
            return SearchSortType.GENERAL
        try:
            return SearchSortType(config.SORT_TYPE)
        except ValueError:
            utils.logger.warning(
                f"[XiaoHongShuCrawler.get_search_sort] Unknown sort type '{config.SORT_TYPE}', fallback to general"
            )
            return SearchSortType.GENERAL

    @staticmethod
    def extract_search_result_items(notes_res: Dict) -> List[Dict]:
        items = notes_res.get("items") or []
        if isinstance(items, dict):
            items = list(items.values())
        if not isinstance(items, list):
            return []
        return [
            item
            for item in items
            if isinstance(item, dict) and item.get("model_type") not in ("rec_query", "hot_query")
        ]

    async def fetch_note_details_from_search_items(self, items: List[Dict]) -> List[Dict]:
        if not items:
            return []
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("id"),
                xsec_source=post_item.get("xsec_source"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            )
            for post_item in items
        ]
        note_details = await asyncio.gather(*task_list)
        return [note_detail for note_detail in note_details if note_detail]

    async def store_note_details(self, note_details: List[Dict]) -> tuple[List[str], List[str]]:
        note_ids: List[str] = []
        xsec_tokens: List[str] = []
        for note_detail in note_details:
            processed_note = await self.process_note_item(note_detail)
            if not processed_note:
                continue
            await self.get_notice_media(processed_note)
            note_ids.append(processed_note.get("note_id"))
            xsec_tokens.append(processed_note.get("xsec_token"))
        return note_ids, xsec_tokens

    async def search(self) -> None:
        search_mode = getattr(config, "XHS_SEARCH_MODE", "normal")
        if search_mode == "normal":
            await self.search_by_keywords()
        elif search_mode == "daily_limit_in_time_range":
            await self.search_by_keywords_in_time_range()
        else:
            utils.logger.warning(f"[XiaoHongShuCrawler.search] Unknown XHS_SEARCH_MODE: {search_mode}")
        return

        """Search for notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.search] Begin search Xiaohongshu keywords")
        xhs_limit_count = 20  # Xiaohongshu limit page fixed value
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[XiaoHongShuCrawler.search] Current search keyword: {keyword}")
            page = 1
            search_id = get_search_id()
            while (page - start_page + 1) * xhs_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(f"[XiaoHongShuCrawler.search] search Xiaohongshu keyword: {keyword}, page: {page}")
                    note_ids: List[str] = []
                    xsec_tokens: List[str] = []
                    self.xhs_client.headers["referer"] = self.build_search_result_url(keyword)
                    notes_res = await self.xhs_client.get_note_by_keyword(
                        keyword=keyword,
                        search_id=search_id,
                        page=page,
                        sort=(SearchSortType(config.SORT_TYPE) if config.SORT_TYPE != "" else SearchSortType.GENERAL),
                    )
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Search notes response: {notes_res}")
                    if not notes_res or not notes_res.get("has_more", False):
                        utils.logger.info("[XiaoHongShuCrawler.search] No more content!")
                        break
                    semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
                    task_list = [
                        self.get_note_detail_async_task(
                            note_id=post_item.get("id"),
                            xsec_source=post_item.get("xsec_source"),
                            xsec_token=post_item.get("xsec_token"),
                            semaphore=semaphore,
                        ) for post_item in notes_res.get("items", {}) if post_item.get("model_type") not in ("rec_query", "hot_query")
                    ]
                    note_details = await asyncio.gather(*task_list)
                    for note_detail in note_details:
                        if note_detail:
                            await xhs_store.update_xhs_note(note_detail)
                            await self.get_notice_media(note_detail)
                            note_ids.append(note_detail.get("note_id"))
                            xsec_tokens.append(note_detail.get("xsec_token"))
                    page += 1
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Note details: {note_details}")
                    await self.batch_get_note_comments(note_ids, xsec_tokens)

                    # Sleep after each page navigation
                    await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                    utils.logger.info(f"[XiaoHongShuCrawler.search] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}")
                except DataFetchError as exc:
                    if "没有权限访问" in str(exc):
                        if await self.page_requires_login():
                            utils.logger.error(
                                "[XiaoHongShuCrawler.search] Search request was rejected because the browser page is still logged out."
                            )
                        else:
                            utils.logger.error(
                                "[XiaoHongShuCrawler.search] Search request was rejected even though the page no longer shows the login prompt. "
                                "The current account may be risk-controlled or not allowed to access search."
                            )
                    else:
                        utils.logger.error(f"[XiaoHongShuCrawler.search] Get note detail error: {exc}")
                    break

    async def search_by_keywords(self) -> None:
        """Search for notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.search_by_keywords] Begin search Xiaohongshu keywords")
        xhs_limit_count = 20
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            keyword = keyword.strip()
            if not keyword:
                continue

            source_keyword_var.set(keyword)
            utils.logger.info(f"[XiaoHongShuCrawler.search_by_keywords] Current search keyword: {keyword}")
            page = 1
            search_id = get_search_id()
            while (page - start_page + 1) * xhs_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[XiaoHongShuCrawler.search_by_keywords] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search_by_keywords] Search Xiaohongshu keyword: {keyword}, page: {page}"
                    )
                    self.xhs_client.headers["referer"] = self.build_search_result_url(keyword)
                    notes_res = await self.xhs_client.get_note_by_keyword(
                        keyword=keyword,
                        search_id=search_id,
                        page=page,
                        sort=self.get_search_sort(),
                    )
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search_by_keywords] Search notes response: {notes_res}"
                    )
                    if not notes_res:
                        utils.logger.info("[XiaoHongShuCrawler.search_by_keywords] No more content!")
                        break

                    items = self.extract_search_result_items(notes_res)
                    note_details = await self.fetch_note_details_from_search_items(items)
                    note_ids, xsec_tokens = await self.store_note_details(note_details)
                    page += 1
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search_by_keywords] Note details: {note_details}"
                    )
                    await self.batch_get_note_comments(note_ids, xsec_tokens)

                    if not notes_res.get("has_more", False):
                        utils.logger.info("[XiaoHongShuCrawler.search_by_keywords] No more content!")
                        break

                    await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search_by_keywords] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}"
                    )
                except DataFetchError as exc:
                    if "没有权限访问" in str(exc):
                        if await self.page_requires_login():
                            utils.logger.error(
                                "[XiaoHongShuCrawler.search_by_keywords] Search request was rejected because the browser page is still logged out."
                            )
                        else:
                            utils.logger.error(
                                "[XiaoHongShuCrawler.search_by_keywords] Search request was rejected even though the page no longer shows the login prompt. "
                                "The current account may be risk-controlled or not allowed to access search."
                            )
                    else:
                        utils.logger.error(
                            f"[XiaoHongShuCrawler.search_by_keywords] Get note detail error: {exc}"
                        )
                    break

    async def search_by_keywords_in_time_range(self) -> None:
        utils.logger.info("[XiaoHongShuCrawler.search_by_keywords_in_time_range] Begin time-range bucket search")
        xhs_limit_count = 20
        start_page = config.START_PAGE
        start_day_date = datetime.strptime(config.START_DAY, "%Y-%m-%d").date()
        end_day_date = datetime.strptime(config.END_DAY, "%Y-%m-%d").date()
        if start_day_date > end_day_date:
            raise ValueError("START_DAY cannot be later than END_DAY")
        if config.CRAWLER_MAX_NOTES_COUNT < xhs_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = xhs_limit_count

        try:
            for keyword in config.KEYWORDS.split(","):
                keyword = keyword.strip()
                if not keyword:
                    continue

                source_keyword_var.set(keyword)
                utils.logger.info(
                    f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Current search keyword: {keyword}"
                )
                resume_state = self.get_time_range_resume_state(keyword) or {}
                resume_day = str(resume_state.get("resume_day", "")).strip()
                resume_page = max(start_page, int(resume_state.get("resume_page", start_page) or start_page))
                resume_notes_count_this_day = int(resume_state.get("notes_count_this_day", 0) or 0)
                total_notes_crawled_for_keyword = int(
                    resume_state.get("total_notes_crawled_for_keyword", 0) or 0
                )
                keyword_completed = False
                failed_day_encountered = False

                if resume_day:
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Resuming keyword '{keyword}' from {resume_day} page {resume_page}"
                    )

                for day_value in self.iter_days(start_day_date, end_day_date):
                    day_str = day_value.isoformat()
                    if resume_day and day_str < resume_day:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Skip completed day {day_str} for keyword '{keyword}'"
                        )
                        continue

                    if total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT:
                        utils.logger.info(
                            f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Reached CRAWLER_MAX_NOTES_COUNT limit for keyword '{keyword}'"
                        )
                        self.on_time_range_keyword_completed(
                            keyword=keyword,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )
                        keyword_completed = True
                        break

                    page = resume_page if resume_day == day_str else start_page
                    notes_count_this_day = resume_notes_count_this_day if resume_day == day_str else 0
                    day_failed = False
                    day_failure_reason = ""
                    day_completion_reason = "no_more_content"
                    config.OUTPUT_DATE_OVERRIDE = day_str
                    search_id = get_search_id()

                    while True:
                        self.on_progress_heartbeat(
                            kind="page_started",
                            cursor=f"keyword={keyword}|day={day_str}|page={page}",
                            day=day_str,
                            page=page,
                            notes_count_this_day=notes_count_this_day,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )
                        if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                            day_completion_reason = "reached_max_notes_per_day"
                            break

                        try:
                            utils.logger.info(
                                f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Search keyword '{keyword}', day {day_str}, page {page}"
                            )
                            self.xhs_client.headers["referer"] = self.build_search_result_url(keyword)
                            notes_res = await self.xhs_client.get_note_by_keyword(
                                keyword=keyword,
                                search_id=search_id,
                                page=page,
                                sort=self.get_search_sort(latest=True),
                            )
                            if not notes_res:
                                day_completion_reason = "no_more_content"
                                break

                            items = self.extract_search_result_items(notes_res)
                            if not items:
                                next_page = page + 1
                                self.on_time_range_page_completed(
                                    keyword=keyword,
                                    day=day_str,
                                    next_page=next_page,
                                    notes_count_this_day=notes_count_this_day,
                                    total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                                )
                                if not notes_res.get("has_more", False):
                                    day_completion_reason = "no_more_content"
                                    break
                                page = next_page
                                continue

                            note_details = await self.fetch_note_details_from_search_items(items)
                            day_matched_note_details: List[Dict] = []
                            saw_younger_content = False
                            saw_older_content = False
                            saw_unknown_content = False

                            for note_detail in note_details:
                                note_created_day = self.get_note_created_date(note_detail)
                                if note_created_day is None:
                                    saw_unknown_content = True
                                    continue
                                if note_created_day > day_value:
                                    saw_younger_content = True
                                    continue
                                if note_created_day < day_value:
                                    saw_older_content = True
                                    continue
                                day_matched_note_details.append(note_detail)

                            remaining_capacity = max(config.MAX_NOTES_PER_DAY - notes_count_this_day, 0)
                            if remaining_capacity and len(day_matched_note_details) > remaining_capacity:
                                day_matched_note_details = day_matched_note_details[:remaining_capacity]
                                saw_older_content = True

                            note_ids, xsec_tokens = await self.store_note_details(day_matched_note_details)
                            matched_count = len(note_ids)
                            notes_count_this_day += matched_count
                            total_notes_crawled_for_keyword += matched_count
                            await self.batch_get_note_comments(note_ids, xsec_tokens)

                            next_page = page + 1
                            self.on_time_range_page_completed(
                                keyword=keyword,
                                day=day_str,
                                next_page=next_page,
                                notes_count_this_day=notes_count_this_day,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                            )

                            if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                                day_completion_reason = "reached_max_notes_per_day"
                                break
                            if saw_older_content:
                                day_completion_reason = "encountered_older_day_content"
                                break
                            if not notes_res.get("has_more", False):
                                day_completion_reason = "no_more_content"
                                break
                            if not day_matched_note_details and not saw_younger_content and not saw_unknown_content:
                                day_completion_reason = "no_target_day_content"
                                break

                            page = next_page
                            await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                            utils.logger.info(
                                f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}"
                            )
                        except DataFetchError as exc:
                            day_failed = True
                            if "没有权限访问" in str(exc):
                                if await self.page_requires_login():
                                    day_failure_reason = "search_request_rejected_logged_out"
                                    utils.logger.error(
                                        "[XiaoHongShuCrawler.search_by_keywords_in_time_range] Search request was rejected because the browser page is still logged out."
                                    )
                                else:
                                    day_failure_reason = "search_request_rejected_permission_denied"
                                    utils.logger.error(
                                        "[XiaoHongShuCrawler.search_by_keywords_in_time_range] Search request was rejected even though the page no longer shows the login prompt. "
                                        "The current account may be risk-controlled or not allowed to access search."
                                    )
                            else:
                                day_failure_reason = self.format_exception_details(exc)
                                utils.logger.error(
                                    f"[XiaoHongShuCrawler.search_by_keywords_in_time_range] Get note detail error: {exc}"
                                )
                            next_day = (day_value + timedelta(days=1)).isoformat() if day_value < end_day_date else None
                            self.on_time_range_day_failed(
                                keyword=keyword,
                                day=day_str,
                                next_day=next_day,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                                reason=day_failure_reason,
                            )
                            failed_day_encountered = True
                            break

                    if day_failed:
                        break

                    next_day = (day_value + timedelta(days=1)).isoformat() if day_value < end_day_date else None
                    self.on_time_range_day_completed(
                        keyword=keyword,
                        day=day_str,
                        next_day=next_day,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        reason=day_completion_reason,
                    )

                if not keyword_completed and not failed_day_encountered:
                    self.on_time_range_keyword_completed(
                        keyword=keyword,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                    )
        finally:
            config.OUTPUT_DATE_OVERRIDE = ""

    async def get_creators_and_notes(self) -> None:
        """Get creator's notes and retrieve their comment information."""
        utils.logger.info("[XiaoHongShuCrawler.get_creators_and_notes] Begin get Xiaohongshu creators")
        for creator_url in config.XHS_CREATOR_ID_LIST:
            try:
                # Parse creator URL to get user_id and security tokens
                creator_info: CreatorUrlInfo = parse_creator_info_from_url(creator_url)
                utils.logger.info(f"[XiaoHongShuCrawler.get_creators_and_notes] Parse creator URL info: {creator_info}")
                user_id = creator_info.user_id

                # get creator detail info from web html content
                createor_info: Dict = await self.xhs_client.get_creator_info(
                    user_id=user_id,
                    xsec_token=creator_info.xsec_token,
                    xsec_source=creator_info.xsec_source
                )
                if createor_info:
                    await xhs_store.save_creator(user_id, creator=createor_info)
            except ValueError as e:
                utils.logger.error(f"[XiaoHongShuCrawler.get_creators_and_notes] Failed to parse creator URL: {e}")
                continue

            # Use fixed crawling interval
            crawl_interval = config.CRAWLER_MAX_SLEEP_SEC
            # Get all note information of the creator
            all_notes_list = await self.xhs_client.get_all_notes_by_creator(
                user_id=user_id,
                crawl_interval=crawl_interval,
                callback=self.fetch_creator_notes_detail,
                xsec_token=creator_info.xsec_token,
                xsec_source=creator_info.xsec_source,
            )

            note_ids = []
            xsec_tokens = []
            for note_item in all_notes_list:
                note_ids.append(note_item.get("note_id"))
                xsec_tokens.append(note_item.get("xsec_token"))
            await self.batch_get_note_comments(note_ids, xsec_tokens)

    async def fetch_creator_notes_detail(self, note_list: List[Dict]):
        """Concurrently obtain the specified post list and save the data"""
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_detail_async_task(
                note_id=post_item.get("note_id"),
                xsec_source=post_item.get("xsec_source"),
                xsec_token=post_item.get("xsec_token"),
                semaphore=semaphore,
            ) for post_item in note_list
        ]

        note_details = await asyncio.gather(*task_list)
        for note_detail in note_details:
            if note_detail:
                processed_note = await self.process_note_item(note_detail)
                if not processed_note:
                    continue
                await self.get_notice_media(processed_note)

    async def get_specified_notes(self):
        """Get the information and comments of the specified post

        Note: Must specify note_id, xsec_source, xsec_token
        """
        get_note_detail_task_list = []
        for full_note_url in config.XHS_SPECIFIED_NOTE_URL_LIST:
            note_url_info: NoteUrlInfo = parse_note_info_from_note_url(full_note_url)
            utils.logger.info(f"[XiaoHongShuCrawler.get_specified_notes] Parse note url info: {note_url_info}")
            crawler_task = self.get_note_detail_async_task(
                note_id=note_url_info.note_id,
                xsec_source=note_url_info.xsec_source,
                xsec_token=note_url_info.xsec_token,
                semaphore=asyncio.Semaphore(config.MAX_CONCURRENCY_NUM),
            )
            get_note_detail_task_list.append(crawler_task)

        need_get_comment_note_ids = []
        xsec_tokens = []
        note_details = await asyncio.gather(*get_note_detail_task_list)
        for note_detail in note_details:
            if note_detail:
                processed_note = await self.process_note_item(note_detail)
                if not processed_note:
                    continue
                need_get_comment_note_ids.append(processed_note.get("note_id", ""))
                xsec_tokens.append(processed_note.get("xsec_token", ""))
                await self.get_notice_media(processed_note)
        await self.batch_get_note_comments(need_get_comment_note_ids, xsec_tokens)

    async def get_note_detail_async_task(
        self,
        note_id: str,
        xsec_source: str,
        xsec_token: str,
        semaphore: asyncio.Semaphore,
    ) -> Optional[Dict]:
        """Get note detail

        Args:
            note_id:
            xsec_source:
            xsec_token:
            semaphore:

        Returns:
            Dict: note detail
        """
        note_detail = None
        utils.logger.info(f"[get_note_detail_async_task] Begin get note detail, note_id: {note_id}")
        async with semaphore:
            try:
                self.on_progress_heartbeat(
                    kind="detail_started",
                    cursor=f"note_id={note_id}",
                    day=getattr(self, "current_day", None),
                )
                try:
                    note_detail = await self.xhs_client.get_note_by_id(note_id, xsec_source, xsec_token)
                except RetryError:
                    pass

                if not note_detail:
                    note_detail = await self.xhs_client.get_note_by_id_from_html(note_id, xsec_source, xsec_token,
                                                                                 enable_cookie=True)
                    if not note_detail:
                        raise Exception(f"[get_note_detail_async_task] Failed to get note detail, Id: {note_id}")

                note_detail.update({"xsec_token": xsec_token, "xsec_source": xsec_source})

                # Sleep after fetching note detail
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[get_note_detail_async_task] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching note {note_id}")
                self.on_progress_heartbeat(
                    kind="detail_completed",
                    cursor=f"note_id={note_id}",
                    day=getattr(self, "current_day", None),
                )

                return note_detail

            except NoteNotFoundError as ex:
                utils.logger.warning(f"[XiaoHongShuCrawler.get_note_detail_async_task] Note not found: {note_id}, {ex}")
                return None
            except RetryError as ex:
                utils.logger.error(
                    f"[XiaoHongShuCrawler.get_note_detail_async_task] Retry exhausted while fetching note detail {note_id}: {ex}"
                )
                return None
            except DataFetchError as ex:
                utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail_async_task] Get note detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(f"[XiaoHongShuCrawler.get_note_detail_async_task] have not fund note detail note_id:{note_id}, err: {ex}")
                return None
            except Exception as ex:
                utils.logger.error(
                    f"[XiaoHongShuCrawler.get_note_detail_async_task] Unexpected error for note {note_id}: {ex}"
                )
                return None

    async def batch_get_note_comments(self, note_list: List[str], xsec_tokens: List[str]):
        """Batch get note comments"""
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[XiaoHongShuCrawler.batch_get_note_comments] Begin batch get note comments, note list: {note_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for index, note_id in enumerate(note_list):
            task = asyncio.create_task(
                self.get_comments(note_id=note_id, xsec_token=xsec_tokens[index], semaphore=semaphore),
                name=note_id,
            )
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_comments(self, note_id: str, xsec_token: str, semaphore: asyncio.Semaphore):
        """Get note comments with keyword filtering and quantity limitation"""
        async with semaphore:
            self.on_progress_heartbeat(
                kind="comment_started",
                cursor=f"note_id={note_id}",
                day=getattr(self, "current_day", None),
            )
            utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Begin get note id comments {note_id}")
            # Use fixed crawling interval
            crawl_interval = config.CRAWLER_MAX_SLEEP_SEC
            await self.xhs_client.get_note_all_comments(
                note_id=note_id,
                xsec_token=xsec_token,
                crawl_interval=crawl_interval,
                callback=xhs_store.batch_update_xhs_note_comments,
                max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
            )

            # Sleep after fetching comments
            await asyncio.sleep(crawl_interval)
            utils.logger.info(f"[XiaoHongShuCrawler.get_comments] Sleeping for {crawl_interval} seconds after fetching comments for note {note_id}")
            self.on_progress_heartbeat(
                kind="comment_completed",
                cursor=f"note_id={note_id}",
                day=getattr(self, "current_day", None),
            )

    async def create_xhs_client(self, httpx_proxy: Optional[str]) -> XiaoHongShuClient:
        """Create Xiaohongshu client"""
        utils.logger.info("[XiaoHongShuCrawler.create_xhs_client] Begin create Xiaohongshu API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        xhs_client_obj = XiaoHongShuClient(
            proxy=httpx_proxy,
            headers={
                "accept": "application/json, text/plain, */*",
                "accept-language": "zh-CN,zh;q=0.9",
                "cache-control": "no-cache",
                "content-type": "application/json;charset=UTF-8",
                "origin": "https://www.xiaohongshu.com",
                "pragma": "no-cache",
                "priority": "u=1, i",
                "referer": "https://www.xiaohongshu.com/",
                "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-site",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
                "Cookie": cookie_str,
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
            proxy_ip_pool=self.ip_proxy_pool,  # Pass proxy pool for automatic refresh
        )
        return xhs_client_obj

    @staticmethod
    def get_launch_channel_candidates() -> List[Optional[str]]:
        launcher = BrowserLauncher()
        channels: List[Optional[str]] = []
        for browser_path in launcher.detect_browser_paths():
            lower_path = browser_path.lower()
            if "chrome.exe" in lower_path and "google\\chrome" in lower_path and "chrome" not in channels:
                channels.append("chrome")
            elif "msedge.exe" in lower_path and "msedge" not in channels:
                channels.append("msedge")
        channels.append(None)
        return channels

    async def try_launch_browser_context(
        self,
        chromium: BrowserType,
        *,
        persistent: bool,
        headless: bool,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        user_data_dir: Optional[str] = None,
    ) -> BrowserContext:
        last_error: Optional[Exception] = None
        for channel in self.get_launch_channel_candidates():
            launch_kwargs = {
                "headless": headless,
                "proxy": playwright_proxy,
            }
            context_kwargs = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": user_agent,
            }
            if channel:
                launch_kwargs["channel"] = channel

            try:
                if persistent:
                    browser_context = await chromium.launch_persistent_context(
                        user_data_dir=user_data_dir,
                        accept_downloads=True,
                        viewport=context_kwargs["viewport"],
                        user_agent=context_kwargs["user_agent"],
                        **launch_kwargs,  # type: ignore[arg-type]
                    )
                    utils.logger.info(
                        f"[XiaoHongShuCrawler.launch_browser] Created persistent context using {channel or 'bundled Chromium'}"
                    )
                    return browser_context

                browser = await chromium.launch(**launch_kwargs)  # type: ignore[arg-type]
                browser_context = await browser.new_context(**context_kwargs)
                utils.logger.info(
                    f"[XiaoHongShuCrawler.launch_browser] Created browser context using {channel or 'bundled Chromium'}"
                )
                return browser_context
            except Exception as exc:
                last_error = exc
                utils.logger.warning(
                    f"[XiaoHongShuCrawler.launch_browser] Failed to launch with {channel or 'bundled Chromium'}: "
                    f"{self.format_exception_details(exc)}"
                )

        if last_error is not None:
            raise last_error
        raise RuntimeError("No browser launch candidates were available")

    async def launch_browser(
        self,
        chromium: BrowserType,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """Launch browser and create browser context"""
        utils.logger.info("[XiaoHongShuCrawler.launch_browser] Begin create browser context ...")
        if config.SAVE_LOGIN_STATE:
            user_data_dir = os.path.join(
                os.getcwd(), "browser_data", config.USER_DATA_DIR % config.PLATFORM
            )  # type: ignore
            return await self.try_launch_browser_context(
                chromium,
                persistent=True,
                headless=headless,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
                user_data_dir=user_data_dir,
            )

        return await self.try_launch_browser_context(
            chromium,
            persistent=False,
            headless=headless,
            playwright_proxy=playwright_proxy,
            user_agent=user_agent,
        )

    async def launch_browser_with_cdp(
        self,
        playwright: Playwright,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """Launch browser using CDP mode"""
        try:
            self.cdp_manager = CDPBrowserManager()
            browser_context = await self.cdp_manager.launch_and_connect(
                playwright=playwright,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
                headless=headless,
            )

            # Display browser information
            browser_info = await self.cdp_manager.get_browser_info()
            utils.logger.info(f"[XiaoHongShuCrawler] CDP browser info: {browser_info}")

            return browser_context

        except Exception as e:
            utils.logger.error(f"[XiaoHongShuCrawler] CDP mode launch failed, falling back to standard mode: {e}")
            # Fall back to standard mode
            chromium = playwright.chromium
            return await self.launch_browser(chromium, playwright_proxy, user_agent, headless)

    async def close_session(self) -> None:
        if getattr(self, "cdp_manager", None):
            try:
                await self.cdp_manager.cleanup(force=True)
            except Exception:
                pass
            self.cdp_manager = None
        elif getattr(self, "browser_context", None):
            try:
                await self.browser_context.close()
            except Exception:
                pass

        self.browser_context = None
        self.context_page = None
        self.xhs_client = None
        if self._playwright_manager is not None:
            try:
                await self._playwright_manager.__aexit__(None, None, None)
            except Exception:
                pass
        self._playwright = None
        self._playwright_manager = None

    async def close(self):
        await self.close_session()
        utils.logger.info("[XiaoHongShuCrawler.close] Browser context closed ...")

    async def get_notice_media(self, note_detail: Dict):
        if not config.ENABLE_GET_MEIDAS:
            utils.logger.info(f"[XiaoHongShuCrawler.get_notice_media] Crawling image mode is not enabled")
            return
        await self.get_note_images(note_detail)
        await self.get_notice_video(note_detail)

    async def get_note_images(self, note_item: Dict):
        """Get note images. Please use get_notice_media

        Args:
            note_item: Note item dictionary
        """
        if not config.ENABLE_GET_MEIDAS:
            return
        note_id = note_item.get("note_id")
        image_list: List[Dict] = note_item.get("image_list", [])

        for img in image_list:
            if img.get("url_default") != "":
                img.update({"url": img.get("url_default")})

        if not image_list:
            return
        picNum = 0
        for pic in image_list:
            url = pic.get("url")
            if not url:
                continue
            content = await self.xhs_client.get_note_media(url)
            await asyncio.sleep(random.random())
            if content is None:
                continue
            extension_file_name = f"{picNum}.jpg"
            picNum += 1
            await xhs_store.update_xhs_note_image(note_id, content, extension_file_name)

    async def get_notice_video(self, note_item: Dict):
        """Get note videos. Please use get_notice_media

        Args:
            note_item: Note item dictionary
        """
        if not config.ENABLE_GET_MEIDAS:
            return
        note_id = note_item.get("note_id")

        videos = xhs_store.get_video_url_arr(note_item)

        if not videos:
            return
        videoNum = 0
        for url in videos:
            content = await self.xhs_client.get_note_media(url)
            await asyncio.sleep(random.random())
            if content is None:
                continue
            extension_file_name = f"{videoNum}.mp4"
            videoNum += 1
            await xhs_store.update_xhs_note_video(note_id, content, extension_file_name)

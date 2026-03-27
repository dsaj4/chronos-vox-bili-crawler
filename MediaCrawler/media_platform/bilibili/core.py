# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/media_platform/bilibili/core.py
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

# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/2 18:44
# @Desc    : Bilibili Crawler

import asyncio
import csv
import json
import os
from pathlib import Path
import traceback
# import random  # Removed as we now use fixed config.CRAWLER_MAX_SLEEP_SEC intervals
from asyncio import Task
from typing import Dict, List, Optional, Set, Tuple, Union
from datetime import datetime, timedelta
import httpx
import pandas as pd
from sqlalchemy import select

from playwright.async_api import (
    BrowserContext,
    BrowserType,
    Page,
    Playwright,
    async_playwright,
)
from playwright._impl._errors import TargetClosedError

import config
from base.base_crawler import AbstractCrawler
from database.db_session import get_session
from database.models import BilibiliVideo
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import bilibili as bilibili_store
from tools import utils
from tools.browser_launcher import BrowserLauncher
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import BilibiliClient
from .exception import DataFetchError
from .field import SearchOrderType
from .help import parse_video_info_from_url, parse_creator_info_from_url
from .login import BilibiliLogin


class BilibiliCrawler(AbstractCrawler):
    context_page: Page
    bili_client: BilibiliClient
    browser_context: BrowserContext
    cdp_manager: Optional[CDPBrowserManager]

    def __init__(self):
        self.index_url = "https://www.bilibili.com"
        self.user_agent = utils.get_user_agent()
        self.cdp_manager = None
        self.ip_proxy_pool = None  # Proxy IP pool for automatic proxy refresh
        self.existing_titles: Set[str] = set()
        self._playwright_manager = None
        self._playwright = None
        self._session_recovery_count = 0

    @staticmethod
    def is_playwright_startup_access_error(exc: Exception) -> bool:
        if not isinstance(exc, PermissionError):
            return False
        error_text = str(exc).lower()
        return "winerror 5" in error_text or "拒绝访问" in error_text or "access is denied" in error_text

    async def prepare_session(self):
        playwright_proxy_format, httpx_proxy_format = None, None
        if config.ENABLE_IP_PROXY:
            self.ip_proxy_pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, enable_validate_ip=True)
            ip_proxy_info: IpInfoModel = await self.ip_proxy_pool.get_proxy()
            playwright_proxy_format, httpx_proxy_format = utils.format_proxy_info(ip_proxy_info)

        self._playwright_manager = async_playwright()
        playwright = await self._playwright_manager.__aenter__()
        self._playwright = playwright

        # Choose launch mode based on configuration
        if config.ENABLE_CDP_MODE:
            utils.logger.info("[BilibiliCrawler] Launching browser using CDP mode")
            self.browser_context = await self.launch_browser_with_cdp(
                playwright,
                playwright_proxy_format,
                self.user_agent,
                headless=config.CDP_HEADLESS,
            )
        else:
            utils.logger.info("[BilibiliCrawler] Launching browser using standard mode")
            chromium = playwright.chromium
            self.browser_context = await self.launch_browser(chromium, None, self.user_agent, headless=config.HEADLESS)
            await self.browser_context.add_init_script(path="libs/stealth.min.js")

        self.context_page = await self.browser_context.new_page()
        await self.context_page.goto(self.index_url)

        self.bili_client = await self.create_bilibili_client(httpx_proxy_format)
        self.existing_titles = await self.load_existing_titles()
        if not await self.bili_client.pong():
            login_obj = BilibiliLogin(
                login_type=config.LOGIN_TYPE,
                login_phone="",  # your phone number
                browser_context=self.browser_context,
                context_page=self.context_page,
                cookie_str=config.COOKIES,
            )
            await login_obj.begin()
            await self.bili_client.update_cookies(browser_context=self.browser_context)

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
                    f"[BilibiliCrawler.prepare_session_with_retries] Playwright startup failed with "
                    f"{self.format_exception_details(exc)}; retrying {attempt}/{max_attempts} after {retry_delay:.1f}s"
                )
                await asyncio.sleep(retry_delay)

    async def run_current_config(self):
        crawler_type_var.set(config.CRAWLER_TYPE)
        if config.CRAWLER_TYPE == "search":
            await self.search()
        elif config.CRAWLER_TYPE == "detail":
            await self.get_specified_videos(config.BILI_SPECIFIED_ID_LIST)
        elif config.CRAWLER_TYPE == "creator":
            if config.CREATOR_MODE:
                for creator_url in config.BILI_CREATOR_ID_LIST:
                    try:
                        creator_info = parse_creator_info_from_url(creator_url)
                        utils.logger.info(f"[BilibiliCrawler.start] Parsed creator ID: {creator_info.creator_id} from {creator_url}")
                        await self.get_creator_videos(int(creator_info.creator_id))
                    except ValueError as e:
                        utils.logger.error(f"[BilibiliCrawler.start] Failed to parse creator URL: {e}")
                        continue
            else:
                await self.get_all_creator_details(config.BILI_CREATOR_ID_LIST)
        utils.logger.info("[BilibiliCrawler.start] Bilibili Crawler finished ...")

    async def close_session(self):
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
        self.bili_client = None
        if self._playwright_manager is not None:
            try:
                await self._playwright_manager.__aexit__(None, None, None)
            except Exception:
                pass
        self._playwright = None
        self._playwright_manager = None

    @staticmethod
    def format_exception_details(exc: Exception) -> str:
        message = str(exc).strip()
        if message:
            return f"{exc.__class__.__name__}: {message}"
        return exc.__class__.__name__

    @classmethod
    def is_recoverable_session_error(cls, exc: Exception) -> bool:
        if isinstance(exc, TargetClosedError):
            return True
        error_text = cls.format_exception_details(exc).lower()
        markers = [
            "target page, context or browser has been closed",
            "connection closed while reading from the driver",
            "browser has been closed",
            "page has been closed",
            "context has been closed",
            "target closed",
            "playwright",
        ]
        return any(marker in error_text for marker in markers)

    async def recover_session(self, reason: Exception) -> None:
        self._session_recovery_count += 1
        utils.logger.warning(
            f"[BilibiliCrawler.recover_session] Recovering browser session in-process "
            f"(attempt {self._session_recovery_count}) because of {self.format_exception_details(reason)}"
        )
        try:
            await self.close_session()
        finally:
            await self.prepare_session_with_retries()

    async def start(self):
        await self.prepare_session_with_retries()
        try:
            await self.run_current_config()
        finally:
            await self.close_session()

    @staticmethod
    def normalize_title(title: str) -> str:
        return " ".join((title or "").split())

    def should_skip_title(self, title: str) -> bool:
        normalized_title = self.normalize_title(title)
        return bool(normalized_title) and normalized_title in self.existing_titles

    def remember_title(self, title: str):
        normalized_title = self.normalize_title(title)
        if normalized_title:
            self.existing_titles.add(normalized_title)

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

    def get_data_base_path(self, file_type: str) -> Path:
        if config.SAVE_DATA_PATH:
            return Path(config.SAVE_DATA_PATH) / "bili" / file_type
        return Path("data") / "bili" / file_type

    async def load_existing_titles(self) -> Set[str]:
        try:
            if config.SAVE_DATA_OPTION in {"db", "sqlite", "postgres"}:
                titles = await self.load_existing_titles_from_db()
            elif config.SAVE_DATA_OPTION == "mongodb":
                titles = await self.load_existing_titles_from_mongodb()
            elif config.SAVE_DATA_OPTION in {"json", "jsonl", "csv", "excel"}:
                titles = self.load_existing_titles_from_files(config.SAVE_DATA_OPTION)
            else:
                titles = set()
            utils.logger.info(f"[BilibiliCrawler.load_existing_titles] Loaded {len(titles)} existing titles for resume mode")
            return titles
        except Exception as exc:
            utils.logger.warning(f"[BilibiliCrawler.load_existing_titles] Failed to load existing titles: {exc}")
            return set()

    async def load_existing_titles_from_db(self) -> Set[str]:
        async with get_session() as session:
            if session is None:
                return set()
            result = await session.execute(select(BilibiliVideo.title))
            return {
                normalized
                for (title,) in result.all()
                if (normalized := self.normalize_title(title))
            }

    async def load_existing_titles_from_mongodb(self) -> Set[str]:
        from database.mongodb_store_base import MongoDBStoreBase

        mongo_store = MongoDBStoreBase(collection_prefix="bilibili")
        rows = await mongo_store.find_many("contents", {})
        return {
            normalized
            for row in rows
            if isinstance(row, dict)
            and (normalized := self.normalize_title(row.get("title", "")))
        }

    def load_existing_titles_from_files(self, file_type: str) -> Set[str]:
        base_path = self.get_data_base_path(file_type)
        if not base_path.exists():
            return set()

        patterns = {
            "json": ["*_contents_*.json", "*_videos_*.json"],
            "jsonl": ["*_contents_*.jsonl", "*_videos_*.jsonl"],
            "csv": ["*_contents_*.csv", "*_videos_*.csv"],
            "excel": ["*_contents_*.xlsx", "*_videos_*.xlsx"],
        }
        titles: Set[str] = set()
        for pattern in patterns.get(file_type, []):
            for file_path in base_path.glob(pattern):
                titles.update(self.extract_titles_from_file(file_path, file_type))
        return titles

    def extract_titles_from_file(self, file_path: Path, file_type: str) -> Set[str]:
        titles: Set[str] = set()
        if file_type == "json":
            with file_path.open("r", encoding="utf-8") as file_obj:
                data = json.load(file_obj)
            if isinstance(data, dict):
                data = [data]
            for item in data:
                if isinstance(item, dict):
                    normalized = self.normalize_title(item.get("title", ""))
                    if normalized:
                        titles.add(normalized)
        elif file_type == "jsonl":
            with file_path.open("r", encoding="utf-8") as file_obj:
                for line in file_obj:
                    line = line.strip()
                    if not line:
                        continue
                    item = json.loads(line)
                    if isinstance(item, dict):
                        normalized = self.normalize_title(item.get("title", ""))
                        if normalized:
                            titles.add(normalized)
        elif file_type == "csv":
            with file_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                reader = csv.DictReader(file_obj)
                for row in reader:
                    normalized = self.normalize_title((row or {}).get("title", ""))
                    if normalized:
                        titles.add(normalized)
        elif file_type == "excel":
            dataframe = pd.read_excel(file_path)
            if "title" in dataframe.columns:
                for title in dataframe["title"].tolist():
                    normalized = self.normalize_title(str(title) if pd.notna(title) else "")
                    if normalized:
                        titles.add(normalized)
        return titles

    async def process_video_item(self, video_item: Optional[Dict], semaphore: asyncio.Semaphore) -> Optional[str]:
        if not video_item:
            return None

        video_view: Dict = video_item.get("View", {})
        title = video_view.get("title", "")
        if self.should_skip_title(title):
            utils.logger.info(f"[BilibiliCrawler.process_video_item] Skip duplicated title: {title}")
            return None

        self.remember_title(title)
        video_id = video_view.get("aid")
        await bilibili_store.update_bilibili_video(video_item)
        await bilibili_store.update_up_info(video_item)
        await self.get_bilibili_video(video_item, semaphore)
        return str(video_id) if video_id else None

    async def search(self):
        """
        search bilibili video
        """
        # Search for video and retrieve their comment information.
        if config.BILI_SEARCH_MODE == "normal":
            await self.search_by_keywords()
        elif config.BILI_SEARCH_MODE == "all_in_time_range":
            await self.search_by_keywords_in_time_range(daily_limit=False)
        elif config.BILI_SEARCH_MODE == "daily_limit_in_time_range":
            await self.search_by_keywords_in_time_range(daily_limit=True)
        else:
            utils.logger.warning(f"Unknown BILI_SEARCH_MODE: {config.BILI_SEARCH_MODE}")

    @staticmethod
    async def get_pubtime_datetime(
        start: str = config.START_DAY,
        end: str = config.END_DAY,
    ) -> Tuple[str, str]:
        """
        Get bilibili publish start timestamp pubtime_begin_s and publish end timestamp pubtime_end_s
        ---
        :param start: Publish date start time, YYYY-MM-DD
        :param end: Publish date end time, YYYY-MM-DD

        Note
        ---
        - Search time range is from start to end, including both start and end
        - To search content from the same day, to include search content from that day, pubtime_end_s should be pubtime_begin_s plus one day minus one second, i.e., the last second of start day
            - For example, searching only 2024-01-05 content, pubtime_begin_s = 1704384000, pubtime_end_s = 1704470399
              Converted to readable datetime objects: pubtime_begin_s = datetime.datetime(2024, 1, 5, 0, 0), pubtime_end_s = datetime.datetime(2024, 1, 5, 23, 59, 59)
        - To search content from start to end, to include search content from end day, pubtime_end_s should be pubtime_end_s plus one day minus one second, i.e., the last second of end day
            - For example, searching 2024-01-05 - 2024-01-06 content, pubtime_begin_s = 1704384000, pubtime_end_s = 1704556799
              Converted to readable datetime objects: pubtime_begin_s = datetime.datetime(2024, 1, 5, 0, 0), pubtime_end_s = datetime.datetime(2024, 1, 6, 23, 59, 59)
        """
        # Convert start and end to datetime objects
        start_day: datetime = datetime.strptime(start, "%Y-%m-%d")
        end_day: datetime = datetime.strptime(end, "%Y-%m-%d")
        if start_day > end_day:
            raise ValueError("Wrong time range, please check your start and end argument, to ensure that the start cannot exceed end")
        elif start_day == end_day:  # Searching content from the same day
            end_day = (start_day + timedelta(days=1) - timedelta(seconds=1))  # Set end_day to start_day + 1 day - 1 second
        else:  # Searching from start to end
            end_day = (end_day + timedelta(days=1) - timedelta(seconds=1))  # Set end_day to end_day + 1 day - 1 second
        # Convert back to timestamps
        return str(int(start_day.timestamp())), str(int(end_day.timestamp()))

    async def search_by_keywords(self):
        """
        search bilibili video with keywords in normal mode
        :return:
        """
        utils.logger.info("[BilibiliCrawler.search_by_keywords] Begin search bilibli keywords")
        bili_limit_count = 20  # bilibili limit page fixed value
        if config.CRAWLER_MAX_NOTES_COUNT < bili_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = bili_limit_count
        start_page = config.START_PAGE  # start page number
        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[BilibiliCrawler.search_by_keywords] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * bili_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[BilibiliCrawler.search_by_keywords] Skip page: {page}")
                    page += 1
                    continue

                utils.logger.info(f"[BilibiliCrawler.search_by_keywords] search bilibili keyword: {keyword}, page: {page}")
                video_id_list: List[str] = []
                videos_res = await self.bili_client.search_video_by_keyword(
                    keyword=keyword,
                    page=page,
                    page_size=bili_limit_count,
                    order=SearchOrderType.DEFAULT,
                    pubtime_begin_s=0,  # Publish date start timestamp
                    pubtime_end_s=0,  # Publish date end timestamp
                )
                video_list: List[Dict] = videos_res.get("result")

                if not video_list:
                    utils.logger.info(f"[BilibiliCrawler.search_by_keywords] No more videos for '{keyword}', moving to next keyword.")
                    break

                semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
                task_list = []
                try:
                    task_list = [self.get_video_info_task(aid=video_item.get("aid"), bvid="", semaphore=semaphore) for video_item in video_list]
                except Exception as e:
                    utils.logger.warning(f"[BilibiliCrawler.search_by_keywords] error in the task list. The video for this page will not be included. {e}")
                video_items = await asyncio.gather(*task_list)
                for video_item in video_items:
                    video_id = await self.process_video_item(video_item, semaphore)
                    if video_id:
                        video_id_list.append(video_id)
                page += 1

                # Sleep after page navigation
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[BilibiliCrawler.search_by_keywords] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}")

                await self.batch_get_video_comments(video_id_list)

    async def search_by_keywords_in_time_range(self, daily_limit: bool):
        """
        Search bilibili video with keywords in a given time range.
        :param daily_limit: if True, strictly limit the number of notes per day and total.
        """
        utils.logger.info(f"[BilibiliCrawler.search_by_keywords_in_time_range] Begin search with daily_limit={daily_limit}")
        bili_limit_count = 20
        start_page = config.START_PAGE
        end_day_date = datetime.strptime(config.END_DAY, "%Y-%m-%d").date()

        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(f"[BilibiliCrawler.search_by_keywords_in_time_range] Current search keyword: {keyword}")
            resume_state = self.get_time_range_resume_state(keyword) or {}
            resume_day = str(resume_state.get("resume_day", "")).strip()
            resume_page = max(start_page, int(resume_state.get("resume_page", start_page) or start_page))
            resume_notes_count_this_day = int(resume_state.get("notes_count_this_day", 0) or 0)
            total_notes_crawled_for_keyword = int(resume_state.get("total_notes_crawled_for_keyword", 0) or 0)
            keyword_completed = False
            failed_day_encountered = False

            if resume_day:
                utils.logger.info(
                    f"[BilibiliCrawler.search_by_keywords_in_time_range] Resuming keyword '{keyword}' from {resume_day} page {resume_page}"
                )

            for day in pd.date_range(start=config.START_DAY, end=config.END_DAY, freq="D"):
                day_str = day.strftime("%Y-%m-%d")
                if resume_day and day_str < resume_day:
                    utils.logger.info(
                        f"[BilibiliCrawler.search_by_keywords_in_time_range] Skip completed day {day_str} for keyword '{keyword}'"
                    )
                    continue

                if (daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT):
                    utils.logger.info(f"[BilibiliCrawler.search] Reached CRAWLER_MAX_NOTES_COUNT limit for keyword '{keyword}', skipping remaining days.")
                    self.on_time_range_keyword_completed(
                        keyword=keyword,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                    )
                    keyword_completed = True
                    break

                if (not daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT):
                    utils.logger.info(f"[BilibiliCrawler.search] Reached CRAWLER_MAX_NOTES_COUNT limit for keyword '{keyword}', skipping remaining days.")
                    self.on_time_range_keyword_completed(
                        keyword=keyword,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                    )
                    keyword_completed = True
                    break

                pubtime_begin_s, pubtime_end_s = await self.get_pubtime_datetime(start=day_str, end=day_str)
                page = resume_page if resume_day == day_str else start_page
                notes_count_this_day = resume_notes_count_this_day if resume_day == day_str else 0
                request_retry_count = 0
                session_recovery_count = 0
                day_failed = False
                day_failure_reason = ""

                while True:
                    if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                        utils.logger.info(f"[BilibiliCrawler.search] Reached MAX_NOTES_PER_DAY limit for {day.ctime()}.")
                        next_day = day.date() + timedelta(days=1)
                        self.on_time_range_day_completed(
                            keyword=keyword,
                            day=day_str,
                            next_day=next_day.strftime("%Y-%m-%d") if next_day <= end_day_date else None,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                            reason="max_notes_per_day_reached",
                        )
                        break
                    if (daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT):
                        utils.logger.info(f"[BilibiliCrawler.search] Reached CRAWLER_MAX_NOTES_COUNT limit for keyword '{keyword}'.")
                        self.on_time_range_keyword_completed(
                            keyword=keyword,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )
                        keyword_completed = True
                        break
                    if (not daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT):
                        self.on_time_range_keyword_completed(
                            keyword=keyword,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )
                        keyword_completed = True
                        break

                    try:
                        utils.logger.info(f"[BilibiliCrawler.search] search bilibili keyword: {keyword}, date: {day.ctime()}, page: {page}")
                        video_id_list: List[str] = []
                        videos_res = await self.bili_client.search_video_by_keyword(
                            keyword=keyword,
                            page=page,
                            page_size=bili_limit_count,
                            order=SearchOrderType.DEFAULT,
                            pubtime_begin_s=pubtime_begin_s,
                            pubtime_end_s=pubtime_end_s,
                        )
                        video_list: List[Dict] = videos_res.get("result")

                        if not video_list:
                            utils.logger.info(f"[BilibiliCrawler.search] No more videos for '{keyword}' on {day.ctime()}, moving to next day.")
                            next_day = day.date() + timedelta(days=1)
                            self.on_time_range_day_completed(
                                keyword=keyword,
                                day=day_str,
                                next_day=next_day.strftime("%Y-%m-%d") if next_day <= end_day_date else None,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                                reason="no_more_results",
                            )
                            break

                        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
                        task_list = [self.get_video_info_task(aid=video_item.get("aid"), bvid="", semaphore=semaphore) for video_item in video_list]
                        video_items = await asyncio.gather(*task_list)

                        for video_item in video_items:
                            if (daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT):
                                break
                            if (not daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT):
                                break
                            if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                                break

                            video_id = await self.process_video_item(video_item, semaphore)
                            if not video_id:
                                continue

                            notes_count_this_day += 1
                            total_notes_crawled_for_keyword += 1
                            video_id_list.append(video_id)

                        next_page = page + 1

                        # Sleep after page navigation
                        await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                        utils.logger.info(f"[BilibiliCrawler.search_by_keywords_in_time_range] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page}")

                        await self.batch_get_video_comments(video_id_list)
                        self.on_time_range_page_completed(
                            keyword=keyword,
                            day=day_str,
                            next_page=next_page,
                            notes_count_this_day=notes_count_this_day,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )
                        page = next_page
                        request_retry_count = 0
                        session_recovery_count = 0

                    except Exception as e:
                        error_detail = self.format_exception_details(e)
                        if isinstance(e, httpx.ConnectError):
                            if request_retry_count < 3:
                                request_retry_count += 1
                                utils.logger.warning(
                                    f"[BilibiliCrawler.search] ConnectError on {day.ctime()} page {page}, "
                                    f"same-process retry {request_retry_count}/3: {error_detail}"
                                )
                                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC * request_retry_count)
                                continue

                            if session_recovery_count < 2:
                                session_recovery_count += 1
                                request_retry_count = 0
                                utils.logger.warning(
                                    f"[BilibiliCrawler.search] ConnectError on {day.ctime()} page {page}, "
                                    f"recover session {session_recovery_count}/2: {error_detail}"
                                )
                                utils.logger.debug(traceback.format_exc())
                                await self.recover_session(e)
                                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                                continue

                            day_failed = True
                            day_failure_reason = f"connect_error:{error_detail}"
                            utils.logger.error(
                                f"[BilibiliCrawler.search] Mark day as failed after retries on {day.ctime()}: {error_detail}"
                            )
                            utils.logger.debug(traceback.format_exc())
                            break

                        if self.is_recoverable_session_error(e):
                            utils.logger.warning(
                                f"[BilibiliCrawler.search] Recoverable session error on {day.ctime()} page {page}: {error_detail}"
                            )
                            utils.logger.debug(traceback.format_exc())
                            await self.recover_session(e)
                            await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                            continue

                        utils.logger.error(f"[BilibiliCrawler.search] Error searching on {day.ctime()}: {error_detail}")
                        utils.logger.debug(traceback.format_exc())
                        day_failed = True
                        day_failure_reason = f"search_error:{error_detail}"
                        break

                if day_failed:
                    failed_day_encountered = True
                    next_day = day.date() + timedelta(days=1)
                    self.on_time_range_day_failed(
                        keyword=keyword,
                        day=day_str,
                        next_day=next_day.strftime("%Y-%m-%d") if next_day <= end_day_date else None,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        reason=day_failure_reason or "search_failed",
                    )

                if keyword_completed:
                    break

                resume_day = ""
                resume_page = start_page
                resume_notes_count_this_day = 0

            if not keyword_completed and not failed_day_encountered:
                self.on_time_range_keyword_completed(
                    keyword=keyword,
                    total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                )

    async def batch_get_video_comments(self, video_id_list: List[str]):
        """
        batch get video comments
        :param video_id_list:
        :return:
        """
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(f"[BilibiliCrawler.batch_get_note_comments] Crawling comment mode is not enabled")
            return

        utils.logger.info(f"[BilibiliCrawler.batch_get_video_comments] video ids:{video_id_list}")
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for video_id in video_id_list:
            task = asyncio.create_task(self.get_comments(video_id, semaphore), name=video_id)
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_comments(self, video_id: str, semaphore: asyncio.Semaphore):
        """
        get comment for video id
        :param video_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                utils.logger.info(f"[BilibiliCrawler.get_comments] begin get video_id: {video_id} comments ...")
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[BilibiliCrawler.get_comments] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching comments for video {video_id}")
                await self.bili_client.get_video_all_comments(
                    video_id=video_id,
                    crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
                    is_fetch_sub_comments=config.ENABLE_GET_SUB_COMMENTS,
                    callback=bilibili_store.batch_update_bilibili_video_comments,
                    max_count=getattr(
                        config,
                        "BILI_MAX_COMMENT_ITEMS_PER_VIDEO",
                        config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
                    ),
                    max_sub_comment_count=getattr(config, "BILI_MAX_SUB_COMMENTS_PER_VIDEO", 0),
                )

            except DataFetchError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_comments] get video_id: {video_id} comment error: {ex}")
            except Exception as e:
                utils.logger.error(f"[BilibiliCrawler.get_comments] may be been blocked, err:{e}")
                # Propagate the exception to be caught by the main loop
                raise

    async def get_creator_videos(self, creator_id: int):
        """
        get videos for a creator
        :return:
        """
        ps = 30
        pn = 1
        while True:
            result = await self.bili_client.get_creator_videos(creator_id, pn, ps)
            video_bvids_list = [video["bvid"] for video in result["list"]["vlist"]]
            await self.get_specified_videos(video_bvids_list)
            if int(result["page"]["count"]) <= pn * ps:
                break
            await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
            utils.logger.info(f"[BilibiliCrawler.get_creator_videos] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {pn}")
            pn += 1

    async def get_specified_videos(self, video_url_list: List[str]):
        """
        get specified videos info from URLs or BV IDs
        :param video_url_list: List of video URLs or BV IDs
        :return:
        """
        utils.logger.info("[BilibiliCrawler.get_specified_videos] Parsing video URLs...")
        bvids_list = []
        for video_url in video_url_list:
            try:
                video_info = parse_video_info_from_url(video_url)
                bvids_list.append(video_info.video_id)
                utils.logger.info(f"[BilibiliCrawler.get_specified_videos] Parsed video ID: {video_info.video_id} from {video_url}")
            except ValueError as e:
                utils.logger.error(f"[BilibiliCrawler.get_specified_videos] Failed to parse video URL: {e}")
                continue

        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [self.get_video_info_task(aid=0, bvid=video_id, semaphore=semaphore) for video_id in bvids_list]
        video_details = await asyncio.gather(*task_list)
        video_aids_list = []
        for video_detail in video_details:
            video_aid = await self.process_video_item(video_detail, semaphore)
            if video_aid:
                video_aids_list.append(video_aid)
        await self.batch_get_video_comments(video_aids_list)

    async def get_video_info_task(self, aid: int, bvid: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
        """
        Get video detail task
        :param aid:
        :param bvid:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                result = await self.bili_client.get_video_info(aid=aid, bvid=bvid)

                # Sleep after fetching video details
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[BilibiliCrawler.get_video_info_task] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching video details {bvid or aid}")

                return result
            except DataFetchError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_video_info_task] Get video detail error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_video_info_task] have not fund note detail video_id:{bvid}, err: {ex}")
                return None

    async def get_video_play_url_task(self, aid: int, cid: int, semaphore: asyncio.Semaphore) -> Union[Dict, None]:
        """
        Get video play url
        :param aid:
        :param cid:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                result = await self.bili_client.get_video_play_url(aid=aid, cid=cid)
                return result
            except DataFetchError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_video_play_url_task] Get video play url error: {ex}")
                return None
            except KeyError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_video_play_url_task] have not fund play url from :{aid}|{cid}, err: {ex}")
                return None

    async def create_bilibili_client(self, httpx_proxy: Optional[str]) -> BilibiliClient:
        """
        create bilibili client
        :param httpx_proxy: httpx proxy
        :return: bilibili client
        """
        utils.logger.info("[BilibiliCrawler.create_bilibili_client] Begin create bilibili API client ...")
        cookie_str, cookie_dict = utils.convert_cookies(await self.browser_context.cookies())
        bilibili_client_obj = BilibiliClient(
            proxy=httpx_proxy,
            headers={
                "User-Agent": self.user_agent,
                "Cookie": cookie_str,
                "Origin": "https://www.bilibili.com",
                "Referer": "https://www.bilibili.com",
                "Content-Type": "application/json;charset=UTF-8",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
            proxy_ip_pool=self.ip_proxy_pool,  # Pass proxy pool for automatic refresh
        )
        return bilibili_client_obj

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
                        f"[BilibiliCrawler.launch_browser] Created persistent context using "
                        f"{channel or 'bundled Chromium'}"
                    )
                    return browser_context

                browser = await chromium.launch(**launch_kwargs)  # type: ignore[arg-type]
                browser_context = await browser.new_context(**context_kwargs)
                utils.logger.info(
                    f"[BilibiliCrawler.launch_browser] Created browser context using "
                    f"{channel or 'bundled Chromium'}"
                )
                return browser_context
            except Exception as exc:
                last_error = exc
                utils.logger.warning(
                    f"[BilibiliCrawler.launch_browser] Failed to launch with "
                    f"{channel or 'bundled Chromium'}: {self.format_exception_details(exc)}"
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
        """
        launch browser and create browser context
        :param chromium: chromium browser
        :param playwright_proxy: playwright proxy
        :param user_agent: user agent
        :param headless: headless mode
        :return: browser context
        """
        utils.logger.info("[BilibiliCrawler.launch_browser] Begin create browser context ...")
        if config.SAVE_LOGIN_STATE:
            # feat issue #14
            # we will save login state to avoid login every time
            user_data_dir = os.path.join(os.getcwd(), "browser_data", config.USER_DATA_DIR % config.PLATFORM)  # type: ignore
            browser_context = await self.try_launch_browser_context(
                chromium,
                persistent=True,
                headless=headless,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
                user_data_dir=user_data_dir,
            )
            return browser_context
        else:
            browser_context = await self.try_launch_browser_context(
                chromium,
                persistent=False,
                headless=headless,
                playwright_proxy=playwright_proxy,
                user_agent=user_agent,
            )
            return browser_context

    async def launch_browser_with_cdp(
        self,
        playwright: Playwright,
        playwright_proxy: Optional[Dict],
        user_agent: Optional[str],
        headless: bool = True,
    ) -> BrowserContext:
        """
        Launch browser using CDP mode
        """
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
            utils.logger.info(f"[BilibiliCrawler] CDP browser info: {browser_info}")

            return browser_context

        except Exception as e:
            utils.logger.error(f"[BilibiliCrawler] CDP mode launch failed, fallback to standard mode: {e}")
            # Fallback to standard mode
            chromium = playwright.chromium
            return await self.launch_browser(chromium, playwright_proxy, user_agent, headless)

    async def close(self):
        """Close browser context"""
        try:
            # If using CDP mode, special handling is required
            if self.cdp_manager:
                await self.cdp_manager.cleanup()
                self.cdp_manager = None
            elif self.browser_context:
                await self.browser_context.close()
            utils.logger.info("[BilibiliCrawler.close] Browser context closed ...")
        except TargetClosedError:
            utils.logger.warning("[BilibiliCrawler.close] Browser context was already closed.")
        except Exception as e:
            utils.logger.error(f"[BilibiliCrawler.close] An error occurred during close: {e}")

    async def get_bilibili_video(self, video_item: Dict, semaphore: asyncio.Semaphore):
        """
        download bilibili video
        :param video_item:
        :param semaphore:
        :return:
        """
        if not config.ENABLE_GET_MEIDAS:
            utils.logger.info(f"[BilibiliCrawler.get_bilibili_video] Crawling image mode is not enabled")
            return
        video_item_view: Dict = video_item.get("View")
        aid = video_item_view.get("aid")
        cid = video_item_view.get("cid")
        result = await self.get_video_play_url_task(aid, cid, semaphore)
        if result is None:
            utils.logger.info("[BilibiliCrawler.get_bilibili_video] get video play url failed")
            return
        durl_list = result.get("durl")
        max_size = -1
        video_url = ""
        for durl in durl_list:
            size = durl.get("size")
            if size > max_size:
                max_size = size
                video_url = durl.get("url")
        if video_url == "":
            utils.logger.info("[BilibiliCrawler.get_bilibili_video] get video url failed")
            return

        content = await self.bili_client.get_video_media(video_url)
        await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
        utils.logger.info(f"[BilibiliCrawler.get_bilibili_video] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching video {aid}")
        if content is None:
            return
        extension_file_name = f"video.mp4"
        await bilibili_store.store_video(aid, content, extension_file_name)

    async def get_all_creator_details(self, creator_url_list: List[str]):
        """
        creator_url_list: get details for creator from creator URL list
        """
        utils.logger.info(f"[BilibiliCrawler.get_all_creator_details] Crawling the details of creators")
        utils.logger.info(f"[BilibiliCrawler.get_all_creator_details] Parsing creator URLs...")

        creator_id_list = []
        for creator_url in creator_url_list:
            try:
                creator_info = parse_creator_info_from_url(creator_url)
                creator_id_list.append(int(creator_info.creator_id))
                utils.logger.info(f"[BilibiliCrawler.get_all_creator_details] Parsed creator ID: {creator_info.creator_id} from {creator_url}")
            except ValueError as e:
                utils.logger.error(f"[BilibiliCrawler.get_all_creator_details] Failed to parse creator URL: {e}")
                continue

        utils.logger.info(f"[BilibiliCrawler.get_all_creator_details] creator ids:{creator_id_list}")

        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        try:
            for creator_id in creator_id_list:
                task = asyncio.create_task(self.get_creator_details(creator_id, semaphore), name=str(creator_id))
                task_list.append(task)
        except Exception as e:
            utils.logger.warning(f"[BilibiliCrawler.get_all_creator_details] error in the task list. The creator will not be included. {e}")

        await asyncio.gather(*task_list)

    async def get_creator_details(self, creator_id: int, semaphore: asyncio.Semaphore):
        """
        get details for creator id
        :param creator_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            creator_unhandled_info: Dict = await self.bili_client.get_creator_info(creator_id)
            creator_info: Dict = {
                "id": creator_id,
                "name": creator_unhandled_info.get("name"),
                "sign": creator_unhandled_info.get("sign"),
                "avatar": creator_unhandled_info.get("face"),
            }
        await self.get_fans(creator_info, semaphore)
        await self.get_followings(creator_info, semaphore)
        await self.get_dynamics(creator_info, semaphore)

    async def get_fans(self, creator_info: Dict, semaphore: asyncio.Semaphore):
        """
        get fans for creator id
        :param creator_info:
        :param semaphore:
        :return:
        """
        creator_id = creator_info["id"]
        async with semaphore:
            try:
                utils.logger.info(f"[BilibiliCrawler.get_fans] begin get creator_id: {creator_id} fans ...")
                await self.bili_client.get_creator_all_fans(
                    creator_info=creator_info,
                    crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
                    callback=bilibili_store.batch_update_bilibili_creator_fans,
                    max_count=config.CRAWLER_MAX_CONTACTS_COUNT_SINGLENOTES,
                )

            except DataFetchError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_fans] get creator_id: {creator_id} fans error: {ex}")
            except Exception as e:
                utils.logger.error(f"[BilibiliCrawler.get_fans] may be been blocked, err:{e}")

    async def get_followings(self, creator_info: Dict, semaphore: asyncio.Semaphore):
        """
        get followings for creator id
        :param creator_info:
        :param semaphore:
        :return:
        """
        creator_id = creator_info["id"]
        async with semaphore:
            try:
                utils.logger.info(f"[BilibiliCrawler.get_followings] begin get creator_id: {creator_id} followings ...")
                await self.bili_client.get_creator_all_followings(
                    creator_info=creator_info,
                    crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
                    callback=bilibili_store.batch_update_bilibili_creator_followings,
                    max_count=config.CRAWLER_MAX_CONTACTS_COUNT_SINGLENOTES,
                )

            except DataFetchError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_followings] get creator_id: {creator_id} followings error: {ex}")
            except Exception as e:
                utils.logger.error(f"[BilibiliCrawler.get_followings] may be been blocked, err:{e}")

    async def get_dynamics(self, creator_info: Dict, semaphore: asyncio.Semaphore):
        """
        get dynamics for creator id
        :param creator_info:
        :param semaphore:
        :return:
        """
        creator_id = creator_info["id"]
        async with semaphore:
            try:
                utils.logger.info(f"[BilibiliCrawler.get_dynamics] begin get creator_id: {creator_id} dynamics ...")
                await self.bili_client.get_creator_all_dynamics(
                    creator_info=creator_info,
                    crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
                    callback=bilibili_store.batch_update_bilibili_creator_dynamics,
                    max_count=config.CRAWLER_MAX_DYNAMICS_COUNT_SINGLENOTES,
                )

            except DataFetchError as ex:
                utils.logger.error(f"[BilibiliCrawler.get_dynamics] get creator_id: {creator_id} dynamics error: {ex}")
            except Exception as e:
                utils.logger.error(f"[BilibiliCrawler.get_dynamics] may be been blocked, err:{e}")

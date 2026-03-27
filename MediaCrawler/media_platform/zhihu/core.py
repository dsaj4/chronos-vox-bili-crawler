# -*- coding: utf-8 -*-
# Copyright (c) 2025 relakkes@gmail.com
#
# This file is part of MediaCrawler project.
# Repository: https://github.com/NanmiCoder/MediaCrawler/blob/main/media_platform/zhihu/core.py
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
import asyncio
import csv
import json
import os
from asyncio import Task
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, cast

from playwright.async_api import BrowserContext, BrowserType, Page, Playwright, async_playwright
from sqlalchemy import select

import config
from base.base_crawler import AbstractCrawler
from constant import zhihu as constant
from database.db_session import get_session
from database.models import ZhihuContent as ZhihuContentRecord
from model.m_zhihu import ZhihuContent, ZhihuCreator
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import zhihu as zhihu_store
from tools import utils
from tools.browser_launcher import BrowserLauncher
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import ZhiHuClient
from .exception import DataFetchError
from .field import SearchSort, SearchTime, SearchType
from .help import ZhihuExtractor, judge_zhihu_url
from .login import ZhiHuLogin


class ZhihuCrawler(AbstractCrawler):
    context_page: Optional[Page]
    zhihu_client: Optional[ZhiHuClient]
    browser_context: Optional[BrowserContext]
    cdp_manager: Optional[CDPBrowserManager]

    def __init__(self) -> None:
        self.index_url = "https://www.zhihu.com"
        self.user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        )
        self._extractor = ZhihuExtractor()
        self.cdp_manager = None
        self.ip_proxy_pool = None
        self.context_page = None
        self.zhihu_client = None
        self.browser_context = None
        self._playwright_manager = None
        self._playwright = None
        self.existing_content_ids: Set[str] = set()

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
    def normalize_content_id(content_id: object) -> str:
        return str(content_id or "").strip()

    def should_skip_content_id(self, content_id: object) -> bool:
        normalized_content_id = self.normalize_content_id(content_id)
        return bool(normalized_content_id) and normalized_content_id in self.existing_content_ids

    def remember_content_id(self, content_id: object) -> None:
        normalized_content_id = self.normalize_content_id(content_id)
        if normalized_content_id:
            self.existing_content_ids.add(normalized_content_id)

    def get_data_base_path(self, file_type: str) -> Path:
        if config.SAVE_DATA_PATH:
            return Path(config.SAVE_DATA_PATH) / "zhihu" / file_type
        return Path("data") / "zhihu" / file_type

    async def load_existing_content_ids(self) -> Set[str]:
        try:
            if config.SAVE_DATA_OPTION in {"db", "sqlite", "postgres"}:
                content_ids = await self.load_existing_content_ids_from_db()
            elif config.SAVE_DATA_OPTION == "mongodb":
                content_ids = await self.load_existing_content_ids_from_mongodb()
            elif config.SAVE_DATA_OPTION in {"json", "jsonl", "csv", "excel"}:
                content_ids = self.load_existing_content_ids_from_files(config.SAVE_DATA_OPTION)
            else:
                content_ids = set()
            utils.logger.info(
                f"[ZhihuCrawler.load_existing_content_ids] Loaded {len(content_ids)} existing content ids for resume mode"
            )
            return content_ids
        except Exception as exc:
            utils.logger.warning(f"[ZhihuCrawler.load_existing_content_ids] Failed to load existing content ids: {exc}")
            return set()

    async def load_existing_content_ids_from_db(self) -> Set[str]:
        async with get_session() as session:
            if session is None:
                return set()
            result = await session.execute(select(ZhihuContentRecord.content_id))
            return {self.normalize_content_id(content_id) for (content_id,) in result.all() if self.normalize_content_id(content_id)}

    async def load_existing_content_ids_from_mongodb(self) -> Set[str]:
        from database.mongodb_store_base import MongoDBStoreBase

        mongo_store = MongoDBStoreBase(collection_prefix="zhihu")
        rows = await mongo_store.find_many("contents", {})
        return {
            normalized
            for row in rows
            if isinstance(row, dict) and (normalized := self.normalize_content_id(row.get("content_id")))
        }

    def load_existing_content_ids_from_files(self, file_type: str) -> Set[str]:
        base_path = self.get_data_base_path(file_type)
        if not base_path.exists():
            return set()

        patterns = {
            "json": ["*_contents_*.json"],
            "jsonl": ["*_contents_*.jsonl"],
            "csv": ["*_contents_*.csv"],
            "excel": ["*_contents_*.xlsx"],
        }
        content_ids: Set[str] = set()
        for pattern in patterns.get(file_type, []):
            for file_path in base_path.glob(pattern):
                content_ids.update(self.extract_content_ids_from_file(file_path, file_type))
        return content_ids

    def extract_content_ids_from_file(self, file_path: Path, file_type: str) -> Set[str]:
        content_ids: Set[str] = set()
        try:
            if file_type == "json":
                with file_path.open("r", encoding="utf-8") as file_obj:
                    data = json.load(file_obj)
                if isinstance(data, dict):
                    data = [data]
                for item in data:
                    if isinstance(item, dict):
                        normalized = self.normalize_content_id(item.get("content_id"))
                        if normalized:
                            content_ids.add(normalized)
            elif file_type == "jsonl":
                with file_path.open("r", encoding="utf-8") as file_obj:
                    for line in file_obj:
                        line = line.strip()
                        if not line:
                            continue
                        item = json.loads(line)
                        if isinstance(item, dict):
                            normalized = self.normalize_content_id(item.get("content_id"))
                            if normalized:
                                content_ids.add(normalized)
            elif file_type == "csv":
                with file_path.open("r", encoding="utf-8-sig", newline="") as file_obj:
                    reader = csv.DictReader(file_obj)
                    for row in reader:
                        normalized = self.normalize_content_id((row or {}).get("content_id"))
                        if normalized:
                            content_ids.add(normalized)
            elif file_type == "excel":
                import pandas as pd

                dataframe = pd.read_excel(file_path)
                if "content_id" in dataframe.columns:
                    for content_id in dataframe["content_id"].tolist():
                        normalized = self.normalize_content_id(str(content_id) if pd.notna(content_id) else "")
                        if normalized:
                            content_ids.add(normalized)
        except Exception as exc:
            utils.logger.warning(
                f"[ZhihuCrawler.extract_content_ids_from_file] Failed to read existing content ids from {file_path}: {exc}"
            )
        return content_ids

    async def process_content_item(self, content_item: ZhihuContent) -> Optional[ZhihuContent]:
        if self.should_skip_content_id(content_item.content_id):
            utils.logger.info(
                f"[ZhihuCrawler.process_content_item] Skip duplicated content id: {content_item.content_id}"
            )
            return None
        self.remember_content_id(content_item.content_id)
        await zhihu_store.update_zhihu_content(content_item)
        return content_item

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
            utils.logger.info("[ZhihuCrawler] Launching browser in CDP mode")
            self.browser_context = await self.launch_browser_with_cdp(
                playwright,
                playwright_proxy_format,
                self.user_agent,
                headless=config.CDP_HEADLESS,
            )
        else:
            utils.logger.info("[ZhihuCrawler] Launching browser in standard mode")
            self.browser_context = await self.launch_browser(
                playwright.chromium,
                playwright_proxy_format,
                self.user_agent,
                headless=config.HEADLESS,
            )
            await self.browser_context.add_init_script(path="libs/stealth.min.js")

        self.context_page = await self.browser_context.new_page()
        await self.context_page.goto(self.index_url, wait_until="domcontentloaded")
        self.zhihu_client = await self.create_zhihu_client(httpx_proxy_format)
        self.existing_content_ids = await self.load_existing_content_ids()
        if not await self.zhihu_client.pong():
            login_obj = ZhiHuLogin(
                login_type=config.LOGIN_TYPE,
                login_phone="",
                browser_context=self.browser_context,
                context_page=self.context_page,
                cookie_str=config.COOKIES,
            )
            await login_obj.begin()
            await self.zhihu_client.update_cookies(browser_context=self.browser_context)

        utils.logger.info(
            "[ZhihuCrawler.prepare_session] Navigating to search page to get Zhihu search cookies, this process takes about 5 seconds"
        )
        await self.context_page.goto(
            f"{self.index_url}/search?q=python&search_source=Guess&utm_content=search_hot&type=content"
        )
        await asyncio.sleep(5)
        await self.zhihu_client.update_cookies(browser_context=self.browser_context)

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
                    f"[ZhihuCrawler.prepare_session_with_retries] Playwright startup failed with "
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
        utils.logger.info("[ZhihuCrawler.run_current_config] Zhihu crawler finished ...")

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
        self.zhihu_client = None
        if self._playwright_manager is not None:
            try:
                await self._playwright_manager.__aexit__(None, None, None)
            except Exception:
                pass
        self._playwright = None
        self._playwright_manager = None

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

    def get_stream_resume_state(self, keyword: str) -> Optional[Dict]:
        return None

    def on_stream_page_completed(
        self,
        *,
        keyword: str,
        next_page: int,
        pages_completed: int,
        contents_emitted: int,
        comments_emitted: int,
        last_emitted_day: Optional[str],
        last_result_created_day: Optional[str],
    ) -> None:
        return None

    def on_stream_keyword_completed(
        self,
        *,
        keyword: str,
        pages_completed: int,
        contents_emitted: int,
        comments_emitted: int,
        last_emitted_day: Optional[str],
        last_result_created_day: Optional[str],
        reason: str,
    ) -> None:
        return None

    def on_stream_failed(
        self,
        *,
        keyword: str,
        failed_page: int,
        pages_completed: int,
        contents_emitted: int,
        comments_emitted: int,
        last_emitted_day: Optional[str],
        last_result_created_day: Optional[str],
        reason: str,
    ) -> None:
        return None

    @staticmethod
    def supported_oldest_day() -> date:
        return date.today() - timedelta(days=364)

    @classmethod
    def resolve_search_time_for_day(cls, target_day: date) -> Optional[SearchTime]:
        age_days = (date.today() - target_day).days
        if age_days < 0:
            raise ValueError("target day cannot be in the future")
        if age_days == 0:
            return SearchTime.ONE_DAY
        if age_days <= 6:
            return SearchTime.ONE_WEEK
        if age_days <= 29:
            return SearchTime.ONE_MONTH
        if age_days <= 89:
            return SearchTime.THREE_MONTH
        if age_days <= 181:
            return SearchTime.HALF_YEAR
        if age_days <= 364:
            return SearchTime.ONE_YEAR
        return None

    @staticmethod
    def iter_days(start_day: date, end_day: date) -> Iterable[date]:
        current_day = start_day
        while current_day <= end_day:
            yield current_day
            current_day += timedelta(days=1)

    @staticmethod
    def get_content_created_date(content: ZhihuContent) -> Optional[date]:
        created_time = int(content.created_time or 0)
        if created_time <= 0:
            return None
        if created_time > 1_000_000_000_000:
            created_time //= 1000
        try:
            return datetime.fromtimestamp(created_time).date()
        except (OSError, OverflowError, ValueError):
            return None

    @staticmethod
    def get_content_created_day_str(content: ZhihuContent) -> Optional[str]:
        content_day = ZhihuCrawler.get_content_created_date(content)
        return content_day.isoformat() if content_day else None

    async def process_content_item_for_output_day(
        self,
        content_item: ZhihuContent,
        *,
        output_day: str,
    ) -> Optional[ZhihuContent]:
        previous_output_day = getattr(config, "OUTPUT_DATE_OVERRIDE", "")
        config.OUTPUT_DATE_OVERRIDE = output_day
        try:
            return await self.process_content_item(content_item)
        finally:
            config.OUTPUT_DATE_OVERRIDE = previous_output_day

    @staticmethod
    def group_content_items_by_output_day(content_list: List[ZhihuContent]) -> Dict[str, List[ZhihuContent]]:
        grouped: Dict[str, List[ZhihuContent]] = {}
        for content_item in content_list:
            output_day = ZhihuCrawler.get_content_created_day_str(content_item)
            if not output_day:
                continue
            grouped.setdefault(output_day, []).append(content_item)
        return grouped

    async def search(self) -> None:
        search_mode = getattr(config, "ZHIHU_SEARCH_MODE", "normal")
        if search_mode == "normal":
            await self.search_by_keywords()
        elif search_mode == "all_in_time_range":
            await self.search_by_keywords_in_time_range(daily_limit=False)
        elif search_mode == "daily_limit_in_time_range":
            await self.search_by_keywords_in_time_range(daily_limit=True)
        elif search_mode == "one_year_stream_bucketed":
            await self.search_by_keywords_in_one_year_stream_bucketed()
        else:
            utils.logger.warning(f"[ZhihuCrawler.search] Unknown ZHIHU_SEARCH_MODE: {search_mode}")

    async def search_by_keywords(self) -> None:
        utils.logger.info("[ZhihuCrawler.search_by_keywords] Begin search zhihu keywords")
        zhihu_limit_count = 20
        if config.CRAWLER_MAX_NOTES_COUNT < zhihu_limit_count:
            config.CRAWLER_MAX_NOTES_COUNT = zhihu_limit_count
        start_page = config.START_PAGE
        for keyword in config.KEYWORDS.split(","):
            keyword = keyword.strip()
            if not keyword:
                continue
            source_keyword_var.set(keyword)
            utils.logger.info(f"[ZhihuCrawler.search_by_keywords] Current search keyword: {keyword}")
            page = 1
            while (page - start_page + 1) * zhihu_limit_count <= config.CRAWLER_MAX_NOTES_COUNT:
                if page < start_page:
                    utils.logger.info(f"[ZhihuCrawler.search_by_keywords] Skip page {page}")
                    page += 1
                    continue

                try:
                    utils.logger.info(
                        f"[ZhihuCrawler.search_by_keywords] Search zhihu keyword: {keyword}, page: {page}"
                    )
                    content_list = await self.zhihu_client.get_note_by_keyword(keyword=keyword, page=page)
                    if not content_list:
                        utils.logger.info("[ZhihuCrawler.search_by_keywords] No more content")
                        break

                    page += 1
                    processed_content_list: List[ZhihuContent] = []
                    for content in content_list:
                        processed_content = await self.process_content_item(content)
                        if processed_content:
                            processed_content_list.append(processed_content)

                    await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                    utils.logger.info(
                        f"[ZhihuCrawler.search_by_keywords] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page-1}"
                    )
                    await self.batch_get_content_comments(processed_content_list)
                except DataFetchError as exc:
                    utils.logger.error(
                        f"[ZhihuCrawler.search_by_keywords] Search content error: {self.format_exception_details(exc)}"
                    )
                    return

    async def search_by_keywords_in_time_range(self, daily_limit: bool) -> None:
        utils.logger.info(
            f"[ZhihuCrawler.search_by_keywords_in_time_range] Begin search with daily_limit={daily_limit}"
        )
        zhihu_limit_count = 20
        start_page = config.START_PAGE
        start_day_date = datetime.strptime(config.START_DAY, "%Y-%m-%d").date()
        end_day_date = datetime.strptime(config.END_DAY, "%Y-%m-%d").date()
        if start_day_date > end_day_date:
            raise ValueError("START_DAY cannot be later than END_DAY")

        for keyword in config.KEYWORDS.split(","):
            keyword = keyword.strip()
            if not keyword:
                continue

            source_keyword_var.set(keyword)
            utils.logger.info(
                f"[ZhihuCrawler.search_by_keywords_in_time_range] Current search keyword: {keyword}"
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
                    f"[ZhihuCrawler.search_by_keywords_in_time_range] Resuming keyword '{keyword}' from {resume_day} page {resume_page}"
                )

            for day_value in self.iter_days(start_day_date, end_day_date):
                day_str = day_value.isoformat()
                if resume_day and day_str < resume_day:
                    utils.logger.info(
                        f"[ZhihuCrawler.search_by_keywords_in_time_range] Skip completed day {day_str} for keyword '{keyword}'"
                    )
                    continue

                if total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT:
                    utils.logger.info(
                        f"[ZhihuCrawler.search_by_keywords_in_time_range] Reached CRAWLER_MAX_NOTES_COUNT limit for keyword '{keyword}'"
                    )
                    self.on_time_range_keyword_completed(
                        keyword=keyword,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                    )
                    keyword_completed = True
                    break

                search_time = self.resolve_search_time_for_day(day_value)
                if search_time is None:
                    reason = (
                        f"target day {day_str} is older than Zhihu's supported search window "
                        f"({self.supported_oldest_day().isoformat()} and later)"
                    )
                    utils.logger.warning(
                        f"[ZhihuCrawler.search_by_keywords_in_time_range] {reason}"
                    )
                    self.on_time_range_day_failed(
                        keyword=keyword,
                        day=day_str,
                        next_day=(day_value + timedelta(days=1)).isoformat() if day_value < end_day_date else None,
                        total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        reason=reason,
                    )
                    failed_day_encountered = True
                    break

                page = resume_page if resume_day == day_str else start_page
                notes_count_this_day = resume_notes_count_this_day if resume_day == day_str else 0
                day_failed = False
                day_failure_reason = ""
                config.OUTPUT_DATE_OVERRIDE = day_str

                while True:
                    if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                        utils.logger.info(
                            f"[ZhihuCrawler.search_by_keywords_in_time_range] Reached MAX_NOTES_PER_DAY limit for {day_str}"
                        )
                        next_day = day_value + timedelta(days=1)
                        self.on_time_range_day_completed(
                            keyword=keyword,
                            day=day_str,
                            next_day=next_day.isoformat() if next_day <= end_day_date else None,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                            reason="max_notes_per_day_reached",
                        )
                        break

                    if total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT:
                        self.on_time_range_keyword_completed(
                            keyword=keyword,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )
                        keyword_completed = True
                        break

                    try:
                        utils.logger.info(
                            f"[ZhihuCrawler.search_by_keywords_in_time_range] Search zhihu keyword: {keyword}, date: {day_str}, page: {page}, search_time={search_time.value or 'default'}"
                        )
                        content_list = await self.zhihu_client.get_note_by_keyword(
                            keyword=keyword,
                            page=page,
                            sort=SearchSort.CREATE_TIME,
                            search_time=search_time,
                        )
                        if not content_list:
                            utils.logger.info(
                                f"[ZhihuCrawler.search_by_keywords_in_time_range] No more content for '{keyword}' on {day_str}"
                            )
                            next_day = day_value + timedelta(days=1)
                            self.on_time_range_day_completed(
                                keyword=keyword,
                                day=day_str,
                                next_day=next_day.isoformat() if next_day <= end_day_date else None,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                                reason="no_more_results",
                            )
                            break

                        processed_content_list: List[ZhihuContent] = []
                        saw_younger_content = False
                        saw_target_content = False
                        saw_older_content = False

                        for content in content_list:
                            content_day = self.get_content_created_date(content)
                            if content_day is None:
                                continue
                            if content_day > day_value:
                                saw_younger_content = True
                                continue
                            if content_day < day_value:
                                saw_older_content = True
                                continue

                            saw_target_content = True
                            processed_content = await self.process_content_item(content)
                            if not processed_content:
                                continue

                            processed_content_list.append(processed_content)
                            notes_count_this_day += 1
                            total_notes_crawled_for_keyword += 1

                            if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                                break
                            if daily_limit and total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT:
                                break

                        await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                        utils.logger.info(
                            f"[ZhihuCrawler.search_by_keywords_in_time_range] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page}"
                        )
                        await self.batch_get_content_comments(processed_content_list)

                        next_page = page + 1
                        self.on_time_range_page_completed(
                            keyword=keyword,
                            day=day_str,
                            next_page=next_page,
                            notes_count_this_day=notes_count_this_day,
                            total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                        )

                        if notes_count_this_day >= config.MAX_NOTES_PER_DAY:
                            next_day = day_value + timedelta(days=1)
                            self.on_time_range_day_completed(
                                keyword=keyword,
                                day=day_str,
                                next_day=next_day.isoformat() if next_day <= end_day_date else None,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                                reason="max_notes_per_day_reached",
                            )
                            break

                        if total_notes_crawled_for_keyword >= config.CRAWLER_MAX_NOTES_COUNT:
                            self.on_time_range_keyword_completed(
                                keyword=keyword,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                            )
                            keyword_completed = True
                            break

                        if saw_older_content or (not saw_target_content and not saw_younger_content):
                            next_day = day_value + timedelta(days=1)
                            self.on_time_range_day_completed(
                                keyword=keyword,
                                day=day_str,
                                next_day=next_day.isoformat() if next_day <= end_day_date else None,
                                total_notes_crawled_for_keyword=total_notes_crawled_for_keyword,
                                reason="reached_older_content_boundary",
                            )
                            break

                        page = next_page
                    except Exception as exc:
                        day_failed = True
                        day_failure_reason = self.format_exception_details(exc)
                        utils.logger.error(
                            f"[ZhihuCrawler.search_by_keywords_in_time_range] Error searching {day_str} page {page}: {day_failure_reason}"
                        )
                        break

                if day_failed:
                    failed_day_encountered = True
                    next_day = day_value + timedelta(days=1)
                    self.on_time_range_day_failed(
                        keyword=keyword,
                        day=day_str,
                        next_day=next_day.isoformat() if next_day <= end_day_date else None,
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

    async def search_by_keywords_in_one_year_stream_bucketed(self) -> None:
        utils.logger.info(
            "[ZhihuCrawler.search_by_keywords_in_one_year_stream_bucketed] "
            "Begin one-year streaming search with real-time daily bucketing"
        )
        start_page = max(int(getattr(config, "START_PAGE", 1) or 1), 1)

        for keyword in config.KEYWORDS.split(","):
            source_keyword_var.set(keyword)
            utils.logger.info(
                "[ZhihuCrawler.search_by_keywords_in_one_year_stream_bucketed] "
                f"Current search keyword: {keyword}"
            )
            resume_state = self.get_stream_resume_state(keyword) or {}
            page = max(int(resume_state.get("resume_page") or start_page), 1)
            pages_completed = max(int(resume_state.get("pages_completed") or 0), 0)
            contents_emitted = max(
                int(
                    resume_state.get("contents_emitted")
                    or resume_state.get("total_notes_crawled_for_keyword")
                    or 0
                ),
                0,
            )
            comments_emitted = max(int(resume_state.get("comments_emitted") or 0), 0)
            last_emitted_day = (
                str(resume_state.get("last_emitted_day") or resume_state.get("current_day") or "").strip()
                or None
            )
            last_result_created_day = (
                str(resume_state.get("last_result_created_day") or "").strip() or None
            )

            while True:
                try:
                    utils.logger.info(
                        "[ZhihuCrawler.search_by_keywords_in_one_year_stream_bucketed] "
                        f"Search zhihu keyword: {keyword}, page: {page}, search_time={SearchTime.ONE_YEAR.value}"
                    )
                    content_list = await self.zhihu_client.get_note_by_keyword(
                        keyword=keyword,
                        page=page,
                        sort=SearchSort.CREATE_TIME,
                        note_type=SearchType.DEFAULT,
                        search_time=SearchTime.ONE_YEAR,
                    )
                    if not content_list:
                        self.on_stream_keyword_completed(
                            keyword=keyword,
                            pages_completed=pages_completed,
                            contents_emitted=contents_emitted,
                            comments_emitted=comments_emitted,
                            last_emitted_day=last_emitted_day,
                            last_result_created_day=last_result_created_day,
                            reason="no_more_results",
                        )
                        break

                    processed_content_list: List[ZhihuContent] = []
                    for content in content_list:
                        content_day = self.get_content_created_day_str(content)
                        if not content_day:
                            continue
                        last_result_created_day = content_day
                        processed_content = await self.process_content_item_for_output_day(
                            content,
                            output_day=content_day,
                        )
                        if not processed_content:
                            continue
                        processed_content_list.append(processed_content)
                        contents_emitted += 1
                        last_emitted_day = content_day

                    await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                    utils.logger.info(
                        "[ZhihuCrawler.search_by_keywords_in_one_year_stream_bucketed] "
                        f"Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after page {page}"
                    )
                    comments_emitted += await self.batch_get_content_comments_by_output_day(
                        processed_content_list
                    )

                    pages_completed += 1
                    next_page = page + 1
                    self.on_stream_page_completed(
                        keyword=keyword,
                        next_page=next_page,
                        pages_completed=pages_completed,
                        contents_emitted=contents_emitted,
                        comments_emitted=comments_emitted,
                        last_emitted_day=last_emitted_day,
                        last_result_created_day=last_result_created_day,
                    )
                    page = next_page
                except Exception as exc:
                    failure_reason = self.format_exception_details(exc)
                    utils.logger.error(
                        "[ZhihuCrawler.search_by_keywords_in_one_year_stream_bucketed] "
                        f"Error searching keyword {keyword} page {page}: {failure_reason}"
                    )
                    self.on_stream_failed(
                        keyword=keyword,
                        failed_page=page,
                        pages_completed=pages_completed,
                        contents_emitted=contents_emitted,
                        comments_emitted=comments_emitted,
                        last_emitted_day=last_emitted_day,
                        last_result_created_day=last_result_created_day,
                        reason=failure_reason,
                    )
                    raise

    async def batch_get_content_comments_by_output_day(self, content_list: List[ZhihuContent]) -> int:
        total_comments = 0
        previous_output_day = getattr(config, "OUTPUT_DATE_OVERRIDE", "")
        try:
            for output_day, grouped_content in self.group_content_items_by_output_day(content_list).items():
                config.OUTPUT_DATE_OVERRIDE = output_day
                total_comments += await self.batch_get_content_comments(grouped_content)
        finally:
            config.OUTPUT_DATE_OVERRIDE = previous_output_day
        return total_comments

    async def batch_get_content_comments(self, content_list: List[ZhihuContent]) -> int:
        """
        Batch get content comments
        Args:
            content_list:

        Returns:

        """
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(
                f"[ZhihuCrawler.batch_get_content_comments] Crawling comment mode is not enabled"
            )
            return 0

        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for content_item in content_list:
            task = asyncio.create_task(
                self.get_comments(content_item, semaphore), name=content_item.content_id
            )
            task_list.append(task)
        comment_counts = await asyncio.gather(*task_list)
        return sum(int(comment_count or 0) for comment_count in comment_counts)

    async def get_comments(
        self, content_item: ZhihuContent, semaphore: asyncio.Semaphore
    ) -> int:
        """
        Get note comments with keyword filtering and quantity limitation
        Args:
            content_item:
            semaphore:

        Returns:

        """
        async with semaphore:
            utils.logger.info(
                f"[ZhihuCrawler.get_comments] Begin get note id comments {content_item.content_id}"
            )

            # Sleep before fetching comments
            await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
            utils.logger.info(f"[ZhihuCrawler.get_comments] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds before fetching comments for content {content_item.content_id}")

            all_comments = await self.zhihu_client.get_note_all_comments(
                content=content_item,
                crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
                callback=zhihu_store.batch_update_zhihu_note_comments,
            )
            return len(all_comments)

    async def get_creators_and_notes(self) -> None:
        """
        Get creator's information and their notes and comments
        Returns:

        """
        utils.logger.info("[ZhihuCrawler.get_creators_and_notes] Begin get zhihu creators")
        for user_link in config.ZHIHU_CREATOR_URL_LIST:
            utils.logger.info(
                f"[ZhihuCrawler.get_creators_and_notes] Begin get creator {user_link}"
            )
            user_url_token = user_link.split("/")[-1]
            # get creator detail info from web html content
            createor_info: ZhihuCreator = await self.zhihu_client.get_creator_info(
                url_token=user_url_token
            )
            if not createor_info:
                utils.logger.info(
                    f"[ZhihuCrawler.get_creators_and_notes] Creator {user_url_token} not found"
                )
                continue

            utils.logger.info(
                f"[ZhihuCrawler.get_creators_and_notes] Creator info: {createor_info}"
            )
            await zhihu_store.save_creator(creator=createor_info)

            # By default, only answer information is extracted, uncomment below if articles and videos are needed

            # Get all anwser information of the creator
            all_content_list = await self.zhihu_client.get_all_anwser_by_creator(
                creator=createor_info,
                crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
                callback=zhihu_store.batch_update_zhihu_contents,
            )

            # Get all articles of the creator's contents
            # all_content_list = await self.zhihu_client.get_all_articles_by_creator(
            #     creator=createor_info,
            #     crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
            #     callback=zhihu_store.batch_update_zhihu_contents
            # )

            # Get all videos of the creator's contents
            # all_content_list = await self.zhihu_client.get_all_videos_by_creator(
            #     creator=createor_info,
            #     crawl_interval=config.CRAWLER_MAX_SLEEP_SEC,
            #     callback=zhihu_store.batch_update_zhihu_contents
            # )

            # Get all comments of the creator's contents
            await self.batch_get_content_comments(all_content_list)

    async def get_note_detail(
        self, full_note_url: str, semaphore: asyncio.Semaphore
    ) -> Optional[ZhihuContent]:
        """
        Get note detail
        Args:
            full_note_url: str
            semaphore:

        Returns:

        """
        async with semaphore:
            utils.logger.info(
                f"[ZhihuCrawler.get_specified_notes] Begin get specified note {full_note_url}"
            )
            # Judge note type
            note_type: str = judge_zhihu_url(full_note_url)
            if note_type == constant.ANSWER_NAME:
                question_id = full_note_url.split("/")[-3]
                answer_id = full_note_url.split("/")[-1]
                utils.logger.info(
                    f"[ZhihuCrawler.get_specified_notes] Get answer info, question_id: {question_id}, answer_id: {answer_id}"
                )
                result = await self.zhihu_client.get_answer_info(question_id, answer_id)

                # Sleep after fetching answer details
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[ZhihuCrawler.get_note_detail] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching answer details {answer_id}")

                return result

            elif note_type == constant.ARTICLE_NAME:
                article_id = full_note_url.split("/")[-1]
                utils.logger.info(
                    f"[ZhihuCrawler.get_specified_notes] Get article info, article_id: {article_id}"
                )
                result = await self.zhihu_client.get_article_info(article_id)

                # Sleep after fetching article details
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[ZhihuCrawler.get_note_detail] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching article details {article_id}")

                return result

            elif note_type == constant.VIDEO_NAME:
                video_id = full_note_url.split("/")[-1]
                utils.logger.info(
                    f"[ZhihuCrawler.get_specified_notes] Get video info, video_id: {video_id}"
                )
                result = await self.zhihu_client.get_video_info(video_id)

                # Sleep after fetching video details
                await asyncio.sleep(config.CRAWLER_MAX_SLEEP_SEC)
                utils.logger.info(f"[ZhihuCrawler.get_note_detail] Sleeping for {config.CRAWLER_MAX_SLEEP_SEC} seconds after fetching video details {video_id}")

                return result

    async def get_specified_notes(self):
        """
        Get the information and comments of the specified post
        Returns:

        """
        get_note_detail_task_list = []
        for full_note_url in config.ZHIHU_SPECIFIED_ID_LIST:
            # remove query params
            full_note_url = full_note_url.split("?")[0]
            crawler_task = self.get_note_detail(
                full_note_url=full_note_url,
                semaphore=asyncio.Semaphore(config.MAX_CONCURRENCY_NUM),
            )
            get_note_detail_task_list.append(crawler_task)

        need_get_comment_notes: List[ZhihuContent] = []
        note_details = await asyncio.gather(*get_note_detail_task_list)
        for index, note_detail in enumerate(note_details):
            if not note_detail:
                utils.logger.info(
                    f"[ZhihuCrawler.get_specified_notes] Note {config.ZHIHU_SPECIFIED_ID_LIST[index]} not found"
                )
                continue

            note_detail = cast(ZhihuContent, note_detail)  # only for type check
            need_get_comment_notes.append(note_detail)
            await zhihu_store.update_zhihu_content(note_detail)

        await self.batch_get_content_comments(need_get_comment_notes)

    async def create_zhihu_client(self, httpx_proxy: Optional[str]) -> ZhiHuClient:
        """Create zhihu client"""
        utils.logger.info(
            "[ZhihuCrawler.create_zhihu_client] Begin create zhihu API client ..."
        )
        cookie_str, cookie_dict = utils.convert_cookies(
            await self.browser_context.cookies()
        )
        zhihu_client_obj = ZhiHuClient(
            proxy=httpx_proxy,
            headers={
                "accept": "*/*",
                "accept-language": "zh-CN,zh;q=0.9",
                "cookie": cookie_str,
                "priority": "u=1, i",
                "referer": "https://www.zhihu.com/search?q=python&time_interval=a_year&type=content",
                "user-agent": self.user_agent,
                "x-api-version": "3.0.91",
                "x-app-za": "OS=Web",
                "x-requested-with": "fetch",
                "x-zse-93": "101_3_3.0",
            },
            playwright_page=self.context_page,
            cookie_dict=cookie_dict,
            proxy_ip_pool=self.ip_proxy_pool,  # Pass proxy pool for automatic refresh
        )
        return zhihu_client_obj

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
                        f"[ZhihuCrawler.launch_browser] Created persistent context using {channel or 'bundled Chromium'}"
                    )
                    return browser_context

                browser = await chromium.launch(**launch_kwargs)  # type: ignore[arg-type]
                browser_context = await browser.new_context(**context_kwargs)
                utils.logger.info(
                    f"[ZhihuCrawler.launch_browser] Created browser context using {channel or 'bundled Chromium'}"
                )
                return browser_context
            except Exception as exc:
                last_error = exc
                utils.logger.warning(
                    f"[ZhihuCrawler.launch_browser] Failed to launch with {channel or 'bundled Chromium'}: "
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
        utils.logger.info("[ZhihuCrawler.launch_browser] Begin create browser context ...")
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
            utils.logger.info(f"[ZhihuCrawler] CDP browser info: {browser_info}")

            return browser_context

        except Exception as e:
            utils.logger.error(f"[ZhihuCrawler] CDP mode launch failed, falling back to standard mode: {e}")
            chromium = playwright.chromium
            return await self.launch_browser(chromium, playwright_proxy, user_agent, headless)

    async def close(self):
        await self.close_session()
        utils.logger.info("[ZhihuCrawler.close] Browser context closed ...")

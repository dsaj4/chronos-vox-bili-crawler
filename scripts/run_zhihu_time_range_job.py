from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, timedelta
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
from media_platform.zhihu.core import ZhihuCrawler  # noqa: E402


DEFAULT_CHECKPOINT_PATH = ROOT_DIR / "artifacts" / "zhihu_crawl_monitor" / "checkpoint.json"
PREFERRED_JOB_PATH = ROOT_DIR / "artifacts" / "zhihu_crawl_monitor" / "preferred_job.json"
STREAM_SEARCH_MODE = "one_year_stream_bucketed"
DEFAULT_SEARCH_MODE = STREAM_SEARCH_MODE
STREAM_RESUME_OVERLAP_PAGES = 3
DAILY_SEARCH_MODES = {"daily_limit_in_time_range", "all_in_time_range"}


def configure_console_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def is_job_long_backfill(job: dict[str, Any]) -> bool:
    return bool(job.get("continuous_backfill")) and bool(parse_iso_date(str(job.get("oldest_day") or "")))


def supported_oldest_day() -> date:
    return ZhihuCrawler.supported_oldest_day()


def same_job_series(args: argparse.Namespace, job: dict[str, Any]) -> bool:
    return (
        str(job.get("platform") or "") == "zhihu"
        and str(job.get("keyword") or "") == args.keyword
        and str(job.get("search_mode") or DEFAULT_SEARCH_MODE) == args.search_mode
        and bool(job.get("continuous_backfill")) == bool(args.continuous_backfill)
        and str(job.get("save_data_option") or "") == str(args.save_data_option)
        and str(job.get("save_data_path") or "") == str(Path(args.save_data_path).resolve())
    )


def choose_preferred_job(*jobs: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [job for job in jobs if isinstance(job, dict) and is_job_long_backfill(job)]
    if not candidates:
        return None
    best = candidates[0]
    for candidate in candidates[1:]:
        best_oldest = parse_iso_date(str(best.get("oldest_day") or ""))
        candidate_oldest = parse_iso_date(str(candidate.get("oldest_day") or ""))
        if best_oldest is None or (candidate_oldest is not None and candidate_oldest <= best_oldest):
            best = candidate
    return best


def parse_args() -> argparse.Namespace:
    today = date.today().isoformat()
    parser = argparse.ArgumentParser(description="Run a Zhihu crawl job with checkpointed recovery.")
    parser.add_argument("--keyword", default="ai")
    parser.add_argument("--start-day", default=today)
    parser.add_argument("--end-day", default=today)
    parser.add_argument("--oldest-day", default=supported_oldest_day().isoformat())
    parser.add_argument("--continuous-backfill", action="store_true")
    parser.add_argument(
        "--search-mode",
        choices=[STREAM_SEARCH_MODE, "daily_limit_in_time_range", "all_in_time_range"],
        default=DEFAULT_SEARCH_MODE,
    )
    parser.add_argument("--save-data-option", default="json")
    parser.add_argument("--save-data-path", default=str(ROOT_DIR / "artifacts" / "ai_crawl_data"))
    parser.add_argument("--checkpoint-path", default=str(DEFAULT_CHECKPOINT_PATH))
    parser.add_argument("--login-type", default="qrcode")
    parser.add_argument("--max-notes-per-day", type=int, default=5)
    parser.add_argument("--max-comment-items", type=int, default=500)
    parser.add_argument("--max-sub-comment-items", type=int, default=50)
    parser.add_argument("--max-concurrency-num", type=int, default=1)
    parser.add_argument("--crawler-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--disable-cdp", action="store_true")
    parser.add_argument("--disable-comments", action="store_true")
    parser.add_argument("--disable-sub-comments", action="store_true")
    return parser.parse_args()


def adopt_checkpoint_job_if_preferred(args: argparse.Namespace, checkpoint_path: Path) -> argparse.Namespace:
    checkpoint_job: dict[str, Any] = {}
    if checkpoint_path.exists():
        try:
            checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            checkpoint_job = checkpoint.get("job") if isinstance(checkpoint.get("job"), dict) else {}
        except json.JSONDecodeError:
            checkpoint_job = {}

    preferred_job: dict[str, Any] = {}
    if PREFERRED_JOB_PATH.exists():
        try:
            preferred_job = json.loads(PREFERRED_JOB_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            preferred_job = {}

    incoming_job = {
        "platform": "zhihu",
        "keyword": args.keyword,
        "search_mode": args.search_mode,
        "start_day": args.start_day,
        "end_day": args.end_day,
        "oldest_day": args.oldest_day,
        "continuous_backfill": args.continuous_backfill,
        "save_data_option": args.save_data_option,
        "save_data_path": str(Path(args.save_data_path).resolve()),
    }
    selected_job = choose_preferred_job(checkpoint_job, preferred_job, incoming_job)
    if not selected_job or not same_job_series(args, selected_job):
        return args
    if selected_job == incoming_job:
        return args

    adopted = argparse.Namespace(**vars(args))
    adopted.keyword = str(selected_job.get("keyword") or adopted.keyword)
    adopted.search_mode = str(selected_job.get("search_mode") or adopted.search_mode)
    adopted.start_day = str(selected_job.get("start_day") or adopted.start_day)
    adopted.end_day = str(selected_job.get("end_day") or adopted.end_day)
    adopted.oldest_day = str(selected_job.get("oldest_day") or adopted.oldest_day)
    adopted.continuous_backfill = bool(selected_job.get("continuous_backfill"))
    adopted.save_data_option = str(selected_job.get("save_data_option") or adopted.save_data_option)
    adopted.save_data_path = str(selected_job.get("save_data_path") or adopted.save_data_path)
    print(
        {
            "event": "adopt_checkpoint_job",
            "reason": "preserve_existing_long_backfill",
            "keyword": adopted.keyword,
            "search_mode": adopted.search_mode,
            "start_day": adopted.start_day,
            "end_day": adopted.end_day,
            "oldest_day": adopted.oldest_day,
            "continuous_backfill": adopted.continuous_backfill,
        },
        flush=True,
    )
    return adopted


class CheckpointedZhihuCrawler(ZhihuCrawler):
    def __init__(self, checkpoint_path: Path, args: argparse.Namespace):
        super().__init__()
        self.checkpoint_path = checkpoint_path
        self.args = args
        self.current_day = args.start_day
        self.stream_resume_overlap_pages = STREAM_RESUME_OVERLAP_PAGES

    @property
    def uses_stream_search(self) -> bool:
        return self.args.search_mode == STREAM_SEARCH_MODE

    def job_identity(self) -> dict[str, Any]:
        return {
            "platform": "zhihu",
            "keyword": self.args.keyword,
            "search_mode": self.args.search_mode,
            "start_day": self.args.start_day,
            "end_day": self.args.end_day,
            "oldest_day": self.args.oldest_day,
            "continuous_backfill": self.args.continuous_backfill,
            "save_data_option": self.args.save_data_option,
            "save_data_path": str(Path(self.args.save_data_path).resolve()),
            "stream_resume_overlap_pages": self.stream_resume_overlap_pages,
        }

    def read_checkpoint(self) -> dict[str, Any]:
        if not self.checkpoint_path.exists():
            return {}
        try:
            return json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def write_checkpoint(self, payload: dict[str, Any]) -> None:
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        payload["updated_at"] = datetime.now().astimezone().isoformat(timespec="seconds")
        payload["job"] = self.job_identity()
        tmp_path = self.checkpoint_path.with_suffix(self.checkpoint_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.checkpoint_path)

    @staticmethod
    def previous_day(day_str: str) -> str:
        return (datetime.strptime(day_str, "%Y-%m-%d").date() - timedelta(days=1)).isoformat()

    def is_before_oldest_day(self, day_str: str) -> bool:
        return datetime.strptime(day_str, "%Y-%m-%d").date() < datetime.strptime(self.args.oldest_day, "%Y-%m-%d").date()

    def checkpoint_matches_current_job(self, checkpoint_job: dict[str, Any]) -> bool:
        if not checkpoint_job:
            return False
        current_job = self.job_identity()
        base_matches = (
            checkpoint_job.get("platform") == current_job.get("platform")
            and checkpoint_job.get("keyword") == current_job.get("keyword")
            and checkpoint_job.get("search_mode", DEFAULT_SEARCH_MODE) == current_job.get("search_mode")
            and checkpoint_job.get("save_data_option") == current_job.get("save_data_option")
            and checkpoint_job.get("save_data_path") == current_job.get("save_data_path")
            and bool(checkpoint_job.get("continuous_backfill")) == bool(current_job.get("continuous_backfill"))
        )
        if not base_matches:
            return False
        if self.uses_stream_search:
            return True
        return (
            checkpoint_job.get("start_day") == current_job.get("start_day")
            and checkpoint_job.get("end_day") == current_job.get("end_day")
            and checkpoint_job.get("oldest_day") == current_job.get("oldest_day")
        )

    def checkpoint_base_payload(self) -> dict[str, Any]:
        return {
            "keyword": self.args.keyword,
            "mode": "continuous_backfill" if self.args.continuous_backfill else "single_range",
            "current_day": self.current_day,
            "resume_day": self.current_day,
            "resume_page": 1,
            "notes_count_this_day": 0,
            "total_notes_crawled_for_keyword": 0,
            "last_completed_day": None,
            "last_completed_page": None,
            "last_failed_day": None,
            "failure_reason": None,
        }

    def stream_checkpoint_base_payload(self) -> dict[str, Any]:
        return {
            "state": "running",
            "keyword": self.args.keyword,
            "mode": STREAM_SEARCH_MODE,
            "current_day": None,
            "resume_day": None,
            "resume_page": 1,
            "notes_count_this_day": None,
            "total_notes_crawled_for_keyword": 0,
            "pages_completed": 0,
            "contents_emitted": 0,
            "comments_emitted": 0,
            "last_emitted_day": None,
            "last_result_created_day": None,
            "last_completed_day": None,
            "last_completed_page": None,
            "last_failed_day": None,
            "last_failed_page": None,
            "failure_reason": None,
            "reason": "initialized",
        }

    def mark_backfill_completed(self, *, last_completed_day: str | None, reason: str) -> None:
        payload = self.checkpoint_base_payload()
        payload.update(
            {
                "state": "completed",
                "current_day": None,
                "resume_day": None,
                "resume_page": None,
                "notes_count_this_day": 0,
                "last_completed_day": last_completed_day,
                "last_completed_page": None,
                "reason": reason,
            }
        )
        self.write_checkpoint(payload)

    def advance_after_completed_day(self, completed_day: str, reason: str) -> None:
        next_day = self.previous_day(completed_day)
        if self.is_before_oldest_day(next_day):
            self.current_day = completed_day
            self.mark_backfill_completed(last_completed_day=completed_day, reason="reached_oldest_day")
            return

        self.current_day = next_day
        payload = self.checkpoint_base_payload()
        payload.update(
            {
                "state": "running",
                "last_completed_day": completed_day,
                "last_completed_page": None,
                "reason": reason,
            }
        )
        self.write_checkpoint(payload)

    def advance_after_failed_day(self, failed_day: str, reason: str) -> None:
        next_day = self.previous_day(failed_day)
        if self.is_before_oldest_day(next_day):
            self.current_day = failed_day
            self.mark_backfill_completed(last_completed_day=None, reason="reached_oldest_day_after_failed_day")
            return

        self.current_day = next_day
        payload = self.checkpoint_base_payload()
        payload.update(
            {
                "state": "running",
                "last_failed_day": failed_day,
                "failure_reason": reason,
                "reason": "advanced_after_failed_day",
            }
        )
        self.write_checkpoint(payload)

    def initialize_stream_checkpoint(self) -> None:
        self.write_checkpoint(self.stream_checkpoint_base_payload())

    def initialize_daily_checkpoint(self) -> None:
        self.current_day = self.args.start_day
        payload = self.checkpoint_base_payload()
        payload.update({"state": "running", "reason": "initialized"})
        self.write_checkpoint(payload)

    def ensure_checkpoint(self) -> None:
        checkpoint = self.read_checkpoint()
        checkpoint_job = checkpoint.get("job") if isinstance(checkpoint.get("job"), dict) else {}
        if self.uses_stream_search:
            if checkpoint.get("state") == "completed":
                self.initialize_stream_checkpoint()
                return
            if checkpoint and checkpoint.get("mode") != STREAM_SEARCH_MODE:
                self.initialize_stream_checkpoint()
                return
            if not self.checkpoint_matches_current_job(checkpoint_job):
                self.initialize_stream_checkpoint()
                return
            self.write_checkpoint({**checkpoint, "job": self.job_identity()})
            return

        checkpoint_oldest_day = parse_iso_date(str(checkpoint_job.get("oldest_day") or ""))
        current_oldest_day = parse_iso_date(str(self.args.oldest_day or ""))
        if checkpoint.get("state") == "completed":
            self.initialize_daily_checkpoint()
            return
        if not self.checkpoint_matches_current_job(checkpoint_job):
            self.initialize_daily_checkpoint()
            return
        if self.args.continuous_backfill:
            if checkpoint.get("state") == "day_completed":
                last_completed_day = str(checkpoint.get("last_completed_day") or checkpoint.get("current_day") or self.args.start_day)
                self.advance_after_completed_day(last_completed_day, reason="advanced_after_restart")
                return
            elif checkpoint.get("state") == "day_failed":
                last_failed_day = str(checkpoint.get("last_failed_day") or checkpoint.get("current_day") or self.args.start_day)
                self.advance_after_failed_day(last_failed_day, reason="advanced_after_restart_failed_day")
                return
        if checkpoint_oldest_day and current_oldest_day and current_oldest_day <= checkpoint_oldest_day:
            checkpoint["job"] = self.job_identity()
            self.write_checkpoint(checkpoint)
        self.current_day = str(checkpoint.get("current_day") or checkpoint.get("resume_day") or self.args.start_day)

    def get_time_range_resume_state(self, keyword: str) -> dict[str, Any] | None:
        checkpoint = self.read_checkpoint()
        checkpoint_job = checkpoint.get("job") if isinstance(checkpoint.get("job"), dict) else {}
        if not self.checkpoint_matches_current_job(checkpoint_job):
            return None
        if checkpoint.get("state") != "running":
            return None
        if checkpoint.get("keyword") not in (None, keyword):
            return None
        return checkpoint

    def get_stream_resume_state(self, keyword: str) -> dict[str, Any] | None:
        checkpoint = self.read_checkpoint()
        checkpoint_job = checkpoint.get("job") if isinstance(checkpoint.get("job"), dict) else {}
        if checkpoint.get("mode") != STREAM_SEARCH_MODE:
            return None
        if not self.checkpoint_matches_current_job(checkpoint_job):
            return None
        if checkpoint.get("state") not in {"running", "failed"}:
            return None
        if checkpoint.get("keyword") not in (None, keyword):
            return None
        resume_state = dict(checkpoint)
        resume_page = max(int(checkpoint.get("resume_page") or 1), 1)
        resume_state["resume_page"] = max(1, resume_page - self.stream_resume_overlap_pages)
        return resume_state

    def on_time_range_page_completed(
        self,
        *,
        keyword: str,
        day: str,
        next_page: int,
        notes_count_this_day: int,
        total_notes_crawled_for_keyword: int,
    ) -> None:
        self.current_day = day
        self.write_checkpoint(
            {
                "state": "running",
                "keyword": keyword,
                "mode": "continuous_backfill" if self.args.continuous_backfill else "single_range",
                "current_day": day,
                "resume_day": day,
                "resume_page": next_page,
                "notes_count_this_day": notes_count_this_day,
                "total_notes_crawled_for_keyword": total_notes_crawled_for_keyword,
                "last_completed_day": day,
                "last_completed_page": next_page - 1,
                "reason": "page_completed",
            }
        )

    def on_time_range_day_completed(
        self,
        *,
        keyword: str,
        day: str,
        next_day: str | None,
        total_notes_crawled_for_keyword: int,
        reason: str,
    ) -> None:
        self.current_day = day
        self.write_checkpoint(
            {
                "state": "running",
                "keyword": keyword,
                "mode": "continuous_backfill" if self.args.continuous_backfill else "single_range",
                "current_day": day,
                "resume_day": next_day or day,
                "resume_page": 1,
                "notes_count_this_day": 0,
                "total_notes_crawled_for_keyword": total_notes_crawled_for_keyword,
                "last_completed_day": day,
                "last_completed_page": None,
                "reason": reason,
            }
        )

    def on_time_range_keyword_completed(
        self,
        *,
        keyword: str,
        total_notes_crawled_for_keyword: int,
    ) -> None:
        if self.args.continuous_backfill:
            self.write_checkpoint(
                {
                    "state": "day_completed",
                    "keyword": keyword,
                    "mode": "continuous_backfill",
                    "current_day": self.current_day,
                    "resume_day": None,
                    "resume_page": None,
                    "notes_count_this_day": 0,
                    "total_notes_crawled_for_keyword": total_notes_crawled_for_keyword,
                    "last_completed_day": self.current_day,
                    "last_completed_page": None,
                    "reason": "day_completed",
                }
            )
            return

        self.write_checkpoint(
            {
                "state": "completed",
                "keyword": keyword,
                "mode": "single_range",
                "current_day": None,
                "resume_day": None,
                "resume_page": None,
                "notes_count_this_day": 0,
                "total_notes_crawled_for_keyword": total_notes_crawled_for_keyword,
                "last_completed_day": self.args.end_day,
                "last_completed_page": None,
                "reason": "keyword_completed",
            }
        )

    def on_time_range_day_failed(
        self,
        *,
        keyword: str,
        day: str,
        next_day: str | None,
        total_notes_crawled_for_keyword: int,
        reason: str,
    ) -> None:
        self.current_day = day
        self.write_checkpoint(
            {
                "state": "day_failed",
                "keyword": keyword,
                "mode": "continuous_backfill" if self.args.continuous_backfill else "single_range",
                "current_day": day,
                "resume_day": next_day or day,
                "resume_page": 1,
                "notes_count_this_day": 0,
                "total_notes_crawled_for_keyword": total_notes_crawled_for_keyword,
                "last_completed_day": None,
                "last_completed_page": None,
                "last_failed_day": day,
                "failure_reason": reason,
                "reason": "day_failed",
            }
        )

    def on_stream_page_completed(
        self,
        *,
        keyword: str,
        next_page: int,
        pages_completed: int,
        contents_emitted: int,
        comments_emitted: int,
        last_emitted_day: str | None,
        last_result_created_day: str | None,
    ) -> None:
        self.current_day = last_emitted_day or self.current_day
        self.write_checkpoint(
            {
                "state": "running",
                "keyword": keyword,
                "mode": STREAM_SEARCH_MODE,
                "current_day": last_emitted_day,
                "resume_day": last_emitted_day,
                "resume_page": next_page,
                "notes_count_this_day": None,
                "total_notes_crawled_for_keyword": contents_emitted,
                "pages_completed": pages_completed,
                "contents_emitted": contents_emitted,
                "comments_emitted": comments_emitted,
                "last_emitted_day": last_emitted_day,
                "last_result_created_day": last_result_created_day,
                "last_completed_day": last_emitted_day,
                "last_completed_page": next_page - 1,
                "last_failed_day": None,
                "last_failed_page": None,
                "failure_reason": None,
                "reason": "page_completed",
            }
        )

    def on_stream_keyword_completed(
        self,
        *,
        keyword: str,
        pages_completed: int,
        contents_emitted: int,
        comments_emitted: int,
        last_emitted_day: str | None,
        last_result_created_day: str | None,
        reason: str,
    ) -> None:
        self.current_day = last_emitted_day or self.current_day
        self.write_checkpoint(
            {
                "state": "completed",
                "keyword": keyword,
                "mode": STREAM_SEARCH_MODE,
                "current_day": last_emitted_day,
                "resume_day": None,
                "resume_page": None,
                "notes_count_this_day": None,
                "total_notes_crawled_for_keyword": contents_emitted,
                "pages_completed": pages_completed,
                "contents_emitted": contents_emitted,
                "comments_emitted": comments_emitted,
                "last_emitted_day": last_emitted_day,
                "last_result_created_day": last_result_created_day,
                "last_completed_day": last_emitted_day,
                "last_completed_page": pages_completed,
                "last_failed_day": None,
                "last_failed_page": None,
                "failure_reason": None,
                "reason": reason,
            }
        )

    def on_stream_failed(
        self,
        *,
        keyword: str,
        failed_page: int,
        pages_completed: int,
        contents_emitted: int,
        comments_emitted: int,
        last_emitted_day: str | None,
        last_result_created_day: str | None,
        reason: str,
    ) -> None:
        self.current_day = last_emitted_day or self.current_day
        self.write_checkpoint(
            {
                "state": "failed",
                "keyword": keyword,
                "mode": STREAM_SEARCH_MODE,
                "current_day": last_emitted_day,
                "resume_day": last_emitted_day,
                "resume_page": failed_page,
                "notes_count_this_day": None,
                "total_notes_crawled_for_keyword": contents_emitted,
                "pages_completed": pages_completed,
                "contents_emitted": contents_emitted,
                "comments_emitted": comments_emitted,
                "last_emitted_day": last_emitted_day,
                "last_result_created_day": last_result_created_day,
                "last_completed_day": last_emitted_day,
                "last_completed_page": max(failed_page - 1, 0),
                "last_failed_day": last_emitted_day,
                "last_failed_page": failed_page,
                "failure_reason": reason,
                "reason": "stream_failed",
            }
        )

    def get_active_day(self) -> str:
        checkpoint = self.read_checkpoint()
        current_day = str(checkpoint.get("current_day") or checkpoint.get("resume_day") or self.args.start_day)
        self.current_day = current_day
        return current_day


def configure_job(args: argparse.Namespace, *, start_day: str | None = None, end_day: str | None = None) -> None:
    effective_start_day = start_day or args.start_day
    effective_end_day = end_day or args.end_day
    start_day_date = parse_iso_date(effective_start_day)
    end_day_date = parse_iso_date(effective_end_day)

    if args.search_mode in DAILY_SEARCH_MODES:
        if start_day_date is None or end_day_date is None:
            raise ValueError("start-day and end-day must be valid ISO dates")
        if start_day_date > end_day_date:
            raise ValueError("start-day cannot be later than end-day")
        if start_day_date < supported_oldest_day():
            raise ValueError(
                f"Zhihu time-range crawling only supports days on or after {supported_oldest_day().isoformat()}"
            )
        total_days = (end_day_date - start_day_date).days + 1
        crawler_max_notes_count = max(total_days * args.max_notes_per_day, args.max_notes_per_day)
        output_date_override = effective_start_day
    else:
        crawler_max_notes_count = 1_000_000_000
        output_date_override = ""

    config.PLATFORM = "zhihu"
    config.LOGIN_TYPE = args.login_type
    config.CRAWLER_TYPE = "search"
    config.KEYWORDS = args.keyword
    config.SAVE_DATA_OPTION = args.save_data_option
    config.SAVE_DATA_PATH = args.save_data_path
    config.OUTPUT_DATE_OVERRIDE = output_date_override
    config.ZHIHU_SEARCH_MODE = args.search_mode
    config.START_DAY = effective_start_day
    config.END_DAY = effective_end_day
    config.START_PAGE = 1
    config.MAX_NOTES_PER_DAY = args.max_notes_per_day
    config.CRAWLER_MAX_NOTES_COUNT = crawler_max_notes_count
    config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES = args.max_comment_items
    setattr(config, "ZHIHU_MAX_SUB_COMMENTS_COUNT_SINGLENOTES", args.max_sub_comment_items)
    config.MAX_CONCURRENCY_NUM = args.max_concurrency_num
    config.CRAWLER_MAX_SLEEP_SEC = args.crawler_sleep_seconds
    config.ENABLE_GET_COMMENTS = not args.disable_comments
    config.ENABLE_GET_SUB_COMMENTS = not args.disable_sub_comments
    config.HEADLESS = args.headless
    config.CDP_HEADLESS = args.headless
    config.ENABLE_CDP_MODE = not args.disable_cdp
    config.SAVE_LOGIN_STATE = True


async def run_stream_job(crawler: CheckpointedZhihuCrawler, args: argparse.Namespace, checkpoint_path: Path) -> None:
    configure_job(args)
    print(
        {
            "platform": config.PLATFORM,
            "crawler_type": config.CRAWLER_TYPE,
            "keyword": config.KEYWORDS,
            "search_mode": config.ZHIHU_SEARCH_MODE,
            "save_data_option": config.SAVE_DATA_OPTION,
            "save_data_path": config.SAVE_DATA_PATH,
            "max_comment_items": config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
            "max_sub_comment_items": getattr(config, "ZHIHU_MAX_SUB_COMMENTS_COUNT_SINGLENOTES", args.max_sub_comment_items),
            "enable_cdp_mode": config.ENABLE_CDP_MODE,
            "checkpoint_path": str(checkpoint_path),
            "oldest_day_accepted_for_compatibility": args.oldest_day,
            "stream_resume_overlap_pages": crawler.stream_resume_overlap_pages,
        },
        flush=True,
    )
    try:
        await crawler.start()
    except PermissionError as exc:
        if crawler.is_playwright_startup_access_error(exc):
            raise RuntimeError(
                "Playwright browser bootstrap was denied by the current environment "
                "(WinError 5 / access denied). Please run this crawler from a normal local terminal session."
            ) from exc
        raise


async def run_daily_mode_job(crawler: CheckpointedZhihuCrawler, args: argparse.Namespace, checkpoint_path: Path) -> None:
    if args.continuous_backfill:
        initial_day = crawler.get_active_day()
        if parse_iso_date(initial_day) and parse_iso_date(initial_day) < supported_oldest_day():
            crawler.mark_backfill_completed(last_completed_day=None, reason="reached_zhihu_search_window_limit")
            return
        configure_job(args, start_day=initial_day, end_day=initial_day)
        try:
            await crawler.prepare_session_with_retries()
        except PermissionError as exc:
            if crawler.is_playwright_startup_access_error(exc):
                raise RuntimeError(
                    "Playwright browser bootstrap was denied by the current environment "
                    "(WinError 5 / access denied). Please run this crawler from a normal local terminal session."
                ) from exc
            raise
        try:
            while True:
                active_day = crawler.get_active_day()
                active_day_date = parse_iso_date(active_day)
                if active_day_date and active_day_date < supported_oldest_day():
                    crawler.mark_backfill_completed(
                        last_completed_day=None,
                        reason="reached_zhihu_search_window_limit",
                    )
                    break
                if crawler.is_before_oldest_day(active_day):
                    crawler.mark_backfill_completed(last_completed_day=None, reason="before_oldest_day")
                    break

                configure_job(args, start_day=active_day, end_day=active_day)
                print(
                    {
                        "platform": config.PLATFORM,
                        "crawler_type": config.CRAWLER_TYPE,
                        "keyword": config.KEYWORDS,
                        "current_day": active_day,
                        "oldest_day": args.oldest_day,
                        "continuous_backfill": True,
                        "search_mode": config.ZHIHU_SEARCH_MODE,
                        "save_data_option": config.SAVE_DATA_OPTION,
                        "save_data_path": config.SAVE_DATA_PATH,
                        "max_notes_per_day": config.MAX_NOTES_PER_DAY,
                        "max_comment_items": config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
                        "max_sub_comment_items": getattr(config, "ZHIHU_MAX_SUB_COMMENTS_COUNT_SINGLENOTES", args.max_sub_comment_items),
                        "enable_cdp_mode": config.ENABLE_CDP_MODE,
                        "checkpoint_path": str(checkpoint_path),
                    },
                    flush=True,
                )
                crawler.current_day = active_day
                await crawler.run_current_config()

                checkpoint = crawler.read_checkpoint()
                if checkpoint.get("state") == "day_completed":
                    crawler.advance_after_completed_day(active_day, reason="advanced_to_previous_day")
                    continue
                if checkpoint.get("state") == "day_failed":
                    crawler.advance_after_failed_day(active_day, reason=checkpoint.get("failure_reason") or "failed_day")
                    continue
                break
        finally:
            await crawler.close_session()
        return

    configure_job(args)
    print(
        {
            "platform": config.PLATFORM,
            "crawler_type": config.CRAWLER_TYPE,
            "keyword": config.KEYWORDS,
            "start_day": config.START_DAY,
            "end_day": config.END_DAY,
            "continuous_backfill": False,
            "search_mode": config.ZHIHU_SEARCH_MODE,
            "save_data_option": config.SAVE_DATA_OPTION,
            "save_data_path": config.SAVE_DATA_PATH,
            "max_notes_per_day": config.MAX_NOTES_PER_DAY,
            "max_comment_items": config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
            "max_sub_comment_items": getattr(config, "ZHIHU_MAX_SUB_COMMENTS_COUNT_SINGLENOTES", args.max_sub_comment_items),
            "enable_cdp_mode": config.ENABLE_CDP_MODE,
            "checkpoint_path": str(checkpoint_path),
        },
        flush=True,
    )
    try:
        await crawler.start()
    except PermissionError as exc:
        if crawler.is_playwright_startup_access_error(exc):
            raise RuntimeError(
                "Playwright browser bootstrap was denied by the current environment "
                "(WinError 5 / access denied). Please run this crawler from a normal local terminal session."
            ) from exc
        raise


async def main_async() -> None:
    configure_console_output()
    os.chdir(MEDIA_CRAWLER_ROOT)
    args = parse_args()
    checkpoint_path = Path(args.checkpoint_path)
    args = adopt_checkpoint_job_if_preferred(args, checkpoint_path)
    crawler = CheckpointedZhihuCrawler(checkpoint_path=checkpoint_path, args=args)
    crawler.ensure_checkpoint()

    if args.search_mode == STREAM_SEARCH_MODE:
        await run_stream_job(crawler, args, checkpoint_path)
        return

    await run_daily_mode_job(crawler, args, checkpoint_path)


if __name__ == "__main__":
    asyncio.run(main_async())

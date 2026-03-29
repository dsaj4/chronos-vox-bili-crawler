from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Callable


ROOT_DIR = Path(__file__).resolve().parents[1]
MEDIA_CRAWLER_ROOT = ROOT_DIR / "MediaCrawler"
MEDIA_CRAWLER_PYTHON = MEDIA_CRAWLER_ROOT / ".venv" / "Scripts" / "python.exe"
DEFAULT_SAVE_DATA_PATH = ROOT_DIR / "artifacts" / "ai_crawl_data"

RUNNING_STATES = {"starting", "running", "restarting"}
TERMINAL_STATES = {"completed", "stopped", "error"}
TERMINAL_JOB_STATES = {"completed", "stopped"}
ACTIVE_STATES = RUNNING_STATES | {"stopping"}
FATAL_FAILURE_CLASSES = {
    "playwright_bootstrap_denied",
    "login_required",
    "account_permission_denied",
}

PLAYWRIGHT_PERMISSION_MARKERS = [
    "PermissionError: [WinError 5]",
    "Playwright startup failed with PermissionError",
    "access is denied",
    "拒绝访问",
]
BROWSER_LAUNCH_MARKERS = [
    "Browser failed to start within",
    "CDP mode launch failed",
    "CDP browser launch failed",
    "BrowserType.launch_persistent_context",
    "browser has been closed",
]
LOGIN_REQUIRED_MARKERS = [
    "login required",
    "please login",
    "please sign in",
    "need login",
    "qrcode login failed",
    "qrcode was not found on the page",
    "login was not confirmed before timeout",
    "search_request_rejected_logged_out",
    "未登录",
    "登录失败",
    "扫码登录",
]
ACCOUNT_PERMISSION_MARKERS = [
    "没有权限访问",
    "无权限访问",
    "search_request_rejected_permission_denied",
    "permission denied",
    "account does not have permission",
    "no permission to access",
    "您当前登录的账号没有权限访问",
]
NETWORK_ERROR_MARKERS = [
    "ConnectionError",
    "ClientConnectorError",
    "ConnectionResetError",
    "NameResolutionError",
    "timed out",
    "timeout",
    "ECONNRESET",
    "network is unreachable",
]

HEARTBEAT_PROGRESS_KEYS = (
    "heartbeat_at",
    "heartbeat_kind",
    "heartbeat_cursor",
)

CHECKPOINT_PROGRESS_KEYS = (
    *HEARTBEAT_PROGRESS_KEYS,
    "state",
    "current_day",
    "resume_day",
    "resume_page",
    "notes_count_this_day",
    "total_notes_crawled_for_keyword",
    "pages_completed",
    "contents_emitted",
    "comments_emitted",
    "last_emitted_day",
    "last_result_created_day",
    "last_completed_day",
    "last_completed_page",
    "last_failed_day",
    "last_failed_page",
    "failure_reason",
    "updated_at",
)


def now() -> datetime:
    return datetime.now().astimezone()


def now_iso() -> str:
    return now().isoformat(timespec="seconds")


def default_bili_oldest_day() -> str:
    return "2009-06-26"


def default_zhihu_oldest_day() -> str:
    return (date.today() - timedelta(days=364)).isoformat()


def default_xhs_oldest_day() -> str:
    return "2013-01-01"


def monitor_python() -> Path:
    if MEDIA_CRAWLER_PYTHON.exists():
        return MEDIA_CRAWLER_PYTHON
    return Path(sys.executable)


@dataclass(frozen=True)
class PlatformSpec:
    name: str
    label: str
    runner_script_name: str
    runtime_dir_name: str
    default_oldest_day_factory: Callable[[], str]
    default_stall_timeout_seconds: int
    default_max_comment_items: int
    default_max_sub_comment_items: int | None

    @property
    def runner_path(self) -> Path:
        return ROOT_DIR / "scripts" / self.runner_script_name

    @property
    def runtime_dir(self) -> Path:
        return ROOT_DIR / "artifacts" / self.runtime_dir_name

    @property
    def status_path(self) -> Path:
        return self.runtime_dir / "status.json"

    @property
    def stop_flag_path(self) -> Path:
        return self.runtime_dir / "stop.flag"

    @property
    def pid_path(self) -> Path:
        return self.runtime_dir / "monitor.pid"

    @property
    def monitor_log_path(self) -> Path:
        return self.runtime_dir / "monitor.log"

    @property
    def latest_crawler_summary_path(self) -> Path:
        return self.runtime_dir / "crawler.log"

    @property
    def checkpoint_path(self) -> Path:
        return self.runtime_dir / "checkpoint.json"

    @property
    def preferred_job_path(self) -> Path:
        return self.runtime_dir / "preferred_job.json"

    @property
    def latest_run_path(self) -> Path:
        return self.runtime_dir / "latest_run.json"

    @property
    def runs_dir(self) -> Path:
        return self.runtime_dir / "runs"

    def default_oldest_day(self) -> str:
        return self.default_oldest_day_factory()


PLATFORM_SPECS: dict[str, PlatformSpec] = {
    "bili": PlatformSpec(
        name="bili",
        label="Bilibili",
        runner_script_name="run_bili_ai_time_range_job.py",
        runtime_dir_name="ai_crawl_monitor",
        default_oldest_day_factory=default_bili_oldest_day,
        default_stall_timeout_seconds=900,
        default_max_comment_items=500,
        default_max_sub_comment_items=50,
    ),
    "zhihu": PlatformSpec(
        name="zhihu",
        label="Zhihu",
        runner_script_name="run_zhihu_time_range_job.py",
        runtime_dir_name="zhihu_crawl_monitor",
        default_oldest_day_factory=default_zhihu_oldest_day,
        default_stall_timeout_seconds=600,
        default_max_comment_items=500,
        default_max_sub_comment_items=50,
    ),
    "xhs": PlatformSpec(
        name="xhs",
        label="Xiaohongshu",
        runner_script_name="run_xhs_time_range_job.py",
        runtime_dir_name="xhs_crawl_monitor",
        default_oldest_day_factory=default_xhs_oldest_day,
        default_stall_timeout_seconds=900,
        default_max_comment_items=200,
        default_max_sub_comment_items=None,
    ),
}


def get_platform_spec(platform: str) -> PlatformSpec:
    try:
        return PLATFORM_SPECS[platform]
    except KeyError as exc:
        raise ValueError(f"Unsupported platform: {platform}") from exc


def ensure_runtime_dir(spec: PlatformSpec) -> None:
    spec.runtime_dir.mkdir(parents=True, exist_ok=True)
    spec.runs_dir.mkdir(parents=True, exist_ok=True)
    DEFAULT_SAVE_DATA_PATH.mkdir(parents=True, exist_ok=True)


def parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.astimezone()
    return parsed


def seconds_since(timestamp: Any) -> float | None:
    parsed = parse_timestamp(timestamp)
    if parsed is None:
        return None
    return max((now() - parsed).total_seconds(), 0.0)


def process_probe(pid: int | None) -> tuple[bool | None, str | None]:
    if not pid:
        return False, None
    if sys.platform == "win32":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        output = result.stdout.strip()
        stderr = (result.stderr or "").strip()
        combined = f"{output}\n{stderr}".strip()
        if result.returncode == 0:
            return bool(output and "No tasks are running" not in output), None
        if "Access denied" in combined or "拒绝访问" in combined:
            return None, "tasklist_access_denied"
        return False, combined or f"tasklist_returncode_{result.returncode}"
    try:
        os.kill(pid, 0)
    except (OSError, SystemError, ValueError):
        return False, None
    return True, None


def process_exists(pid: int | None) -> bool:
    exists, _ = process_probe(pid)
    return exists is True


def terminate_process_tree(pid: int | None) -> None:
    if not process_exists(pid):
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def append_monitor_log(spec: PlatformSpec, message: str) -> None:
    ensure_runtime_dir(spec)
    with spec.monitor_log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def write_console_json(payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
        sys.stdout.buffer.write(b"\n")


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def is_job_long_backfill(job: dict[str, Any]) -> bool:
    return bool(job.get("continuous_backfill")) and bool(parse_iso_date(str(job.get("oldest_day") or "")))


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


def same_job_series(platform: str, left: dict[str, Any], right: dict[str, Any]) -> bool:
    search_mode_matches = True
    if platform == "zhihu":
        search_mode_matches = (
            str(left.get("search_mode") or "one_year_stream_bucketed")
            == str(right.get("search_mode") or "one_year_stream_bucketed")
        )
    return (
        left.get("platform") == right.get("platform") == platform
        and left.get("keyword") == right.get("keyword")
        and search_mode_matches
        and bool(left.get("continuous_backfill")) == bool(right.get("continuous_backfill"))
        and str(left.get("save_data_option") or "") == str(right.get("save_data_option") or "")
        and str(left.get("save_data_path") or "") == str(right.get("save_data_path") or "")
    )


def job_identity_from_args(spec: PlatformSpec, args: argparse.Namespace, *, disable_cdp: bool | None = None) -> dict[str, Any]:
    payload = {
        "platform": spec.name,
        "keyword": args.keyword,
        "start_day": args.start_day,
        "end_day": args.end_day,
        "oldest_day": args.oldest_day,
        "continuous_backfill": bool(args.continuous_backfill),
        "save_data_option": args.save_data_option,
        "save_data_path": str(Path(args.save_data_path).resolve()),
        "login_type": args.login_type,
        "headless": bool(args.headless),
        "disable_cdp": bool(args.disable_cdp if disable_cdp is None else disable_cdp),
        "disable_comments": bool(args.disable_comments),
        "disable_sub_comments": bool(args.disable_sub_comments),
        "max_notes_per_day": args.max_notes_per_day,
        "max_comment_items": args.max_comment_items,
        "max_sub_comment_items": args.max_sub_comment_items,
        "max_concurrency_num": args.max_concurrency_num,
        "crawler_sleep_seconds": args.crawler_sleep_seconds,
    }
    if getattr(args, "search_mode", None) is not None:
        payload["search_mode"] = args.search_mode
    return payload


def tail_last_non_empty_line(path: Path) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in reversed(lines):
        line = line.strip()
        if line:
            return line
    return ""


def read_tail_text(path: Path, *, max_bytes: int = 262144) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as handle:
        try:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(size - max_bytes, 0))
        except OSError:
            handle.seek(0)
        return handle.read().decode("utf-8", errors="ignore")


def checkpoint_progress_marker(checkpoint: dict[str, Any]) -> str:
    if not checkpoint:
        return ""
    payload = {key: checkpoint.get(key) for key in CHECKPOINT_PROGRESS_KEYS if key in checkpoint}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def checkpoint_heartbeat_marker(checkpoint: dict[str, Any]) -> str:
    if not checkpoint:
        return ""
    payload = {key: checkpoint.get(key) for key in HEARTBEAT_PROGRESS_KEYS if checkpoint.get(key)}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True) if payload else ""


def progress_reference_timestamp(checkpoint: dict[str, Any], raw_payload: dict[str, Any] | None = None) -> str | None:
    raw_payload = raw_payload or {}
    for candidate in (
        checkpoint.get("heartbeat_at"),
        raw_payload.get("progress_heartbeat_at"),
        raw_payload.get("last_progress_at"),
        raw_payload.get("last_output_at"),
        checkpoint.get("updated_at"),
        raw_payload.get("updated_at"),
    ):
        if candidate:
            return str(candidate)
    return None


def progress_is_recent(reference_timestamp: str | None, stall_timeout_seconds: int | None) -> bool:
    if not reference_timestamp:
        return False
    timeout_seconds = max(int(stall_timeout_seconds or 0), 1)
    age_seconds = seconds_since(reference_timestamp)
    if age_seconds is None:
        return False
    return age_seconds <= timeout_seconds


def classify_failure_text(text: str, *, fallback: str = "child_exit_nonzero") -> str:
    lowered = text.lower()
    if any(marker.lower() in lowered for marker in PLAYWRIGHT_PERMISSION_MARKERS):
        return "playwright_bootstrap_denied"
    if any(marker.lower() in lowered for marker in ACCOUNT_PERMISSION_MARKERS):
        return "account_permission_denied"
    if any(marker.lower() in lowered for marker in LOGIN_REQUIRED_MARKERS):
        return "login_required"
    if any(marker.lower() in lowered for marker in BROWSER_LAUNCH_MARKERS):
        return "browser_launch_failed"
    if any(marker.lower() in lowered for marker in NETWORK_ERROR_MARKERS):
        return "network_error"
    return fallback


def classify_failure(
    *,
    explicit_reason: str | None = None,
    checkpoint: dict[str, Any] | None = None,
    log_path: Path | None = None,
    default: str = "child_exit_nonzero",
) -> tuple[str, str]:
    parts = []
    if explicit_reason:
        parts.append(explicit_reason)
    if checkpoint:
        failure_reason = checkpoint.get("failure_reason")
        if failure_reason:
            parts.append(str(failure_reason))
    if log_path and log_path.exists():
        parts.append(read_tail_text(log_path))
    combined = "\n".join(part for part in parts if part).strip()
    if not combined:
        return default, explicit_reason or ""
    return classify_failure_text(combined, fallback=default), combined


def build_parser(*, fixed_platform: str | None = None) -> argparse.ArgumentParser:
    description = "Unified crawler monitor CLI."
    if fixed_platform:
        description = f"{get_platform_spec(fixed_platform).label} crawler monitor."

    parser = argparse.ArgumentParser(description=description)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_run_args(target: argparse.ArgumentParser) -> None:
        spec = get_platform_spec(fixed_platform) if fixed_platform else None
        today = date.today().isoformat()
        if fixed_platform is None:
            target.add_argument("--platform", choices=sorted(PLATFORM_SPECS.keys()), required=True)
        target.add_argument("--keyword", default="ai")
        target.add_argument("--start-day", default=today)
        target.add_argument("--end-day", default=today)
        target.add_argument("--oldest-day", default=spec.default_oldest_day() if spec else None)
        target.add_argument("--continuous-backfill", action="store_true")
        target.add_argument(
            "--search-mode",
            choices=["one_year_stream_bucketed", "daily_limit_in_time_range", "all_in_time_range"],
            default=None,
        )
        target.add_argument("--check-interval-seconds", type=int, default=30)
        target.add_argument(
            "--stall-timeout-seconds",
            type=int,
            default=spec.default_stall_timeout_seconds if spec else None,
        )
        target.add_argument("--max-notes-per-day", type=int, default=5)
        target.add_argument(
            "--max-comment-items",
            type=int,
            default=spec.default_max_comment_items if spec else None,
        )
        target.add_argument(
            "--max-sub-comment-items",
            type=int,
            default=spec.default_max_sub_comment_items if spec else None,
        )
        target.add_argument("--max-concurrency-num", type=int, default=1)
        target.add_argument("--crawler-sleep-seconds", type=float, default=2.0)
        target.add_argument("--save-data-option", default="json")
        target.add_argument("--save-data-path", default=str(DEFAULT_SAVE_DATA_PATH))
        target.add_argument("--login-type", default="qrcode")
        target.add_argument("--headless", action="store_true")
        target.add_argument("--disable-cdp", action="store_true")
        target.add_argument("--disable-comments", action="store_true")
        target.add_argument("--disable-sub-comments", action="store_true")

    run_parser = subparsers.add_parser("run", help="Run the monitor loop.")
    add_common_run_args(run_parser)

    start_parser = subparsers.add_parser("start", help="Start the monitor in the background.")
    add_common_run_args(start_parser)

    status_parser = subparsers.add_parser("status", help="Print current monitor status.")
    if fixed_platform is None:
        status_parser.add_argument("--platform", choices=sorted(PLATFORM_SPECS.keys()), required=True)
    status_parser.add_argument("--json", action="store_true")

    stop_parser = subparsers.add_parser("stop", help="Ask the running monitor to stop.")
    if fixed_platform is None:
        stop_parser.add_argument("--platform", choices=sorted(PLATFORM_SPECS.keys()), required=True)

    return parser


def resolve_platform_and_defaults(args: argparse.Namespace, *, fixed_platform: str | None = None) -> PlatformSpec:
    if fixed_platform:
        args.platform = fixed_platform
    spec = get_platform_spec(args.platform)
    if getattr(args, "command", None) in {"run", "start"}:
        if args.oldest_day is None:
            args.oldest_day = spec.default_oldest_day()
        if args.stall_timeout_seconds is None:
            args.stall_timeout_seconds = spec.default_stall_timeout_seconds
        if args.max_comment_items is None:
            args.max_comment_items = spec.default_max_comment_items
        if args.max_sub_comment_items is None:
            args.max_sub_comment_items = spec.default_max_sub_comment_items
        if spec.name == "zhihu" and getattr(args, "search_mode", None) is None:
            args.search_mode = "one_year_stream_bucketed"
    return spec


def normalized_job_from_status_payload(spec: PlatformSpec, payload: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, Any]:
    payload_job = dict(payload["job"]) if isinstance(payload.get("job"), dict) else {}
    checkpoint_job = dict(checkpoint["job"]) if isinstance(checkpoint.get("job"), dict) else {}
    if payload_job or checkpoint_job:
        merged_job = dict(checkpoint_job)
        merged_job.update({key: value for key, value in payload_job.items() if value is not None})
        return merged_job
    return {
        "platform": spec.name,
        "keyword": payload.get("keyword", checkpoint.get("keyword", "ai")),
        "search_mode": payload.get("search_mode", checkpoint.get("search_mode")),
        "start_day": payload.get("start_day"),
        "end_day": payload.get("end_day"),
        "oldest_day": payload.get("oldest_day"),
        "continuous_backfill": bool(payload.get("continuous_backfill")),
        "save_data_option": payload.get("save_data_option", "json"),
        "save_data_path": payload.get("save_data_path", str(DEFAULT_SAVE_DATA_PATH.resolve())),
    }


def normalize_status_payload(spec: PlatformSpec) -> dict[str, Any]:
    ensure_runtime_dir(spec)
    raw_payload = read_json(spec.status_path)
    checkpoint = dict(raw_payload.get("checkpoint") or {})
    if not checkpoint:
        checkpoint = read_json(spec.checkpoint_path)
    latest_run = read_json(spec.latest_run_path)
    job = normalized_job_from_status_payload(spec, raw_payload, checkpoint)
    monitor_state = str(raw_payload.get("monitor_state") or "idle")
    child_pid = raw_payload.get("child_pid")
    monitor_pid = raw_payload.get("monitor_pid")
    child_exit_code = raw_payload.get("child_exit_code")
    run_id = raw_payload.get("run_id") or raw_payload.get("current_run_id") or latest_run.get("run_id")
    if run_id is None and monitor_pid:
        run_id = f"legacy-{monitor_pid}"
    last_progress_at = (
        raw_payload.get("last_progress_at")
        or checkpoint.get("updated_at")
        or raw_payload.get("last_output_at")
        or raw_payload.get("updated_at")
        or raw_payload.get("last_heartbeat_at")
    )
    latest_crawler_log = (
        raw_payload.get("latest_crawler_log")
        or latest_run.get("crawler_log_path")
        or raw_payload.get("crawler_log_path")
        or str(spec.latest_crawler_summary_path.resolve())
    )
    latest_monitor_log = (
        raw_payload.get("latest_monitor_log")
        or raw_payload.get("monitor_log_path")
        or str(spec.monitor_log_path.resolve())
    )
    stall_timeout_seconds = int(
        raw_payload.get("stall_timeout_seconds")
        or (job.get("stall_timeout_seconds") if isinstance(job, dict) else 0)
        or spec.default_stall_timeout_seconds
    )
    failure_class = raw_payload.get("failure_class")
    failure_reason = raw_payload.get("failure_reason") or checkpoint.get("failure_reason")
    if not failure_reason and (monitor_state == "error" or (isinstance(child_exit_code, int) and child_exit_code not in (None, 0))):
        failure_reason = raw_payload.get("message") or ""
    if not failure_class and (
        monitor_state == "error" or (isinstance(child_exit_code, int) and child_exit_code not in (None, 0))
    ):
        failure_class, inferred_reason = classify_failure(
            explicit_reason=failure_reason,
            checkpoint=checkpoint,
            log_path=Path(latest_crawler_log) if latest_crawler_log else None,
        )
        if not failure_reason:
            failure_reason = inferred_reason

    progress_heartbeat_at = progress_reference_timestamp(checkpoint, raw_payload)
    healthy = monitor_state in RUNNING_STATES and progress_is_recent(progress_heartbeat_at, stall_timeout_seconds)
    monitor_live, monitor_live_reason = process_probe(monitor_pid if isinstance(monitor_pid, int) else None)
    child_live, child_live_reason = process_probe(child_pid if isinstance(child_pid, int) else None)
    inferred_live = raw_payload.get("live")
    if inferred_live is None:
        if monitor_state in RUNNING_STATES:
            inferred_live = child_live if child_pid else monitor_live
        elif monitor_state in ACTIVE_STATES:
            inferred_live = monitor_live
        else:
            inferred_live = False

    degraded_reason = raw_payload.get("degraded_reason") or ""
    degraded = bool(raw_payload.get("degraded"))
    if monitor_state in RUNNING_STATES:
        if not healthy:
            degraded = True
            degraded_reason = "progress_stale"
        elif raw_payload.get("consecutive_failures", 0):
            degraded = True
            degraded_reason = "recovering_after_failures"
        elif raw_payload.get("current_disable_cdp", raw_payload.get("disable_cdp", False)):
            degraded = True
            degraded_reason = "running_with_disable_cdp"
        elif inferred_live is False:
            degraded = True
            degraded_reason = child_live_reason or monitor_live_reason or "process_not_observed"
        else:
            degraded = False
            degraded_reason = ""
    else:
        degraded = False
        degraded_reason = ""

    normalized = {
        "platform": spec.name,
        "monitor_state": monitor_state,
        "job_state": checkpoint.get("state") or raw_payload.get("job_state") or monitor_state,
        "healthy": healthy,
        "live": inferred_live,
        "degraded": degraded,
        "degraded_reason": degraded_reason,
        "monitor_pid": monitor_pid,
        "child_pid": child_pid,
        "child_exit_code": child_exit_code,
        "run_id": run_id,
        "restart_count": raw_payload.get("restart_count", 0),
        "consecutive_failures": raw_payload.get("consecutive_failures", 0),
        "current_day": checkpoint.get("current_day") or checkpoint.get("last_emitted_day"),
        "resume_day": checkpoint.get("resume_day") or checkpoint.get("last_emitted_day"),
        "resume_page": checkpoint.get("resume_page"),
        "notes_count_this_day": checkpoint.get("notes_count_this_day"),
        "total_notes_crawled_for_keyword": checkpoint.get("total_notes_crawled_for_keyword")
        if checkpoint.get("total_notes_crawled_for_keyword") is not None
        else checkpoint.get("contents_emitted"),
        "pages_completed": checkpoint.get("pages_completed"),
        "contents_emitted": checkpoint.get("contents_emitted"),
        "comments_emitted": checkpoint.get("comments_emitted"),
        "last_emitted_day": checkpoint.get("last_emitted_day"),
        "last_result_created_day": checkpoint.get("last_result_created_day"),
        "progress_heartbeat_at": progress_heartbeat_at,
        "heartbeat_kind": checkpoint.get("heartbeat_kind"),
        "heartbeat_cursor": checkpoint.get("heartbeat_cursor"),
        "last_progress_at": last_progress_at,
        "failure_class": failure_class,
        "failure_reason": failure_reason,
        "job": job,
        "checkpoint_path": str(spec.checkpoint_path.resolve()),
        "status_path": str(spec.status_path.resolve()),
        "latest_monitor_log": str(Path(latest_monitor_log).resolve()) if latest_monitor_log else str(spec.monitor_log_path.resolve()),
        "latest_crawler_log": str(Path(latest_crawler_log).resolve()) if latest_crawler_log else str(spec.latest_crawler_summary_path.resolve()),
        "message": raw_payload.get("message", ""),
        "checkpoint": checkpoint,
        "updated_at": raw_payload.get("updated_at") or raw_payload.get("last_heartbeat_at") or now_iso(),
        "last_output_at": raw_payload.get("last_output_at"),
        "last_log_line": raw_payload.get("last_log_line")
        or raw_payload.get("latest_crawler_log_line")
        or tail_last_non_empty_line(Path(latest_crawler_log)) if latest_crawler_log else "",
        "last_repair_action": raw_payload.get("last_repair_action", "none"),
        "current_disable_cdp": raw_payload.get("current_disable_cdp", raw_payload.get("disable_cdp", False)),
        "check_interval_seconds": raw_payload.get("check_interval_seconds", 30),
        "monitor_log_path": str(spec.monitor_log_path.resolve()),
        "crawler_log_path": str(spec.latest_crawler_summary_path.resolve()),
        "latest_run_path": str(spec.latest_run_path.resolve()),
        "stall_timeout_seconds": stall_timeout_seconds,
    }
    return normalized


class UnifiedCrawlerMonitor:
    def __init__(self, spec: PlatformSpec, args: argparse.Namespace):
        self.spec = spec
        self.args = args
        self.child_process: subprocess.Popen[str] | None = None
        self.child_log_handle = None
        self.current_run_id: str | None = None
        self.current_run_dir: Path | None = None
        self.current_run_log_path: Path | None = None
        self.current_run_manifest_path: Path | None = None
        self.restart_count = 0
        self.consecutive_failures = 0
        self.last_repair_action = "none"
        self.current_disable_cdp = bool(args.disable_cdp)
        self.state = "idle"
        self.failure_class: str | None = None
        self.failure_reason = ""
        self.last_progress_at: str | None = None
        self._last_progress_monotonic = 0.0
        self._last_progress_marker = ""
        self._last_heartbeat_marker = ""
        self._progress_seen_in_current_run = False
        self._run_started_at: str | None = None

    def adopt_checkpoint_job_if_preferred(self) -> None:
        checkpoint = read_json(self.spec.checkpoint_path)
        checkpoint_job = checkpoint.get("job") if isinstance(checkpoint.get("job"), dict) else {}
        preferred_job = read_json(self.spec.preferred_job_path)
        incoming_job = job_identity_from_args(self.spec, self.args, disable_cdp=self.current_disable_cdp)
        selected_job = choose_preferred_job(checkpoint_job, preferred_job, incoming_job)
        if selected_job and preferred_job != selected_job:
            write_json(self.spec.preferred_job_path, selected_job)
        if not selected_job or not same_job_series(self.spec.name, incoming_job, selected_job):
            return
        if selected_job == incoming_job:
            return

        self.args.keyword = str(selected_job.get("keyword") or self.args.keyword)
        self.args.start_day = str(selected_job.get("start_day") or self.args.start_day)
        self.args.end_day = str(selected_job.get("end_day") or self.args.end_day)
        self.args.oldest_day = str(selected_job.get("oldest_day") or self.args.oldest_day)
        self.args.continuous_backfill = bool(selected_job.get("continuous_backfill"))
        self.args.save_data_option = str(selected_job.get("save_data_option") or self.args.save_data_option)
        self.args.save_data_path = str(selected_job.get("save_data_path") or self.args.save_data_path)
        self.last_repair_action = "adopted_checkpoint_job"
        append_monitor_log(self.spec, "Preserved preferred long backfill job instead of shorter incoming config")

    def bootstrap(self) -> None:
        ensure_runtime_dir(self.spec)
        self.adopt_checkpoint_job_if_preferred()
        existing = normalize_status_payload(self.spec)
        existing_pid = existing.get("monitor_pid")
        existing_state = existing.get("monitor_state")
        if existing_state in ACTIVE_STATES and process_exists(existing_pid if isinstance(existing_pid, int) else None):
            if existing_pid != os.getpid():
                raise RuntimeError(f"Monitor is already running with PID {existing_pid}")
        if self.spec.stop_flag_path.exists():
            self.spec.stop_flag_path.unlink()
        write_text(self.spec.pid_path, f"{os.getpid()}\n")
        self.state = "starting"
        checkpoint = read_json(self.spec.checkpoint_path)
        self._last_progress_marker = checkpoint_progress_marker(checkpoint)
        self._last_heartbeat_marker = checkpoint_heartbeat_marker(checkpoint)
        self.last_progress_at = progress_reference_timestamp(checkpoint) or now_iso()
        self._last_progress_monotonic = time.monotonic()
        self.write_status(message="monitor_booting")

    def build_child_command(self) -> list[str]:
        command = [
            str(monitor_python()),
            str(self.spec.runner_path),
            "--keyword",
            self.args.keyword,
            "--start-day",
            self.args.start_day,
            "--end-day",
            self.args.end_day,
            "--oldest-day",
            self.args.oldest_day,
            "--save-data-option",
            self.args.save_data_option,
            "--save-data-path",
            self.args.save_data_path,
            "--checkpoint-path",
            str(self.spec.checkpoint_path),
            "--login-type",
            self.args.login_type,
            "--max-notes-per-day",
            str(self.args.max_notes_per_day),
            "--max-comment-items",
            str(self.args.max_comment_items),
            "--max-concurrency-num",
            str(self.args.max_concurrency_num),
            "--crawler-sleep-seconds",
            str(self.args.crawler_sleep_seconds),
        ]
        if self.spec.default_max_sub_comment_items is not None and self.args.max_sub_comment_items is not None:
            command.extend(["--max-sub-comment-items", str(self.args.max_sub_comment_items)])
        if self.args.continuous_backfill:
            command.append("--continuous-backfill")
        if self.args.headless:
            command.append("--headless")
        if self.current_disable_cdp:
            command.append("--disable-cdp")
        if self.args.disable_comments:
            command.append("--disable-comments")
        if self.args.disable_sub_comments:
            command.append("--disable-sub-comments")
        return command

    def next_run_id(self) -> str:
        return f"{now().strftime('%Y%m%dT%H%M%S')}-{self.restart_count + 1:03d}"

    def start_child(self) -> None:
        ensure_runtime_dir(self.spec)
        self.current_run_id = self.next_run_id()
        self.current_run_dir = self.spec.runs_dir / self.current_run_id
        self.current_run_dir.mkdir(parents=True, exist_ok=True)
        self.current_run_log_path = self.current_run_dir / "crawler.log"
        self.current_run_manifest_path = self.current_run_dir / "run_manifest.json"
        self._run_started_at = now_iso()
        self._progress_seen_in_current_run = False
        checkpoint = read_json(self.spec.checkpoint_path)
        self._last_progress_marker = checkpoint_progress_marker(checkpoint)
        self._last_heartbeat_marker = checkpoint_heartbeat_marker(checkpoint)
        self.last_progress_at = progress_reference_timestamp(checkpoint) or self._run_started_at
        self._last_progress_monotonic = time.monotonic()
        self.failure_class = None
        self.failure_reason = ""
        self.state = "starting"

        if self.child_log_handle:
            self.close_child_log()
        self.child_log_handle = self.current_run_log_path.open("w", encoding="utf-8")
        command = self.build_child_command()
        append_monitor_log(self.spec, f"Starting child run {self.current_run_id}: {' '.join(command)}")
        self.child_log_handle.write(f"[{self._run_started_at}] === child run {self.current_run_id} start ===\n")
        self.child_log_handle.flush()
        self.child_process = subprocess.Popen(
            command,
            cwd=str(ROOT_DIR),
            stdout=self.child_log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        self.state = "running"
        self.last_repair_action = "started_child"
        self.write_status(message="child_started")

    def close_child_log(self) -> None:
        if not self.child_log_handle:
            return
        self.child_log_handle.flush()
        self.child_log_handle.close()
        self.child_log_handle = None

    def terminate_child(self) -> None:
        if not self.child_process or self.child_process.poll() is not None:
            return
        pid = self.child_process.pid
        append_monitor_log(self.spec, f"Stopping child PID {pid}")
        self.child_process.terminate()
        try:
            self.child_process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            append_monitor_log(self.spec, f"Child PID {pid} did not stop in time, killing")
            self.child_process.kill()
            self.child_process.wait(timeout=10)

    def current_checkpoint(self) -> dict[str, Any]:
        return read_json(self.spec.checkpoint_path)

    def refresh_progress(self) -> None:
        checkpoint = self.current_checkpoint()
        heartbeat_marker = checkpoint_heartbeat_marker(checkpoint)
        marker = checkpoint_progress_marker(checkpoint)
        if heartbeat_marker and heartbeat_marker != self._last_heartbeat_marker:
            self._last_heartbeat_marker = heartbeat_marker
            self._last_progress_marker = marker
            self._last_progress_monotonic = time.monotonic()
            self.last_progress_at = progress_reference_timestamp(checkpoint) or now_iso()
            self._progress_seen_in_current_run = True
            self.consecutive_failures = 0
        elif marker and marker != self._last_progress_marker:
            self._last_heartbeat_marker = heartbeat_marker
            self._last_progress_marker = marker
            self._last_progress_monotonic = time.monotonic()
            self.last_progress_at = progress_reference_timestamp(checkpoint) or now_iso()
            self._progress_seen_in_current_run = True
            self.consecutive_failures = 0
        elif not marker and self.current_run_log_path and self.current_run_log_path.exists():
            self.last_progress_at = datetime.fromtimestamp(
                self.current_run_log_path.stat().st_mtime
            ).astimezone().isoformat(timespec="seconds")

    def child_is_stalled(self) -> bool:
        if self.args.stall_timeout_seconds <= 0:
            return False
        if not self.child_process or self.child_process.poll() is not None:
            return False
        checkpoint = self.current_checkpoint()
        heartbeat_marker = checkpoint_heartbeat_marker(checkpoint)
        marker = checkpoint_progress_marker(checkpoint)
        if heartbeat_marker:
            return (time.monotonic() - self._last_progress_monotonic) >= self.args.stall_timeout_seconds
        if marker:
            return (time.monotonic() - self._last_progress_monotonic) >= self.args.stall_timeout_seconds
        if self.current_run_log_path and self.current_run_log_path.exists():
            seconds_since_log_update = time.time() - self.current_run_log_path.stat().st_mtime
            return seconds_since_log_update >= self.args.stall_timeout_seconds
        return False

    def should_enable_disable_cdp_fallback(self, failure_class: str) -> bool:
        return (
            not self.current_disable_cdp
            and self.consecutive_failures >= 3
            and not self._progress_seen_in_current_run
            and failure_class in {"browser_launch_failed", "child_exit_nonzero", "network_error", "data_fetch_error"}
        )

    def refresh_latest_crawler_summary(self) -> None:
        full_log_path = self.current_run_log_path
        last_line = tail_last_non_empty_line(full_log_path) if full_log_path else ""
        lines = [
            f"updated_at={now_iso()}",
            f"platform={self.spec.name}",
            f"run_id={self.current_run_id or ''}",
            f"monitor_state={self.state}",
            f"failure_class={self.failure_class or ''}",
            f"failure_reason={(self.failure_reason or '').splitlines()[0] if self.failure_reason else ''}",
            f"full_log={str(full_log_path.resolve()) if full_log_path and full_log_path.exists() else ''}",
            f"last_line={last_line}",
        ]
        write_text(self.spec.latest_crawler_summary_path, "\n".join(lines) + "\n")

    def write_run_manifest(self, *, child_exit_code: int | None = None) -> None:
        checkpoint = self.current_checkpoint()
        payload = {
            "platform": self.spec.name,
            "run_id": self.current_run_id,
            "started_at": self._run_started_at,
            "ended_at": now_iso() if child_exit_code is not None or self.state in TERMINAL_STATES else None,
            "monitor_pid": os.getpid(),
            "child_pid": self.child_process.pid if self.child_process and self.child_process.poll() is None else None,
            "child_exit_code": child_exit_code,
            "monitor_state": self.state,
            "failure_class": self.failure_class,
            "failure_reason": self.failure_reason,
            "job": job_identity_from_args(self.spec, self.args, disable_cdp=self.current_disable_cdp),
            "checkpoint_path": str(self.spec.checkpoint_path.resolve()),
            "monitor_log_path": str(self.spec.monitor_log_path.resolve()),
            "crawler_log_path": str(self.current_run_log_path.resolve()) if self.current_run_log_path else None,
            "latest_crawler_summary_path": str(self.spec.latest_crawler_summary_path.resolve()),
            "artifacts_path": str(Path(self.args.save_data_path).resolve()),
            "restart_count": self.restart_count,
            "consecutive_failures": self.consecutive_failures,
            "current_disable_cdp": self.current_disable_cdp,
            "last_progress_at": self.last_progress_at,
            "progress_heartbeat_at": progress_reference_timestamp(checkpoint),
        }
        if self.current_run_manifest_path:
            write_json(self.current_run_manifest_path, payload)
        write_json(self.spec.latest_run_path, payload)

    def write_status(self, *, child_exit_code: int | None = None, message: str = "") -> None:
        checkpoint = self.current_checkpoint()
        last_output_at = None
        if self.current_run_log_path and self.current_run_log_path.exists():
            last_output_at = datetime.fromtimestamp(
                self.current_run_log_path.stat().st_mtime
            ).astimezone().isoformat(timespec="seconds")

        self.refresh_latest_crawler_summary()
        self.write_run_manifest(child_exit_code=child_exit_code)

        child_pid = self.child_process.pid if self.child_process and self.child_process.poll() is None else None
        progress_heartbeat_at = progress_reference_timestamp(checkpoint) or self.last_progress_at or last_output_at
        healthy = self.state in RUNNING_STATES and progress_is_recent(
            progress_heartbeat_at,
            self.args.stall_timeout_seconds,
        )
        live = child_pid is not None if self.state in RUNNING_STATES else False
        degraded = False
        degraded_reason = ""
        if self.state in RUNNING_STATES:
            if not healthy:
                degraded = True
                degraded_reason = "progress_stale"
            elif self.consecutive_failures > 0:
                degraded = True
                degraded_reason = "recovering_after_failures"
            elif self.current_disable_cdp:
                degraded = True
                degraded_reason = "running_with_disable_cdp"

        payload = {
            "platform": self.spec.name,
            "updated_at": now_iso(),
            "monitor_state": self.state,
            "job_state": checkpoint.get("state") or self.state,
            "healthy": healthy,
            "live": live,
            "degraded": degraded,
            "degraded_reason": degraded_reason,
            "message": message,
            "monitor_pid": os.getpid(),
            "child_pid": child_pid,
            "child_exit_code": child_exit_code,
            "run_id": self.current_run_id,
            "restart_count": self.restart_count,
            "consecutive_failures": self.consecutive_failures,
            "check_interval_seconds": self.args.check_interval_seconds,
            "stall_timeout_seconds": self.args.stall_timeout_seconds,
            "current_day": checkpoint.get("current_day") or checkpoint.get("last_emitted_day"),
            "resume_day": checkpoint.get("resume_day") or checkpoint.get("last_emitted_day"),
            "resume_page": checkpoint.get("resume_page"),
            "notes_count_this_day": checkpoint.get("notes_count_this_day"),
            "total_notes_crawled_for_keyword": checkpoint.get("total_notes_crawled_for_keyword")
            if checkpoint.get("total_notes_crawled_for_keyword") is not None
            else checkpoint.get("contents_emitted"),
            "pages_completed": checkpoint.get("pages_completed"),
            "contents_emitted": checkpoint.get("contents_emitted"),
            "comments_emitted": checkpoint.get("comments_emitted"),
            "last_emitted_day": checkpoint.get("last_emitted_day"),
            "last_result_created_day": checkpoint.get("last_result_created_day"),
            "progress_heartbeat_at": progress_heartbeat_at,
            "heartbeat_kind": checkpoint.get("heartbeat_kind"),
            "heartbeat_cursor": checkpoint.get("heartbeat_cursor"),
            "last_progress_at": self.last_progress_at,
            "failure_class": self.failure_class,
            "failure_reason": self.failure_reason,
            "job": job_identity_from_args(self.spec, self.args, disable_cdp=self.current_disable_cdp),
            "checkpoint_path": str(self.spec.checkpoint_path.resolve()),
            "status_path": str(self.spec.status_path.resolve()),
            "latest_monitor_log": str(self.spec.monitor_log_path.resolve()),
            "latest_crawler_log": str(self.current_run_log_path.resolve()) if self.current_run_log_path else "",
            "last_repair_action": self.last_repair_action,
            "current_disable_cdp": self.current_disable_cdp,
            "last_output_at": last_output_at,
            "last_log_line": tail_last_non_empty_line(self.current_run_log_path) if self.current_run_log_path else "",
            "checkpoint": checkpoint,
            "monitor_log_path": str(self.spec.monitor_log_path.resolve()),
            "crawler_log_path": str(self.spec.latest_crawler_summary_path.resolve()),
            "latest_run_path": str(self.spec.latest_run_path.resolve()),
        }
        write_json(self.spec.status_path, payload)

    def sleep_with_stop_checks(self, seconds: float) -> None:
        remaining = max(float(seconds), 0.0)
        while remaining > 0:
            if self.spec.stop_flag_path.exists():
                return
            chunk = min(1.0, remaining)
            time.sleep(chunk)
            remaining -= chunk

    def finalize_error(self, *, child_exit_code: int | None, failure_class: str, failure_reason: str, message: str) -> int:
        self.failure_class = failure_class
        self.failure_reason = failure_reason
        self.state = "error"
        self.write_status(child_exit_code=child_exit_code, message=message)
        append_monitor_log(self.spec, message)
        return child_exit_code or 1

    def cleanup(self) -> None:
        self.close_child_log()
        if self.spec.pid_path.exists():
            try:
                pid_text = self.spec.pid_path.read_text(encoding="utf-8").strip()
            except OSError:
                pid_text = ""
            if pid_text == str(os.getpid()):
                try:
                    self.spec.pid_path.unlink()
                except OSError:
                    pass

    def run(self) -> int:
        try:
            self.bootstrap()
            while True:
                if self.spec.stop_flag_path.exists():
                    self.state = "stopping"
                    self.failure_class = "operator_stop"
                    self.failure_reason = "stop_requested"
                    self.write_status(message="stop_requested")
                    append_monitor_log(self.spec, "Stop flag detected, shutting down")
                    self.terminate_child()
                    self.close_child_log()
                    self.child_process = None
                    self.state = "stopped"
                    self.write_status(message="monitor_stopped")
                    return 0

                if self.child_process is None:
                    self.start_child()
                    self.sleep_with_stop_checks(self.args.check_interval_seconds)
                    continue

                exit_code = self.child_process.poll()
                if exit_code is None:
                    self.refresh_progress()
                    if self.child_is_stalled():
                        self.restart_count += 1
                        self.consecutive_failures += 1
                        self.last_repair_action = "restart_after_checkpoint_stall"
                        self.failure_class = "checkpoint_stalled"
                        self.failure_reason = (
                            f"Checkpoint did not advance for {self.args.stall_timeout_seconds} seconds."
                        )
                        self.state = "restarting"
                        self.write_status(message="checkpoint_stalled")
                        append_monitor_log(self.spec, "Checkpoint progress stalled; restarting child")
                        self.terminate_child()
                        self.close_child_log()
                        self.child_process = None
                        self.start_child()
                        continue

                    self.state = "running"
                    self.failure_class = None
                    self.failure_reason = ""
                    self.write_status(message="child_running")
                    self.sleep_with_stop_checks(self.args.check_interval_seconds)
                    continue

                self.close_child_log()
                self.child_process = None
                if exit_code == 0:
                    checkpoint = self.current_checkpoint()
                    job_state = str(checkpoint.get("state") or "")
                    if job_state not in TERMINAL_JOB_STATES:
                        self.restart_count += 1
                        self.consecutive_failures += 1
                        self.failure_class = "unexpected_clean_exit"
                        self.failure_reason = (
                            "Child exited with code 0 before checkpoint reached a terminal state "
                            f"(checkpoint.state={job_state or 'unknown'})."
                        )
                        if self.consecutive_failures >= 6:
                            return self.finalize_error(
                                child_exit_code=0,
                                failure_class=self.failure_class,
                                failure_reason=self.failure_reason,
                                message=(
                                    "Crawler exited cleanly before completion too many times. "
                                    "Automatic recovery budget was exhausted."
                                ),
                            )
                        self.last_repair_action = "restart_after_unexpected_clean_exit"
                        self.state = "restarting"
                        self.write_status(child_exit_code=0, message="child_exited_without_completion")
                        append_monitor_log(
                            self.spec,
                            "Child exited with code 0 before completion; restarting from checkpoint",
                        )
                        self.sleep_with_stop_checks(min(60, 5 * self.consecutive_failures))
                        self.start_child()
                        continue
                    self.consecutive_failures = 0
                    self.failure_class = None
                    self.failure_reason = ""
                    self.state = "completed"
                    self.last_repair_action = "child_completed"
                    self.write_status(child_exit_code=0, message="child_completed")
                    append_monitor_log(self.spec, "Child completed successfully")
                    return 0

                self.restart_count += 1
                self.consecutive_failures += 1
                failure_class, failure_reason = classify_failure(
                    checkpoint=self.current_checkpoint(),
                    log_path=self.current_run_log_path,
                )
                self.failure_class = failure_class
                self.failure_reason = failure_reason

                if self.should_enable_disable_cdp_fallback(failure_class):
                    self.current_disable_cdp = True
                    self.last_repair_action = "disable_cdp_after_repeated_startup_failures"
                    self.state = "restarting"
                    self.write_status(child_exit_code=exit_code, message="retry_with_disable_cdp")
                    append_monitor_log(self.spec, "Repeated startup failures detected; retrying with CDP disabled")
                    self.start_child()
                    continue

                if failure_class in FATAL_FAILURE_CLASSES:
                    message = {
                        "playwright_bootstrap_denied": (
                            "Playwright browser bootstrap was denied by the current environment "
                            "(WinError 5 / access denied). Run the crawler from a normal local terminal."
                        ),
                        "login_required": "Crawler stopped because an interactive login is required.",
                        "account_permission_denied": "Crawler stopped because the current account does not have access.",
                    }[failure_class]
                    return self.finalize_error(
                        child_exit_code=exit_code,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                        message=message,
                    )

                if self.consecutive_failures >= 6:
                    return self.finalize_error(
                        child_exit_code=exit_code,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                        message=(
                            f"Crawler exited repeatedly with {failure_class}. "
                            "Automatic recovery budget was exhausted."
                        ),
                    )

                self.last_repair_action = "restart_same_config"
                self.state = "restarting"
                self.write_status(child_exit_code=exit_code, message=f"child_exited_{exit_code}")
                append_monitor_log(
                    self.spec,
                    f"Child exited with code {exit_code}; restarting with failure_class={failure_class}",
                )
                self.sleep_with_stop_checks(min(60, 5 * self.consecutive_failures))
                self.start_child()
        finally:
            self.cleanup()


def serialize_run_args(args: argparse.Namespace, *, include_platform: bool) -> list[str]:
    command = ["run"]
    if include_platform:
        command.extend(["--platform", args.platform])
    command.extend(
        [
            "--keyword",
            args.keyword,
            "--start-day",
            args.start_day,
            "--end-day",
            args.end_day,
            "--oldest-day",
            args.oldest_day,
            "--check-interval-seconds",
            str(args.check_interval_seconds),
            "--stall-timeout-seconds",
            str(args.stall_timeout_seconds),
            "--max-notes-per-day",
            str(args.max_notes_per_day),
            "--max-comment-items",
            str(args.max_comment_items),
            "--max-concurrency-num",
            str(args.max_concurrency_num),
            "--crawler-sleep-seconds",
            str(args.crawler_sleep_seconds),
            "--save-data-option",
            args.save_data_option,
            "--save-data-path",
            args.save_data_path,
            "--login-type",
            args.login_type,
        ]
    )
    if args.max_sub_comment_items is not None:
        command.extend(["--max-sub-comment-items", str(args.max_sub_comment_items)])
    if getattr(args, "search_mode", None) and args.platform == "zhihu":
        command.extend(["--search-mode", str(args.search_mode)])
    if args.continuous_backfill:
        command.append("--continuous-backfill")
    if args.headless:
        command.append("--headless")
    if args.disable_cdp:
        command.append("--disable-cdp")
    if args.disable_comments:
        command.append("--disable-comments")
    if args.disable_sub_comments:
        command.append("--disable-sub-comments")
    return command


def start_background_monitor(
    spec: PlatformSpec,
    args: argparse.Namespace,
    *,
    entry_script: Path,
    include_platform: bool,
) -> int:
    existing = normalize_status_payload(spec)
    existing_pid = existing.get("monitor_pid")
    if existing.get("monitor_state") in RUNNING_STATES and process_exists(existing_pid if isinstance(existing_pid, int) else None):
        payload = {"event": "already_running", **existing}
        write_console_json(payload)
        return 0

    command = [str(monitor_python()), str(entry_script), *serialize_run_args(args, include_platform=include_platform)]
    kwargs: dict[str, Any] = {
        "cwd": str(ROOT_DIR),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if sys.platform == "win32":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(command, **kwargs)
    time.sleep(2)
    payload = normalize_status_payload(spec)
    if payload.get("monitor_state") in RUNNING_STATES and process_exists(
        payload.get("monitor_pid") if isinstance(payload.get("monitor_pid"), int) else None
    ):
        payload = {"event": "started", **payload}
    else:
        payload = {
            "event": "started",
            "platform": spec.name,
            "monitor_pid": process.pid,
            "status_path": str(spec.status_path.resolve()),
            "message": "monitor_process_spawned",
        }
    write_console_json(payload)
    return 0


def print_status(spec: PlatformSpec, *, as_json: bool) -> int:
    payload = normalize_status_payload(spec)
    if as_json:
        write_console_json(payload)
        return 0
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def request_stop(spec: PlatformSpec) -> int:
    ensure_runtime_dir(spec)
    payload = normalize_status_payload(spec)
    write_text(spec.stop_flag_path, f"{now_iso()}\n")
    child_pid = payload.get("child_pid")
    if isinstance(child_pid, int):
        terminate_process_tree(child_pid)
    payload["monitor_state"] = "stopping"
    payload["healthy"] = False
    payload["failure_class"] = "operator_stop"
    payload["failure_reason"] = "stop_requested"
    payload["message"] = "stop_requested"
    payload["updated_at"] = now_iso()
    write_json(spec.status_path, payload)
    write_console_json(
        {
            "event": "stop_requested",
            "platform": spec.name,
            "stop_flag": str(spec.stop_flag_path.resolve()),
            "status_path": str(spec.status_path.resolve()),
        }
    )
    return 0


def main(argv: list[str] | None = None, *, fixed_platform: str | None = None, entry_script: Path | None = None) -> int:
    parser = build_parser(fixed_platform=fixed_platform)
    args = parser.parse_args(argv)
    spec = resolve_platform_and_defaults(args, fixed_platform=fixed_platform)
    ensure_runtime_dir(spec)

    if args.command == "status":
        return print_status(spec, as_json=bool(args.json))
    if args.command == "stop":
        return request_stop(spec)
    if args.command == "start":
        return start_background_monitor(
            spec,
            args,
            entry_script=entry_script or Path(sys.argv[0]).resolve(),
            include_platform=fixed_platform is None,
        )
    if args.command == "run":
        monitor = UnifiedCrawlerMonitor(spec, args)
        try:
            return monitor.run()
        except KeyboardInterrupt:
            monitor.state = "stopped"
            monitor.failure_class = "operator_stop"
            monitor.failure_reason = "keyboard_interrupt"
            monitor.write_status(message="keyboard_interrupt")
            append_monitor_log(spec, "Monitor interrupted by keyboard")
            return 130
    parser.error(f"Unsupported command: {args.command}")
    return 2


def main_for_platform(platform: str, argv: list[str] | None = None, *, entry_script: Path | None = None) -> int:
    return main(argv=argv, fixed_platform=platform, entry_script=entry_script)

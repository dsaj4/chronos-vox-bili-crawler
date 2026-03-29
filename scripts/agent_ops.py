from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

from monitor_core import (
    DEFAULT_SAVE_DATA_PATH,
    PLATFORM_SPECS,
    ROOT_DIR,
    RUNNING_STATES,
    get_platform_spec,
    monitor_python,
    normalize_status_payload,
    now_iso,
    parse_timestamp,
    process_exists,
    read_json,
    read_tail_text,
    write_json,
)


AGENT_OPS_DIR = ROOT_DIR / "artifacts" / "agent_ops"
LATEST_AUDIT_PATH = AGENT_OPS_DIR / "latest_audit.json"
KEYWORD_STATE_PATH = AGENT_OPS_DIR / "keyword_state.json"
SUMMARYS_DIR = AGENT_OPS_DIR / "summaries"
KEYWORD_QUEUE_PATH = ROOT_DIR / "config" / "keyword_queue.json"
CRAWL_MONITOR_SCRIPT = ROOT_DIR / "scripts" / "crawl_monitor.py"
BUCKET_PATTERN = re.compile(r"^search_(contents|comments|creators)_(\d{4}-\d{2}-\d{2})\.json$")

ISSUE_NONE = "none"
ISSUE_LOGIN = "login"
ISSUE_PROJECT = "project"

EXIT_CODE_OK = 0
EXIT_CODE_LOGIN_REQUIRED = 10
EXIT_CODE_PROJECT_AUTOFIX = 20
EXIT_CODE_PROJECT_MANUAL = 30
EXIT_CODE_READY_FOR_NEXT_KEYWORD = 40

CONTENT_TIME_FIELDS = {
    "bili": "create_time",
    "zhihu": "created_time",
    "xhs": "time",
}

REQUIRED_FIELDS = {
    "bili": {
        "contents": {"video_id", "title", "video_url", "source_keyword", "create_time"},
        "comments": {"comment_id", "video_id", "content", "create_time"},
        "creators": {"user_id", "nickname"},
    },
    "zhihu": {
        "contents": {"content_id", "content_type", "title", "content_url", "created_time", "source_keyword"},
        "comments": {"comment_id", "content_id", "content_type", "content", "publish_time"},
        "creators": set(),
    },
    "xhs": {
        "contents": {"note_id", "title", "note_url", "time", "source_keyword"},
        "comments": {"comment_id", "note_id", "content", "create_time"},
        "creators": set(),
    },
}

LOGIN_BLOCKING_MARKERS = [
    "waiting for scan code login",
    "scan code login",
    "qrcode login failed",
    "qrcode was not found on the page",
    "login was not confirmed before timeout",
    "search_request_rejected_logged_out",
]

SOURCE_BUG_MARKERS = [
    "traceback (most recent call last):",
    "modulenotfounderror",
    "importerror",
    "syntaxerror",
    "attributeerror",
    "typeerror",
    "valueerror",
    "keyerror",
    "indexerror",
    "jsondecodeerror",
    "filenotfounderror",
    "assertionerror",
]

HIGH_RISK_PROJECT_SUBCLASSES = {"artifact_corruption"}


def ensure_agent_ops_dir() -> None:
    AGENT_OPS_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARYS_DIR.mkdir(parents=True, exist_ok=True)


def write_console_json(payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace"))
        sys.stdout.buffer.write(b"\n")


def slugify(value: object) -> str:
    cleaned = str(value or "").strip().lower()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in cleaned)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "item"


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def relative_to_root(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT_DIR.resolve()))
    except ValueError:
        return str(path.resolve())


def bucket_day_from_filename(path: Path) -> str | None:
    match = BUCKET_PATTERN.match(path.name)
    return match.group(2) if match else None


def bucket_kind_from_filename(path: Path) -> str | None:
    match = BUCKET_PATTERN.match(path.name)
    return match.group(1) if match else None


def platform_data_dirs(platform: str, job: dict[str, Any]) -> list[Path]:
    preferred_root = Path(str(job.get("save_data_path") or DEFAULT_SAVE_DATA_PATH)).resolve()
    candidates = [
        preferred_root / platform / "json",
        DEFAULT_SAVE_DATA_PATH.resolve() / platform / "json",
        ROOT_DIR / "MediaCrawler" / "data" / platform / "json",
    ]
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def latest_bucket_files(data_dir: Path) -> dict[str, Path]:
    latest: dict[str, tuple[float, Path]] = {}
    if not data_dir.exists():
        return {}
    for path in sorted(data_dir.glob("search_*.json")):
        if not path.is_file():
            continue
        kind = bucket_kind_from_filename(path)
        if not kind:
            continue
        score = path.stat().st_mtime
        current = latest.get(kind)
        if current is None or score >= current[0]:
            latest[kind] = (score, path)
    return {kind: item[1] for kind, item in latest.items()}


def load_json_list(path: Path) -> tuple[list[dict[str, Any]] | None, str | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, str(exc)
    if not isinstance(payload, list):
        return None, "json_root_is_not_list"
    normalized = [item for item in payload if isinstance(item, dict)]
    return normalized, None


def timestamp_to_day(platform: str, value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if platform == "xhs" and numeric > 10_000_000_000:
        numeric /= 1000.0
    try:
        return datetime.fromtimestamp(numeric).astimezone().date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def content_record_day(platform: str, item: dict[str, Any]) -> str | None:
    return timestamp_to_day(platform, item.get(CONTENT_TIME_FIELDS[platform]))


def summarize_bucket_file(platform: str, path: Path, kind: str, *, expected_keyword: str) -> dict[str, Any]:
    record_count = 0
    sample_fields: list[str] = []
    sample_checks: list[str] = []
    sample_ok = True
    parsed_items, parse_error = load_json_list(path)
    bucket_day = bucket_day_from_filename(path)
    record_day = None
    if parse_error:
        sample_ok = False
        sample_checks.append(f"parse_error:{parse_error}")
    elif parsed_items is None:
        sample_ok = False
        sample_checks.append("parsed_items_missing")
    elif not parsed_items:
        sample_ok = False
        sample_checks.append("empty_bucket_file")
    else:
        record_count = len(parsed_items)
        sample = parsed_items[0]
        sample_fields = sorted(sample.keys())
        required = REQUIRED_FIELDS[platform][kind]
        missing_fields = sorted(required - set(sample.keys()))
        if missing_fields:
            sample_ok = False
            sample_checks.append("missing_fields:" + ",".join(missing_fields))
        if kind == "contents":
            record_day = content_record_day(platform, sample)
            if bucket_day and record_day and bucket_day != record_day:
                sample_ok = False
                sample_checks.append(f"bucket_day_mismatch:{bucket_day}!={record_day}")
            if expected_keyword and sample.get("source_keyword") != expected_keyword:
                sample_ok = False
                sample_checks.append("source_keyword_mismatch")
        elif kind == "comments":
            if bucket_day and not (path.parent / f"search_contents_{bucket_day}.json").exists():
                sample_ok = False
                sample_checks.append(f"missing_content_bucket_for_comment_day:{bucket_day}")
    stat = path.stat()
    output_root = path.parents[2].resolve() if len(path.parents) > 2 else path.parent.resolve()
    return {
        "kind": kind,
        "path": str(path.resolve()),
        "relative_path": relative_to_root(path),
        "output_root": str(output_root),
        "bucket_day": bucket_day,
        "record_count": record_count,
        "mtime": datetime.fromtimestamp(stat.st_mtime).astimezone().isoformat(timespec="seconds"),
        "size_bytes": stat.st_size,
        "sample_ok": sample_ok,
        "sample_checks": sample_checks,
        "sample_fields": sample_fields,
        "sample_record_day": record_day,
        "parse_error": parse_error,
    }


def newest_file_summaries(file_summaries: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    newest = None
    newest_score = -1.0
    newest_content = None
    newest_content_score = -1.0
    for summary in file_summaries:
        score = datetime.fromisoformat(summary["mtime"]).timestamp()
        if score >= newest_score:
            newest_score = score
            newest = summary
        if summary["kind"] == "contents" and score >= newest_content_score:
            newest_content_score = score
            newest_content = summary
    return newest, newest_content


def artifact_progress_state(
    current: dict[str, Any] | None,
    previous: dict[str, Any] | None,
    *,
    progress_heartbeat_at: str | None,
    monitor_state: str,
) -> tuple[bool | None, str]:
    if monitor_state not in RUNNING_STATES:
        return None, ""
    if not current or not previous:
        return None, ""
    current_heartbeat = parse_timestamp(progress_heartbeat_at)
    previous_heartbeat = parse_timestamp(previous.get("progress_heartbeat_at"))
    if current_heartbeat is None or previous_heartbeat is None:
        return None, ""
    if current_heartbeat <= previous_heartbeat:
        return None, ""
    if (
        current.get("path") == previous.get("path")
        and current.get("size_bytes") == previous.get("size_bytes")
        and current.get("record_count") == previous.get("record_count")
        and current.get("mtime") == previous.get("mtime")
    ):
        return False, "heartbeat_advanced_but_artifact_did_not_change"
    return True, ""


def artifact_audit(
    platform: str,
    job: dict[str, Any],
    status_payload: dict[str, Any],
    *,
    previous_result: dict[str, Any] | None,
) -> tuple[str, dict[str, Any], list[str]]:
    expected_keyword = str(job.get("keyword") or "")
    expected_output_root = Path(str(job.get("save_data_path") or DEFAULT_SAVE_DATA_PATH)).resolve()
    output_dirs = platform_data_dirs(platform, job)
    existing_dirs = [path for path in output_dirs if path.exists()]
    output_roots = sorted({str(path.parents[1].resolve()) for path in existing_dirs})
    file_summaries: list[dict[str, Any]] = []
    for data_dir in existing_dirs:
        latest = latest_bucket_files(data_dir)
        for kind, path in latest.items():
            summary = summarize_bucket_file(platform, path, kind, expected_keyword=expected_keyword)
            summary["is_expected_root"] = summary.get("output_root") == str(expected_output_root)
            file_summaries.append(summary)
    newest_file, newest_content = newest_file_summaries(file_summaries)
    issues: list[str] = []
    status = "healthy"
    if len(output_roots) > 1:
        issues.append("multiple_output_roots_detected")
        status = "degraded"
    if not existing_dirs:
        status = "missing"
        issues.append("no_output_directory_found")
    preferred_root_corrupted = False
    nonpreferred_root_issues: list[str] = []
    for summary in file_summaries:
        if not summary["sample_ok"]:
            if summary.get("is_expected_root", False):
                issues.extend(summary["sample_checks"])
                preferred_root_corrupted = True
            else:
                nonpreferred_root_issues.extend([f"nonpreferred_root_{item}" for item in summary["sample_checks"]])
    if preferred_root_corrupted:
        status = "corrupted"
    elif nonpreferred_root_issues:
        issues.extend(nonpreferred_root_issues)
        if status == "healthy":
            status = "degraded"
    previous_snapshot = None
    previous_heartbeat = None
    if previous_result:
        previous_details = previous_result.get("artifact_details")
        if isinstance(previous_details, dict):
            previous_snapshot = previous_details.get("latest_data_snapshot")
        previous_heartbeat = previous_result.get("progress_heartbeat_at")
    progressing, progress_reason = artifact_progress_state(
        newest_file,
        previous_snapshot if isinstance(previous_snapshot, dict) else None,
        progress_heartbeat_at=status_payload.get("progress_heartbeat_at"),
        monitor_state=str(status_payload.get("monitor_state") or ""),
    )
    if progressing is False:
        issues.append(progress_reason)
        if status == "healthy":
            status = "degraded"
    counts_zero = safe_int(status_payload.get("total_notes_crawled_for_keyword")) == 0 and safe_int(
        status_payload.get("contents_emitted")
    ) == 0
    if status == "missing" and counts_zero and status_payload.get("job_state") == "completed":
        status = "no_results"
        issues = [issue for issue in issues if issue != "no_output_directory_found"]
    details = {
        "status": status,
        "issues": issues,
        "expected_output_root": str(expected_output_root),
        "output_roots": output_roots,
        "data_directories": [str(path.resolve()) for path in output_dirs],
        "latest_files": sorted(file_summaries, key=lambda item: item["mtime"], reverse=True),
        "latest_data_snapshot": newest_file,
        "latest_content_snapshot": newest_content,
        "progressing": progressing,
        "progress_reason": progress_reason,
        "previous_progress_heartbeat_at": previous_heartbeat,
    }
    return status, details, issues


def detect_login_blocked(status_payload: dict[str, Any], tail_text: str) -> bool:
    failure_class = str(status_payload.get("failure_class") or "")
    if failure_class in {"login_required", "account_permission_denied"}:
        return True
    lowered = tail_text.lower()
    return any(marker in lowered for marker in LOGIN_BLOCKING_MARKERS)


def detect_source_bug(log_text: str) -> bool:
    lowered = log_text.lower()
    return any(marker in lowered for marker in SOURCE_BUG_MARKERS)


def workspace_status() -> dict[str, Any]:
    git_path = ROOT_DIR.parent / "tools" / "MinGit" / "cmd" / "git.exe"
    command = [str(git_path)] if git_path.exists() else ["git"]
    command.extend(
        [
            "-c",
            f"safe.directory={ROOT_DIR.as_posix()}",
            "-C",
            str(ROOT_DIR),
            "status",
            "--short",
        ]
    )
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return {"available": False, "dirty": None, "entries": []}
    entries = [line.rstrip() for line in result.stdout.splitlines() if line.strip()]
    return {
        "available": result.returncode == 0,
        "dirty": bool(entries),
        "entries": entries,
    }


def completion_verdict_for_status(
    status_payload: dict[str, Any],
    *,
    artifact_health: str,
    tail_text: str,
    issue_class: str,
) -> str:
    monitor_state = str(status_payload.get("monitor_state") or "")
    job_state = str(status_payload.get("job_state") or "")
    total_notes = safe_int(status_payload.get("total_notes_crawled_for_keyword"))
    if issue_class == ISSUE_LOGIN:
        return "blocked_by_login"
    if monitor_state in RUNNING_STATES:
        return "running"
    if issue_class == ISSUE_PROJECT and monitor_state in {"error", "stopped"}:
        return "project_error"
    if monitor_state == "completed" and job_state == "completed":
        if detect_source_bug(tail_text):
            return "project_error"
        if total_notes == 0 and artifact_health in {"healthy", "no_results", "missing"}:
            return "completed_no_results"
        if artifact_health in {"healthy", "degraded"}:
            return "completed"
        return "project_error"
    if monitor_state in {"stopped", "error"}:
        if job_state == "completed":
            if total_notes == 0 and artifact_health in {"healthy", "no_results", "missing"}:
                return "completed_no_results"
            return "completed" if artifact_health in {"healthy", "degraded"} else "project_error"
        return "interrupted"
    if issue_class == ISSUE_PROJECT:
        return "project_error"
    return "interrupted"


def determine_issue(
    status_payload: dict[str, Any],
    *,
    artifact_health: str,
    artifact_issues: list[str],
    tail_text: str,
    workspace: dict[str, Any],
) -> tuple[str, str, str]:
    failure_class = str(status_payload.get("failure_class") or "")
    monitor_state = str(status_payload.get("monitor_state") or "")
    job_state = str(status_payload.get("job_state") or "")
    status_degraded = bool(status_payload.get("degraded"))
    if detect_login_blocked(status_payload, tail_text):
        if failure_class == "account_permission_denied" or "permission" in tail_text.lower():
            return ISSUE_LOGIN, "account_permission_denied", "request_login"
        return ISSUE_LOGIN, "login_required", "request_login"
    if artifact_health == "corrupted":
        return ISSUE_PROJECT, "artifact_corruption", "manual_artifact_recovery_required"
    if "multiple_output_roots_detected" in artifact_issues:
        return ISSUE_PROJECT, "config_drift", "correct_output_path_and_restart"
    if failure_class in {"browser_launch_failed", "network_error"}:
        return ISSUE_PROJECT, "browser_or_network", "restart_monitor"
    if detect_source_bug(tail_text):
        if workspace.get("dirty"):
            return ISSUE_PROJECT, "source_bug", "source_fix_blocked_by_dirty_workspace"
        return ISSUE_PROJECT, "source_bug", "source_fix_required"
    if monitor_state == "running" and (artifact_health == "degraded" or status_degraded):
        return ISSUE_PROJECT, "runtime_restartable", "restart_monitor"
    if monitor_state in {"stopped", "error"} and job_state != "completed":
        return ISSUE_PROJECT, "runtime_restartable", "restart_monitor"
    if failure_class in {"child_exit_nonzero", "unexpected_clean_exit", "checkpoint_stalled", "data_fetch_error"}:
        return ISSUE_PROJECT, "runtime_restartable", "restart_monitor"
    if monitor_state == "completed" and job_state == "completed":
        return ISSUE_NONE, "none", "advance_keyword"
    return ISSUE_NONE, "none", "none"


def exit_code_for_result(result: dict[str, Any]) -> int:
    completion_verdict = result.get("completion_verdict")
    issue_class = result.get("issue_class")
    issue_subclass = result.get("issue_subclass")
    if completion_verdict in {"completed", "completed_no_results"}:
        return EXIT_CODE_READY_FOR_NEXT_KEYWORD
    if issue_class == ISSUE_LOGIN:
        return EXIT_CODE_LOGIN_REQUIRED
    if issue_class == ISSUE_PROJECT:
        if issue_subclass in HIGH_RISK_PROJECT_SUBCLASSES or result.get("action") == "source_fix_blocked_by_dirty_workspace":
            return EXIT_CODE_PROJECT_MANUAL
        return EXIT_CODE_PROJECT_AUTOFIX
    return EXIT_CODE_OK


def aggregate_exit_code(results: list[dict[str, Any]]) -> int:
    codes = [exit_code_for_result(result) for result in results]
    for preferred in (
        EXIT_CODE_PROJECT_MANUAL,
        EXIT_CODE_PROJECT_AUTOFIX,
        EXIT_CODE_LOGIN_REQUIRED,
        EXIT_CODE_READY_FOR_NEXT_KEYWORD,
    ):
        if preferred in codes:
            return preferred
    return EXIT_CODE_OK


def build_login_command(platform: str, keyword: str) -> str:
    script_map = {
        "bili": "start_bili_ai_monitor.ps1",
        "zhihu": "start_zhihu_monitor.ps1",
        "xhs": "start_xhs_monitor.ps1",
    }
    script = script_map[platform]
    return (
        "powershell -ExecutionPolicy Bypass -File "
        f".\\scripts\\{script} -Action foreground -Keyword {keyword}"
    )


def previous_audit_results() -> dict[str, Any]:
    payload = read_json(LATEST_AUDIT_PATH)
    results = payload.get("results")
    if isinstance(results, dict):
        return results
    return {}


def audit_platform(platform: str, *, previous_result: dict[str, Any] | None = None) -> dict[str, Any]:
    spec = get_platform_spec(platform)
    status_payload = normalize_status_payload(spec)
    job = dict(status_payload.get("job") or {})
    latest_log_path = Path(str(status_payload.get("latest_crawler_log") or ""))
    tail_text = read_tail_text(latest_log_path, max_bytes=131072) if latest_log_path.exists() else ""
    artifact_health, artifact_details, artifact_issues = artifact_audit(
        platform,
        job,
        status_payload,
        previous_result=previous_result,
    )
    workspace = workspace_status()
    issue_class, issue_subclass, action = determine_issue(
        status_payload,
        artifact_health=artifact_health,
        artifact_issues=artifact_issues,
        tail_text=tail_text,
        workspace=workspace,
    )
    completion_verdict = completion_verdict_for_status(
        status_payload,
        artifact_health=artifact_health,
        tail_text=tail_text,
        issue_class=issue_class,
    )
    if completion_verdict in {"completed", "completed_no_results"}:
        action = "advance_keyword"
    login_required = issue_class == ISSUE_LOGIN
    result = {
        "platform": platform,
        "audited_at": now_iso(),
        "issue_class": issue_class,
        "issue_subclass": issue_subclass,
        "action": action,
        "completion_verdict": completion_verdict,
        "artifact_health": artifact_health,
        "artifact_details": artifact_details,
        "login_required": login_required,
        "status": {
            "monitor_state": status_payload.get("monitor_state"),
            "job_state": status_payload.get("job_state"),
            "healthy": status_payload.get("healthy"),
            "live": status_payload.get("live"),
            "degraded": status_payload.get("degraded"),
            "degraded_reason": status_payload.get("degraded_reason"),
            "monitor_pid": status_payload.get("monitor_pid"),
            "child_pid": status_payload.get("child_pid"),
            "child_exit_code": status_payload.get("child_exit_code"),
            "run_id": status_payload.get("run_id"),
            "restart_count": status_payload.get("restart_count"),
            "consecutive_failures": status_payload.get("consecutive_failures"),
            "failure_class": status_payload.get("failure_class"),
            "failure_reason": status_payload.get("failure_reason"),
            "progress_heartbeat_at": status_payload.get("progress_heartbeat_at"),
            "heartbeat_kind": status_payload.get("heartbeat_kind"),
            "heartbeat_cursor": status_payload.get("heartbeat_cursor"),
            "last_progress_at": status_payload.get("last_progress_at"),
            "last_log_line": status_payload.get("last_log_line"),
            "current_day": status_payload.get("current_day"),
            "resume_day": status_payload.get("resume_day"),
            "resume_page": status_payload.get("resume_page"),
            "notes_count_this_day": status_payload.get("notes_count_this_day"),
            "total_notes_crawled_for_keyword": status_payload.get("total_notes_crawled_for_keyword"),
            "contents_emitted": status_payload.get("contents_emitted"),
            "comments_emitted": status_payload.get("comments_emitted"),
            "pages_completed": status_payload.get("pages_completed"),
            "last_emitted_day": status_payload.get("last_emitted_day"),
            "last_result_created_day": status_payload.get("last_result_created_day"),
            "last_completed_day": status_payload.get("last_completed_day"),
        },
        "job": job,
        "paths": {
            "checkpoint_path": status_payload.get("checkpoint_path"),
            "status_path": status_payload.get("status_path"),
            "latest_monitor_log": status_payload.get("latest_monitor_log"),
            "latest_crawler_log": status_payload.get("latest_crawler_log"),
            "latest_run_path": status_payload.get("latest_run_path"),
        },
        "login_command": build_login_command(platform, str(job.get("keyword") or "ai")),
        "workspace": workspace,
        "progress_heartbeat_at": status_payload.get("progress_heartbeat_at"),
        "exit_code": EXIT_CODE_OK,
        "checkpoint": {
            "state": status_payload.get("job_state"),
            "current_day": status_payload.get("current_day"),
            "resume_day": status_payload.get("resume_day"),
            "resume_page": status_payload.get("resume_page"),
            "notes_count_this_day": status_payload.get("notes_count_this_day"),
            "total_notes_crawled_for_keyword": status_payload.get("total_notes_crawled_for_keyword"),
            "contents_emitted": status_payload.get("contents_emitted"),
            "comments_emitted": status_payload.get("comments_emitted"),
            "pages_completed": status_payload.get("pages_completed"),
            "last_emitted_day": status_payload.get("last_emitted_day"),
            "last_result_created_day": status_payload.get("last_result_created_day"),
            "last_completed_day": status_payload.get("last_completed_day"),
            "updated_at": status_payload.get("progress_heartbeat_at") or status_payload.get("last_progress_at"),
        },
    }
    latest_run_path = Path(str(status_payload.get("latest_run_path") or ""))
    result["latest_run"] = read_json(latest_run_path) if latest_run_path.exists() else {}
    result["exit_code"] = exit_code_for_result(result)
    return result


def audit_platforms(platform: str) -> dict[str, Any]:
    ensure_agent_ops_dir()
    platforms = sorted(PLATFORM_SPECS.keys()) if platform == "all" else [platform]
    previous_results = previous_audit_results()
    results = {
        name: audit_platform(name, previous_result=previous_results.get(name) if isinstance(previous_results.get(name), dict) else None)
        for name in platforms
    }
    payload = {
        "audited_at": now_iso(),
        "platform": platform,
        "results": results,
        "exit_code": aggregate_exit_code(list(results.values())),
    }
    merged_results = previous_results
    merged_results.update(results)
    write_json(
        LATEST_AUDIT_PATH,
        {
            "audited_at": payload["audited_at"],
            "results": merged_results,
        },
    )
    return payload


def monitor_command_args(command: str, platform: str, job: dict[str, Any] | None = None) -> list[str]:
    args = [str(monitor_python()), str(CRAWL_MONITOR_SCRIPT), command, "--platform", platform]
    if not job:
        if command == "status":
            args.append("--json")
        return args
    args.extend(
        [
            "--keyword",
            str(job["keyword"]),
            "--start-day",
            str(job["start_day"]),
            "--end-day",
            str(job["end_day"]),
            "--oldest-day",
            str(job["oldest_day"]),
            "--check-interval-seconds",
            str(job["check_interval_seconds"]),
            "--stall-timeout-seconds",
            str(job["stall_timeout_seconds"]),
            "--max-notes-per-day",
            str(job["max_notes_per_day"]),
            "--max-comment-items",
            str(job["max_comment_items"]),
            "--max-concurrency-num",
            str(job["max_concurrency_num"]),
            "--crawler-sleep-seconds",
            str(job["crawler_sleep_seconds"]),
            "--save-data-option",
            str(job["save_data_option"]),
            "--save-data-path",
            str(job["save_data_path"]),
            "--login-type",
            str(job["login_type"]),
        ]
    )
    if job.get("search_mode"):
        args.extend(["--search-mode", str(job["search_mode"])])
    max_sub_comment_items = job.get("max_sub_comment_items")
    if max_sub_comment_items is not None:
        args.extend(["--max-sub-comment-items", str(max_sub_comment_items)])
    if job.get("continuous_backfill", True):
        args.append("--continuous-backfill")
    if job.get("headless"):
        args.append("--headless")
    if job.get("disable_cdp"):
        args.append("--disable-cdp")
    if job.get("disable_comments"):
        args.append("--disable-comments")
    if job.get("disable_sub_comments"):
        args.append("--disable-sub-comments")
    if command == "status":
        args.append("--json")
    return args


def normalized_monitor_job(platform: str, source_job: dict[str, Any], *, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    spec = get_platform_spec(platform)
    today = date.today().isoformat()
    payload = {
        "platform": platform,
        "keyword": str(source_job.get("keyword") or "ai"),
        "start_day": str(source_job.get("start_day") or today),
        "end_day": str(source_job.get("end_day") or today),
        "oldest_day": str(source_job.get("oldest_day") or spec.default_oldest_day()),
        "continuous_backfill": bool(source_job.get("continuous_backfill", True)),
        "save_data_option": str(source_job.get("save_data_option") or "json"),
        "save_data_path": str(Path(str(source_job.get("save_data_path") or DEFAULT_SAVE_DATA_PATH)).resolve()),
        "login_type": str(source_job.get("login_type") or "qrcode"),
        "headless": bool(source_job.get("headless", False)),
        "disable_cdp": bool(source_job.get("disable_cdp", False)),
        "disable_comments": bool(source_job.get("disable_comments", False)),
        "disable_sub_comments": bool(source_job.get("disable_sub_comments", False)),
        "max_notes_per_day": safe_int(source_job.get("max_notes_per_day"), 5),
        "max_comment_items": safe_int(source_job.get("max_comment_items"), spec.default_max_comment_items),
        "max_sub_comment_items": source_job.get("max_sub_comment_items", spec.default_max_sub_comment_items),
        "max_concurrency_num": safe_int(source_job.get("max_concurrency_num"), 1),
        "crawler_sleep_seconds": safe_float(source_job.get("crawler_sleep_seconds"), 2.0),
        "check_interval_seconds": safe_int(source_job.get("check_interval_seconds"), 30),
        "stall_timeout_seconds": safe_int(source_job.get("stall_timeout_seconds"), spec.default_stall_timeout_seconds),
        "search_mode": source_job.get("search_mode", "one_year_stream_bucketed" if platform == "zhihu" else None),
    }
    if overrides:
        payload.update(overrides)
    return payload


def preferred_job_payload(job: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "platform": job["platform"],
        "keyword": job["keyword"],
        "start_day": job["start_day"],
        "end_day": job["end_day"],
        "oldest_day": job["oldest_day"],
        "continuous_backfill": bool(job.get("continuous_backfill", True)),
        "save_data_option": job["save_data_option"],
        "save_data_path": str(Path(str(job["save_data_path"])).resolve()),
    }
    if job.get("search_mode"):
        payload["search_mode"] = job["search_mode"]
    return payload


def run_monitor_command(command: str, platform: str, job: dict[str, Any] | None = None) -> tuple[int, dict[str, Any], str]:
    args = monitor_command_args(command, platform, job)
    result = subprocess.run(
        args,
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=45,
    )
    payload: dict[str, Any] = {}
    stdout = result.stdout.strip()
    if stdout:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            payload = {"raw_stdout": stdout}
    return result.returncode, payload, stdout or result.stderr.strip()


def wait_for_monitor_stop(platform: str, *, timeout_seconds: int = 45) -> dict[str, Any]:
    import time

    deadline = datetime.now().timestamp() + timeout_seconds
    payload: dict[str, Any] = {}
    while datetime.now().timestamp() < deadline:
        _, payload, _ = run_monitor_command("status", platform)
        state = str(payload.get("monitor_state") or "")
        pid = payload.get("monitor_pid")
        if state not in RUNNING_STATES and not process_exists(pid if isinstance(pid, int) else None):
            return payload
        time.sleep(1)
    _, payload, _ = run_monitor_command("status", platform)
    return payload


def repair_platform(platform: str, *, dry_run: bool = False) -> dict[str, Any]:
    audit_payload = audit_platforms(platform)
    result = dict(audit_payload["results"][platform])
    job = normalized_monitor_job(platform, result.get("job") or {})
    actions_taken: list[str] = []
    if result["issue_class"] == ISSUE_NONE:
        result["repair_status"] = "noop"
        result["actions_taken"] = actions_taken
        result["exit_code"] = exit_code_for_result(result)
        return result
    if result["issue_class"] == ISSUE_LOGIN:
        if dry_run:
            actions_taken.append("would_request_login")
        else:
            _, status_payload, _ = run_monitor_command("status", platform)
            if str(status_payload.get("monitor_state") or "") in RUNNING_STATES:
                run_monitor_command("stop", platform)
            actions_taken.append("request_login")
        result["repair_status"] = "blocked_by_login"
        result["actions_taken"] = actions_taken
        result["exit_code"] = EXIT_CODE_LOGIN_REQUIRED
        return result

    subclass = result["issue_subclass"]
    if subclass == "artifact_corruption":
        result["repair_status"] = "manual_required"
        result["actions_taken"] = actions_taken
        result["exit_code"] = EXIT_CODE_PROJECT_MANUAL
        return result

    if subclass == "source_bug":
        result["repair_status"] = "source_fix_required"
        result["actions_taken"] = actions_taken
        result["exit_code"] = exit_code_for_result(result)
        return result

    corrected_job = dict(job)
    if subclass == "config_drift":
        corrected_job["save_data_path"] = str(DEFAULT_SAVE_DATA_PATH.resolve())
        actions_taken.append("correct_output_path")
        if not dry_run:
            spec = get_platform_spec(platform)
            write_json(spec.preferred_job_path, preferred_job_payload(corrected_job))

    if subclass == "browser_or_network" and not corrected_job.get("disable_cdp"):
        corrected_job["disable_cdp"] = True
        actions_taken.append("enable_disable_cdp_fallback")

    spec = get_platform_spec(platform)
    if spec.stop_flag_path.exists():
        actions_taken.append("clear_stale_stop_flag")
        if not dry_run:
            try:
                spec.stop_flag_path.unlink()
            except OSError:
                pass

    if dry_run:
        actions_taken.append("would_restart_monitor")
        result["repair_status"] = "planned_restart"
        result["repaired_job"] = corrected_job
        result["actions_taken"] = actions_taken
        result["exit_code"] = EXIT_CODE_PROJECT_AUTOFIX
        return result

    _, status_payload, _ = run_monitor_command("status", platform)
    if str(status_payload.get("monitor_state") or "") in RUNNING_STATES:
        run_monitor_command("stop", platform)
        wait_for_monitor_stop(platform)
        actions_taken.append("stop_running_monitor")

    start_code, start_payload, _ = run_monitor_command("start", platform, corrected_job)
    actions_taken.append("start_monitor")
    result["repair_status"] = "restart_requested" if start_code == 0 else "restart_failed"
    result["repaired_job"] = corrected_job
    result["actions_taken"] = actions_taken
    result["start_payload"] = start_payload
    result["exit_code"] = EXIT_CODE_PROJECT_AUTOFIX if start_code == 0 else EXIT_CODE_PROJECT_MANUAL
    return result


def load_keyword_queue() -> dict[str, Any]:
    payload = read_json(KEYWORD_QUEUE_PATH)
    keywords_raw = payload.get("keywords")
    normalized_keywords: list[dict[str, Any]] = []
    if isinstance(keywords_raw, list):
        for item in keywords_raw:
            if isinstance(item, str):
                keyword = item.strip()
                if keyword:
                    normalized_keywords.append({"keyword": keyword, "enabled": True})
            elif isinstance(item, dict):
                keyword = str(item.get("keyword") or "").strip()
                if keyword:
                    normalized_keywords.append({"keyword": keyword, "enabled": bool(item.get("enabled", True))})
    if not normalized_keywords:
        normalized_keywords = [{"keyword": "ai", "enabled": True}]
    return {
        "version": int(payload.get("version") or 1),
        "keywords": normalized_keywords,
    }


def enabled_keywords(queue_payload: dict[str, Any]) -> list[str]:
    return [str(item["keyword"]) for item in queue_payload.get("keywords", []) if item.get("enabled", True)]


def load_keyword_state() -> dict[str, Any]:
    payload = read_json(KEYWORD_STATE_PATH)
    if not isinstance(payload.get("platforms"), dict):
        return {"updated_at": now_iso(), "platforms": {}}
    return payload


def save_keyword_state(payload: dict[str, Any]) -> None:
    payload["updated_at"] = now_iso()
    write_json(KEYWORD_STATE_PATH, payload)


def initial_platform_keyword_state(platform: str, queue_keywords: list[str], audit_result: dict[str, Any]) -> dict[str, Any]:
    current_keyword = str(audit_result.get("job", {}).get("keyword") or (queue_keywords[0] if queue_keywords else "ai"))
    if current_keyword in queue_keywords:
        current_index = queue_keywords.index(current_keyword)
    else:
        current_index = 0 if queue_keywords else None
    return {
        "current_index": current_index,
        "current_keyword": current_keyword,
        "run_state": audit_result.get("status", {}).get("monitor_state"),
        "completion_verdict": audit_result.get("completion_verdict"),
        "last_summary_path": "",
        "last_started_at": audit_result.get("status", {}).get("progress_heartbeat_at"),
        "last_completed_at": audit_result.get("audited_at")
        if audit_result.get("completion_verdict") in {"completed", "completed_no_results"}
        else None,
        "blocked_reason": audit_result.get("issue_subclass") if audit_result.get("issue_class") == ISSUE_LOGIN else "",
    }


def ensure_keyword_state(platforms: list[str], audit_results: dict[str, Any]) -> dict[str, Any]:
    queue_payload = load_keyword_queue()
    queue_keywords = enabled_keywords(queue_payload)
    state = load_keyword_state()
    platform_state = state.setdefault("platforms", {})
    for platform in platforms:
        current = platform_state.get(platform)
        audit_result = audit_results[platform]
        if not isinstance(current, dict):
            platform_state[platform] = initial_platform_keyword_state(platform, queue_keywords, audit_result)
            continue
        current_keyword = str(current.get("current_keyword") or "")
        if current_keyword in queue_keywords:
            current_index = queue_keywords.index(current_keyword)
        else:
            audit_keyword = str(audit_result.get("job", {}).get("keyword") or (queue_keywords[0] if queue_keywords else "ai"))
            current_index = queue_keywords.index(audit_keyword) if audit_keyword in queue_keywords else 0
            current_keyword = audit_keyword
        current.update(
            {
                "current_index": current_index,
                "current_keyword": current_keyword,
                "run_state": audit_result.get("status", {}).get("monitor_state"),
                "completion_verdict": audit_result.get("completion_verdict"),
                "blocked_reason": audit_result.get("issue_subclass") if audit_result.get("issue_class") == ISSUE_LOGIN else "",
            }
        )
        if audit_result.get("completion_verdict") in {"completed", "completed_no_results"}:
            current["last_completed_at"] = audit_result.get("audited_at")
    save_keyword_state(state)
    return state


def latest_bucket_day_from_audit(audit_result: dict[str, Any]) -> str | None:
    details = audit_result.get("artifact_details")
    if not isinstance(details, dict):
        return None
    latest_content = details.get("latest_content_snapshot")
    if isinstance(latest_content, dict):
        return latest_content.get("bucket_day")
    latest_files = details.get("latest_files")
    if isinstance(latest_files, list):
        for item in latest_files:
            if isinstance(item, dict) and item.get("bucket_day"):
                return item.get("bucket_day")
    return None


def write_keyword_summary(
    platform: str,
    audit_result: dict[str, Any],
    *,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    ensure_agent_ops_dir()
    keyword = str(audit_result.get("job", {}).get("keyword") or "ai")
    run_id = str(audit_result.get("status", {}).get("run_id") or "no-run")
    target_dir = output_dir or SUMMARYS_DIR / platform
    target_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{slugify(keyword)}_{slugify(run_id)}"
    json_path = target_dir / f"{stem}.json"
    md_path = target_dir / f"{stem}.md"
    latest_bucket_day = latest_bucket_day_from_audit(audit_result)
    latest_files = audit_result.get("artifact_details", {}).get("latest_files", [])
    latest_file_paths = [item.get("relative_path") for item in latest_files[:3] if isinstance(item, dict)]
    checkpoint = audit_result.get("checkpoint", {})
    status_payload = audit_result.get("status", {})
    latest_run = audit_result.get("latest_run", {})
    artifact_details = audit_result.get("artifact_details", {})
    sample_snapshot = artifact_details.get("latest_content_snapshot") or artifact_details.get("latest_data_snapshot") or {}
    content_count = safe_int(
        checkpoint.get("contents_emitted")
        or checkpoint.get("total_notes_crawled_for_keyword")
        or status_payload.get("contents_emitted")
        or status_payload.get("total_notes_crawled_for_keyword")
    )
    comment_count = safe_int(checkpoint.get("comments_emitted") or status_payload.get("comments_emitted"))
    verdict_label = {
        "completed": "正常完成",
        "completed_no_results": "无结果完成",
        "running": "仍在爬取中",
        "interrupted": "任务中断",
        "blocked_by_login": "登录阻塞",
        "project_error": "项目错误",
    }.get(str(audit_result.get("completion_verdict")), "状态未明")
    summary_payload = {
        "platform": platform,
        "keyword": keyword,
        "run_id": run_id,
        "completion_verdict": audit_result.get("completion_verdict"),
        "completion_label": verdict_label,
        "monitor_state": status_payload.get("monitor_state"),
        "checkpoint_state": checkpoint.get("state"),
        "failure_class": status_payload.get("failure_class"),
        "failure_reason": status_payload.get("failure_reason"),
        "run_started_at": latest_run.get("started_at"),
        "run_ended_at": latest_run.get("ended_at"),
        "content_count": content_count,
        "comment_count": comment_count,
        "latest_bucket_day": latest_bucket_day,
        "latest_files": latest_file_paths,
        "artifact_health": audit_result.get("artifact_health"),
        "artifact_issues": artifact_details.get("issues", []),
        "artifact_sample": {
            "path": sample_snapshot.get("relative_path"),
            "bucket_day": sample_snapshot.get("bucket_day"),
            "record_count": sample_snapshot.get("record_count"),
            "sample_ok": sample_snapshot.get("sample_ok"),
            "sample_checks": sample_snapshot.get("sample_checks", []),
            "sample_fields": sample_snapshot.get("sample_fields", []),
        },
        "issue_class": audit_result.get("issue_class"),
        "issue_subclass": audit_result.get("issue_subclass"),
        "next_action": {
            "completed": "advance_keyword",
            "completed_no_results": "advance_keyword",
            "running": "wait_and_monitor",
            "interrupted": "restart_current",
            "blocked_by_login": "request_login",
            "project_error": "restart_current" if audit_result.get("issue_subclass") != "source_bug" else "source_fix_required",
        }.get(str(audit_result.get("completion_verdict")), "wait_and_monitor"),
        "generated_at": now_iso(),
    }
    markdown_lines = [
        f"# {platform} keyword run summary",
        "",
        f"- keyword: `{summary_payload['keyword']}`",
        f"- run_id: `{summary_payload['run_id']}`",
        f"- completion_verdict: `{summary_payload['completion_verdict']}` ({summary_payload['completion_label']})",
        f"- monitor_state / checkpoint_state: `{summary_payload['monitor_state']}` / `{summary_payload['checkpoint_state']}`",
        f"- failure_class: `{summary_payload['failure_class'] or 'none'}`",
        f"- run_started_at / run_ended_at: `{summary_payload['run_started_at'] or 'n/a'}` / `{summary_payload['run_ended_at'] or 'n/a'}`",
        f"- content_count: `{summary_payload['content_count']}`",
        f"- comment_count: `{summary_payload['comment_count']}`",
        f"- latest_bucket_day: `{summary_payload['latest_bucket_day'] or 'n/a'}`",
        f"- artifact_health: `{summary_payload['artifact_health']}`",
        f"- next_action: `{summary_payload['next_action']}`",
    ]
    if latest_file_paths:
        markdown_lines.append(f"- latest_files: `{', '.join(latest_file_paths)}`")
    artifact_issues = summary_payload["artifact_issues"]
    if artifact_issues:
        markdown_lines.append(f"- artifact_issues: `{', '.join(str(item) for item in artifact_issues)}`")
    artifact_sample = summary_payload["artifact_sample"]
    if artifact_sample["path"]:
        markdown_lines.append(
            f"- artifact_sample: `{artifact_sample['path']} | sample_ok={artifact_sample['sample_ok']} | checks={','.join(artifact_sample['sample_checks']) or 'none'}`"
        )
    json_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    return {
        "summary": summary_payload,
        "json_path": str(json_path.resolve()),
        "markdown_path": str(md_path.resolve()),
    }


def next_keyword(queue_keywords: list[str], current_index: int | None) -> tuple[int | None, str | None]:
    if not queue_keywords:
        return None, None
    if current_index is None:
        return 0, queue_keywords[0]
    next_index = current_index + 1
    if next_index >= len(queue_keywords):
        return None, None
    return next_index, queue_keywords[next_index]


def advance_platform_keyword(platform: str, *, start_next: bool, dry_run: bool = False) -> dict[str, Any]:
    audit_payload = audit_platforms(platform)
    audit_result = audit_payload["results"][platform]
    state = ensure_keyword_state([platform], audit_payload["results"])
    platform_state = state["platforms"][platform]
    queue_payload = load_keyword_queue()
    queue_keywords = enabled_keywords(queue_payload)
    result = {
        "platform": platform,
        "queue_keywords": queue_keywords,
        "current_state": dict(platform_state),
        "audit": audit_result,
        "actions_taken": [],
    }
    verdict = str(audit_result.get("completion_verdict") or "")
    if verdict == "blocked_by_login":
        platform_state["run_state"] = audit_result.get("status", {}).get("monitor_state")
        platform_state["completion_verdict"] = verdict
        platform_state["blocked_reason"] = audit_result.get("issue_subclass")
        save_keyword_state(state)
        result.update({"status": "blocked_by_login", "exit_code": EXIT_CODE_LOGIN_REQUIRED})
        return result
    if verdict == "interrupted":
        if dry_run:
            result["actions_taken"].append("would_restart_current_keyword")
            result.update({"status": "restart_current_required", "exit_code": EXIT_CODE_PROJECT_AUTOFIX})
            return result
        restart_result = restart_current_platform(platform, dry_run=False)
        result["actions_taken"].append("restart_current_keyword")
        result["restart_result"] = restart_result
        result.update({"status": "restart_current_requested", "exit_code": EXIT_CODE_PROJECT_AUTOFIX})
        return result
    if verdict not in {"completed", "completed_no_results"}:
        result.update({"status": "not_ready", "exit_code": EXIT_CODE_OK})
        return result

    summary_result = write_keyword_summary(platform, audit_result)
    result["actions_taken"].append("write_summary")
    result["summary_result"] = summary_result
    platform_state["last_summary_path"] = summary_result["json_path"]
    platform_state["last_completed_at"] = now_iso()

    next_index, next_value = next_keyword(queue_keywords, platform_state.get("current_index"))
    if next_value is None:
        platform_state["completion_verdict"] = verdict
        save_keyword_state(state)
        result.update({"status": "queue_exhausted", "exit_code": EXIT_CODE_READY_FOR_NEXT_KEYWORD})
        return result

    if dry_run:
        result["actions_taken"].append("would_advance_keyword")
        if start_next:
            result["actions_taken"].append("would_start_next_keyword")
        result.update(
            {
                "status": "advanced_dry_run",
                "next_keyword": next_value,
                "exit_code": EXIT_CODE_READY_FOR_NEXT_KEYWORD,
            }
        )
        return result

    platform_state["current_index"] = next_index
    platform_state["current_keyword"] = next_value
    platform_state["completion_verdict"] = "pending_next_start"
    platform_state["blocked_reason"] = ""
    next_job = normalized_monitor_job(platform, audit_result.get("job") or {}, overrides={"keyword": next_value})
    spec = get_platform_spec(platform)
    write_json(spec.preferred_job_path, preferred_job_payload(next_job))
    result["actions_taken"].append("advance_keyword")
    if start_next:
        start_code, start_payload, _ = run_monitor_command("start", platform, next_job)
        result["actions_taken"].append("start_next_keyword")
        result["start_payload"] = start_payload
        result["start_exit_code"] = start_code
        platform_state["run_state"] = "starting" if start_code == 0 else "error"
        platform_state["last_started_at"] = now_iso()
    save_keyword_state(state)
    result.update(
        {
            "status": "advanced",
            "next_keyword": next_value,
            "exit_code": EXIT_CODE_READY_FOR_NEXT_KEYWORD,
        }
    )
    return result


def restart_current_platform(platform: str, *, dry_run: bool = False) -> dict[str, Any]:
    audit_payload = audit_platforms(platform)
    audit_result = audit_payload["results"][platform]
    state = ensure_keyword_state([platform], audit_payload["results"])
    platform_state = state["platforms"][platform]
    current_keyword = str(platform_state.get("current_keyword") or audit_result.get("job", {}).get("keyword") or "ai")
    job = normalized_monitor_job(platform, audit_result.get("job") or {}, overrides={"keyword": current_keyword})
    result = {
        "platform": platform,
        "current_keyword": current_keyword,
        "actions_taken": [],
    }
    if dry_run:
        result["actions_taken"].append("would_restart_current_keyword")
        result["restart_job"] = job
        result["status"] = "restart_planned"
        result["exit_code"] = EXIT_CODE_PROJECT_AUTOFIX
        return result
    spec = get_platform_spec(platform)
    write_json(spec.preferred_job_path, preferred_job_payload(job))
    result["actions_taken"].append("update_preferred_job")
    status_payload = audit_result.get("status", {})
    if str(status_payload.get("monitor_state") or "") in RUNNING_STATES:
        run_monitor_command("stop", platform)
        wait_for_monitor_stop(platform)
        result["actions_taken"].append("stop_running_monitor")
    start_code, start_payload, _ = run_monitor_command("start", platform, job)
    platform_state["run_state"] = "starting" if start_code == 0 else "error"
    platform_state["completion_verdict"] = "running" if start_code == 0 else "project_error"
    platform_state["last_started_at"] = now_iso()
    platform_state["blocked_reason"] = ""
    save_keyword_state(state)
    result["actions_taken"].append("start_monitor")
    result["start_payload"] = start_payload
    result["status"] = "restart_requested" if start_code == 0 else "restart_failed"
    result["exit_code"] = EXIT_CODE_PROJECT_AUTOFIX if start_code == 0 else EXIT_CODE_PROJECT_MANUAL
    return result

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any
from types import SimpleNamespace


ROOT_DIR = Path(__file__).resolve().parents[1]
MEDIA_CRAWLER_ROOT = ROOT_DIR / "MediaCrawler"
MEDIA_CRAWLER_PYTHON = MEDIA_CRAWLER_ROOT / ".venv" / "Scripts" / "python.exe"
RUNNER_PATH = ROOT_DIR / "scripts" / "run_bili_ai_time_range_job.py"
RUNTIME_DIR = ROOT_DIR / "artifacts" / "ai_crawl_monitor"
STATUS_PATH = RUNTIME_DIR / "status.json"
STOP_FLAG_PATH = RUNTIME_DIR / "stop.flag"
MONITOR_LOG_PATH = RUNTIME_DIR / "monitor.log"
CHILD_LOG_PATH = RUNTIME_DIR / "crawler.log"
CHECKPOINT_PATH = RUNTIME_DIR / "checkpoint.json"
PREFERRED_JOB_PATH = RUNTIME_DIR / "preferred_job.json"
DEFAULT_SAVE_DATA_PATH = ROOT_DIR / "artifacts" / "ai_crawl_data"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def ensure_runtime_dir() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_SAVE_DATA_PATH.mkdir(parents=True, exist_ok=True)


def process_exists(pid: int | None) -> bool:
    if not pid:
        return False
    if sys.platform == "win32":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        output = result.stdout.strip()
        return result.returncode == 0 and output and "No tasks are running" not in output
    try:
        os.kill(pid, 0)
    except (OSError, SystemError, ValueError):
        return False
    return True


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def append_monitor_log(message: str) -> None:
    ensure_runtime_dir()
    with MONITOR_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(f"[{now_iso()}] {message}\n")


def tail_last_non_empty_line(path: Path) -> str:
    if not path.exists():
        return ""
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in reversed(lines):
        line = line.strip()
        if line:
            return line
    return ""


def load_checkpoint_snapshot() -> dict[str, Any]:
    checkpoint = read_json(CHECKPOINT_PATH)
    return checkpoint if checkpoint else {}


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def is_job_long_backfill(job: dict[str, Any]) -> bool:
    return bool(job.get("continuous_backfill")) and bool(parse_iso_date(str(job.get("oldest_day") or "")))


def should_preserve_checkpoint_job(incoming_job: dict[str, Any], checkpoint_job: dict[str, Any]) -> bool:
    if not checkpoint_job:
        return False
    if not is_job_long_backfill(checkpoint_job):
        return False

    incoming_oldest_day = parse_iso_date(str(incoming_job.get("oldest_day") or ""))
    checkpoint_oldest_day = parse_iso_date(str(checkpoint_job.get("oldest_day") or ""))
    if checkpoint_oldest_day is None:
        return False
    if incoming_oldest_day is None:
        return True
    return checkpoint_oldest_day < incoming_oldest_day


def same_job_series(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        left.get("platform") == right.get("platform") == "bili"
        and left.get("keyword") == right.get("keyword")
        and bool(left.get("continuous_backfill")) == bool(right.get("continuous_backfill"))
        and str(left.get("save_data_option") or "") == str(right.get("save_data_option") or "")
        and str(left.get("save_data_path") or "") == str(right.get("save_data_path") or "")
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor and auto-recover the Bilibili ai crawl job.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    today = date.today().isoformat()

    run_parser = subparsers.add_parser("run", help="Run the monitor loop.")
    run_parser.add_argument("--keyword", default="ai")
    run_parser.add_argument("--start-day", default=today)
    run_parser.add_argument("--end-day", default=today)
    run_parser.add_argument("--oldest-day", default="2009-06-26")
    run_parser.add_argument("--continuous-backfill", action="store_true")
    run_parser.add_argument("--check-interval-seconds", type=int, default=30)
    run_parser.add_argument("--stall-timeout-seconds", type=int, default=300)
    run_parser.add_argument("--max-notes-per-day", type=int, default=5)
    run_parser.add_argument("--max-comment-items", type=int, default=500)
    run_parser.add_argument("--max-sub-comment-items", type=int, default=50)
    run_parser.add_argument("--max-concurrency-num", type=int, default=1)
    run_parser.add_argument("--crawler-sleep-seconds", type=float, default=2.0)
    run_parser.add_argument("--save-data-option", default="json")
    run_parser.add_argument("--save-data-path", default=str(DEFAULT_SAVE_DATA_PATH))
    run_parser.add_argument("--login-type", default="qrcode")
    run_parser.add_argument("--headless", action="store_true")
    run_parser.add_argument("--disable-cdp", action="store_true")
    run_parser.add_argument("--disable-comments", action="store_true")
    run_parser.add_argument("--disable-sub-comments", action="store_true")

    status_parser = subparsers.add_parser("status", help="Print current monitor status.")
    status_parser.add_argument("--json", action="store_true")

    subparsers.add_parser("stop", help="Ask the running monitor to stop.")
    return parser


class BiliAiMonitor:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.child_process: subprocess.Popen[str] | None = None
        self.child_log_handle = None
        self.restart_count = 0
        self.consecutive_failures = 0
        self.current_run_id = 0
        self.last_repair_action = "none"
        self.current_disable_cdp = args.disable_cdp
        self.state = "idle"

    def job_identity_from_args(self) -> dict[str, Any]:
        return {
            "platform": "bili",
            "keyword": self.args.keyword,
            "start_day": self.args.start_day,
            "end_day": self.args.end_day,
            "oldest_day": self.args.oldest_day,
            "continuous_backfill": self.args.continuous_backfill,
            "save_data_option": self.args.save_data_option,
            "save_data_path": str(Path(self.args.save_data_path).resolve()),
        }

    def adopt_checkpoint_job_if_preferred(self) -> None:
        checkpoint = load_checkpoint_snapshot()
        checkpoint_job = checkpoint.get("job")
        incoming_job = self.job_identity_from_args()
        preferred_job = read_json(PREFERRED_JOB_PATH) if PREFERRED_JOB_PATH.exists() else {}
        selected_job = choose_preferred_job(
            checkpoint_job if isinstance(checkpoint_job, dict) else {},
            preferred_job,
            incoming_job,
        )
        if selected_job and (not preferred_job or preferred_job != selected_job):
            write_json(PREFERRED_JOB_PATH, selected_job)

        if not selected_job or not same_job_series(incoming_job, selected_job):
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
        append_monitor_log(
            "Preserved preferred long backfill job instead of adopting incoming shorter config"
        )
        self.last_repair_action = "adopted_checkpoint_job"

    def bootstrap_status(self) -> None:
        self.adopt_checkpoint_job_if_preferred()
        existing_status = read_json(STATUS_PATH)
        existing_pid = existing_status.get("monitor_pid")
        existing_state = existing_status.get("monitor_state")
        active_states = {"starting", "running", "restarting"}
        if existing_state in active_states and process_exists(existing_pid) and existing_pid != os.getpid():
            raise RuntimeError(f"Monitor is already running with PID {existing_pid}")
        if STOP_FLAG_PATH.exists():
            STOP_FLAG_PATH.unlink()

    def build_child_command(self) -> list[str]:
        command = [
            str(MEDIA_CRAWLER_PYTHON),
            str(RUNNER_PATH),
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
            str(CHECKPOINT_PATH),
            "--login-type",
            self.args.login_type,
            "--max-notes-per-day",
            str(self.args.max_notes_per_day),
            "--max-comment-items",
            str(self.args.max_comment_items),
            "--max-sub-comment-items",
            str(self.args.max_sub_comment_items),
            "--max-concurrency-num",
            str(self.args.max_concurrency_num),
            "--crawler-sleep-seconds",
            str(self.args.crawler_sleep_seconds),
        ]
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

    def write_status(self, *, child_exit_code: int | None = None, extra_message: str | None = None) -> None:
        child_log_mtime = CHILD_LOG_PATH.stat().st_mtime if CHILD_LOG_PATH.exists() else None
        last_output_at = (
            datetime.fromtimestamp(child_log_mtime).astimezone().isoformat(timespec="seconds")
            if child_log_mtime
            else None
        )
        checkpoint = load_checkpoint_snapshot()
        payload = {
            "monitor_state": self.state,
            "healthy": self.state in {"starting", "running", "restarting", "completed"},
            "monitor_pid": os.getpid(),
            "child_pid": self.child_process.pid if self.child_process else None,
            "keyword": self.args.keyword,
            "start_day": self.args.start_day,
            "end_day": self.args.end_day,
            "oldest_day": self.args.oldest_day,
            "continuous_backfill": self.args.continuous_backfill,
            "save_data_path": self.args.save_data_path,
            "save_data_option": self.args.save_data_option,
            "max_notes_per_day": self.args.max_notes_per_day,
            "max_comment_items": self.args.max_comment_items,
            "max_sub_comment_items": self.args.max_sub_comment_items,
            "current_run_id": self.current_run_id,
            "restart_count": self.restart_count,
            "consecutive_failures": self.consecutive_failures,
            "disable_cdp": self.current_disable_cdp,
            "last_repair_action": self.last_repair_action,
            "last_heartbeat_at": now_iso(),
            "last_output_at": last_output_at,
            "last_log_line": tail_last_non_empty_line(CHILD_LOG_PATH),
            "child_exit_code": child_exit_code,
            "message": extra_message or "",
            "checkpoint_path": str(CHECKPOINT_PATH),
            "checkpoint": checkpoint,
            "monitor_log_path": str(MONITOR_LOG_PATH),
            "crawler_log_path": str(CHILD_LOG_PATH),
            "status_path": str(STATUS_PATH),
        }
        write_json(STATUS_PATH, payload)

    def start_child(self) -> None:
        ensure_runtime_dir()
        self.current_run_id += 1
        self.state = "starting"
        command = self.build_child_command()
        append_monitor_log(f"Starting child run {self.current_run_id}: {' '.join(command)}")
        self.write_status(extra_message="Starting crawler child process")
        self.child_log_handle = CHILD_LOG_PATH.open("a", encoding="utf-8")
        self.child_log_handle.write(f"\n[{now_iso()}] === child run {self.current_run_id} start ===\n")
        self.child_log_handle.flush()
        self.child_process = subprocess.Popen(
            command,
            cwd=str(ROOT_DIR),
            stdout=self.child_log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        self.state = "running"
        self.write_status(extra_message="Crawler child process is running")

    def close_child_log(self) -> None:
        if self.child_log_handle:
            self.child_log_handle.flush()
            self.child_log_handle.close()
            self.child_log_handle = None

    def terminate_child(self) -> None:
        if not self.child_process or self.child_process.poll() is not None:
            return
        append_monitor_log(f"Stopping child PID {self.child_process.pid}")
        self.child_process.terminate()
        try:
            self.child_process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            append_monitor_log(f"Child PID {self.child_process.pid} did not stop in time, killing")
            self.child_process.kill()
            self.child_process.wait(timeout=10)

    def child_last_output_age_seconds(self) -> float:
        if not CHILD_LOG_PATH.exists():
            return 0.0
        return time.time() - CHILD_LOG_PATH.stat().st_mtime

    def should_repair_with_disable_cdp(self) -> bool:
        last_line = tail_last_non_empty_line(CHILD_LOG_PATH)
        failure_markers = [
            "Browser failed to start within",
            "CDP mode launch failed",
            "CDP browser launch failed",
        ]
        return not self.current_disable_cdp and any(marker in last_line for marker in failure_markers)

    def prepare_restart(self, reason: str) -> None:
        self.restart_count += 1
        self.consecutive_failures += 1
        self.state = "restarting"
        if self.should_repair_with_disable_cdp():
            self.current_disable_cdp = True
            self.last_repair_action = "disabled_cdp_after_browser_launch_failure"
        else:
            self.last_repair_action = "restart_same_config"
        append_monitor_log(f"Preparing restart #{self.restart_count}: {reason}; repair={self.last_repair_action}")
        self.write_status(extra_message=reason)
        self.sleep_with_stop_checks(min(60, 5 * self.consecutive_failures))

    def sleep_with_stop_checks(self, seconds: int | float) -> None:
        remaining = max(float(seconds), 0.0)
        while remaining > 0:
            if STOP_FLAG_PATH.exists():
                return
            sleep_chunk = min(1.0, remaining)
            time.sleep(sleep_chunk)
            remaining -= sleep_chunk

    def run(self) -> int:
        self.bootstrap_status()
        self.start_child()
        while True:
            if STOP_FLAG_PATH.exists():
                self.state = "stopping"
                self.write_status(extra_message="Stop flag detected, stopping monitor")
                append_monitor_log("Stop flag detected, shutting down")
                self.terminate_child()
                self.close_child_log()
                self.child_process = None
                self.state = "stopped"
                self.write_status(extra_message="Monitor stopped")
                return 0

            if self.child_process is None:
                self.start_child()

            exit_code = self.child_process.poll() if self.child_process else None
            if exit_code is None:
                if self.args.stall_timeout_seconds > 0 and self.child_last_output_age_seconds() > self.args.stall_timeout_seconds:
                    append_monitor_log("Child appears stalled, terminating for restart")
                    self.terminate_child()
                    self.close_child_log()
                    self.prepare_restart("Crawler stalled and was restarted")
                    self.start_child()
                    continue

                self.state = "running"
                self.write_status(extra_message="Crawler running normally")
                self.sleep_with_stop_checks(self.args.check_interval_seconds)
                continue

            self.close_child_log()
            if exit_code == 0:
                self.state = "completed"
                self.consecutive_failures = 0
                self.child_process = None
                self.write_status(child_exit_code=exit_code, extra_message="Crawler completed successfully")
                append_monitor_log("Child completed successfully")
                return 0

            append_monitor_log(f"Child exited abnormally with code {exit_code}")
            self.write_status(child_exit_code=exit_code, extra_message=f"Crawler exited abnormally with code {exit_code}")
            self.prepare_restart(f"Crawler exited abnormally with code {exit_code}")
            self.start_child()


def print_status(as_json: bool) -> int:
    ensure_runtime_dir()
    payload = read_json(STATUS_PATH)
    if not payload:
        payload = {
            "monitor_state": "idle",
            "healthy": False,
            "message": "No monitor status found",
            "status_path": str(STATUS_PATH),
        }
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def request_stop() -> int:
    ensure_runtime_dir()
    payload = read_json(STATUS_PATH)
    STOP_FLAG_PATH.write_text(now_iso(), encoding="utf-8")

    child_pid = payload.get("child_pid")
    if process_exists(child_pid):
        try:
            if sys.platform == "win32":
                subprocess.run(
                    ["taskkill", "/PID", str(child_pid), "/T"],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                )
            else:
                os.kill(child_pid, signal.SIGTERM)
        except (OSError, SystemError, ValueError):
            pass

    payload.update(
        {
            "monitor_state": "stopping",
            "healthy": False,
            "message": "Stop requested",
            "last_heartbeat_at": now_iso(),
        }
    )
    write_json(STATUS_PATH, payload)
    append_monitor_log("Stop requested")
    print("Stop flag written.")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "status":
        return print_status(as_json=args.json)
    if args.command == "stop":
        return request_stop()

    ensure_runtime_dir()
    if not MEDIA_CRAWLER_PYTHON.exists():
        raise FileNotFoundError(f"MediaCrawler Python not found: {MEDIA_CRAWLER_PYTHON}")
    if not RUNNER_PATH.exists():
        raise FileNotFoundError(f"Runner not found: {RUNNER_PATH}")

    monitor = BiliAiMonitor(args)
    try:
        return monitor.run()
    except KeyboardInterrupt:
        monitor.state = "stopped"
        monitor.write_status(extra_message="Monitor interrupted by keyboard")
        append_monitor_log("Monitor interrupted by keyboard")
        monitor.terminate_child()
        monitor.close_child_log()
        return 130
    except Exception as exc:
        append_monitor_log(f"Monitor crashed: {exc}")
        write_json(
            STATUS_PATH,
            {
                "monitor_state": "error",
                "healthy": False,
                "message": str(exc),
                "monitor_pid": os.getpid(),
                "last_heartbeat_at": now_iso(),
                "monitor_log_path": str(MONITOR_LOG_PATH),
                "crawler_log_path": str(CHILD_LOG_PATH),
                "status_path": str(STATUS_PATH),
            },
        )
        raise


if __name__ == "__main__":
    sys.exit(main())

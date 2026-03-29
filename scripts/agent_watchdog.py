from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from agent_ops import (
    AGENT_OPS_DIR,
    EXIT_CODE_LOGIN_REQUIRED,
    EXIT_CODE_OK,
    EXIT_CODE_PROJECT_AUTOFIX,
    EXIT_CODE_PROJECT_MANUAL,
    EXIT_CODE_READY_FOR_NEXT_KEYWORD,
    ISSUE_LOGIN,
    ISSUE_PROJECT,
    PLATFORM_SPECS,
    advance_platform_keyword,
    audit_platforms,
    ensure_agent_ops_dir,
    ensure_keyword_state,
    now_iso,
    repair_platform,
    write_console_json,
)


WATCHDOG_DIR = AGENT_OPS_DIR / "watchdog"
LATEST_CYCLE_PATH = WATCHDOG_DIR / "latest_cycle.json"
HISTORY_DIR = WATCHDOG_DIR / "history"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a full agent watchdog cycle for crawler monitoring.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    once_parser = subparsers.add_parser("run-once", help="Run one audit/repair/advance cycle.")
    once_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], default="all")
    once_parser.add_argument("--json", action="store_true")
    once_parser.add_argument("--output", default="")
    once_parser.add_argument("--dry-run-repair", action="store_true")
    once_parser.add_argument("--no-auto-advance", action="store_true")

    loop_parser = subparsers.add_parser("run-loop", help="Run the watchdog in a 15-minute style loop.")
    loop_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], default="all")
    loop_parser.add_argument("--interval-seconds", type=int, default=900)
    loop_parser.add_argument("--json", action="store_true")
    loop_parser.add_argument("--dry-run-repair", action="store_true")
    loop_parser.add_argument("--no-auto-advance", action="store_true")

    return parser


def ensure_watchdog_dirs() -> None:
    ensure_agent_ops_dir()
    WATCHDOG_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def exit_code_from_values(values: list[int]) -> int:
    for preferred in (
        EXIT_CODE_PROJECT_MANUAL,
        EXIT_CODE_PROJECT_AUTOFIX,
        EXIT_CODE_LOGIN_REQUIRED,
        EXIT_CODE_READY_FOR_NEXT_KEYWORD,
    ):
        if preferred in values:
            return preferred
    return EXIT_CODE_OK


def build_login_request(platform: str, audit_result: dict[str, Any]) -> dict[str, Any]:
    return {
        "platform": platform,
        "keyword": audit_result.get("job", {}).get("keyword"),
        "issue_subclass": audit_result.get("issue_subclass"),
        "message": (
            "需要人工登录后再继续。"
            if audit_result.get("issue_subclass") == "login_required"
            else "当前账号权限不足，需要切换账号或重新确认登录态。"
        ),
        "login_command": audit_result.get("login_command"),
        "latest_crawler_log": audit_result.get("paths", {}).get("latest_crawler_log"),
    }


def maybe_write_output(path_value: str, payload: dict[str, Any]) -> None:
    if not path_value:
        return
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_cycle(platform: str, *, dry_run_repair: bool, auto_advance: bool) -> dict[str, Any]:
    ensure_watchdog_dirs()
    audit_payload = audit_platforms(platform)
    platforms = sorted(PLATFORM_SPECS.keys()) if platform == "all" else [platform]
    ensure_keyword_state(platforms, audit_payload["results"])
    cycle_platforms: dict[str, Any] = {}
    exit_codes: list[int] = [int(audit_payload.get("exit_code", EXIT_CODE_OK))]

    for name in platforms:
        audit_result = audit_payload["results"][name]
        platform_result: dict[str, Any] = {
            "audit": audit_result,
            "login_request": None,
            "repair": None,
            "advance": None,
        }
        if audit_result.get("issue_class") == ISSUE_LOGIN:
            platform_result["login_request"] = build_login_request(name, audit_result)
            exit_codes.append(EXIT_CODE_LOGIN_REQUIRED)
        elif audit_result.get("issue_class") == ISSUE_PROJECT:
            repair_result = repair_platform(name, dry_run=dry_run_repair)
            platform_result["repair"] = repair_result
            exit_codes.append(int(repair_result.get("exit_code", EXIT_CODE_PROJECT_AUTOFIX)))
        elif auto_advance and audit_result.get("completion_verdict") in {"completed", "completed_no_results"}:
            advance_result = advance_platform_keyword(name, start_next=True, dry_run=False)
            platform_result["advance"] = advance_result
            exit_codes.append(int(advance_result.get("exit_code", EXIT_CODE_READY_FOR_NEXT_KEYWORD)))
        cycle_platforms[name] = platform_result

    payload = {
        "cycled_at": now_iso(),
        "platform": platform,
        "auto_advance": auto_advance,
        "dry_run_repair": dry_run_repair,
        "results": cycle_platforms,
        "exit_code": exit_code_from_values(exit_codes),
    }
    LATEST_CYCLE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    history_path = HISTORY_DIR / f"{datetime.now().strftime('%Y%m%dT%H%M%S')}_{platform}.json"
    history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["latest_cycle_path"] = str(LATEST_CYCLE_PATH.resolve())
    payload["history_path"] = str(history_path.resolve())
    return payload


def run_once_command(args: argparse.Namespace) -> int:
    payload = run_cycle(
        args.platform,
        dry_run_repair=bool(args.dry_run_repair),
        auto_advance=not bool(args.no_auto_advance),
    )
    maybe_write_output(args.output, payload)
    if args.json:
        write_console_json(payload)
    else:
        write_console_json(payload)
    return int(payload["exit_code"])


def run_loop_command(args: argparse.Namespace) -> int:
    interval_seconds = max(60, int(args.interval_seconds))
    while True:
        payload = run_cycle(
            args.platform,
            dry_run_repair=bool(args.dry_run_repair),
            auto_advance=not bool(args.no_auto_advance),
        )
        if args.json:
            write_console_json(payload)
        time.sleep(interval_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run-once":
        return run_once_command(args)
    if args.command == "run-loop":
        return run_loop_command(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

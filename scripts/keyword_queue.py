from __future__ import annotations

import argparse
import json

from agent_ops import (
    EXIT_CODE_OK,
    PLATFORM_SPECS,
    advance_platform_keyword,
    audit_platforms,
    enabled_keywords,
    ensure_keyword_state,
    load_keyword_queue,
    load_keyword_state,
    restart_current_platform,
    write_console_json,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Shared keyword queue and per-platform progress manager.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status_parser = subparsers.add_parser("status", help="Show keyword queue config and per-platform state.")
    status_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], default="all")
    status_parser.add_argument("--json", action="store_true")

    advance_parser = subparsers.add_parser("advance", help="Advance one platform to the next keyword when safe.")
    advance_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], required=True)
    advance_parser.add_argument("--start-next", action="store_true")
    advance_parser.add_argument("--dry-run", action="store_true")
    advance_parser.add_argument("--json", action="store_true")

    restart_parser = subparsers.add_parser("restart-current", help="Restart the current keyword for one platform.")
    restart_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], required=True)
    restart_parser.add_argument("--dry-run", action="store_true")
    restart_parser.add_argument("--json", action="store_true")

    return parser


def print_payload(payload: dict, as_json: bool) -> None:
    if as_json:
        write_console_json(payload)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


def aggregate_exit_code(results: list[dict]) -> int:
    preferred_order = [30, 20, 10, 40, 0]
    codes = {int(result.get("exit_code", EXIT_CODE_OK)) for result in results}
    for code in preferred_order:
        if code in codes:
            return code
    return EXIT_CODE_OK


def status_command(args: argparse.Namespace) -> int:
    queue_payload = load_keyword_queue()
    platforms = sorted(PLATFORM_SPECS.keys()) if args.platform == "all" else [args.platform]
    audit_payload = audit_platforms(args.platform if args.platform == "all" else args.platform)
    ensure_keyword_state(platforms, audit_payload["results"])
    state = load_keyword_state()
    payload = {
        "queue": queue_payload,
        "enabled_keywords": enabled_keywords(queue_payload),
        "platforms": {platform: state["platforms"].get(platform, {}) for platform in platforms},
        "audit": audit_payload["results"] if args.platform == "all" else audit_payload["results"][args.platform],
        "exit_code": audit_payload["exit_code"],
    }
    print_payload(payload, args.json)
    return int(payload["exit_code"])


def advance_command(args: argparse.Namespace) -> int:
    platforms = sorted(PLATFORM_SPECS.keys()) if args.platform == "all" else [args.platform]
    results = {
        platform: advance_platform_keyword(platform, start_next=bool(args.start_next), dry_run=bool(args.dry_run))
        for platform in platforms
    }
    payload = {
        "command": "advance",
        "platform": args.platform,
        "results": results,
        "exit_code": aggregate_exit_code(list(results.values())),
    }
    print_payload(payload, args.json)
    return int(payload["exit_code"])


def restart_command(args: argparse.Namespace) -> int:
    platforms = sorted(PLATFORM_SPECS.keys()) if args.platform == "all" else [args.platform]
    results = {
        platform: restart_current_platform(platform, dry_run=bool(args.dry_run))
        for platform in platforms
    }
    payload = {
        "command": "restart-current",
        "platform": args.platform,
        "results": results,
        "exit_code": aggregate_exit_code(list(results.values())),
    }
    print_payload(payload, args.json)
    return int(payload["exit_code"])


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "status":
        return status_command(args)
    if args.command == "advance":
        return advance_command(args)
    if args.command == "restart-current":
        return restart_command(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

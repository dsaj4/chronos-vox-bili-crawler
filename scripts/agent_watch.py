from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from agent_ops import (
    EXIT_CODE_OK,
    PLATFORM_SPECS,
    aggregate_exit_code,
    audit_platforms,
    repair_platform,
    write_console_json,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Agent-facing audit and repair entrypoint.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit_parser = subparsers.add_parser("audit", help="Audit monitor state, artifacts, and completion status.")
    audit_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], required=True)
    audit_parser.add_argument("--json", action="store_true")
    audit_parser.add_argument("--output", default="")

    repair_parser = subparsers.add_parser("repair", help="Apply no-code repairs and emit next action.")
    repair_parser.add_argument("--platform", choices=["all", *sorted(PLATFORM_SPECS.keys())], required=True)
    repair_parser.add_argument("--json", action="store_true")
    repair_parser.add_argument("--output", default="")
    repair_parser.add_argument("--dry-run", action="store_true")

    return parser


def maybe_write_output(path_value: str, payload: dict[str, Any]) -> None:
    if not path_value:
        return
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def audit_command(args: argparse.Namespace) -> int:
    payload = audit_platforms(args.platform)
    maybe_write_output(args.output, payload)
    if args.json:
        write_console_json(payload)
    else:
        write_console_json(payload)
    return int(payload.get("exit_code", EXIT_CODE_OK))


def repair_command(args: argparse.Namespace) -> int:
    if args.platform == "all":
        payload = {
            "platform": "all",
            "repaired_at": None,
            "results": {name: repair_platform(name, dry_run=bool(args.dry_run)) for name in sorted(PLATFORM_SPECS.keys())},
        }
        payload["repaired_at"] = next(iter(payload["results"].values())).get("audited_at") if payload["results"] else None
        exit_code = aggregate_exit_code(list(payload["results"].values()))
        payload["exit_code"] = exit_code
    else:
        result = repair_platform(args.platform, dry_run=bool(args.dry_run))
        payload = {
            "platform": args.platform,
            "repaired_at": result.get("audited_at"),
            "result": result,
            "exit_code": int(result.get("exit_code", EXIT_CODE_OK)),
        }
        exit_code = payload["exit_code"]
    maybe_write_output(args.output, payload)
    write_console_json(payload)
    return int(exit_code)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "audit":
        return audit_command(args)
    if args.command == "repair":
        return repair_command(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

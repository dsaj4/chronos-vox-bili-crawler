from __future__ import annotations

import argparse
import json
from pathlib import Path

from agent_ops import PLATFORM_SPECS, audit_platforms, write_console_json, write_keyword_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export a per-keyword run summary for one platform.")
    parser.add_argument("--platform", choices=sorted(PLATFORM_SPECS.keys()), required=True)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    audit_payload = audit_platforms(args.platform)
    audit_result = audit_payload["results"][args.platform]
    output_dir = Path(args.output_dir) if args.output_dir else None
    summary_result = write_keyword_summary(args.platform, audit_result, output_dir=output_dir)
    payload = {
        "platform": args.platform,
        "audit_exit_code": audit_payload["exit_code"],
        "summary": summary_result["summary"],
        "json_path": summary_result["json_path"],
        "markdown_path": summary_result["markdown_path"],
    }
    if args.json:
        write_console_json(payload)
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CHECKPOINT_PATH = ROOT_DIR / "artifacts" / "ai_crawl_monitor" / "checkpoint.json"


def canonicalize_text(value: object) -> str:
    return str(value or "").strip()


def short_hash(*parts: object, length: int = 10) -> str:
    payload = "|".join(canonicalize_text(part).lower() for part in parts)
    digest = sha256(payload.encode("utf-8")).hexdigest()
    return digest[:length]


def slugify(value: object) -> str:
    cleaned = canonicalize_text(value).lower()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in cleaned)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "item"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a crawl result manifest for Chronos-Vox.")
    parser.add_argument("--checkpoint-path", default=str(DEFAULT_CHECKPOINT_PATH))
    parser.add_argument("--save-data-path", default="")
    parser.add_argument("--task-id", default="")
    parser.add_argument("--dataset-id", default="")
    parser.add_argument("--keyword", default="")
    parser.add_argument("--platforms", default="bili")
    parser.add_argument("--start-day", default="")
    parser.add_argument("--end-day", default="")
    parser.add_argument("--schema-version", default="chronos-vox.crawl-result-manifest.v1")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def resolve_job_context(args: argparse.Namespace) -> dict[str, Any]:
    checkpoint = load_json(Path(args.checkpoint_path))
    job = checkpoint.get("job") if isinstance(checkpoint.get("job"), dict) else {}
    resolved_keyword = canonicalize_text(args.keyword) or canonicalize_text(job.get("keyword")) or "ai"
    resolved_platforms = [platform.strip() for platform in canonicalize_text(args.platforms).split(",") if platform.strip()]
    resolved_start_day = canonicalize_text(args.start_day) or canonicalize_text(job.get("start_day")) or ""
    resolved_end_day = canonicalize_text(args.end_day) or canonicalize_text(job.get("end_day")) or ""
    resolved_task_id = canonicalize_text(args.task_id) or f"task_{slugify(resolved_keyword)}_{short_hash(resolved_keyword, resolved_start_day, resolved_end_day)}"
    resolved_dataset_id = canonicalize_text(args.dataset_id) or f"dataset_{slugify(resolved_task_id)}"
    raw_record_count = int(checkpoint.get("total_notes_crawled_for_keyword") or 0)
    save_data_path = canonicalize_text(args.save_data_path) or canonicalize_text(job.get("save_data_path")) or ""

    output_files: list[str] = []
    if save_data_path:
        data_path = Path(save_data_path)
        if data_path.exists():
            for file_path in sorted(data_path.rglob("*")):
                if file_path.is_file():
                    output_files.append(str(file_path.resolve()))

    return {
        "task_id": resolved_task_id,
        "dataset_id": resolved_dataset_id,
        "keyword": resolved_keyword,
        "platforms": resolved_platforms,
        "time_range": {
            "start_at": resolved_start_day,
            "end_at": resolved_end_day,
        },
        "raw_record_count": raw_record_count,
        "output_files": output_files,
        "schema_version": args.schema_version,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def main() -> int:
    args = parse_args()
    payload = resolve_job_context(args)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


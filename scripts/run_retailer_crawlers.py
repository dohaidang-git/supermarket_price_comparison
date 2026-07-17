#!/usr/bin/env python3
"""Run retailer crawlers into canonical raw run directories."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
VIETNAM_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")
RETAILERS: dict[str, dict[str, Any]] = {
    "bachhoaxanh": {
        "script": "scrapers/bachhoaxanh/crawler.py",
        "store_name": "Bach Hoa Xanh",
        "base_args": ["--max-scrolls", "40"],
    },
    "go": {
        "script": "scrapers/go/crawler.py",
        "store_name": "GO!",
        "base_args": [],
    },
    "lottemart": {
        "script": "scrapers/lotte/crawler.py",
        "store_name": "Lotte Mart",
        "base_args": ["--category-id", "0"],
    },
    "mmvietnam": {
        "script": "scrapers/mmvietnam/crawler.py",
        "store_name": "MM Mega Market",
        "base_args": ["--store-code", "10010"],
    },
}


def now_iso() -> str:
    return datetime.now(VIETNAM_TZ).replace(microsecond=0).isoformat()


def make_run_id() -> str:
    return datetime.now(VIETNAM_TZ).strftime("%Y%m%d_%H%M%S")


def run_dir(output_root: Path, retailer_id: str, run_date: str, run_id: str) -> Path:
    return output_root / f"store={retailer_id}" / f"date={run_date}" / f"run_id={run_id}"


def write_metadata(path: Path, metadata: dict[str, Any]) -> None:
    path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def crawler_command(retailer_id: str, config: dict[str, Any], output_dir: Path, args: argparse.Namespace) -> list[str]:
    command = [sys.executable, str(ROOT / config["script"]), *config["base_args"], "--output-dir", str(output_dir)]
    if args.test_limit:
        if retailer_id in {"bachhoaxanh", "go"}:
            command += ["--max-categories", str(args.test_limit)]
        else:
            command += ["--max-pages", str(args.test_limit)]
    return command


def run_one(retailer_id: str, args: argparse.Namespace, run_id: str, run_date: str) -> dict[str, Any]:
    config = RETAILERS[retailer_id]
    target = run_dir(Path(args.output_root), retailer_id, run_date, run_id)
    target.mkdir(parents=True, exist_ok=True)
    metadata_path = target / "metadata.json"
    command = crawler_command(retailer_id, config, target, args)
    metadata: dict[str, Any] = {
        "run_id": run_id,
        "run_date": run_date,
        "retailer_id": retailer_id,
        "store_id": retailer_id,
        "store_name": config["store_name"],
        "crawler": config["script"],
        "status": "running",
        "started_at": now_iso(),
        "finished_at": None,
        "output_dir": target.as_posix(),
        "command": command,
        "records_written": None,
        "error_message": None,
    }
    write_metadata(metadata_path, metadata)
    try:
        env = os.environ.copy()
        existing_pythonpath = env.get("PYTHONPATH")
        env["PYTHONPATH"] = f"{ROOT}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(ROOT)
        completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
        metadata["return_code"] = completed.returncode
        metadata["stdout"] = completed.stdout[-12000:]
        metadata["stderr"] = completed.stderr[-12000:]
        metadata["status"] = "success" if completed.returncode == 0 else "failed"
        if completed.returncode != 0:
            metadata["error_message"] = f"crawler exited with code {completed.returncode}"
    except Exception as error:
        metadata["status"] = "failed"
        metadata["error_message"] = str(error)
        metadata["return_code"] = None
    metadata["finished_at"] = now_iso()
    metadata["output_files"] = sorted(path.name for path in target.glob("*.jsonl"))
    write_metadata(metadata_path, metadata)
    print(json.dumps({key: metadata[key] for key in ("retailer_id", "run_id", "status", "output_dir", "output_files", "error_message")}, ensure_ascii=False))
    return metadata


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run retailer crawlers into raw/store=*/date=*/run_id=*.")
    parser.add_argument("--retailers", nargs="+", choices=sorted(RETAILERS), default=sorted(RETAILERS))
    parser.add_argument("--run-id", default=None, help="Reuse a run id for a controlled rerun.")
    parser.add_argument("--run-date", default=datetime.now(VIETNAM_TZ).date().isoformat())
    parser.add_argument("--output-root", type=Path, default=ROOT / "raw")
    parser.add_argument("--test-limit", type=int, help="Max categories/pages per retailer for a smoke run.")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    run_id = args.run_id or make_run_id()
    summaries = []
    for retailer_id in args.retailers:
        target = run_dir(args.output_root, retailer_id, args.run_date, run_id)
        command = crawler_command(retailer_id, RETAILERS[retailer_id], target, args)
        if args.dry_run:
            print(json.dumps({"retailer_id": retailer_id, "run_id": run_id, "command": command}, ensure_ascii=False))
            continue
        result = run_one(retailer_id, args, run_id, args.run_date)
        summaries.append(result)
        if result["status"] == "failed" and not args.continue_on_error:
            break
    if args.dry_run:
        return 0
    failed = sum(summary["status"] == "failed" for summary in summaries)
    print(json.dumps({"run_id": run_id, "retailers": summaries, "status": "failed" if failed else "success"}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

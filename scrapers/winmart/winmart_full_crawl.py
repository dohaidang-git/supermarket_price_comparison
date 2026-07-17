#!/usr/bin/env python3
"""Run the WinMart raw crawl pipeline end to end for one shared run_id."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from winmart_config_scraper import load_config
from winmart_scraper import (
    STORE_ID,
    STORE_NAME,
    append_jsonl,
    build_output_paths,
    now_local_iso,
    write_json,
)


DEFAULT_CONFIG = "configs/winmart_categories.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run WinMart promotion-first crawl, hydrate, and merge.")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to WinMart category YAML config.")
    parser.add_argument("--out-dir", default="raw", help="Raw output directory.")
    parser.add_argument("--run-id", default=None, help="Optional run id. Defaults to current timestamp.")
    parser.add_argument("--run-date", help="Optional YYYY-MM-DD partition date, used by scheduled runs.")
    parser.add_argument("--limit-categories", type=int, default=None, help="Limit categories for both crawl steps.")

    parser.add_argument("--config-max-pages", type=int, default=None, help="Pass --max-pages to config API scraper.")
    parser.add_argument("--config-page-size", type=int, default=None, help="Pass --page-size to config API scraper.")
    parser.add_argument("--config-delay", type=float, default=None, help="Pass --delay to config API scraper.")
    parser.add_argument(
        "--stop-after-no-discount-pages",
        type=int,
        default=None,
        help="Pass --stop-after-no-discount-pages to config API scraper.",
    )

    parser.add_argument("--promo-scrolls", type=int, default=None, help="Pass --scrolls to promo card scraper.")
    parser.add_argument("--promo-wait-ms", type=int, default=None, help="Pass --wait-ms to promo card scraper.")
    parser.add_argument("--promo-limit-records", type=int, default=None, help="Pass --limit-records to promo card scraper.")
    parser.add_argument("--headed", action="store_true", help="Run promo card browser headed.")
    parser.add_argument("--screenshot", action="store_true", help="Save promo card screenshots.")
    parser.add_argument("--skip-robots-check", action="store_true", help="Skip robots checks in promo card scraper.")

    parser.add_argument("--skip-promo", action="store_true", help="Skip Playwright promo card crawl.")
    parser.add_argument("--skip-hydrate", action="store_true", help="Skip hydrating products for unmatched promo cards.")
    parser.add_argument("--hydrate-limit", type=int, default=None, help="Maximum unmatched promo card products to hydrate.")
    parser.add_argument("--hydrate-timeout-ms", type=int, default=None, help="Pass --timeout-ms to hydrate step.")
    parser.add_argument("--hydrate-wait-ms", type=int, default=None, help="Pass --wait-ms to hydrate step.")
    parser.add_argument("--skip-hydrate-detail-pages", action="store_true", help="Only search existing raw payloads in hydrate step.")
    parser.add_argument("--skip-merge", action="store_true", help="Skip merge step.")
    parser.add_argument(
        "--fail-on-promo-error",
        action="store_true",
        help="Return failure if promo card crawl fails. By default merge still runs if products exist.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    return parser.parse_args()


def add_option(command: list[str], flag: str, value: Any) -> None:
    if value is not None:
        command.extend([flag, str(value)])


def command_for_log(command: list[str]) -> str:
    return " ".join(command)


def run_step(
    step_name: str,
    command: list[str],
    cwd: Path,
    log_path: Path,
    dry_run: bool,
) -> dict[str, Any]:
    started_at = now_local_iso()
    append_jsonl(
        log_path,
        {
            "event": "pipeline_step_start",
            "step": step_name,
            "command": command_for_log(command),
            "timestamp": started_at,
        },
    )

    if dry_run:
        result = {
            "step": step_name,
            "command": command,
            "returncode": None,
            "started_at": started_at,
            "ended_at": now_local_iso(),
            "status": "dry_run",
        }
    else:
        completed = subprocess.run(command, cwd=str(cwd), check=False)  # noqa: S603 - command is built from fixed script paths.
        result = {
            "step": step_name,
            "command": command,
            "returncode": completed.returncode,
            "started_at": started_at,
            "ended_at": now_local_iso(),
            "status": "success" if completed.returncode == 0 else "failed",
        }

    append_jsonl(
        log_path,
        {
            "event": "pipeline_step_end",
            "step": step_name,
            "returncode": result["returncode"],
            "status": result["status"],
            "timestamp": result["ended_at"],
        },
    )
    return result


def load_metadata(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def main() -> int:
    args = parse_args()
    if args.run_date:
        os.environ["WINMART_RUN_DATE"] = args.run_date
    script_path = Path(__file__).resolve()
    workspace = script_path.parents[2] if script_path.parent.name == "winmart" else script_path.parents[1]
    scripts_dir = Path(__file__).resolve().parent
    paths = build_output_paths(Path(args.out_dir), args.run_id)
    config_payload = load_config(Path(args.config))
    crawl_options = config_payload.get("crawl_options") or {}
    hydrate_enabled = bool(crawl_options.get("hydrate_unmatched_promo_cards", True))
    run_id = paths.run_dir.name.removeprefix("run_id=")
    crawl_timestamp = now_local_iso()

    metadata_config = paths.run_dir / "metadata_config_api.json"
    metadata_promo = paths.run_dir / "metadata_promo_cards.json"
    metadata_hydrate = paths.run_dir / "metadata_hydrate_promos.json"
    metadata_merge = paths.run_dir / "metadata_merge.json"
    metadata_pipeline = paths.run_dir / "metadata.json"

    append_jsonl(
        paths.log_jsonl,
        {
            "event": "pipeline_start",
            "run_id": run_id,
            "config": args.config,
            "timestamp": crawl_timestamp,
        },
    )

    config_command = [
        sys.executable,
        str(scripts_dir / "winmart_config_scraper.py"),
        "--config",
        args.config,
        "--out-dir",
        args.out_dir,
        "--run-id",
        run_id,
        "--metadata-name",
        metadata_config.name,
    ]
    add_option(config_command, "--limit-categories", args.limit_categories)
    add_option(config_command, "--max-pages", args.config_max_pages)
    add_option(config_command, "--page-size", args.config_page_size)
    add_option(config_command, "--delay", args.config_delay)
    add_option(config_command, "--stop-after-no-discount-pages", args.stop_after_no_discount_pages)

    steps: list[dict[str, Any]] = []
    steps.append(run_step("config_api", config_command, workspace, paths.log_jsonl, args.dry_run))

    config_ok = steps[-1]["status"] in {"success", "dry_run"}
    promo_ok = True
    if config_ok and not args.skip_promo:
        promo_command = [
            sys.executable,
            str(scripts_dir / "winmart_promo_card_scraper.py"),
            "--config",
            args.config,
            "--out-dir",
            args.out_dir,
            "--run-id",
            run_id,
            "--metadata-name",
            metadata_promo.name,
        ]
        add_option(promo_command, "--limit-categories", args.limit_categories)
        add_option(promo_command, "--scrolls", args.promo_scrolls)
        add_option(promo_command, "--wait-ms", args.promo_wait_ms)
        add_option(promo_command, "--limit-records", args.promo_limit_records)
        if args.headed:
            promo_command.append("--headed")
        if args.screenshot:
            promo_command.append("--screenshot")
        if args.skip_robots_check:
            promo_command.append("--skip-robots-check")

        steps.append(run_step("promo_cards", promo_command, workspace, paths.log_jsonl, args.dry_run))
        promo_ok = steps[-1]["status"] in {"success", "dry_run"}
    elif args.skip_promo:
        steps.append(
            {
                "step": "promo_cards",
                "command": [],
                "returncode": None,
                "started_at": now_local_iso(),
                "ended_at": now_local_iso(),
                "status": "skipped",
            }
        )

    hydrate_ok = True
    if config_ok and promo_ok and not args.skip_promo and not args.skip_hydrate and hydrate_enabled:
        hydrate_command = [
            sys.executable,
            str(scripts_dir / "winmart_hydrate_promo_products.py"),
            "--config",
            args.config,
            "--run-dir",
            str(paths.run_dir),
            "--metadata-name",
            metadata_hydrate.name,
        ]
        add_option(hydrate_command, "--max-hydrate-items", args.hydrate_limit)
        add_option(hydrate_command, "--timeout-ms", args.hydrate_timeout_ms)
        add_option(hydrate_command, "--wait-ms", args.hydrate_wait_ms)
        if args.headed:
            hydrate_command.append("--headed")
        if args.skip_robots_check:
            hydrate_command.append("--skip-robots-check")
        if args.skip_hydrate_detail_pages:
            hydrate_command.append("--skip-detail-pages")

        steps.append(run_step("hydrate_promo_products", hydrate_command, workspace, paths.log_jsonl, args.dry_run))
        hydrate_ok = steps[-1]["status"] in {"success", "dry_run"}
    elif args.skip_hydrate or args.skip_promo or not hydrate_enabled:
        steps.append(
            {
                "step": "hydrate_promo_products",
                "command": [],
                "returncode": None,
                "started_at": now_local_iso(),
                "ended_at": now_local_iso(),
                "status": "skipped",
            }
        )

    should_merge = (
        config_ok
        and not args.skip_merge
        and (promo_ok or not args.fail_on_promo_error)
        and (hydrate_ok or not args.fail_on_promo_error)
    )
    if should_merge:
        merge_command = [
            sys.executable,
            str(scripts_dir / "winmart_merge_promos.py"),
            "--run-dir",
            str(paths.run_dir),
            "--metadata-name",
            metadata_merge.name,
        ]
        steps.append(run_step("merge_promos", merge_command, workspace, paths.log_jsonl, args.dry_run))
    elif args.skip_merge:
        steps.append(
            {
                "step": "merge_promos",
                "command": [],
                "returncode": None,
                "started_at": now_local_iso(),
                "ended_at": now_local_iso(),
                "status": "skipped",
            }
        )

    failed_steps = [step["step"] for step in steps if step["status"] == "failed"]
    if failed_steps:
        status = "failed" if ("config_api" in failed_steps or args.fail_on_promo_error) else "partial_failed"
    elif any(step["status"] == "dry_run" for step in steps):
        status = "dry_run"
    else:
        status = "success"

    metadata = {
        "store_id": STORE_ID,
        "store_name": STORE_NAME,
        "crawler": "full_crawl",
        "crawl_timestamp": crawl_timestamp,
        "status": status,
        "run_id": run_id,
        "run_date": paths.run_dir.parent.name.removeprefix("date="),
        "run_dir": str(paths.run_dir),
        "config_path": args.config,
        "steps": steps,
        "metadata_config_api": str(metadata_config),
        "metadata_promo_cards": str(metadata_promo),
        "metadata_hydrate_promos": str(metadata_hydrate),
        "metadata_merge": str(metadata_merge),
        "products_jsonl": str(paths.products_jsonl),
        "products_discounted_jsonl": str(paths.run_dir / "products_discounted.jsonl"),
        "promo_hydrated_products_jsonl": str(paths.run_dir / "promo_hydrated_products.jsonl"),
        "promo_cards_jsonl": str(paths.run_dir / "promo_cards.jsonl"),
        "products_enriched_jsonl": str(paths.run_dir / "products_enriched.jsonl"),
        "config_api_summary": load_metadata(metadata_config),
        "promo_cards_summary": load_metadata(metadata_promo),
        "hydrate_summary": load_metadata(metadata_hydrate),
        "merge_summary": load_metadata(metadata_merge),
    }
    write_json(metadata_pipeline, metadata)
    append_jsonl(
        paths.log_jsonl,
        {
            "event": "pipeline_complete",
            "status": status,
            "run_id": run_id,
            "timestamp": now_local_iso(),
        },
    )
    print(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status in {"success", "partial_failed", "dry_run"} else 1


if __name__ == "__main__":
    sys.exit(main())

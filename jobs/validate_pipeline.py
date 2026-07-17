#!/usr/bin/env python3
"""Validate an end-to-end local pipeline run from manifests and output files."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


BLOCK_SEVERITIES = {"BLOCK_RUN", "BLOCK_PUBLISH"}


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def count_jsonl(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def parse_raw_run_dir(raw_run_dir: Path) -> dict[str, str]:
    run_id_part = raw_run_dir.name
    date_part = raw_run_dir.parent.name
    store_part = raw_run_dir.parent.parent.name

    if not run_id_part.startswith("run_id="):
        raise ValueError(f"RAW_RUN_DIR must end with run_id=<run_id>: {raw_run_dir}")
    if not date_part.startswith("date="):
        raise ValueError(f"RAW_RUN_DIR must include date=<yyyy-mm-dd>: {raw_run_dir}")
    if not store_part.startswith("store="):
        raise ValueError(f"RAW_RUN_DIR must include store=<retailer_id>: {raw_run_dir}")

    return {
        "store_part": store_part,
        "date_part": date_part,
        "run_id_part": run_id_part,
        "retailer_id": store_part.split("=", 1)[1],
        "run_date": date_part.split("=", 1)[1],
        "run_id": run_id_part.split("=", 1)[1],
    }


def expected_paths(raw_run_dir: Path, out_dir: Path) -> dict[str, Path]:
    context = parse_raw_run_dir(raw_run_dir)
    store_part = context["store_part"]
    date_part = context["date_part"]
    run_id_part = context["run_id_part"]

    bronze_dir = out_dir / "bronze" / "raw_records" / store_part / date_part / run_id_part
    silver_dir = out_dir / "silver" / store_part / date_part / run_id_part
    gold_dir = out_dir / "gold" / "fact_price_snapshot_daily" / store_part / date_part / run_id_part

    return {
        "bronze_manifest": bronze_dir / "manifest.json",
        "bronze_records": bronze_dir / "bronze_raw_records.jsonl",
        "bronze_quarantine": bronze_dir / "quarantine_records.jsonl",
        "silver_manifest": silver_dir / "manifest.json",
        "silver_products": silver_dir / "retailer_products.jsonl",
        "silver_observations": silver_dir / "product_observations.jsonl",
        "gold_manifest": gold_dir / "manifest.json",
        "gold_snapshot": gold_dir / "price_snapshot_daily.jsonl",
        "gold_validation": gold_dir / "validation_report.json",
    }


def add_issue(issues: list[dict[str, Any]], severity: str, rule_id: str, message: str) -> None:
    issues.append({"severity": severity, "rule_id": rule_id, "message": message})


def load_manifest(path: Path, layer: str, issues: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not path.exists():
        add_issue(issues, "BLOCK_RUN", f"missing_{layer}_manifest", f"{layer} manifest is missing: {path}")
        return None
    try:
        return read_json(path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        add_issue(issues, "BLOCK_RUN", f"invalid_{layer}_manifest", f"{layer} manifest cannot be read: {exc}")
        return None


def check_jsonl_count(
    *,
    path: Path,
    expected_count: Any,
    layer: str,
    rule_id: str,
    issues: list[dict[str, Any]],
) -> None:
    if not path.exists():
        add_issue(issues, "BLOCK_RUN", f"missing_{layer}_output", f"{layer} output is missing: {path}")
        return
    if not isinstance(expected_count, int):
        add_issue(issues, "WARN", f"missing_{layer}_expected_count", f"{layer} manifest has no numeric count for {path}")
        return
    actual_count = count_jsonl(path)
    if actual_count != expected_count:
        add_issue(
            issues,
            "BLOCK_RUN",
            rule_id,
            f"{layer} manifest count {expected_count} does not match file count {actual_count}: {path}",
        )


def product_dataset_count(bronze_manifest: dict[str, Any], product_dataset: str) -> int | None:
    input_files = bronze_manifest.get("input_files")
    if not isinstance(input_files, list):
        return None
    for item in input_files:
        if isinstance(item, dict) and item.get("source_dataset") == product_dataset:
            count = item.get("records_written")
            return count if isinstance(count, int) else None
    return None


def build_airflow_decision(status: str, issues: list[dict[str, Any]]) -> dict[str, Any]:
    block_rules = [issue["rule_id"] for issue in issues if issue["severity"] in BLOCK_SEVERITIES]
    warning_rules = [issue["rule_id"] for issue in issues if issue["severity"] == "WARN"]

    return {
        "task_status": "success" if status == "passed" else "failed",
        "should_publish_gold": status == "passed",
        "should_block_downstream": status != "passed",
        "should_alert": bool(issues),
        "block_rules": block_rules,
        "warning_rules": warning_rules,
    }


def validate_pipeline_run(raw_run_dir: Path, out_dir: Path = Path("warehouse"), strict_warnings: bool = False) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []

    if not raw_run_dir.exists():
        add_issue(issues, "BLOCK_RUN", "missing_raw_run_dir", f"Raw run directory is missing: {raw_run_dir}")

    try:
        context = parse_raw_run_dir(raw_run_dir)
    except ValueError as exc:
        context = {
            "retailer_id": "unknown",
            "run_date": "unknown",
            "run_id": "unknown",
            "store_part": "store=unknown",
            "date_part": "date=unknown",
            "run_id_part": "run_id=unknown",
        }
        add_issue(issues, "BLOCK_RUN", "invalid_raw_run_dir", str(exc))

    paths = expected_paths(Path(context["store_part"]) / context["date_part"] / context["run_id_part"], out_dir)
    bronze_manifest = load_manifest(paths["bronze_manifest"], "bronze", issues)
    silver_manifest = load_manifest(paths["silver_manifest"], "silver", issues)
    gold_manifest = load_manifest(paths["gold_manifest"], "gold", issues)
    gold_validation = load_manifest(paths["gold_validation"], "gold_validation", issues)

    summary: dict[str, Any] = {
        "raw_run_dir": raw_run_dir.as_posix(),
        "out_dir": out_dir.as_posix(),
        "retailer_id": context["retailer_id"],
        "run_date": context["run_date"],
        "run_id": context["run_id"],
        "layers": {},
        "flow_checks": {},
    }

    if bronze_manifest:
        summary["layers"]["bronze"] = {
            "status": bronze_manifest.get("status"),
            "records_written": bronze_manifest.get("records_written"),
            "duplicate_bronze_keys": bronze_manifest.get("duplicate_bronze_keys"),
            "invalid_json_records": bronze_manifest.get("invalid_json_records"),
            "quarantine_records": bronze_manifest.get("quarantine_records"),
            "quality_status_counts": bronze_manifest.get("quality_status_counts", {}),
            "warning_counts": bronze_manifest.get("warning_counts", {}),
        }
        if bronze_manifest.get("status") != "success":
            add_issue(issues, "BLOCK_RUN", "bronze_status_not_success", "Bronze manifest status is not success")
        if bronze_manifest.get("records_written", 0) <= 0:
            add_issue(issues, "BLOCK_RUN", "bronze_no_records", "Bronze wrote no records")
        if bronze_manifest.get("duplicate_bronze_keys", 0) > 0:
            add_issue(issues, "BLOCK_RUN", "duplicate_bronze_keys", "Bronze has duplicate bronze_record_key values")
        if bronze_manifest.get("invalid_json_records", 0) > 0:
            add_issue(issues, "BLOCK_RUN", "bronze_invalid_json", "Bronze found invalid JSON records")
        if bronze_manifest.get("quarantine_records", 0) > 0:
            add_issue(issues, "WARN", "bronze_quarantine_records", "Bronze wrote quarantine records")
        if bronze_manifest.get("warning_counts"):
            add_issue(issues, "WARN", "bronze_warnings", "Bronze has warning counts")
        check_jsonl_count(
            path=paths["bronze_records"],
            expected_count=bronze_manifest.get("records_written"),
            layer="bronze",
            rule_id="bronze_record_count_mismatch",
            issues=issues,
        )

    if silver_manifest:
        summary["layers"]["silver"] = {
            "status": silver_manifest.get("status"),
            "product_dataset": silver_manifest.get("product_dataset"),
            "retailer_products_written": silver_manifest.get("retailer_products_written"),
            "product_observations_written": silver_manifest.get("product_observations_written"),
            "skipped_counts": silver_manifest.get("skipped_counts", {}),
            "package_parse_status_counts": silver_manifest.get("package_parse_status_counts", {}),
            "unit_price_publishable_counts": silver_manifest.get("unit_price_publishable_counts", {}),
        }
        if silver_manifest.get("status") != "success":
            add_issue(issues, "BLOCK_RUN", "silver_status_not_success", "Silver manifest status is not success")
        if silver_manifest.get("retailer_products_written", 0) <= 0:
            add_issue(issues, "BLOCK_RUN", "silver_no_products", "Silver wrote no retailer products")
        if silver_manifest.get("product_observations_written", 0) <= 0:
            add_issue(issues, "BLOCK_RUN", "silver_no_observations", "Silver wrote no product observations")
        check_jsonl_count(
            path=paths["silver_products"],
            expected_count=silver_manifest.get("retailer_products_written"),
            layer="silver_products",
            rule_id="silver_product_count_mismatch",
            issues=issues,
        )
        check_jsonl_count(
            path=paths["silver_observations"],
            expected_count=silver_manifest.get("product_observations_written"),
            layer="silver_observations",
            rule_id="silver_observation_count_mismatch",
            issues=issues,
        )

    if gold_manifest:
        summary["layers"]["gold"] = {
            "status": gold_manifest.get("status"),
            "validation_status": gold_manifest.get("validation_status"),
            "snapshots_written": gold_manifest.get("snapshots_written"),
            "products_read": gold_manifest.get("products_read"),
            "observations_read": gold_manifest.get("observations_read"),
            "block_publish_issues": gold_manifest.get("block_publish_issues"),
            "warn_issues": gold_manifest.get("warn_issues"),
            "selection_stats": gold_manifest.get("selection_stats", {}),
            "unit_price_publishable_counts": gold_manifest.get("unit_price_publishable_counts", {}),
        }
        if gold_manifest.get("status") != "success":
            add_issue(issues, "BLOCK_PUBLISH", "gold_status_not_success", "Gold manifest status is not success")
        if gold_manifest.get("validation_status") != "passed":
            add_issue(issues, "BLOCK_PUBLISH", "gold_validation_not_passed", "Gold validation status is not passed")
        if gold_manifest.get("block_publish_issues", 0) > 0:
            add_issue(issues, "BLOCK_PUBLISH", "gold_block_publish_issues", "Gold has BLOCK_PUBLISH issues")
        if gold_manifest.get("warn_issues", 0) > 0:
            add_issue(issues, "WARN", "gold_warn_issues", "Gold has warning issues")
        if gold_manifest.get("snapshots_written", 0) <= 0:
            add_issue(issues, "BLOCK_PUBLISH", "gold_no_snapshots", "Gold wrote no snapshots")
        check_jsonl_count(
            path=paths["gold_snapshot"],
            expected_count=gold_manifest.get("snapshots_written"),
            layer="gold",
            rule_id="gold_snapshot_count_mismatch",
            issues=issues,
        )

    if gold_validation:
        summary["layers"]["gold_validation"] = {
            "status": gold_validation.get("status"),
            "rows_checked": gold_validation.get("rows_checked"),
            "block_publish_issues": gold_validation.get("block_publish_issues"),
            "warn_issues": gold_validation.get("warn_issues"),
            "issue_counts": gold_validation.get("issue_counts", {}),
        }
        if gold_validation.get("status") != "passed":
            add_issue(issues, "BLOCK_PUBLISH", "gold_validation_report_failed", "Gold validation report status is not passed")

    if bronze_manifest and silver_manifest:
        product_dataset = str(silver_manifest.get("product_dataset") or "")
        enriched_count = product_dataset_count(bronze_manifest, product_dataset)
        silver_observations = silver_manifest.get("product_observations_written")
        summary["flow_checks"]["bronze_selected_dataset_records"] = enriched_count
        summary["flow_checks"]["silver_observations_written"] = silver_observations
        if isinstance(enriched_count, int) and isinstance(silver_observations, int) and silver_observations > enriched_count:
            add_issue(
                issues,
                "BLOCK_RUN",
                "silver_observations_exceed_bronze_dataset",
                "Silver observations exceed selected Bronze product dataset records",
            )

    if silver_manifest and gold_manifest:
        silver_products = silver_manifest.get("retailer_products_written")
        silver_observations = silver_manifest.get("product_observations_written")
        gold_products_read = gold_manifest.get("products_read")
        gold_observations_read = gold_manifest.get("observations_read")
        gold_snapshots = gold_manifest.get("snapshots_written")
        summary["flow_checks"]["silver_products_written"] = silver_products
        summary["flow_checks"]["gold_products_read"] = gold_products_read
        summary["flow_checks"]["gold_observations_read"] = gold_observations_read
        summary["flow_checks"]["gold_snapshots_written"] = gold_snapshots
        if silver_products != gold_products_read:
            add_issue(issues, "BLOCK_RUN", "gold_products_read_mismatch", "Gold products_read does not match Silver products_written")
        if silver_observations != gold_observations_read:
            add_issue(issues, "BLOCK_RUN", "gold_observations_read_mismatch", "Gold observations_read does not match Silver observations_written")
        if isinstance(gold_snapshots, int) and isinstance(gold_observations_read, int) and gold_snapshots > gold_observations_read:
            add_issue(issues, "BLOCK_PUBLISH", "gold_snapshots_exceed_observations", "Gold snapshots exceed observations read")

    if strict_warnings:
        for issue in list(issues):
            if issue["severity"] == "WARN":
                add_issue(issues, "BLOCK_RUN", f"strict_{issue['rule_id']}", f"Strict warnings enabled: {issue['message']}")

    severity_counts = Counter(issue["severity"] for issue in issues)
    status = "failed" if any(issue["severity"] in BLOCK_SEVERITIES for issue in issues) else "passed"
    summary["status"] = status
    summary["airflow_decision"] = build_airflow_decision(status, issues)
    summary["severity_counts"] = dict(sorted(severity_counts.items()))
    summary["issues"] = issues
    return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a local Bronze -> Silver -> Gold pipeline run.")
    parser.add_argument("--raw-run-dir", required=True, help="Raw run directory, e.g. raw/store=winmart/date=2026-07-02/run_id=20260702_155006.")
    parser.add_argument("--out-dir", default="warehouse", help="Pipeline output root directory.")
    parser.add_argument("--report-file", help="Optional path to write the validation report JSON.")
    parser.add_argument("--strict-warnings", action="store_true", help="Fail the validation when WARN issues exist.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    report = validate_pipeline_run(
        raw_run_dir=Path(args.raw_run_dir),
        out_dir=Path(args.out_dir),
        strict_warnings=args.strict_warnings,
    )

    if args.report_file:
        report_path = Path(args.report_file)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 2


if __name__ == "__main__":
    sys.exit(main())

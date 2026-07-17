#!/usr/bin/env python3
"""Validate Spark Gold manifest before promoting Spark output downstream."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


BLOCK_SEVERITIES = {"BLOCK_RUN", "BLOCK_PUBLISH"}


def add_issue(issues: list[dict[str, Any]], severity: str, rule_id: str, message: str) -> None:
    issues.append({"severity": severity, "rule_id": rule_id, "message": message})


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def validate_spark_gold_manifest(manifest_path: Path, *, allow_no_reference: bool = False) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    manifest: dict[str, Any] | None = None

    if not manifest_path.exists():
        add_issue(issues, "BLOCK_RUN", "missing_manifest", f"Manifest is missing: {manifest_path}")
    else:
        try:
            manifest = read_json(manifest_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            add_issue(issues, "BLOCK_RUN", "invalid_manifest", f"Manifest cannot be read: {exc}")

    comparison = manifest.get("comparison") if manifest else None
    summary = {
        "manifest_path": manifest_path.as_posix(),
        "engine": manifest.get("engine") if manifest else None,
        "status": manifest.get("status") if manifest else None,
        "retailer_id": manifest.get("retailer_id") if manifest else None,
        "snapshot_date": manifest.get("snapshot_date") if manifest else None,
        "source_run_id": manifest.get("source_run_id") if manifest else None,
        "snapshots_written": manifest.get("snapshots_written") if manifest else None,
        "comparison": comparison if isinstance(comparison, dict) else None,
    }

    if manifest:
        if manifest.get("status") != "success":
            add_issue(issues, "BLOCK_PUBLISH", "spark_manifest_status_not_success", "Spark manifest status is not success")
        if manifest.get("engine") != "spark":
            add_issue(issues, "WARN", "unexpected_engine", "Manifest engine is not spark")
        if not isinstance(manifest.get("snapshots_written"), int) or manifest.get("snapshots_written", 0) <= 0:
            add_issue(issues, "BLOCK_PUBLISH", "no_snapshots_written", "Spark manifest reports no snapshots written")

    reference_comparison_available = isinstance(comparison, dict) and bool(comparison)
    if not reference_comparison_available:
        if not allow_no_reference:
            add_issue(issues, "BLOCK_PUBLISH", "missing_comparison", "Spark manifest has no Python Gold comparison section")
        reference_validation_status = "not_available" if allow_no_reference else "required_but_missing"
    else:
        reference_validation_status = "passed"
        if comparison.get("snapshot_id_sets_match") is not True:
            add_issue(issues, "BLOCK_PUBLISH", "snapshot_id_sets_not_match", "Spark snapshot id sets do not match Python Gold")
            reference_validation_status = "failed"
        if comparison.get("critical_fields_match") is not True:
            add_issue(issues, "BLOCK_PUBLISH", "critical_fields_not_match", "Spark critical fields do not match Python Gold")
            reference_validation_status = "failed"
        if comparison.get("replacement_ready") is not True:
            add_issue(issues, "BLOCK_PUBLISH", "replacement_not_ready", "Spark manifest replacement_ready is not true")
            reference_validation_status = "failed"
        if comparison.get("missing_in_spark", 0) != 0:
            add_issue(issues, "BLOCK_PUBLISH", "missing_in_spark", "Spark is missing snapshot ids from Python Gold")
            reference_validation_status = "failed"
        if comparison.get("extra_in_spark", 0) != 0:
            add_issue(issues, "BLOCK_PUBLISH", "extra_in_spark", "Spark has extra snapshot ids not present in Python Gold")
            reference_validation_status = "failed"
        if comparison.get("critical_field_mismatches", 0) != 0:
            add_issue(issues, "BLOCK_PUBLISH", "critical_field_mismatches", "Spark has critical field mismatches versus Python Gold")
            reference_validation_status = "failed"

    severity_counts = dict(Counter(issue["severity"] for issue in issues))
    status = "failed" if any(issue["severity"] in BLOCK_SEVERITIES for issue in issues) else "passed"
    return {
        "status": status,
        "manifest_path": manifest_path.as_posix(),
        "summary": summary,
        "severity_counts": severity_counts,
        "issues": issues,
        "allow_no_reference": allow_no_reference,
        "reference_validation_status": reference_validation_status,
        "replacement_ready": bool(isinstance(comparison, dict) and comparison.get("replacement_ready") is True),
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Spark Gold manifest replacement gate.")
    parser.add_argument("--manifest", required=True, help="Path to Spark Gold manifest.json.")
    parser.add_argument(
        "--allow-no-reference",
        action="store_true",
        help="Allow a successful Spark output without Python Gold reference comparison.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    report = validate_spark_gold_manifest(Path(args.manifest), allow_no_reference=args.allow_no_reference)
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())

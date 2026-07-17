#!/usr/bin/env python3
"""Ingest raw crawler JSONL files into a deterministic Bronze envelope."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 1
DEFAULT_INPUTS = (
    "products.jsonl",
    "products_discounted.jsonl",
    "promo_cards.jsonl",
    "promo_hydrated_products.jsonl",
    "products_enriched.jsonl",
    "unmatched_promo_cards.jsonl",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def portable_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_raw_path(run_dir: Path) -> dict[str, str | None]:
    parts = run_dir.as_posix().split("/")
    context: dict[str, str | None] = {
        "retailer_id": None,
        "run_date": None,
        "run_id": None,
    }
    for part in parts:
        if part.startswith("store="):
            context["retailer_id"] = part.split("=", 1)[1]
        elif part.startswith("date="):
            context["run_date"] = part.split("=", 1)[1]
        elif part.startswith("run_id="):
            context["run_id"] = part.split("=", 1)[1]
    return context


def load_metadata(run_dir: Path) -> dict[str, Any]:
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        return {}
    with metadata_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def infer_context(run_dir: Path) -> dict[str, Any]:
    path_context = parse_raw_path(run_dir)
    metadata = load_metadata(run_dir)
    config_summary = metadata.get("config_api_summary") or {}
    promo_summary = metadata.get("promo_cards_summary") or {}

    return {
        "run_id": metadata.get("run_id") or path_context["run_id"],
        "run_date": path_context["run_date"],
        "retailer_id": metadata.get("store_id") or config_summary.get("store_id") or path_context["retailer_id"],
        "store_name": metadata.get("store_name") or config_summary.get("store_name"),
        "store_code": config_summary.get("store_code") or promo_summary.get("store_code"),
        "store_group_code": config_summary.get("store_group_code") or promo_summary.get("store_group_code"),
        "region": config_summary.get("region") or promo_summary.get("region"),
        "metadata_path": portable_path(metadata_path_relative(run_dir)) if (run_dir / "metadata.json").exists() else None,
    }


def metadata_path_relative(run_dir: Path) -> Path:
    return run_dir / "metadata.json"


def source_dataset(path: Path) -> str:
    return path.stem


def discover_input_files(run_dir: Path, requested: list[str] | None) -> list[Path]:
    if requested:
        files = [run_dir / name for name in requested if (run_dir / name).exists()]
    else:
        known_files = [run_dir / name for name in DEFAULT_INPUTS if (run_dir / name).exists()]
        retailer_promotion_files = sorted(run_dir.glob("*_promotions_*.jsonl"))
        files = known_files + [path for path in retailer_promotion_files if path not in known_files]
    return sorted(files, key=lambda item: item.name)


def has_identity(payload: dict[str, Any]) -> bool:
    raw_product = payload.get("raw_product")
    raw_card = payload.get("raw_card")
    return any(
        payload.get(field)
        for field in (
            "source_product_id",
            "item_no",
            "product_url",
            "match_key",
            "source_url",
            "product_name_raw",
        )
    ) or bool(raw_product) or bool(raw_card)


def quality_for_payload(payload: dict[str, Any], run_store_code: str | None) -> tuple[str, list[str], list[str]]:
    warnings: list[str] = []
    quarantine_reasons: list[str] = []

    record_store_code = payload.get("store_code")
    if run_store_code and record_store_code and str(record_store_code) != str(run_store_code):
        quarantine_reasons.append("store_code_mismatch")

    if not has_identity(payload):
        quarantine_reasons.append("missing_raw_payload_identity")

    if not payload.get("source_url"):
        warnings.append("missing_source_url")

    if not payload.get("crawl_timestamp") and not payload.get("observed_at"):
        warnings.append("missing_observed_at")

    if "package_size_raw" in payload and payload.get("package_size_raw") is None:
        warnings.append("package_size_null")

    if quarantine_reasons:
        return "QUARANTINE", warnings, quarantine_reasons
    if warnings:
        return "WARN", warnings, quarantine_reasons
    return "PASS", warnings, quarantine_reasons


def build_record_key(
    *,
    retailer_id: str,
    run_id: str,
    dataset: str,
    source_file: str,
    payload_sha256: str,
    payload_occurrence_index: int,
) -> str:
    key_payload = "|".join(
        [
            str(CONTRACT_VERSION),
            retailer_id,
            run_id,
            dataset,
            source_file,
            payload_sha256,
            str(payload_occurrence_index),
        ]
    )
    return sha256_text(key_payload)


def output_paths(out_dir: Path, retailer_id: str, run_date: str, run_id: str) -> dict[str, Path]:
    base = out_dir / "bronze" / "raw_records" / f"store={retailer_id}" / f"date={run_date}" / f"run_id={run_id}"
    return {
        "base": base,
        "records": base / "bronze_raw_records.jsonl",
        "manifest": base / "manifest.json",
        "quarantine": base / "quarantine_records.jsonl",
    }


def ingest_run(run_dir: Path, out_dir: Path, input_names: list[str] | None = None, ingested_at: str | None = None) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    context = infer_context(run_dir)
    run_id = context.get("run_id")
    retailer_id = context.get("retailer_id")
    run_date = context.get("run_date")

    if not run_id:
        raise ValueError("BLOCK_RUN missing_run_id")
    if not retailer_id:
        raise ValueError("BLOCK_RUN missing_retailer_id")
    if not run_date:
        raise ValueError("BLOCK_RUN missing_run_date")

    files = discover_input_files(run_dir, input_names)
    if not files:
        raise ValueError("BLOCK_RUN no_input_jsonl")

    paths = output_paths(out_dir, str(retailer_id), str(run_date), str(run_id))
    paths["base"].mkdir(parents=True, exist_ok=True)
    ingest_time = ingested_at or utc_now_iso()

    records_written = 0
    quarantine_written = 0
    invalid_json_count = 0
    status_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    quarantine_counts: Counter[str] = Counter()
    file_summaries: list[dict[str, Any]] = []
    bronze_keys: set[str] = set()

    with paths["records"].open("w", encoding="utf-8") as records_handle, paths["quarantine"].open(
        "w", encoding="utf-8"
    ) as quarantine_handle:
        for input_file in files:
            dataset = source_dataset(input_file)
            source_file_hash = sha256_file(input_file)
            source_file_path = portable_path(input_file)
            payload_occurrences: Counter[str] = Counter()
            file_records = 0
            file_quarantine = 0

            with input_file.open("r", encoding="utf-8") as input_handle:
                for line_number, line in enumerate(input_handle, start=1):
                    raw_line = line.rstrip("\n")
                    if not raw_line.strip():
                        continue
                    try:
                        payload = json.loads(raw_line)
                    except json.JSONDecodeError as exc:
                        invalid_json_count += 1
                        file_quarantine += 1
                        quarantine_counts["invalid_json"] += 1
                        quarantine_handle.write(
                            canonical_json(
                                {
                                    "contract_version": CONTRACT_VERSION,
                                    "run_id": run_id,
                                    "retailer_id": retailer_id,
                                    "source_dataset": dataset,
                                    "source_file": source_file_path,
                                    "source_line_number": line_number,
                                    "quality_status": "QUARANTINE",
                                    "quarantine_reasons": ["invalid_json"],
                                    "error_message": str(exc),
                                    "raw_line": raw_line,
                                    "ingested_at": ingest_time,
                                }
                            )
                            + "\n"
                        )
                        continue

                    payload_hash = sha256_text(canonical_json(payload))
                    payload_occurrences[payload_hash] += 1
                    occurrence_index = payload_occurrences[payload_hash]
                    status, warnings, quarantine_reasons = quality_for_payload(payload, context.get("store_code"))
                    key = build_record_key(
                        retailer_id=str(retailer_id),
                        run_id=str(run_id),
                        dataset=dataset,
                        source_file=source_file_path,
                        payload_sha256=payload_hash,
                        payload_occurrence_index=occurrence_index,
                    )
                    bronze_keys.add(key)

                    record = {
                        "bronze_record_key": key,
                        "contract_version": CONTRACT_VERSION,
                        "run_id": run_id,
                        "retailer_id": retailer_id,
                        "store_name": context.get("store_name"),
                        "store_code": context.get("store_code"),
                        "store_group_code": context.get("store_group_code"),
                        "region": context.get("region"),
                        "source_dataset": dataset,
                        "source_file": source_file_path,
                        "source_line_number": line_number,
                        "source_url": payload.get("source_url"),
                        "observed_at": payload.get("crawl_timestamp") or payload.get("observed_at") or payload.get("crawled_at"),
                        "ingested_at": ingest_time,
                        "raw_payload": payload,
                        "payload_sha256": payload_hash,
                        "payload_occurrence_index": occurrence_index,
                        "source_file_sha256": source_file_hash,
                        "raw_run_dir": portable_path(run_dir),
                        "quality_status": status,
                        "quality_warnings": warnings,
                        "quarantine_reasons": quarantine_reasons,
                    }

                    records_handle.write(canonical_json(record) + "\n")
                    records_written += 1
                    file_records += 1
                    status_counts[status] += 1
                    warning_counts.update(warnings)
                    quarantine_counts.update(quarantine_reasons)

                    if status == "QUARANTINE":
                        quarantine_handle.write(canonical_json(record) + "\n")
                        quarantine_written += 1
                        file_quarantine += 1

            file_summaries.append(
                {
                    "source_dataset": dataset,
                    "source_file": source_file_path,
                    "source_file_sha256": source_file_hash,
                    "records_written": file_records,
                    "quarantine_records": file_quarantine,
                }
            )

    records_hash = sha256_file(paths["records"])
    quarantine_hash = sha256_file(paths["quarantine"])
    manifest = {
        "contract_name": "bronze_raw_record",
        "contract_version": CONTRACT_VERSION,
        "status": "success",
        "run_id": run_id,
        "retailer_id": retailer_id,
        "run_date": run_date,
        "raw_run_dir": portable_path(run_dir),
        "bronze_output_file": paths["records"].as_posix(),
        "quarantine_output_file": paths["quarantine"].as_posix(),
        "records_written": records_written,
        "quarantine_records": quarantine_written,
        "invalid_json_records": invalid_json_count,
        "distinct_bronze_keys": len(bronze_keys),
        "duplicate_bronze_keys": records_written - len(bronze_keys),
        "quality_status_counts": dict(sorted(status_counts.items())),
        "warning_counts": dict(sorted(warning_counts.items())),
        "quarantine_reason_counts": dict(sorted(quarantine_counts.items())),
        "input_files": file_summaries,
        "records_file_sha256": records_hash,
        "quarantine_file_sha256": quarantine_hash,
        "ingested_at": ingest_time,
    }

    with paths["manifest"].open("w", encoding="utf-8") as manifest_handle:
        json.dump(manifest, manifest_handle, ensure_ascii=False, indent=2, sort_keys=True)
        manifest_handle.write("\n")

    return manifest


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw crawler JSONL into Bronze envelope.")
    parser.add_argument("--run-dir", required=True, help="Raw crawler run directory.")
    parser.add_argument("--out-dir", default="warehouse", help="Output root directory.")
    parser.add_argument(
        "--input",
        action="append",
        dest="inputs",
        help="Input JSONL filename inside run-dir. Can be repeated. Defaults to known WinMart JSONL files.",
    )
    parser.add_argument(
        "--ingested-at",
        help="Override ingest timestamp for deterministic tests, for example 2026-07-01T00:00:00+00:00.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        manifest = ingest_run(
            run_dir=Path(args.run_dir),
            out_dir=Path(args.out_dir),
            input_names=args.inputs,
            ingested_at=args.ingested_at,
        )
    except ValueError as exc:
        message = str(exc)
        status = "blocked" if message.startswith("BLOCK_RUN") else "failed"
        print(canonical_json({"status": status, "error": message}), file=sys.stderr)
        return 2

    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

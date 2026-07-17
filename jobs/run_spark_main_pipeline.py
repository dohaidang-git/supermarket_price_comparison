#!/usr/bin/env python3
"""Run the transition main pipeline with latest raw input and Spark Gold output."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def find_latest_raw_run(raw_store_dir: Path) -> Path:
    if not raw_store_dir.exists():
        raise FileNotFoundError(f"Raw store directory does not exist: {raw_store_dir}")

    candidates = [
        run_dir
        for date_dir in raw_store_dir.glob("date=*")
        if date_dir.is_dir()
        for run_dir in date_dir.glob("run_id=*")
        if run_dir.is_dir()
    ]
    if not candidates:
        raise FileNotFoundError(f"No raw run directories found under: {raw_store_dir}")

    return sorted(candidates, key=lambda path: (path.parent.name, path.name))[-1]


def parse_run_context(raw_run_dir: Path) -> dict[str, str]:
    run_id_part = raw_run_dir.name
    date_part = raw_run_dir.parent.name
    store_part = raw_run_dir.parent.parent.name
    raw_part = raw_run_dir.parent.parent.parent.name

    if raw_part != "raw":
        raise ValueError(f"RAW_RUN_DIR must live under raw/: {raw_run_dir}")
    if not run_id_part.startswith("run_id="):
        raise ValueError(f"RAW_RUN_DIR must end with run_id=<run_id>: {raw_run_dir}")
    if not date_part.startswith("date="):
        raise ValueError(f"RAW_RUN_DIR must include date=<yyyy-mm-dd>: {raw_run_dir}")
    if not store_part.startswith("store="):
        raise ValueError(f"RAW_RUN_DIR must include store=<retailer_id>: {raw_run_dir}")

    retailer_id = store_part.split("=", 1)[1]
    run_date = date_part.split("=", 1)[1]
    run_id = run_id_part.split("=", 1)[1]
    return {
        "retailer_id": retailer_id,
        "run_date": run_date,
        "run_id": run_id,
        "store_part": store_part,
        "date_part": date_part,
        "run_id_part": run_id_part,
    }


def expected_paths(raw_run_dir: Path, out_dir: Path, spark_out_dir: Path) -> dict[str, Path]:
    context = parse_run_context(raw_run_dir)
    store_part = context["store_part"]
    date_part = context["date_part"]
    run_id_part = context["run_id_part"]

    bronze_dir = out_dir / "bronze" / "raw_records" / store_part / date_part / run_id_part
    silver_dir = out_dir / "silver" / store_part / date_part / run_id_part
    spark_gold_dir = spark_out_dir / "gold" / "fact_price_snapshot_daily" / store_part / date_part / run_id_part
    python_gold_dir = out_dir / "gold" / "fact_price_snapshot_daily" / store_part / date_part / run_id_part
    return {
        "bronze_file": bronze_dir / "bronze_raw_records.jsonl",
        "silver_products_file": silver_dir / "retailer_products.jsonl",
        "silver_observations_file": silver_dir / "product_observations.jsonl",
        "silver_mapping_file": out_dir / "silver" / "product_identity_mapping" / date_part / run_id_part / "product_identity_mapping.jsonl",
        "spark_manifest": spark_gold_dir / "manifest.json",
        "python_gold_file": python_gold_dir / "price_snapshot_daily.jsonl",
    }


def load_state(state_file: Path) -> dict[str, Any]:
    if not state_file.exists():
        return {}
    try:
        return read_json(state_file)
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def should_skip_run(
    state: dict[str, Any],
    raw_run_dir: Path,
    spark_manifest: Path,
    force: bool,
    spark_output_format: str,
) -> bool:
    if force:
        return False
    return (
        state.get("last_processed_raw_run") == raw_run_dir.as_posix()
        and state.get("last_status") == "success"
        and state.get("spark_output_format") == spark_output_format
        and spark_manifest.exists()
    )


def run_command(command: list[str], *, env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(command, check=False, env=env)  # noqa: S603 - fixed local commands only.
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed with exit code {completed.returncode}: {' '.join(command)}")


def bronze_command(python_bin: str, raw_run_dir: Path, out_dir: Path) -> list[str]:
    return [
        python_bin,
        "jobs/bronze/ingest_raw.py",
        "--run-dir",
        raw_run_dir.as_posix(),
        "--out-dir",
        out_dir.as_posix(),
    ]


def silver_command(python_bin: str, bronze_file: Path, out_dir: Path, product_dataset: str) -> list[str]:
    return [
        python_bin,
        "jobs/silver/normalize_products.py",
        "--bronze-file",
        bronze_file.as_posix(),
        "--out-dir",
        out_dir.as_posix(),
        "--product-dataset",
        product_dataset,
    ]


def local_spark_command(run_dir: Path) -> list[str]:
    return ["bash", "scripts/run_spark_gold_snapshot.sh", run_dir.as_posix()]


def docker_spark_command(
    *,
    products_file: Path,
    observations_file: Path,
    mapping_file: Path | None,
    out_dir: Path,
    compare_gold_file: Path | None,
    coalesce: int,
    output_format: str,
) -> list[str]:
    command: list[str] = [
        "docker",
        "compose",
        "-f",
        "infra/spark/docker-compose.yml",
        "run",
        "--rm",
        "spark-gold",
    ]
    if output_format == "hudi":
        command.extend(
            [
                "--packages",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0",
                "--conf",
                "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--conf",
                "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
                "--conf",
                "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                "--conf",
                "spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar",
            ]
        )
    command.extend(
        [
            "jobs/spark/build_gold_price_snapshot_spark.py",
            "--products-file",
            products_file.as_posix(),
            "--observations-file",
            observations_file.as_posix(),
            "--out-dir",
            out_dir.as_posix(),
            "--coalesce",
            str(coalesce),
            "--output-format",
            output_format,
        ]
    )
    if mapping_file and mapping_file.exists():
        command.extend(["--mapping-file", mapping_file.as_posix()])
    if compare_gold_file and compare_gold_file.exists():
        command.extend(["--compare-gold-file", compare_gold_file.as_posix()])
    return command


def manifest_validate_command(python_bin: str, manifest_path: Path) -> list[str]:
    return [
        python_bin,
        "jobs/validate_spark_gold_manifest.py",
        "--manifest",
        manifest_path.as_posix(),
    ]


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    raw_store_dir = Path(args.raw_store_dir)
    latest_raw_run = find_latest_raw_run(raw_store_dir)
    paths = expected_paths(latest_raw_run, Path(args.out_dir), Path(args.spark_out_dir))
    mapping_file = Path(args.mapping_file) if args.mapping_file else paths["silver_mapping_file"]
    state_file = Path(args.state_file)
    state = load_state(state_file)

    if should_skip_run(state, latest_raw_run, paths["spark_manifest"], args.force, args.spark_output_format):
        report = {
            "status": "skipped",
            "reason": "latest_raw_run_already_processed",
            "latest_raw_run": latest_raw_run.as_posix(),
            "state_file": state_file.as_posix(),
            "spark_manifest": paths["spark_manifest"].as_posix(),
            "spark_output_format": args.spark_output_format,
        }
        write_json(
            state_file,
            {
                "last_checked_at": utc_now_iso(),
                "last_processed_raw_run": latest_raw_run.as_posix(),
                "last_status": "success",
                "last_decision": "skipped_same_run",
                "last_pipeline_status": state.get("last_pipeline_status", "success"),
                "spark_output_format": state.get("spark_output_format", args.spark_output_format),
                "spark_manifest": paths["spark_manifest"].as_posix(),
            },
        )
        return report

    compare_gold_file = paths["python_gold_file"] if paths["python_gold_file"].exists() else None
    env = os.environ.copy()
    env["OUT_DIR"] = Path(args.spark_out_dir).as_posix()
    if args.spark_mode == "local":
        if compare_gold_file:
            env["COMPARE_GOLD_FILE"] = compare_gold_file.as_posix()
        else:
            env["COMPARE_GOLD_FILE"] = ""
        env["OUTPUT_FORMAT"] = args.spark_output_format

    if not args.skip_bronze:
        run_command(bronze_command(args.python_bin, latest_raw_run, Path(args.out_dir)))
    if not args.skip_silver:
        run_command(silver_command(args.python_bin, paths["bronze_file"], Path(args.out_dir), args.product_dataset))
    if args.skip_silver and not paths["silver_products_file"].exists():
        raise FileNotFoundError(f"Cannot skip Silver; product file does not exist: {paths['silver_products_file']}")
    if args.skip_silver and not paths["silver_observations_file"].exists():
        raise FileNotFoundError(f"Cannot skip Silver; observation file does not exist: {paths['silver_observations_file']}")

    if args.spark_mode == "docker":
        spark_command = docker_spark_command(
            products_file=paths["silver_products_file"],
            observations_file=paths["silver_observations_file"],
            mapping_file=mapping_file,
            out_dir=Path(args.spark_out_dir),
            compare_gold_file=compare_gold_file,
            coalesce=args.coalesce,
            output_format=args.spark_output_format,
        )
        run_command(spark_command)
    else:
        run_command(local_spark_command(latest_raw_run), env=env)

    status = "success"
    validation_status = "not_run"
    if compare_gold_file:
        run_command(manifest_validate_command(args.python_bin, paths["spark_manifest"]))
        validation_status = "passed"
        status = "success_with_reference_validation"

    report = {
        "status": status,
        "latest_raw_run": latest_raw_run.as_posix(),
        "state_file": state_file.as_posix(),
        "spark_mode": args.spark_mode,
        "spark_output_format": args.spark_output_format,
        "python_gold_reference_used": bool(compare_gold_file),
        "python_gold_reference_file": compare_gold_file.as_posix() if compare_gold_file else None,
        "silver_mapping_file": mapping_file.as_posix() if mapping_file.exists() else None,
        "spark_manifest": paths["spark_manifest"].as_posix(),
        "spark_validation_status": validation_status,
    }
    write_json(
        state_file,
        {
            "last_checked_at": utc_now_iso(),
            "last_processed_raw_run": latest_raw_run.as_posix(),
            "last_status": "success",
            "last_decision": "processed",
            "last_pipeline_status": status,
            "spark_mode": args.spark_mode,
            "spark_output_format": args.spark_output_format,
            "spark_manifest": paths["spark_manifest"].as_posix(),
            "python_gold_reference_used": bool(compare_gold_file),
            "silver_mapping_file": mapping_file.as_posix() if mapping_file.exists() else None,
            "spark_validation_status": validation_status,
        },
    )
    return report


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the main Spark-oriented pipeline on the latest raw run.")
    parser.add_argument("--raw-store-dir", default="raw/store=winmart", help="Root directory containing date=*/run_id=* raw runs.")
    parser.add_argument("--out-dir", default="warehouse", help="Output root for Bronze/Silver and optional Python Gold reference.")
    parser.add_argument("--spark-out-dir", default="warehouse_spark_docker", help="Output root for Spark Gold artifacts.")
    parser.add_argument("--state-file", default=".state/spark_main_pipeline_latest_run.json", help="State file used to remember the latest processed raw run.")
    parser.add_argument("--python-bin", default="python", help="Python executable used for Bronze/Silver/validator jobs.")
    parser.add_argument("--product-dataset", default="auto", help="Bronze source dataset to normalize, or auto for retailer crawler outputs.")
    parser.add_argument("--spark-mode", choices=("docker", "local"), default="docker", help="How to run Spark Gold.")
    parser.add_argument("--spark-output-format", choices=("parquet", "hudi"), default="parquet", help="Spark Gold output format.")
    parser.add_argument("--coalesce", type=int, default=1, help="Spark Gold output partition count.")
    parser.add_argument("--mapping-file", help="Optional global Silver identity mapping to join into Spark Gold.")
    parser.add_argument("--skip-bronze", action="store_true", help="Use an existing Bronze output for the selected raw run.")
    parser.add_argument("--skip-silver", action="store_true", help="Use existing Silver outputs for the selected raw run.")
    parser.add_argument("--force", action="store_true", help="Process the latest raw run even if it matches the remembered state.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        report = run_pipeline(args)
    except Exception as exc:  # noqa: BLE001 - CLI should surface a clean failure.
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 1

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

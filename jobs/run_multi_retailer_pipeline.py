#!/usr/bin/env python3
"""Run crawler -> Bronze -> Python Silver -> Spark Gold for selected retailers."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RETAILERS = ("bachhoaxanh", "go", "lottemart", "mmvietnam")
VIETNAM_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")


def run_command(command: list[str], *, dry_run: bool = False) -> subprocess.CompletedProcess[str] | None:
    print("==>", " ".join(command))
    if dry_run:
        return None
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(f"<== exit={completed.returncode}")
    if completed.returncode != 0 and completed.stdout:
        print(completed.stdout, file=sys.stderr)
    return completed


def hudi_json_command(
    input_file: Path,
    table_path: Path,
    table_name: str,
    record_key: str,
    partition_field: str | None,
    precombine_field: str = "built_at",
) -> list[str]:
    command = [
        "docker", "compose", "-f", "infra/spark/docker-compose.yml", "run", "--rm", "spark-gold",
        "--packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "--conf", "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "--conf", "spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar",
        "jobs/spark/write_jsonl_hudi.py", "--input-file", input_file.as_posix(),
        "--table-path", table_path.as_posix(), "--table-name", table_name,
        "--record-key", record_key, "--precombine-field", precombine_field,
    ]
    if partition_field:
        command.extend(["--partition-field", partition_field])
    return command


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def write_pipeline_manifest(
    warehouse_root: Path,
    run_date: str,
    run_id: str,
    manifest: dict[str, Any],
) -> Path:
    path = warehouse_root / "pipeline_runs" / f"date={run_date}" / f"run_id={run_id}" / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run selected retailer crawlers and the existing Spark pipeline.")
    parser.add_argument("--retailers", nargs="+", default=list(DEFAULT_RETAILERS))
    parser.add_argument("--include-winmart", action="store_true", help="Also run the dedicated WinMart full crawler in the same pipeline.")
    parser.add_argument("--winmart-config", default="configs/winmart_categories.yaml")
    parser.add_argument("--run-id", help="Reuse a crawler run id for a controlled rerun.")
    parser.add_argument("--run-date")
    # Keep defaults workspace-relative because Docker mounts the repository at /workspace.
    parser.add_argument("--raw-root", type=Path, default=Path("raw"))
    parser.add_argument("--warehouse-root", type=Path, default=Path("warehouse"))
    parser.add_argument("--spark-root", type=Path, default=Path("warehouse_spark_docker"))
    parser.add_argument("--spark-mode", choices=("docker", "local"), default="docker")
    parser.add_argument("--spark-output-format", choices=("parquet", "hudi"), default="parquet")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--test-limit", type=int)
    parser.add_argument("--skip-crawlers", action="store_true", help="Reuse existing raw runs and execute Bronze through Gold only.")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    run_id = args.run_id or datetime.now(VIETNAM_TZ).strftime("%Y%m%d_%H%M%S")
    if not args.skip_crawlers:
        crawler_command = [args.python_bin, "scripts/run_retailer_crawlers.py", "--retailers", *args.retailers, "--output-root", args.raw_root.as_posix(), "--run-id", run_id]
        if args.run_date:
            crawler_command += ["--run-date", args.run_date]
        if args.test_limit:
            crawler_command += ["--test-limit", str(args.test_limit)]
        if args.continue_on_error:
            crawler_command.append("--continue-on-error")

        crawler_result = run_command(crawler_command, dry_run=args.dry_run)
        if args.dry_run:
            if args.include_winmart:
                winmart_command = [args.python_bin, "scrapers/winmart/winmart_full_crawl.py", "--config", args.winmart_config, "--out-dir", args.raw_root.as_posix(), "--run-id", run_id]
                if args.test_limit:
                    winmart_command += ["--limit-categories", str(args.test_limit)]
                run_command(winmart_command, dry_run=True)
            return 0
        if crawler_result is None or (crawler_result.returncode != 0 and not args.continue_on_error):
            print("Crawler stage failed; inspect raw/*/metadata.json.", file=sys.stderr)
            return 1

    retailers_to_process = list(args.retailers)
    if args.include_winmart:
        if not args.skip_crawlers:
            winmart_command = [args.python_bin, "scrapers/winmart/winmart_full_crawl.py", "--config", args.winmart_config, "--out-dir", args.raw_root.as_posix(), "--run-id", run_id]
            if args.test_limit:
                winmart_command += ["--limit-categories", str(args.test_limit)]
            winmart_result = run_command(winmart_command, dry_run=False)
            if winmart_result is not None and winmart_result.returncode != 0 and not args.continue_on_error:
                return 1
        retailers_to_process.append("winmart")

    # Complete Bronze/Silver for all sources before deriving cross-retailer identity.
    failures = 0
    failure_details: list[str] = []
    successful_runs: list[tuple[str, Path, Path]] = []
    for retailer_id in retailers_to_process:
        retailer_root = args.raw_root / f"store={retailer_id}"
        candidates = sorted(
            path
            for path in retailer_root.glob("date=*/run_id=*/metadata.json")
            if path.parent.name == f"run_id={run_id}"
            and (not args.run_date or path.parent.parent.name == f"date={args.run_date}")
        )
        if not candidates:
            print(f"No metadata found for {retailer_id}", file=sys.stderr)
            failures += 1
            if not args.continue_on_error:
                break
            continue
        metadata_path = candidates[-1]
        metadata = read_json(metadata_path)
        if metadata.get("status") != "success":
            print(f"Skipping {retailer_id}: crawler status={metadata.get('status')}", file=sys.stderr)
            failures += 1
            if not args.continue_on_error:
                break
            continue

        run_dir = metadata_path.parent
        bronze_command = [
            args.python_bin,
            "jobs/bronze/ingest_raw.py",
            "--run-dir",
            run_dir.as_posix(),
            "--out-dir",
            args.warehouse_root.as_posix(),
        ]
        bronze_result = run_command(bronze_command, dry_run=False)
        if bronze_result is None or bronze_result.returncode != 0:
            failures += 1
            if not args.continue_on_error:
                break
            continue

        date_part = run_dir.parent.name
        run_id_part = run_dir.name
        silver_dir = args.warehouse_root / "silver" / f"store={retailer_id}" / date_part / run_id_part
        silver_command = [
            args.python_bin,
            "jobs/silver/normalize_products.py",
            "--bronze-file",
            (args.warehouse_root / "bronze" / "raw_records" / f"store={retailer_id}" / date_part / run_id_part / "bronze_raw_records.jsonl").as_posix(),
            "--out-dir",
            args.warehouse_root.as_posix(),
            "--product-dataset",
            "auto",
        ]
        silver_result = run_command(silver_command, dry_run=False)
        if silver_result is None or silver_result.returncode != 0:
            failures += 1
            if not args.continue_on_error:
                break
            continue
        commercial_command = [
            args.python_bin,
            "jobs/silver/build_commercial_entities.py",
            "--bronze-file",
            (args.warehouse_root / "bronze" / "raw_records" / f"store={retailer_id}" / date_part / run_id_part / "bronze_raw_records.jsonl").as_posix(),
            "--products-file",
            (silver_dir / "retailer_products.jsonl").as_posix(),
            "--observations-file",
            (silver_dir / "product_observations.jsonl").as_posix(),
            "--out-dir",
            args.warehouse_root.as_posix(),
        ]
        commercial_result = run_command(commercial_command, dry_run=False)
        if commercial_result is None or commercial_result.returncode != 0:
            failures += 1
            if not args.continue_on_error:
                break
            continue
        commercial_dir = args.warehouse_root / "silver" / "commercial_entities" / f"store={retailer_id}" / date_part / run_id_part
        promotion_gold_command = [
            args.python_bin,
            "jobs/gold/build_promotion_gold.py",
            "--promotions-file",
            (commercial_dir / "promotions.jsonl").as_posix(),
            "--promotion-items-file",
            (commercial_dir / "promotion_items.jsonl").as_posix(),
            "--out-dir",
            args.warehouse_root.as_posix(),
        ]
        promotion_gold_result = run_command(promotion_gold_command, dry_run=False)
        if promotion_gold_result is None or promotion_gold_result.returncode != 0:
            failures += 1
            if not args.continue_on_error:
                break
            continue
        if args.spark_output_format == "hudi":
            promotion_gold_dir = args.warehouse_root / "gold"
            hudi_entities = (
                (
                    promotion_gold_dir / "dim_promotion" / f"store={retailer_id}" / date_part / run_id_part / "dim_promotion.jsonl",
                    args.spark_root / "gold" / "dim_promotion_hudi" / f"store={retailer_id}",
                    "dim_promotion", "promotion_key", "observation_date",
                ),
                (
                    promotion_gold_dir / "fact_promotion_item" / f"store={retailer_id}" / date_part / run_id_part / "fact_promotion_item.jsonl",
                    args.spark_root / "gold" / "fact_promotion_item_hudi" / f"store={retailer_id}",
                    "fact_promotion_item", "promotion_item_fact_id", "observation_date",
                ),
            )
            hudi_failed = False
            hudi_failed_table: str | None = None
            hudi_exit_code: int | None = None
            for input_file, table_path, table_name, record_key, partition_field in hudi_entities:
                hudi_result = run_command(hudi_json_command(input_file, table_path, table_name, record_key, partition_field), dry_run=False)
                if hudi_result is None or hudi_result.returncode != 0:
                    hudi_failed = True
                    hudi_failed_table = table_name
                    hudi_exit_code = hudi_result.returncode if hudi_result is not None else None
                    break
            if hudi_failed:
                failures += 1
                failure_details.append(f"retailer={retailer_id}: commercial_hudi_write table={hudi_failed_table} exit={hudi_exit_code}")
                print(f"Commercial Hudi write failed: retailer={retailer_id} table={hudi_failed_table} exit={hudi_exit_code}", file=sys.stderr)
                if not args.continue_on_error:
                    break
                continue
        successful_runs.append((retailer_id, run_dir, silver_dir))

    if not successful_runs:
        fallback_date = args.run_date or "unknown"
        write_pipeline_manifest(args.warehouse_root, fallback_date, run_id, {
            "status": "failed",
            "run_date": fallback_date,
            "run_id": run_id,
            "retailers_completed": [],
            "failures": failures,
            "failure_details": failure_details or ["No retailer completed Bronze/Silver/commercial Hudi stages."],
            "spark_output_format": args.spark_output_format,
            "hudi_dimensions_materialized": False,
            "hudi_facts_materialized": False,
        })
        print("No successful Silver outputs available to build product identity mapping.", file=sys.stderr)
        return 1

    mapping_date_part = successful_runs[0][1].parent.name
    mapping_run_id_part = successful_runs[0][1].name
    if any(run_dir.parent.name != mapping_date_part or run_dir.name != mapping_run_id_part for _, run_dir, _ in successful_runs):
        print("Selected retailers do not share one date/run_id; cannot build one deterministic global mapping.", file=sys.stderr)
        return 1

    # Identity and dimensions are global. Reuse every available Silver output for this exact run,
    # so a one-retailer rerun cannot overwrite global tables with a partial retailer set.
    fact_runs = list(successful_runs)
    global_runs: list[tuple[str, Path, Path]] = []
    for retailer_id in (*DEFAULT_RETAILERS, "winmart"):
        run_dir = args.raw_root / f"store={retailer_id}" / mapping_date_part / mapping_run_id_part
        silver_dir = args.warehouse_root / "silver" / f"store={retailer_id}" / mapping_date_part / mapping_run_id_part
        if (silver_dir / "retailer_products.jsonl").exists() and (silver_dir / "product_observations.jsonl").exists():
            global_runs.append((retailer_id, run_dir, silver_dir))
    if not global_runs:
        print("No Silver outputs available for global product identity mapping.", file=sys.stderr)
        return 1
    mapping_file = args.warehouse_root / "silver" / "product_identity_mapping" / mapping_date_part / mapping_run_id_part / "product_identity_mapping.jsonl"
    mapping_command = [
        args.python_bin,
        "jobs/silver/build_product_identity_mapping.py",
        "--products-files",
        *[(silver_dir / "retailer_products.jsonl").as_posix() for _, _, silver_dir in global_runs],
        "--out-dir",
        args.warehouse_root.as_posix(),
        "--run-date",
        mapping_date_part.split("=", 1)[1],
        "--run-id",
        mapping_run_id_part.split("=", 1)[1],
    ]
    mapping_result = run_command(mapping_command, dry_run=False)
    if mapping_result is None or mapping_result.returncode != 0:
        return 1

    # Taxonomy only applies reviewed category rules; unmapped categories remain reviewable.
    taxonomy_command = [
        args.python_bin,
        "jobs/silver/build_category_taxonomy.py",
        "--products-files",
        *[(silver_dir / "retailer_products.jsonl").as_posix() for _, _, silver_dir in global_runs],
        "--out-dir",
        args.warehouse_root.as_posix(),
        "--run-date",
        mapping_date_part.split("=", 1)[1],
        "--run-id",
        mapping_run_id_part.split("=", 1)[1],
    ]
    taxonomy_result = run_command(taxonomy_command, dry_run=False)
    if taxonomy_result is None or taxonomy_result.returncode != 0:
        return 1

    # Publish dimensions from the same complete Silver set before facts reference their keys.
    dimension_command = [
        args.python_bin,
        "jobs/gold/build_dimensions.py",
        "--products-files",
        *[(silver_dir / "retailer_products.jsonl").as_posix() for _, _, silver_dir in global_runs],
        "--observations-files",
        *[(silver_dir / "product_observations.jsonl").as_posix() for _, _, silver_dir in global_runs],
        "--mapping-file",
        mapping_file.as_posix(),
        "--out-dir",
        args.warehouse_root.as_posix(),
    ]
    dimension_result = run_command(dimension_command, dry_run=False)
    if dimension_result is None or dimension_result.returncode != 0:
        return 1

    if args.spark_output_format == "hudi":
        gold_root = args.warehouse_root / "gold"
        dimension_hudi_entities = (
            (gold_root / "dim_retailer" / mapping_date_part / mapping_run_id_part / "dim_retailer.jsonl", args.spark_root / "gold" / "dim_retailer_hudi", "dim_retailer", "retailer_key", None, "built_at"),
            (gold_root / "dim_date" / mapping_date_part / mapping_run_id_part / "dim_date.jsonl", args.spark_root / "gold" / "dim_date_hudi", "dim_date", "date_key", "year", "date_key"),
            (gold_root / "dim_store" / mapping_date_part / mapping_run_id_part / "dim_store.jsonl", args.spark_root / "gold" / "dim_store_hudi", "dim_store", "store_key", "retailer_id", "built_at"),
            (gold_root / "dim_retailer_product" / mapping_date_part / mapping_run_id_part / "dim_retailer_product.jsonl", args.spark_root / "gold" / "dim_retailer_product_hudi", "dim_retailer_product", "retailer_product_key", "retailer_id", "built_at"),
            (gold_root / "dim_product" / mapping_date_part / mapping_run_id_part / "dim_product.jsonl", args.spark_root / "gold" / "dim_product_hudi", "dim_product", "product_key", None, "built_at"),
        )
        for input_file, table_path, table_name, record_key, partition_field, precombine_field in dimension_hudi_entities:
            if input_file.stat().st_size == 0:
                print(f"Skipping empty Hudi dimension {table_name}")
                continue
            result = run_command(
                hudi_json_command(input_file, table_path, table_name, record_key, partition_field, precombine_field),
                dry_run=False,
            )
            if result is None or result.returncode != 0:
                print(f"Failed to materialize Hudi dimension {table_name}", file=sys.stderr)
                return 1

    # Gold facts only start after mapping and every referenced dimension are published.
    fact_completed_retailers: list[str] = []
    for retailer_id, run_dir, _ in fact_runs:
        retailer_root = args.raw_root / f"store={retailer_id}"
        pipeline_command = [
            args.python_bin,
            "jobs/run_spark_main_pipeline.py",
            "--raw-store-dir",
            retailer_root.as_posix(),
            "--out-dir",
            args.warehouse_root.as_posix(),
            "--spark-out-dir",
            args.spark_root.as_posix(),
            "--state-file",
            (ROOT / ".state" / f"spark_main_pipeline_{retailer_id}.json").as_posix(),
            "--python-bin",
            args.python_bin,
            "--spark-mode",
            args.spark_mode,
            "--spark-output-format",
            args.spark_output_format,
            "--mapping-file",
            mapping_file.as_posix(),
            "--skip-bronze",
            "--skip-silver",
        ]
        # This stage must run even when the same raw run previously produced an old Gold schema.
        pipeline_command.append("--force")
        result = run_command(pipeline_command, dry_run=False)
        if result is None or result.returncode != 0:
            failures += 1
            failure_details.append(f"retailer={retailer_id}: spark_gold_pipeline")
            if not args.continue_on_error:
                break
            continue
        fact_completed_retailers.append(retailer_id)
        print(f"Completed retailer={retailer_id} run_dir={run_dir}")
    completed_retailers = fact_completed_retailers
    missing_retailers = sorted(set(retailers_to_process) - set(completed_retailers))
    failed_retailers = {detail.split(":", 1)[0].removeprefix("retailer=") for detail in failure_details}
    unreported_missing_retailers = [retailer_id for retailer_id in missing_retailers if retailer_id not in failed_retailers]
    if unreported_missing_retailers:
        failures += len(unreported_missing_retailers)
        failure_details.extend(
            f"retailer={retailer_id}: did_not_complete_full_pipeline" for retailer_id in unreported_missing_retailers
        )

    status = "success" if failures == 0 else "completed_with_failures"
    run_date = mapping_date_part.split("=", 1)[1]
    source_run_id = mapping_run_id_part.split("=", 1)[1]
    manifest = {
        "status": status,
        "run_date": run_date,
        "run_id": source_run_id,
        "retailers_requested": retailers_to_process,
        "retailers_completed": completed_retailers,
        "retailers_missing": missing_retailers,
        "global_dimension_retailers": [retailer_id for retailer_id, _, _ in global_runs],
        "failures": failures,
        "failure_details": failure_details,
        "spark_output_format": args.spark_output_format,
        "mapping_file": mapping_file.as_posix(),
        "taxonomy_mapping_file": (args.warehouse_root / "silver" / "category_taxonomy" / mapping_date_part / mapping_run_id_part / "category_mapping.jsonl").as_posix(),
        "gold_dimension_manifest": (args.warehouse_root / "gold" / "dimension_build" / mapping_date_part / mapping_run_id_part / "manifest.json").as_posix(),
        "hudi_dimensions_materialized": args.spark_output_format == "hudi",
        "hudi_facts_materialized": args.spark_output_format == "hudi",
    }
    manifest_path = write_pipeline_manifest(args.warehouse_root, run_date, source_run_id, manifest)
    print(json.dumps({"pipeline_manifest": manifest_path.as_posix(), **manifest}, ensure_ascii=False, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

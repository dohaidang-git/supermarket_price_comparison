#!/usr/bin/env python3
"""Materialize Python Silver JSONL outputs as Spark Parquet datasets."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def read_json(spark: SparkSession, path: Path):
    return spark.read.json(path.resolve().as_posix())


def build_silver_parquet(
    spark: SparkSession,
    products_file: Path,
    observations_file: Path,
    out_dir: Path,
    built_at: str | None = None,
) -> dict[str, Any]:
    products = read_json(spark, products_file)
    observations = read_json(spark, observations_file)
    first = observations.select("retailer_id", "observation_date", "source_run_id").first()
    if not first:
        raise ValueError("Silver observations input is empty")

    retailer_id = str(first["retailer_id"])
    snapshot_date = str(first["observation_date"])
    source_run_id = str(first["source_run_id"])
    base = out_dir / "silver_parquet" / f"store={retailer_id}" / f"date={snapshot_date}" / f"run_id={source_run_id}"
    products_path = base / "retailer_products"
    observations_path = base / "product_observations"
    products.write.mode("overwrite").parquet(products_path.resolve().as_posix())
    observations.write.mode("overwrite").parquet(observations_path.resolve().as_posix())

    manifest = {
        "status": "success",
        "engine": "spark",
        "output_format": "parquet",
        "source_products_file": products_file.as_posix(),
        "source_observations_file": observations_file.as_posix(),
        "retailer_id": retailer_id,
        "snapshot_date": snapshot_date,
        "source_run_id": source_run_id,
        "products_read": products.count(),
        "observations_read": observations.count(),
        "products_parquet": products_path.as_posix(),
        "observations_parquet": observations_path.as_posix(),
        "built_at": built_at or utc_now_iso(),
    }
    base.mkdir(parents=True, exist_ok=True)
    with (base / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write Python Silver JSONL as Spark Parquet.")
    parser.add_argument("--products-file", required=True)
    parser.add_argument("--observations-file", required=True)
    parser.add_argument("--out-dir", default="warehouse_spark_docker")
    parser.add_argument("--built-at")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = SparkSession.builder.appName("supermarket_silver_parquet").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    try:
        print(json.dumps(build_silver_parquet(spark, Path(args.products_file), Path(args.observations_file), Path(args.out_dir), args.built_at), ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

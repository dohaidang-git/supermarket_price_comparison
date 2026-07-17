#!/usr/bin/env python3
"""Build Gold daily price snapshot with Spark and write Parquet or Hudi output."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


CONTRACT_VERSION = 1
CRITICAL_COMPARE_FIELDS = (
    "listed_price",
    "promo_price",
    "current_price",
    "effective_unit_price",
    "comparison_unit",
    "unit_price_publishable",
    "package_total_base_quantity",
    "discount_amount",
    "discount_percent",
    "currency",
    "is_on_promotion",
    "is_price_discount",
)
NUMERIC_COMPARE_FIELDS = {
    "listed_price",
    "promo_price",
    "current_price",
    "effective_unit_price",
    "package_total_base_quantity",
    "discount_amount",
    "discount_percent",
}
BOOLEAN_COMPARE_FIELDS = {"unit_price_publishable", "is_on_promotion", "is_price_discount"}
FIELD_COMPARE_TOLERANCE = 0.0001
HUDI_TABLE_NAME = "fact_price_snapshot_daily"
HUDI_RECORD_KEY_FIELD = "price_snapshot_id"
HUDI_PRECOMBINE_FIELD = "built_at"
HUDI_PARTITION_FIELD = "snapshot_date"
HUDI_TABLE_TYPE = "COPY_ON_WRITE"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def output_paths(out_dir: Path, retailer_id: str, snapshot_date: str, run_id: str) -> dict[str, Path]:
    base = out_dir / "gold" / "fact_price_snapshot_daily" / f"store={retailer_id}" / f"date={snapshot_date}" / f"run_id={run_id}"
    return {
        "base": base,
        "snapshot_parquet": base / "price_snapshot_daily.parquet",
        # A Hudi table is intentionally not partitioned by run_id. Runs are lineage;
        # snapshot_date is the physical Hudi partition and preserves analysis history.
        "snapshot_hudi": out_dir / "gold" / "fact_price_snapshot_daily_hudi" / f"store={retailer_id}",
        "manifest": base / "manifest.json",
    }


def absolute_output_path(path: Path) -> str:
    return path.resolve().as_posix()


def read_silver_input(spark: SparkSession, path: Path) -> DataFrame:
    """Read either the current JSONL reference or Spark Silver Parquet."""
    if path.suffix.lower() == ".jsonl":
        return spark.read.json(absolute_output_path(path))
    return spark.read.parquet(absolute_output_path(path))


def stable_dimension_key(prefix: str, *columns: Any) -> Any:
    """Create the same deterministic surrogate-key format as Gold dimensions."""
    identity = F.concat_ws(
        "|",
        *[F.coalesce(column.cast("string"), F.lit("")) for column in columns],
    )
    return F.concat(F.lit(f"{prefix}_"), F.substring(F.sha2(identity, 256), 1, 24))


def latest_observations(observations: DataFrame) -> DataFrame:
    grain = ["observation_date", "retailer_id", "store_code", "retailer_product_id"]
    window = Window.partitionBy(*grain).orderBy(
        F.col("observed_at").desc_nulls_last(),
        F.col("normalized_at").desc_nulls_last(),
        F.col("observation_id").desc_nulls_last(),
    )
    return observations.withColumn("_row_number", F.row_number().over(window)).filter(F.col("_row_number") == 1).drop("_row_number")


def selection_stats(observations: DataFrame) -> dict[str, int]:
    grain = ["observation_date", "retailer_id", "store_code", "retailer_product_id"]
    duplicate_groups = observations.groupBy(*grain).count().filter(F.col("count") > 1)
    duplicate_group_count = duplicate_groups.count()
    observations_dropped = duplicate_groups.select(F.sum(F.col("count") - F.lit(1)).alias("dropped")).first()["dropped"]
    return {
        "duplicate_observation_groups": int(duplicate_group_count),
        "observations_dropped": int(observations_dropped or 0),
    }


def build_snapshot(
    products: DataFrame,
    observations: DataFrame,
    mappings: DataFrame | None,
    built_at: str,
    run_status: str,
) -> DataFrame:
    selected = latest_observations(observations).filter(F.col("data_quality_status") != F.lit("quarantined"))
    joined = selected.alias("o").join(products.alias("p"), on="retailer_product_id", how="inner")
    if mappings is not None:
        approved_mappings = (
            mappings.filter(F.col("review_status") == F.lit("matched"))
            .select(
                F.col("retailer_id").alias("mapping_retailer_id"),
                F.col("retailer_product_id").alias("mapping_retailer_product_id"),
                F.col("canonical_product_id"),
            )
            .dropDuplicates(["mapping_retailer_id", "mapping_retailer_product_id"])
            .alias("m")
        )
        joined = joined.join(
            approved_mappings,
            (F.col("o.retailer_id") == F.col("m.mapping_retailer_id"))
            & (F.col("retailer_product_id") == F.col("m.mapping_retailer_product_id")),
            how="left",
        )
        canonical_product_id = F.col("m.canonical_product_id")
    else:
        canonical_product_id = F.lit(None).cast("string")

    current_price = F.col("o.current_price").cast("double")
    denominator = F.col("p.package_total_base_quantity").cast("double")
    comparison_unit = F.col("p.measurement_base_unit")
    unit_price_publishable = F.coalesce(F.col("p.unit_price_publishable").cast("boolean"), F.lit(False))
    effective_unit_price = F.when(
        unit_price_publishable & denominator.isNotNull() & (denominator > 0) & current_price.isNotNull() & (current_price > 0) & comparison_unit.isNotNull(),
        F.round(current_price / denominator, 6),
    )

    snapshot_date = F.col("o.observation_date")
    store_code = F.coalesce(F.col("o.store_code"), F.lit("unknown_store"))
    retailer_key = stable_dimension_key("ret", F.col("o.retailer_id"))
    store_key = stable_dimension_key("store", F.col("o.retailer_id"), store_code)
    retailer_product_key = stable_dimension_key("retprod", F.col("o.retailer_id"), F.col("retailer_product_id"))
    product_key = F.when(canonical_product_id.isNotNull(), stable_dimension_key("prod", canonical_product_id))
    price_snapshot_id = F.sha2(
        F.concat(
            snapshot_date,
            F.lit("|"),
            F.col("o.retailer_id"),
            F.lit("|"),
            store_code,
            F.lit("|"),
            F.col("retailer_product_id"),
        ),
        256,
    )

    return joined.select(
        price_snapshot_id.alias("price_snapshot_id"),
        F.lit(CONTRACT_VERSION).alias("contract_version"),
        F.regexp_replace(snapshot_date, "-", "").cast("int").alias("date_key"),
        snapshot_date.alias("snapshot_date"),
        retailer_key.alias("retailer_key"),
        store_key.alias("store_key"),
        retailer_product_key.alias("retailer_product_key"),
        product_key.alias("product_key"),
        canonical_product_id.alias("canonical_product_id"),
        F.col("o.retailer_id").alias("retailer_id"),
        store_code.alias("store_code"),
        F.col("o.store_group_code").alias("store_group_code"),
        F.col("o.region").alias("region"),
        F.col("retailer_product_id"),
        F.col("o.observation_id").alias("observation_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.brand").alias("brand"),
        F.col("p.category_raw").alias("category_raw"),
        F.col("o.listed_price").alias("listed_price"),
        F.col("o.promo_price").alias("promo_price"),
        F.col("o.current_price").alias("current_price"),
        F.coalesce(F.col("o.currency"), F.lit("VND")).alias("currency"),
        effective_unit_price.alias("effective_unit_price"),
        F.when(effective_unit_price.isNotNull(), comparison_unit).alias("comparison_unit"),
        F.col("p.measurement_type").alias("measurement_type"),
        unit_price_publishable.alias("unit_price_publishable"),
        F.col("p.package_total_base_quantity").alias("package_total_base_quantity"),
        F.col("o.discount_amount").alias("discount_amount"),
        F.col("o.discount_percent").alias("discount_percent"),
        F.col("o.availability_status").alias("availability_status"),
        F.coalesce(F.col("o.is_on_promotion").cast("boolean"), F.lit(False)).alias("is_on_promotion"),
        F.coalesce(F.col("o.is_price_discount").cast("boolean"), F.lit(False)).alias("is_price_discount"),
        F.coalesce(F.col("o.has_promo_mechanic").cast("boolean"), F.lit(False)).alias("has_promo_mechanic"),
        F.col("o.observed_at").alias("observed_at"),
        F.lit(run_status).alias("run_status"),
        F.col("o.source_run_id").alias("source_run_id"),
        F.col("o.source_bronze_record_key").alias("source_bronze_record_key"),
        F.col("o.source_file").alias("source_file"),
        F.col("o.source_line_number").alias("source_line_number"),
        F.col("o.data_quality_status").alias("silver_data_quality_status"),
        F.lit(built_at).alias("built_at"),
    )


def field_mismatch_condition(field: str) -> Any:
    expected_col = F.col(f"expected.{field}")
    actual_col = F.col(f"spark.{field}")

    both_null = expected_col.isNull() & actual_col.isNull()
    one_null = expected_col.isNull() != actual_col.isNull()
    if field in NUMERIC_COMPARE_FIELDS:
        return one_null | (~both_null & (F.abs(expected_col.cast("double") - actual_col.cast("double")) > F.lit(FIELD_COMPARE_TOLERANCE)))
    if field in BOOLEAN_COMPARE_FIELDS:
        return one_null | (~both_null & (expected_col.cast("boolean") != actual_col.cast("boolean")))
    return one_null | (~both_null & (expected_col.cast("string") != actual_col.cast("string")))


def compare_with_python_gold(spark: SparkSession, rows: DataFrame, compare_gold_file: str | None) -> dict[str, Any]:
    if not compare_gold_file:
        return {}

    compare_columns = ["price_snapshot_id", *CRITICAL_COMPARE_FIELDS]
    expected = spark.read.json(compare_gold_file).select(*compare_columns).alias("expected")
    actual = rows.select(*compare_columns).alias("spark")
    expected_ids = expected.select("price_snapshot_id").distinct()
    actual_ids = actual.select("price_snapshot_id").distinct()
    missing_in_spark = expected_ids.join(actual_ids, on="price_snapshot_id", how="left_anti").count()
    extra_in_spark = actual_ids.join(expected_ids, on="price_snapshot_id", how="left_anti").count()
    expected_count = expected_ids.count()
    actual_count = actual_ids.count()

    comparable = expected.join(actual, on="price_snapshot_id", how="inner")
    field_mismatch_counts: dict[str, int] = {}
    field_mismatch_samples: dict[str, list[dict[str, Any]]] = {}
    for field in CRITICAL_COMPARE_FIELDS:
        mismatches = comparable.filter(field_mismatch_condition(field))
        mismatch_count = mismatches.count()
        field_mismatch_counts[field] = int(mismatch_count)
        if mismatch_count:
            field_mismatch_samples[field] = [
                row.asDict()
                for row in mismatches.select(
                    "price_snapshot_id",
                    F.col(f"expected.{field}").alias("expected"),
                    F.col(f"spark.{field}").alias("spark"),
                )
                .limit(10)
                .collect()
            ]

    critical_field_mismatches = sum(field_mismatch_counts.values())
    snapshot_id_sets_match = missing_in_spark == 0 and extra_in_spark == 0
    critical_fields_match = critical_field_mismatches == 0
    return {
        "compare_gold_file": compare_gold_file,
        "expected_distinct_snapshot_ids": int(expected_count),
        "spark_distinct_snapshot_ids": int(actual_count),
        "missing_in_spark": int(missing_in_spark),
        "extra_in_spark": int(extra_in_spark),
        "snapshot_id_sets_match": snapshot_id_sets_match,
        "critical_compare_fields": list(CRITICAL_COMPARE_FIELDS),
        "field_compare_tolerance": FIELD_COMPARE_TOLERANCE,
        "field_mismatch_counts": field_mismatch_counts,
        "field_mismatch_samples": field_mismatch_samples,
        "critical_field_mismatches": int(critical_field_mismatches),
        "critical_fields_match": critical_fields_match,
        "replacement_ready": snapshot_id_sets_match and critical_fields_match,
    }


def hudi_write_options(table_name: str = HUDI_TABLE_NAME) -> dict[str, str]:
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": HUDI_RECORD_KEY_FIELD,
        "hoodie.datasource.write.precombine.field": HUDI_PRECOMBINE_FIELD,
        "hoodie.datasource.write.partitionpath.field": HUDI_PARTITION_FIELD,
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.table.type": HUDI_TABLE_TYPE,
        "hoodie.datasource.write.reconcile.schema": "true",
        # Keep the historical price table independent from stale local Hudi
        # metadata-table schemas. The Hudi base table and timeline remain the
        # source used by the logical validation job.
        "hoodie.metadata.enable": "false",
    }


def write_output(rows: DataFrame, *, output_format: str, paths: dict[str, Path], table_name: str = HUDI_TABLE_NAME) -> dict[str, str]:
    if output_format == "parquet":
        rows.write.mode("overwrite").parquet(absolute_output_path(paths["snapshot_parquet"]))
        return {
            "output_format": "parquet",
            "price_snapshot_parquet": paths["snapshot_parquet"].as_posix(),
        }

    if output_format == "hudi":
        rows.write.format("hudi").options(**hudi_write_options(table_name)).mode("append").save(
            absolute_output_path(paths["snapshot_hudi"])
        )
        return {
            "output_format": "hudi",
            "price_snapshot_hudi": paths["snapshot_hudi"].as_posix(),
        }

    raise ValueError(f"Unsupported output_format: {output_format}")


def build_gold_spark(
    *,
    spark: SparkSession,
    products_file: Path,
    observations_file: Path,
    mapping_file: Path | None,
    out_dir: Path,
    built_at: str | None = None,
    run_status: str = "success",
    coalesce: int | None = None,
    compare_gold_file: str | None = None,
    output_format: str = "parquet",
) -> dict[str, Any]:
    build_time = built_at or utc_now_iso()
    products = read_silver_input(spark, products_file)
    observations = read_silver_input(spark, observations_file)
    mappings = read_silver_input(spark, mapping_file) if mapping_file else None
    stats = selection_stats(observations)
    rows = build_snapshot(products, observations, mappings, build_time, run_status)

    if coalesce and coalesce > 0:
        rows = rows.coalesce(coalesce)

    first = rows.select("retailer_id", "snapshot_date", "source_run_id").first()
    if first:
        retailer_id = first["retailer_id"]
        snapshot_date = first["snapshot_date"]
        source_run_id = first["source_run_id"]
    else:
        retailer_id = "unknown"
        snapshot_date = "unknown"
        source_run_id = "unknown"

    paths = output_paths(out_dir, str(retailer_id), str(snapshot_date), str(source_run_id))
    paths["base"].mkdir(parents=True, exist_ok=True)
    output_details = write_output(rows, output_format=output_format, paths=paths)
    snapshots_written = rows.count()
    comparison = compare_with_python_gold(spark, rows, compare_gold_file)

    manifest = {
        "status": "success",
        "engine": "spark",
        "output_format": output_format,
        "contract_version": CONTRACT_VERSION,
        "source_products_file": products_file.as_posix(),
        "source_observations_file": observations_file.as_posix(),
        "source_mapping_file": mapping_file.as_posix() if mapping_file else None,
        "retailer_id": retailer_id,
        "snapshot_date": snapshot_date,
        "source_run_id": source_run_id,
        "products_read": int(products.count()),
        "observations_read": int(observations.count()),
        "snapshots_written": int(snapshots_written),
        "selection_stats": stats,
        "comparison": comparison,
        "built_at": build_time,
    }
    manifest.update(output_details)
    if output_format == "hudi":
        manifest["hudi_table"] = {
            "table_name": HUDI_TABLE_NAME,
            "write_mode": "append_upsert_history",
            "record_key_field": HUDI_RECORD_KEY_FIELD,
            "precombine_field": HUDI_PRECOMBINE_FIELD,
            "partition_field": HUDI_PARTITION_FIELD,
            "table_type": HUDI_TABLE_TYPE,
        }
    with paths["manifest"].open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    return manifest


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Gold daily price snapshot with Spark.")
    parser.add_argument("--products-file", required=True, help="Silver retailer_products JSONL.")
    parser.add_argument("--observations-file", required=True, help="Silver product_observations JSONL.")
    parser.add_argument("--mapping-file", help="Optional approved Silver product identity mapping JSONL.")
    parser.add_argument("--out-dir", default="warehouse_spark", help="Spark output root directory.")
    parser.add_argument("--built-at", help="Override build timestamp for deterministic tests.")
    parser.add_argument("--run-status", default="success", help="Run completeness status to write into Gold rows.")
    parser.add_argument("--coalesce", type=int, help="Optional output partition count for local debugging.")
    parser.add_argument("--compare-gold-file", help="Optional existing Python Gold JSONL used to compare snapshot ids.")
    parser.add_argument("--output-format", choices=("parquet", "hudi"), default="parquet", help="Spark Gold output format.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = (
        SparkSession.builder.appName("supermarket_gold_price_snapshot")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        manifest = build_gold_spark(
            spark=spark,
            products_file=Path(args.products_file),
            observations_file=Path(args.observations_file),
            mapping_file=Path(args.mapping_file) if args.mapping_file else None,
            out_dir=Path(args.out_dir),
            built_at=args.built_at,
            run_status=args.run_status,
            coalesce=args.coalesce,
            compare_gold_file=args.compare_gold_file,
            output_format=args.output_format,
        )
        print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

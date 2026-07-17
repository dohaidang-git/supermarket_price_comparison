#!/usr/bin/env python3
"""Write a Gold JSONL dataset to a persistent Hudi table using append-upsert."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write Gold JSONL to a Hudi history table.")
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--table-path", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--record-key", required=True)
    parser.add_argument("--partition-field", help="Business partition column; omit for a non-partitioned dimension.")
    parser.add_argument("--precombine-field", default="built_at")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    spark = SparkSession.builder.appName(f"supermarket_{args.table_name}_hudi").getOrCreate()
    try:
        rows = spark.read.json(str(Path(args.input_file).resolve()))
        if args.record_key not in rows.columns:
            raise ValueError("Input is missing Hudi record key")
        options = {
            "hoodie.table.name": args.table_name,
            "hoodie.datasource.write.table.name": args.table_name,
            "hoodie.datasource.write.recordkey.field": args.record_key,
            "hoodie.datasource.write.precombine.field": args.precombine_field,
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.reconcile.schema": "true",
            # Existing local tables may carry metadata-table schemas from an
            # earlier Hudi bundle. The metadata index is optional for these
            # small dimension tables; disabling it avoids incompatible Avro
            # metadata upserts while preserving the primary Hudi table data.
            "hoodie.metadata.enable": "false",
        }
        if args.partition_field:
            if args.partition_field not in rows.columns:
                raise ValueError("Input is missing Hudi partition field")
            options.update({
                "hoodie.datasource.write.partitionpath.field": args.partition_field,
                "hoodie.datasource.write.hive_style_partitioning": "true",
                "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
            })
        else:
            options["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
        rows.write.format("hudi").options(**options).mode("append").save(str(Path(args.table_path).resolve()))
        print(json.dumps({"status": "success", "table_name": args.table_name, "table_path": args.table_path, "rows_written": rows.count()}, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())

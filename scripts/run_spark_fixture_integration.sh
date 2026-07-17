#!/usr/bin/env bash
# Run a sanitized one-row Spark -> Hudi -> Hudi-reader integration test.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_ROOT="${OUTPUT_ROOT:-warehouse_spark_fixture}"
SNAPSHOT_DATE="2026-01-01"
TABLE_PATH="$OUTPUT_ROOT/gold/fact_price_snapshot_daily_hudi/store=fixturemart"
HUDI_ARGS=(
  --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar
)

cd "$ROOT"
docker compose -f infra/spark/docker-compose.yml run --rm spark-gold "${HUDI_ARGS[@]}" \
  jobs/spark/build_gold_price_snapshot_spark.py \
  --products-file tests/fixtures/silver/retailer_products.jsonl \
  --observations-file tests/fixtures/silver/product_observations.jsonl \
  --out-dir "$OUTPUT_ROOT" \
  --built-at 2026-01-01T00:00:00+00:00 \
  --coalesce 1 \
  --output-format hudi

docker compose -f infra/spark/docker-compose.yml run --rm spark-gold "${HUDI_ARGS[@]}" \
  jobs/spark/validate_hudi_history_spark.py \
  --table-path "$TABLE_PATH" \
  --snapshot-date "$SNAPSHOT_DATE"

echo "Spark/Hudi fixture integration passed: $TABLE_PATH"

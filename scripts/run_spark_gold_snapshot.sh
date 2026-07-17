#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="${1:-raw/store=winmart/date=2026-07-02/run_id=20260702_155006}"
OUT_DIR="${OUT_DIR:-warehouse_spark}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-25-openjdk}"
if [[ ! -x "${JAVA_HOME}/bin/java" && -x "/usr/lib/jvm/java-25-openjdk/bin/java" ]]; then
  JAVA_HOME="/usr/lib/jvm/java-25-openjdk"
fi
BUILT_AT="${BUILT_AT:-}"
COALESCE="${COALESCE:-1}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-parquet}"
HUDI_SPARK_VERSION="${HUDI_SPARK_VERSION:-3.5}"
HUDI_VERSION="${HUDI_VERSION:-1.2.0}"
HUDI_SCALA_VERSION="${HUDI_SCALA_VERSION:-2.12}"

run_id_part="$(basename "${RUN_DIR}")"
date_part="$(basename "$(dirname "${RUN_DIR}")")"
store_part="$(basename "$(dirname "$(dirname "${RUN_DIR}")")")"

products_file="warehouse/silver/${store_part}/${date_part}/${run_id_part}/retailer_products.jsonl"
observations_file="warehouse/silver/${store_part}/${date_part}/${run_id_part}/product_observations.jsonl"
mapping_file="warehouse/silver/product_identity_mapping/${date_part}/${run_id_part}/product_identity_mapping.jsonl"
default_compare_gold_file="warehouse/gold/fact_price_snapshot_daily/${store_part}/${date_part}/${run_id_part}/price_snapshot_daily.jsonl"
compare_gold_file="${COMPARE_GOLD_FILE:-${default_compare_gold_file}}"

command=(
  "${SPARK_SUBMIT_BIN}"
  --master "local[*]"
)

if [[ "${OUTPUT_FORMAT}" == "hudi" ]]; then
  command+=(
    --packages "org.apache.hudi:hudi-spark${HUDI_SPARK_VERSION}-bundle_${HUDI_SCALA_VERSION}:${HUDI_VERSION}"
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
    --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'
    --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
    --conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
  )
fi

command+=(
  jobs/spark/build_gold_price_snapshot_spark.py
  --products-file "${products_file}"
  --observations-file "${observations_file}"
  --out-dir "${OUT_DIR}"
  --coalesce "${COALESCE}"
  --output-format "${OUTPUT_FORMAT}"
)

if [[ -f "${mapping_file}" ]]; then
  command+=(--mapping-file "${mapping_file}")
fi

if [[ -n "${compare_gold_file}" && -f "${compare_gold_file}" ]]; then
  command+=(--compare-gold-file "${compare_gold_file}")
fi

if [[ -n "${BUILT_AT}" ]]; then
  command+=(--built-at "${BUILT_AT}")
fi

export JAVA_HOME
export PYTHONPATH="${PYTHONPATH:-.}"

"${command[@]}"

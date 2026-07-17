#!/usr/bin/env bash
set -euo pipefail

RAW_RUN_DIR="${1:-${RAW_RUN_DIR:-raw/store=winmart/date=2026-06-29/run_id=20260629_135004}}"
OUT_DIR="${OUT_DIR:-warehouse}"
PYTHON_BIN="${PYTHON_BIN:-python}"
PRODUCT_DATASET="${PRODUCT_DATASET:-auto}"
INGESTED_AT="${INGESTED_AT:-}"
NORMALIZED_AT="${NORMALIZED_AT:-}"
BUILT_AT="${BUILT_AT:-}"
RUN_STATUS="${RUN_STATUS:-success}"

if [[ ! -d "${RAW_RUN_DIR}" ]]; then
  echo "Raw run directory does not exist: ${RAW_RUN_DIR}" >&2
  exit 1
fi

run_id_part="$(basename "${RAW_RUN_DIR}")"
date_part="$(basename "$(dirname "${RAW_RUN_DIR}")")"
store_part="$(basename "$(dirname "$(dirname "${RAW_RUN_DIR}")")")"

if [[ "${run_id_part}" != run_id=* || "${date_part}" != date=* || "${store_part}" != store=* ]]; then
  echo "RAW_RUN_DIR must follow raw/store=<store>/date=<date>/run_id=<run_id>: ${RAW_RUN_DIR}" >&2
  exit 1
fi

bronze_file="${OUT_DIR}/bronze/raw_records/${store_part}/${date_part}/${run_id_part}/bronze_raw_records.jsonl"
silver_dir="${OUT_DIR}/silver/${store_part}/${date_part}/${run_id_part}"
silver_products_file="${silver_dir}/retailer_products.jsonl"
silver_observations_file="${silver_dir}/product_observations.jsonl"
gold_dir="${OUT_DIR}/gold/fact_price_snapshot_daily/${store_part}/${date_part}/${run_id_part}"

run_step() {
  local step_name="$1"
  shift
  echo
  echo "==> ${step_name}"
  "$@"
}

bronze_command=(
  "${PYTHON_BIN}" jobs/bronze/ingest_raw.py
  --run-dir "${RAW_RUN_DIR}"
  --out-dir "${OUT_DIR}"
)
if [[ -n "${INGESTED_AT}" ]]; then
  bronze_command+=(--ingested-at "${INGESTED_AT}")
fi

silver_command=(
  "${PYTHON_BIN}" jobs/silver/normalize_products.py
  --bronze-file "${bronze_file}"
  --out-dir "${OUT_DIR}"
  --product-dataset "${PRODUCT_DATASET}"
)
if [[ -n "${NORMALIZED_AT}" ]]; then
  silver_command+=(--normalized-at "${NORMALIZED_AT}")
fi

gold_command=(
  "${PYTHON_BIN}" jobs/gold/build_daily_price_snapshot.py
  --products-file "${silver_products_file}"
  --observations-file "${silver_observations_file}"
  --out-dir "${OUT_DIR}"
  --run-status "${RUN_STATUS}"
)
if [[ -n "${BUILT_AT}" ]]; then
  gold_command+=(--built-at "${BUILT_AT}")
fi

run_step "Bronze ingest" "${bronze_command[@]}"
run_step "Silver normalize products" "${silver_command[@]}"
run_step "Gold daily price snapshot" "${gold_command[@]}"

echo
echo "==> Pipeline output"
echo "Bronze: ${bronze_file}"
echo "Silver products: ${silver_products_file}"
echo "Silver observations: ${silver_observations_file}"
echo "Gold: ${gold_dir}/price_snapshot_daily.jsonl"
echo "Gold validation: ${gold_dir}/validation_report.json"

#!/usr/bin/env bash
# Validate Spark manifests and logical Hudi price snapshots for a completed daily run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"
RUN_DATE=""
RUN_ID=""
RETAILERS=(bachhoaxanh go lottemart mmvietnam winmart)

usage() {
  echo "Usage: $0 --run-date YYYY-MM-DD --run-id YYYYMMDD_HHMMSS [--retailers retailer ...]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date) RUN_DATE="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --retailers)
      shift
      RETAILERS=()
      while [[ $# -gt 0 && "$1" != --* ]]; do
        RETAILERS+=("$1")
        shift
      done
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$RUN_DATE" && -n "$RUN_ID" ]] || { usage; exit 2; }
[[ -x "$PYTHON_BIN" ]] || { echo "Python not found: $PYTHON_BIN" >&2; exit 2; }

PIPELINE_MANIFEST="$ROOT/warehouse/pipeline_runs/date=$RUN_DATE/run_id=$RUN_ID/manifest.json"
"$PYTHON_BIN" - "$PIPELINE_MANIFEST" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(f"Missing pipeline manifest: {path}")
manifest = json.loads(path.read_text(encoding="utf-8"))
expected = {"status": "success", "failures": 0, "hudi_dimensions_materialized": True, "hudi_facts_materialized": True}
invalid = {key: {"expected": value, "actual": manifest.get(key)} for key, value in expected.items() if manifest.get(key) != value}
if invalid:
    raise SystemExit(f"Pipeline manifest did not pass validation gate: {invalid}")
PY

for retailer in "${RETAILERS[@]}"; do
  spark_manifest="$ROOT/warehouse_spark_docker/gold/fact_price_snapshot_daily/store=$retailer/date=$RUN_DATE/run_id=$RUN_ID/manifest.json"
  python_gold_file="$ROOT/warehouse/gold/fact_price_snapshot_daily/store=$retailer/date=$RUN_DATE/run_id=$RUN_ID/price_snapshot_daily.jsonl"
  "$PYTHON_BIN" "$ROOT/jobs/validate_spark_gold_manifest.py" --manifest "$spark_manifest" --allow-no-reference

  hudi_command=(docker compose -f "$ROOT/infra/spark/docker-compose.yml" run --rm spark-gold \
    --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0 \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar \
    jobs/spark/validate_hudi_history_spark.py \
    --table-path "warehouse_spark_docker/gold/fact_price_snapshot_daily_hudi/store=$retailer" \
    --snapshot-date "$RUN_DATE")
  if [[ -f "$python_gold_file" ]]; then
    hudi_command+=(--compare-gold-file "warehouse/gold/fact_price_snapshot_daily/store=$retailer/date=$RUN_DATE/run_id=$RUN_ID/price_snapshot_daily.jsonl")
  else
    echo "No Python Gold reference for retailer=$retailer; running Hudi structural validation only."
  fi
  "${hudi_command[@]}"
done

echo "Validated Spark manifests and Hudi snapshots for run_id=$RUN_ID run_date=$RUN_DATE"

#!/usr/bin/env bash
# Publish validated local Hudi tables and the run manifest to a MinIO bucket.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DATE=""
RUN_ID=""
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9020}"
MINIO_BUCKET="${MINIO_BUCKET:-supermarket-lakehouse}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv/bin/python}"

usage() {
  echo "Usage: $0 --run-date YYYY-MM-DD --run-id YYYYMMDD_HHMMSS [--endpoint URL] [--bucket NAME]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date) RUN_DATE="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --endpoint) MINIO_ENDPOINT="$2"; shift 2 ;;
    --bucket) MINIO_BUCKET="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$RUN_DATE" && -n "$RUN_ID" ]] || { usage; exit 2; }
[[ -x "$PYTHON_BIN" ]] || { echo "Python not found: $PYTHON_BIN" >&2; exit 2; }
[[ -n "$MINIO_SECRET_KEY" ]] || { echo "MINIO_SECRET_KEY must be set in the environment." >&2; exit 2; }

MANIFEST="$ROOT/warehouse/pipeline_runs/date=$RUN_DATE/run_id=$RUN_ID/manifest.json"
[[ -f "$MANIFEST" ]] || { echo "Missing pipeline manifest: $MANIFEST" >&2; exit 1; }

"$PYTHON_BIN" - "$MANIFEST" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
required = {
    "status": "success",
    "failures": 0,
    "hudi_dimensions_materialized": True,
    "hudi_facts_materialized": True,
}
invalid = {key: {"expected": expected, "actual": manifest.get(key)} for key, expected in required.items() if manifest.get(key) != expected}
if invalid:
    raise SystemExit(f"Refusing MinIO publish because pipeline manifest did not pass: {invalid}")
PY

docker run --rm --network host \
  -v "$ROOT:/workspace:ro" \
  -e "MINIO_ENDPOINT=$MINIO_ENDPOINT" \
  -e "MINIO_BUCKET=$MINIO_BUCKET" \
  -e "MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY" \
  -e "MINIO_SECRET_KEY=$MINIO_SECRET_KEY" \
  -e "RUN_DATE=$RUN_DATE" \
  -e "RUN_ID=$RUN_ID" \
  --entrypoint /bin/sh minio/mc:latest -c '
    set -eu
    mc alias set local "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null
    mc mb --ignore-existing "local/$MINIO_BUCKET" >/dev/null
    for table in /workspace/warehouse_spark_docker/gold/*_hudi; do
      [ -d "$table" ] || continue
      mc mirror --overwrite "$table" "local/$MINIO_BUCKET/gold/$(basename "$table")"
    done
    mc cp \
      "/workspace/warehouse/pipeline_runs/date=$RUN_DATE/run_id=$RUN_ID/manifest.json" \
      "local/$MINIO_BUCKET/pipeline_runs/date=$RUN_DATE/run_id=$RUN_ID/manifest.json"
    mc stat "local/$MINIO_BUCKET/gold/dim_product_hudi/.hoodie/hoodie.properties" >/dev/null
    mc stat "local/$MINIO_BUCKET/gold/fact_price_snapshot_daily_hudi/store=winmart/.hoodie/hoodie.properties" >/dev/null
  '

echo "Published validated Hudi tables to s3://$MINIO_BUCKET/gold/"
echo "Published pipeline manifest to s3://$MINIO_BUCKET/pipeline_runs/date=$RUN_DATE/run_id=$RUN_ID/manifest.json"

#!/usr/bin/env bash
# Back up Airflow metadata before a deployment/migration. Lakehouse data is not touched.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_DIR="$ROOT/infra/airflow"
ENV_FILE="$AIRFLOW_DIR/.env"
BACKUP_DIR="$ROOT/backups/airflow"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env-file) ENV_FILE="$2"; shift 2 ;;
    --backup-dir) BACKUP_DIR="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--env-file PATH] [--backup-dir PATH]"
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -f "$ENV_FILE" ]] || { echo "Missing Airflow env file: $ENV_FILE" >&2; exit 1; }
mkdir -p "$BACKUP_DIR"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_file="$BACKUP_DIR/airflow_metadata_${timestamp}.sql.gz"

docker compose --env-file "$ENV_FILE" -f "$AIRFLOW_DIR/docker-compose.yml" \
  exec -T postgres pg_dump -U airflow airflow | gzip > "$backup_file"

test -s "$backup_file" || { echo "Backup is empty: $backup_file" >&2; exit 1; }
echo "Created Airflow metadata backup: $backup_file"

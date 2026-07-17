#!/usr/bin/env bash
# Deploy a reviewed Git revision on the pipeline host without altering lakehouse runtime data.
set -euo pipefail

DEPLOY_ROOT=""
GIT_REF=""
AIRFLOW_IMAGE=""
SPARK_IMAGE=""
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:9020}"

usage() {
  cat <<'EOF'
Usage:
  deploy_pipeline_host.sh --deploy-root PATH --git-ref SHA --airflow-image IMAGE --spark-image IMAGE

Preconditions:
  - PATH is a clean, pre-provisioned Git clone of this repository.
  - PATH/infra/airflow/.env exists and contains real runtime secrets.
  - No daily pipeline DAG run is active.
  - MinIO is healthy at MINIO_ENDPOINT (default http://127.0.0.1:9020).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy-root) DEPLOY_ROOT="$2"; shift 2 ;;
    --git-ref) GIT_REF="$2"; shift 2 ;;
    --airflow-image) AIRFLOW_IMAGE="$2"; shift 2 ;;
    --spark-image) SPARK_IMAGE="$2"; shift 2 ;;
    --minio-endpoint) MINIO_ENDPOINT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$DEPLOY_ROOT" && -n "$GIT_REF" && -n "$AIRFLOW_IMAGE" && -n "$SPARK_IMAGE" ]] || { usage; exit 2; }
[[ -d "$DEPLOY_ROOT/.git" ]] || { echo "Deploy root must be a Git clone: $DEPLOY_ROOT" >&2; exit 1; }

cd "$DEPLOY_ROOT"
[[ -z "$(git status --porcelain)" ]] || { echo "Refusing deploy: deploy root has uncommitted tracked changes." >&2; exit 1; }
git fetch --tags origin
git cat-file -e "${GIT_REF}^{commit}"
git checkout --detach "$GIT_REF"

ENV_FILE="$DEPLOY_ROOT/infra/airflow/.env"
[[ -f "$ENV_FILE" ]] || { echo "Missing runtime env file: $ENV_FILE" >&2; exit 1; }
grep -q '^PROJECT_ROOT=' "$ENV_FILE" || { echo "Missing PROJECT_ROOT in $ENV_FILE" >&2; exit 1; }
grep -q '^AIRFLOW_WEBSERVER_SECRET_KEY=' "$ENV_FILE" || { echo "Missing Airflow secret in $ENV_FILE" >&2; exit 1; }

set_env_value() {
  local key="$1"
  local value="$2"
  if grep -q "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
  fi
}

set_env_value AIRFLOW_IMAGE "$AIRFLOW_IMAGE"
set_env_value SPARK_IMAGE "$SPARK_IMAGE"

docker info >/dev/null
curl --fail --silent --show-error "$MINIO_ENDPOINT/minio/health/live" >/dev/null

bash "$DEPLOY_ROOT/scripts/backup_airflow_metadata.sh" --env-file "$ENV_FILE"
docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/airflow/docker-compose.yml" config --quiet
docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/spark/docker-compose.yml" config --quiet

docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/airflow/docker-compose.yml" pull airflow-webserver airflow-scheduler
docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/spark/docker-compose.yml" pull spark-gold
docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/airflow/docker-compose.yml" run --rm airflow-permissions
docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/airflow/docker-compose.yml" up airflow-init
docker compose --env-file "$ENV_FILE" -f "$DEPLOY_ROOT/infra/airflow/docker-compose.yml" up -d --force-recreate airflow-webserver airflow-scheduler

airflow_port="$(awk -F= '/^AIRFLOW_WEBSERVER_PORT=/{print $2}' "$ENV_FILE" | tail -n 1)"
airflow_port="${airflow_port:-8088}"
curl --fail --silent --show-error "http://127.0.0.1:${airflow_port}/health" >/dev/null

echo "Deployment succeeded for Git revision: $(git rev-parse HEAD)"

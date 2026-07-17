#!/usr/bin/env bash
# Create local Airflow settings with paths valid for both the host and Docker daemon.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AIRFLOW_DIR="$ROOT/infra/airflow"
ENV_FILE="$AIRFLOW_DIR/.env"

docker_socket="/var/run/docker.sock"
if [[ ! -S "$docker_socket" ]]; then
  echo "Docker socket is unavailable: $docker_socket" >&2
  exit 1
fi
docker_gid="$(stat -c '%g' "$docker_socket")"

generate_secret() {
  python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
}

# Airflow runs as UID 50000 inside its image. Grant that user access only to
# runtime data directories; source code remains owned by the host user.
for runtime_dir in raw warehouse .state; do
  mkdir -p "$ROOT/$runtime_dir"
  setfacl -R -m u:50000:rwX "$ROOT/$runtime_dir"
  find "$ROOT/$runtime_dir" -type d -exec setfacl -m d:u:50000:rwx {} +
done

if [[ -f "$ENV_FILE" ]]; then
  sed -i "s/^DOCKER_GID=.*/DOCKER_GID=$docker_gid/" "$ENV_FILE"
  for setting in \
    "MINIO_ENDPOINT=http://127.0.0.1:9020" \
    "MINIO_BUCKET=supermarket-lakehouse" \
    "MINIO_ACCESS_KEY=minioadmin" \
    "MINIO_SECRET_KEY="; do
    key="${setting%%=*}"
    grep -q "^${key}=" "$ENV_FILE" || printf '%s\n' "$setting" >> "$ENV_FILE"
  done
  echo "Keeping existing $ENV_FILE"
  echo "Verify MINIO_SECRET_KEY in $ENV_FILE before running the MinIO publish task."
else
  umask 077
  airflow_webserver_secret="$(generate_secret)"
  airflow_admin_password="$(generate_secret)"
  printf '%s\n' \
    "PROJECT_ROOT=$ROOT" \
    "AIRFLOW_UID=50000" \
    "DOCKER_GID=$docker_gid" \
    "AIRFLOW_WEBSERVER_PORT=8088" \
    "AIRFLOW_WEBSERVER_SECRET_KEY=$airflow_webserver_secret" \
    "AIRFLOW_ADMIN_USERNAME=admin" \
    "AIRFLOW_ADMIN_PASSWORD=$airflow_admin_password" \
    "AIRFLOW_ADMIN_EMAIL=admin@example.local" \
    "MINIO_ENDPOINT=http://127.0.0.1:9020" \
    "MINIO_BUCKET=supermarket-lakehouse" \
    "MINIO_ACCESS_KEY=minioadmin" \
    "MINIO_SECRET_KEY=" > "$ENV_FILE"
  echo "Created $ENV_FILE"
  echo "Airflow admin password was generated in $ENV_FILE. Store it in a password manager."
  echo "Set MINIO_SECRET_KEY in $ENV_FILE before running the MinIO publish task."
fi

docker compose --env-file "$ENV_FILE" -f "$AIRFLOW_DIR/docker-compose.yml" build
docker compose --env-file "$ENV_FILE" -f "$AIRFLOW_DIR/docker-compose.yml" run --rm airflow-permissions
docker compose --env-file "$ENV_FILE" -f "$AIRFLOW_DIR/docker-compose.yml" up airflow-init
docker compose --env-file "$ENV_FILE" -f "$AIRFLOW_DIR/docker-compose.yml" up -d --force-recreate airflow-webserver airflow-scheduler

echo "Airflow is available at http://127.0.0.1:8088"

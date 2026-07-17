"""Daily crawl-to-Hudi orchestration for the supermarket lakehouse."""

from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


TIMEZONE = pendulum.timezone("Asia/Ho_Chi_Minh")
RETAILERS = ("bachhoaxanh", "go", "lottemart", "mmvietnam")

# A manual DAG trigger can override these values, for example:
# {"run_date": "2026-07-17", "run_id": "20260717_060000", "skip_crawlers": true}
RUN_DATE = "{{ dag_run.conf.get('run_date') if dag_run and dag_run.conf.get('run_date') else data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y-%m-%d') }}"
RUN_ID = "{{ dag_run.conf.get('run_id') if dag_run and dag_run.conf.get('run_id') else data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y%m%d') ~ '_060000' }}"
SKIP_CRAWLERS = "{{ 'true' if dag_run and dag_run.conf.get('skip_crawlers', false) else 'false' }}"


def crawl_command(retailer_id: str) -> str:
    return f"""\
set -euo pipefail
cd \"$PROJECT_ROOT\"
if [[ \"$SKIP_CRAWLERS\" == \"true\" ]]; then
  echo \"Skipping crawler for {retailer_id}; this is a controlled rerun.\"
  exit 0
fi
python scripts/run_retailer_crawlers.py \\
  --retailers {retailer_id} \\
  --output-root raw \\
  --run-date \"$RUN_DATE\" \\
  --run-id \"$RUN_ID\"
"""


with DAG(
    dag_id="daily_supermarket_lakehouse",
    description="Crawl retailer promotions, build Gold Hudi, validate, then publish to MinIO.",
    start_date=datetime(2026, 7, 17, tzinfo=TIMEZONE),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    dagrun_timeout=timedelta(hours=5),
    default_args={"owner": "supermarket-data", "retries": 0},
    render_template_as_native_obj=False,
    tags=["supermarket", "hudi", "daily"],
) as dag:
    crawler_tasks = []
    for retailer_id in RETAILERS:
        crawler_tasks.append(
            BashOperator(
                task_id=f"crawl_{retailer_id}",
                bash_command=crawl_command(retailer_id),
                env={
                    "RUN_DATE": RUN_DATE,
                    "RUN_ID": RUN_ID,
                    "SKIP_CRAWLERS": SKIP_CRAWLERS,
                },
                append_env=True,
                retries=2,
                retry_delay=timedelta(minutes=10),
                execution_timeout=timedelta(hours=2),
            )
        )

    crawl_winmart = BashOperator(
        task_id="crawl_winmart",
        bash_command="""
set -euo pipefail
cd "$PROJECT_ROOT"
if [[ "$SKIP_CRAWLERS" == "true" ]]; then
  echo "Skipping crawler for winmart; this is a controlled rerun."
  exit 0
fi
python scrapers/winmart/winmart_full_crawl.py \
  --config configs/winmart_categories.yaml \
  --out-dir raw \
  --run-date "$RUN_DATE" \
  --run-id "$RUN_ID"
""",
        env={
            "RUN_DATE": RUN_DATE,
            "RUN_ID": RUN_ID,
            "SKIP_CRAWLERS": SKIP_CRAWLERS,
        },
        append_env=True,
        retries=1,
        retry_delay=timedelta(minutes=15),
        execution_timeout=timedelta(hours=2),
    )

    build_hudi = BashOperator(
        task_id="build_bronze_to_hudi",
        bash_command="""
set -euo pipefail
cd "$PROJECT_ROOT"
python jobs/run_multi_retailer_pipeline.py \
  --retailers bachhoaxanh go lottemart mmvietnam \
  --include-winmart \
  --run-date "$RUN_DATE" \
  --run-id "$RUN_ID" \
  --skip-crawlers \
  --spark-output-format hudi \
  --continue-on-error
""",
        env={"RUN_DATE": RUN_DATE, "RUN_ID": RUN_ID},
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(hours=3),
    )

    validate_hudi = BashOperator(
        task_id="validate_spark_and_hudi",
        bash_command="""
set -euo pipefail
cd "$PROJECT_ROOT"
PYTHON_BIN="$(command -v python)" scripts/validate_daily_hudi_run.sh \
  --run-date "$RUN_DATE" \
  --run-id "$RUN_ID"
""",
        env={"RUN_DATE": RUN_DATE, "RUN_ID": RUN_ID},
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )

    publish_minio = BashOperator(
        task_id="publish_validated_hudi_to_minio",
        bash_command="""
set -euo pipefail
cd "$PROJECT_ROOT"
bash scripts/publish_hudi_to_minio.sh \
  --run-date "$RUN_DATE" \
  --run-id "$RUN_ID" \
  --endpoint "${MINIO_ENDPOINT:-http://127.0.0.1:9020}" \
  --bucket "${MINIO_BUCKET:-supermarket-lakehouse}"
""",
        env={"RUN_DATE": RUN_DATE, "RUN_ID": RUN_ID},
        append_env=True,
        execution_timeout=timedelta(hours=1),
    )

    [*crawler_tasks, crawl_winmart] >> build_hudi >> validate_hudi >> publish_minio

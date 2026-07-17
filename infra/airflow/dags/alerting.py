"""Best-effort notification callback for final Airflow task failures."""

from __future__ import annotations

import logging
import os
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)


def notify_task_failure(context: dict[str, Any]) -> None:
    """Send a generic Teams/Slack-compatible webhook notification when configured."""
    webhook_url = os.getenv("PIPELINE_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        LOGGER.info("Pipeline failure webhook is not configured; skipping notification.")
        return

    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")
    dag_id = getattr(dag_run, "dag_id", "unknown_dag")
    run_id = getattr(dag_run, "run_id", "unknown_run")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    log_url = getattr(task_instance, "log_url", "")
    text = f"Supermarket pipeline task failed: dag={dag_id}, run={run_id}, task={task_id}. Log: {log_url}"

    try:
        response = requests.post(webhook_url, json={"text": text}, timeout=10)
        response.raise_for_status()
    except requests.RequestException:
        # Alert delivery must not hide the original task failure or cause retries by itself.
        LOGGER.exception("Unable to deliver pipeline failure notification.")

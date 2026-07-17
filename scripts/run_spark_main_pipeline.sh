#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"

"${PYTHON_BIN}" jobs/run_spark_main_pipeline.py "$@"

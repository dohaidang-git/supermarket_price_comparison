#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
exec "${PYTHON_BIN}" jobs/run_multi_retailer_pipeline.py "$@"

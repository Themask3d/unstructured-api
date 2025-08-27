#!/usr/bin/env bash

set -euo pipefail

export PORT=${PORT:-8000}

# The manager application must run as a single process to manage the worker pool
# and job queue state correctly. The number of unstructured workers is determined
# dynamically inside the Python application.
exec uvicorn gpu_orchestrator.main:app \
    --host 0.0.0.0 \
    --port "$PORT" \
    --log-config logger_config.yaml \
    --timeout-keep-alive 300 \
    --timeout-graceful-shutdown 3600

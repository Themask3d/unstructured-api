#!/usr/bin/env bash

export WORKERS_PER_ENDPOINT=${WORKERS_PER_ENDPOINT:-3}

# Use gunicorn to manage uvicorn workers. This allows us to set a worker timeout
# to prevent gunicorn from killing workers that are processing long-running requests.
exec gunicorn prepline_general.api.app:app \
    -k uvicorn.workers.UvicornWorker \
    -b "0.0.0.0:${PORT:-8001}" \
    -w "$WORKERS_PER_ENDPOINT" \
    --log-level debug \
    --timeout 300 \
    --keep-alive 300

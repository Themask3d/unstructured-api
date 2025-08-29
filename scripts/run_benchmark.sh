#!/usr/bin/env bash
set -euo pipefail

# Configuration
NUM_RUNS=5
BENCHMARK_DIR="benchmark_results"
IMAGE_NAME="unstructured-gpu"
CONTAINER_NAME="unstructured-benchmark"
SAMPLE_DOCS_DIR="unstructured-api/pdf for testing"

# --- Cleanup Function ---
# This function will be called on script exit to ensure the container is stopped.
cleanup() {
    echo "--- Running cleanup ---"
    # The container might not exist if the script fails early, so we check first.
    if [ "$(docker ps -a -q -f name=$CONTAINER_NAME)" ]; then
        echo "Forcefully removing container $CONTAINER_NAME..."
        docker rm -f "$CONTAINER_NAME" > /dev/null
    fi
    echo "--- Cleanup complete ---"
}

# Register the cleanup function to be called on script exit (EXIT)
trap cleanup EXIT

# --- Main Script ---
echo "Starting benchmark..."
echo "Building Docker image ($IMAGE_NAME)..."
docker build -t "$IMAGE_NAME" -f unstructured-api/Dockerfile.gpu .

# Clean up previous results
if [ -d "$BENCHMARK_DIR" ]; then
    echo "Removing previous benchmark results from $BENCHMARK_DIR..."
    rm -rf "$BENCHMARK_DIR"
fi
mkdir -p "$BENCHMARK_DIR"

# Run the benchmark N times
for i in $(seq 1 $NUM_RUNS); do
    echo "--- Starting Run $i of $NUM_RUNS ---"
    RUN_DIR="$BENCHMARK_DIR/run-$i"
    RESULTS_DIR="$RUN_DIR/results"
    mkdir -p "$RESULTS_DIR"

    # Start container in detached mode
    echo "Starting container $CONTAINER_NAME..."
    # We remove the --rm flag and will explicitly remove the container later to avoid race conditions.
    CONTAINER_ID=$(docker run -d --gpus all -p 8000:8000 -p 8001:8001 --name "$CONTAINER_NAME" "$IMAGE_NAME")

    # Wait for the container to start up and for the models to be downloaded and
    # compiled. A simple, static wait is more reliable than a healthcheck here, as
    # the orchestrator is "healthy" long before the workers are ready for a load test.
    echo "Waiting 2 minutes for container to start and warm up..."
    sleep 120
    echo "Container is assumed to be ready."

    echo "--- Verifying GPU access inside the container ---"
    docker exec "$CONTAINER_NAME" nvidia-smi
    echo "-------------------------------------------------"

    # Start capturing logs
    echo "Capturing server logs to $RUN_DIR/server.log..."
    docker logs -f "$CONTAINER_NAME" > "$RUN_DIR/server.log" 2>&1 &
    LOG_PID=$!

    # Run the load test
    echo "Running load test..."
    python3 unstructured-api/scripts/load-test.py "$SAMPLE_DOCS_DIR" --results-dir "$RESULTS_DIR" > "$RUN_DIR/summary.txt"

    # Display the summary for the completed run.
    echo ""
    echo "--- Run $i Summary ---"
    cat "$RUN_DIR/summary.txt"
    echo "--------------------"
    echo ""

    # Stop logging and the container
    echo "Stopping log capture..."
    kill "$LOG_PID"
    wait "$LOG_PID" 2>/dev/null || true # Ignore error if process is already gone

    # This is the primary cleanup for successful runs, ensuring the container is
    # gone before the next loop starts.
    echo "Forcefully removing container $CONTAINER_NAME..."
    docker rm -f "$CONTAINER_NAME" > /dev/null

    echo "--- Finished Run $i ---"
done

# Disable the trap before a successful exit
trap - EXIT

# Generate the final report
echo "Generating final benchmark report..."
python3 unstructured-api/scripts/generate-report.py "$BENCHMARK_DIR"

echo "Benchmark complete. Report generated at $BENCHMARK_DIR/benchmark-report.md"

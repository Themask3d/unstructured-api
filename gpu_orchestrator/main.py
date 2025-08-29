import asyncio
import json
import uuid
import os
import httpx
import socket
import subprocess
import math
from contextlib import closing
from typing import Dict, Any, List, Optional
from subprocess import Popen
import sys

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

def calculate_num_workers():
    """
    Calculates the number of workers by executing a script to find available GPU memory.
    """
    # Allow overriding with an env var for testing or non-GPU environments
    num_workers_env = os.environ.get("NUM_WORKERS")
    if num_workers_env:
        try:
            return int(num_workers_env)
        except ValueError:
            print(f"Warning: Invalid NUM_WORKERS value '{num_workers_env}'. Ignoring.")

    available_gb_ram = 0
    try:
        python_executable = "python"
        script_path = "scripts/get-gpu-memory-gb.py"
        
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"Helper script not found at {script_path}")

        result = subprocess.run(
            [python_executable, script_path],
            capture_output=True,
            text=True,
            check=True,
        )
        raw_output = result.stdout.strip()
        print(f"GPU memory detection script output: '{raw_output}'")
        available_gb_ram = int(raw_output)
    except (FileNotFoundError, subprocess.CalledProcessError, ValueError) as e:
        print(f"Warning: Could not determine GPU memory via script. Falling back to default. Error: {e}")

    if available_gb_ram > 0:
        # Formula: (AVAILABLE_GB_RAM_ON_SERVER / 2) - 1
        num_workers = math.floor(available_gb_ram / 2) - 1
        print(f"Detected {available_gb_ram} GB of GPU memory.")
        # Ensure at least 1 worker if there's any GPU memory
        return max(1, num_workers)
    else:
        # Fallback if no GPU or script fails
        print("Warning: No GPU memory detected or script failed. Defaulting to 1 worker.")
        return 1


# --- Configuration ---
# The number of workers to run, determined dynamically.
NUM_WORKERS = calculate_num_workers()
print(f"Configuring service with {NUM_WORKERS} worker(s).")
API_VERSION = "v0"
BASE_WORKER_PORT = 8001  # Start worker ports from here

# --- Worker Refresh Strategy ---
# Restart a worker after it has processed this many documents.
MAX_DOCS_PER_WORKER = int(os.environ.get("MAX_DOCS_PER_WORKER", "50"))
# Restart a worker if it's been running for this long (e.g., 1 hour).
MAX_WORKER_LIFETIME_SECONDS = int(os.environ.get("MAX_WORKER_LIFETIME_SECONDS", "3600"))
# If the queue has been empty for this long, refresh the oldest worker.
IDLE_REFRESH_INTERVAL_SECONDS = int(os.environ.get("IDLE_REFRESH_INTERVAL_SECONDS", "300")) # 5 minutes
# How often the monitor checks the state of the workers.
MONITOR_INTERVAL_SECONDS = 10


# --- FastAPI App ---
app = FastAPI(
    title="Unstructured GPU Orchestrator",
    description="A service to manage and queue requests for the Unstructured partition API.",
)

# --- In-memory data stores ---
job_queue = asyncio.Queue()
job_store: Dict[str, Dict[str, Any]] = {}


class Job(BaseModel):
    job_id: str
    status: str
    result: Any = None


class Worker:
    def __init__(self, process: Popen, port: int):
        self.process = process
        self.port = port
        self.status: str = "warming_up"  # warming_up, ready, restarting
        self.processed_docs: int = 0
        self.start_time: float = asyncio.get_event_loop().time()
        # Read the concurrency level from an env var to match the worker's config.
        self.max_concurrency: int = int(os.environ.get("WORKERS_PER_ENDPOINT", "3"))
        self.in_flight_requests: int = 0

    @property
    def url(self):
        return f"http://localhost:{self.port}"


class WorkerManager:
    def __init__(self):
        self.workers: Dict[int, Worker] = {}
        self.last_job_time: float = asyncio.get_event_loop().time()

    async def start_worker(self, port: int) -> Worker:
        """Starts a new unstructured worker process."""
        # Command to start a single worker.
        # We use the new worker-start.sh script and override the PORT.
        cmd = ["./scripts/worker-start.sh"]
        env = os.environ.copy()
        env["PORT"] = str(port)
        
        # Enable unstructured's parallel mode for PDF processing.
        # The URL points back to the new internal page processor endpoint in this manager.
        manager_port = os.environ.get("PORT", "8000")
        env["UNSTRUCTURED_PARALLEL_MODE_ENABLED"] = "true"
        env["UNSTRUCTURED_PARALLEL_MODE_URL"] = f"http://localhost:{manager_port}/v0/internal/page_processor"
        env["UNSTRUCTURED_PARALLEL_MODE_THREADS"] = "4"

        print(f"Starting worker on port {port}")

        # Explicitly capture and forward stdout/stderr to the manager's streams
        process = Popen(cmd, env=env, stdout=sys.stdout, stderr=sys.stderr)
        worker = Worker(process, port)
        self.workers[process.pid] = worker
        print(f"Started worker process {process.pid} on port {port}")
        return worker

    async def stop_worker(self, pid: int):
        """Stops a worker process by its PID."""
        if pid in self.workers:
            worker = self.workers[pid]
            worker.process.terminate()
            # Use asyncio.to_thread for the blocking wait call
            await asyncio.to_thread(worker.process.wait)
            del self.workers[pid]
            print(f"Stopped worker process {pid}")

    async def restart_worker(self, pid: int):
        """Stops and restarts a worker, reusing its port."""
        if pid not in self.workers:
            return

        worker = self.workers[pid]
        port = worker.port
        worker.status = "restarting"
        print(f"Restarting worker {pid} on port {port}...")

        await self.stop_worker(pid)
        new_worker = await self.start_worker(port)
        # We now await the warm-up to ensure restarts are sequential and blocking.
        await self.warm_up_worker(new_worker)


    async def warm_up_worker(self, worker: Worker):
        """Sends a sample request to the worker to ensure it's ready."""
        print(f"Warming up worker on port {worker.port}...")
        # Use a single-page PDF for warm-up. Unstructured's parallel mode only triggers for
        # multi-page documents, so this avoids a deadlock where a warming-up worker tries to
        # dispatch pages to other workers that are not yet ready.
        warm_up_file = "sample-docs/embedded-images-tables.pdf"
        
        # Poll the health check endpoint until the worker is ready
        health_check_url = f"{worker.url}/healthcheck"
        max_wait_seconds = 30
        wait_interval = 1
        
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                for attempt in range(max_wait_seconds // wait_interval):
                    try:
                        response = await client.get(health_check_url)
                        if response.status_code == 200:
                            print(f"Worker on port {worker.port} is healthy.")
                            break
                    except httpx.RequestError:
                        pass # Ignore connection errors while polling
                    
                    await asyncio.sleep(wait_interval)
                else: # This 'else' belongs to the 'for' loop, executed if the loop completes without break
                    raise httpx.RequestError(f"Worker health check failed after {max_wait_seconds} seconds.")

            # Now that the worker is healthy, send the warm-up file
            with open(warm_up_file, "rb") as f:
                files = {"files": (os.path.basename(warm_up_file), f, "application/pdf")}
                async with httpx.AsyncClient(timeout=120) as client:
                    response = await client.post(f"{worker.url}/general/{API_VERSION}/general", files=files)
                    response.raise_for_status()
            worker.status = "ready"
            print(f"Worker on port {worker.port} is ready.")
        except (httpx.RequestError, IOError) as e:
            print(f"Failed to warm up worker on port {worker.port}: {e}")
            # If warm-up fails, we should stop the worker.
            await self.stop_worker(worker.process.pid)

    async def find_available_worker(self) -> Optional[Worker]:
        """Finds a worker that is ready to process a job."""
        wait_counter = 0
        while True:
            for worker in self.workers.values():
                if worker.status == "ready" and worker.in_flight_requests < worker.max_concurrency:
                    # The worker is available, so we reserve a slot.
                    return worker

            # Log waiting status every 5 seconds to provide visibility
            if wait_counter % 50 == 0:  # 50 * 0.1s = 5s
                warming_up = sum(1 for w in self.workers.values() if w.status == "warming_up")
                busy = sum(1 for w in self.workers.values() if w.status == "busy")
                restarting = sum(1 for w in self.workers.values() if w.status == "restarting")
                total = len(self.workers)
                print(
                    f"Dispatcher waiting for a ready worker... "
                    f"Total: {total}, Warming Up: {warming_up}, Busy: {busy}, Restarting: {restarting}"
                )

            wait_counter += 1
            await asyncio.sleep(0.1)  # Wait for a worker to become available

    async def monitor_workers(self):
        """Periodically checks worker health and applies refresh logic."""
        while True:
            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)
            
            # --- Proactive Refresh Logic ---
            pids_to_restart = set()
            now = asyncio.get_event_loop().time()

            # Find the oldest, completely idle worker for idle refresh logic
            oldest_idle_worker: Optional[Worker] = None
            
            for pid, worker in self.workers.items():
                # We only consider restarting workers that are fully ready and idle.
                if worker.status != "ready" or worker.in_flight_requests > 0:
                    continue

                # 1. Check max documents processed
                if worker.processed_docs >= MAX_DOCS_PER_WORKER:
                    print(f"Worker {pid} reached max doc limit ({worker.processed_docs}). Flagging for restart.")
                    pids_to_restart.add(pid)
                    continue

                # 2. Check max lifetime
                age = now - worker.start_time
                if age >= MAX_WORKER_LIFETIME_SECONDS:
                    print(f"Worker {pid} reached max lifetime ({age:.0f}s). Flagging for restart.")
                    pids_to_restart.add(pid)
                    continue
                
                # 3. Find the oldest eligible worker for potential idle restart
                if worker.processed_docs > 0: # Only restart workers that have done work
                    if oldest_idle_worker is None or worker.start_time < oldest_idle_worker.start_time:
                        oldest_idle_worker = worker

            # 4. Check for idle server condition
            if oldest_idle_worker and not pids_to_restart: # Don't idle-restart if other restarts are pending
                idle_time = now - self.last_job_time
                if idle_time >= IDLE_REFRESH_INTERVAL_SECONDS:
                    print(f"System idle for {idle_time:.0f}s. Refreshing oldest idle worker {oldest_idle_worker.process.pid}.")
                    pids_to_restart.add(oldest_idle_worker.process.pid)
                    self.last_job_time = now # Reset timer to avoid rapid restarts

            # Execute restarts
            for pid in pids_to_restart:
                await self.restart_worker(pid)


    async def initialize_workers(self, num_workers: int):
        """Starts the initial pool of workers and the monitor task."""
        # Start workers
        for i in range(num_workers):
            port = BASE_WORKER_PORT + i
            worker = await self.start_worker(port)
            # Start warm-up in the background for each worker
            asyncio.create_task(self.warm_up_worker(worker))
        
        # Start the monitoring task
        asyncio.create_task(self.monitor_workers())

    async def shutdown_workers(self):
        """Stops all worker processes."""
        pids = list(self.workers.keys())
        for pid in pids:
            await self.stop_worker(pid)


worker_manager = WorkerManager()


@app.post(f"/{API_VERSION}/partition", response_model=Job, status_code=202)
async def accept_job(
    request: Request,
    files: List[UploadFile] = File(..., description="The files to process"),
    parameters: str = Form("{}", description="JSON string of partition parameters"),
):
    job_id = str(uuid.uuid4())
    job_store[job_id] = {"status": "queued", "result": None}

    file_contents = {file.filename: await file.read() for file in files}
    
    # We also need content-types for the multipart form
    file_content_types = {file.filename: file.content_type for file in files}

    await job_queue.put({
        "job_id": job_id,
        "files": file_contents,
        "content_types": file_content_types,
        "parameters": parameters
    })
    
    # Update the last job time to track activity
    worker_manager.last_job_time = asyncio.get_event_loop().time()

    return Job(job_id=job_id, status="queued")


@app.get(f"/{API_VERSION}/status/{{job_id}}", response_model=Job)
async def get_job_status(job_id: str):
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return Job(job_id=job_id, **job)


@app.post("/v0/internal/page_processor", include_in_schema=False)
async def page_processor(request: Request):
    """
    This internal endpoint receives single-page processing requests from workers
    that have parallel mode enabled. It acts as a stateless, streaming proxy,
    dispatching these page-level jobs to any available worker.
    """
    # We must parse the form to inspect the filename for the job_id.
    # This is a trade-off: it uses more memory in the orchestrator than a pure
    # stream, but it's necessary for the cancellation logic.
    form_data = await request.form()
    
    fwd_files = []
    fwd_data = {}
    job_id = None

    for key, value in form_data.multi_items():
        # We check the type name as a robust way to identify file uploads,
        # as `isinstance` can sometimes fail in complex dependency environments.
        if type(value).__name__ == 'UploadFile':
            upload_file = value # It's an UploadFile object
            if upload_file.filename and "job_id_" in upload_file.filename:
                try:
                    # Extract job_id from filename like "job_id_xyz__original.pdf"
                    job_id = upload_file.filename.split("__")[0].replace("job_id_", "")
                except (IndexError, TypeError):
                    pass # Couldn't parse, proceed without job_id
            
            file_content = await upload_file.read()
            fwd_files.append((key, (upload_file.filename, file_content, upload_file.content_type)))
        else:
            # It's a regular form field
            fwd_data[key] = value

    # --- Cancellation Check ---
    if job_id and job_id in job_store and job_store[job_id]["status"] == "failed":
        print(f"Rejecting page processing for failed job {job_id}")
        return Response(status_code=409, content="Parent job failed or cancelled")
    # ---

    worker = await worker_manager.find_available_worker()
    worker.in_flight_requests += 1

    try:
        url = httpx.URL(f"{worker.url}/general/v0/general")
        client = httpx.AsyncClient(timeout=300.0)
        
        # Forward the parsed and buffered request.
        # We must build the request and then use `send` to enable streaming.
        fwd_request = client.build_request("POST", url, data=fwd_data, files=fwd_files)
        response = await client.send(fwd_request, stream=True)

        async def response_generator(response: httpx.Response):
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            finally:
                await response.aclose()
                await client.aclose()

        return StreamingResponse(
            content=response_generator(response),
            status_code=response.status_code,
            headers=response.headers,
        )

    except Exception as e:
        print(f"Error in page_processor forwarding to worker {worker.port}: {e}")
        if 'client' in locals() and not client.is_closed:
            await client.aclose()
        if 'response' in locals():
            await response.aclose()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        worker.processed_docs += 1
        worker.in_flight_requests -= 1


async def process_jobs():
    while True:
        job_details = await job_queue.get()
        job_id = job_details["job_id"]
        filenames = list(job_details.get("files", {}).keys())
        print(f"Dequeued job {job_id} for files: {filenames}. Finding an available worker...")
        job_store[job_id]["status"] = "processing"

        worker = await worker_manager.find_available_worker()
        worker.in_flight_requests += 1
        print(f"Forwarding job {job_id} to worker on port {worker.port}")

        try:
            # Prepare files for forwarding
            forward_files = []
            for filename, content in job_details["files"].items():
                content_type = job_details["content_types"].get(filename)
                
                # Prepend the job_id to the filename to ensure we can track it
                # through the parallel processing pipeline.
                safe_filename = f"job_id_{job_id}__{filename}"

                forward_files.append(("files", (safe_filename, content, content_type)))
            
            # Prepare data payload
            data_payload = json.loads(job_details["parameters"])

            # We inject the job_id so that the unstructured library, if it supports it,
            # can pass it back to the page_processor for cancellation checks.
            data_payload["job_id"] = job_id

            # --- Enforce GPU strategy ---
            data_payload["strategy"] = "hi_res"
            data_payload["hi_res_model_name"] = "yolox"
            # ---

            # Set a 5-minute timeout for the entire request
            timeout = httpx.Timeout(300.0, connect=10.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(
                    f"{worker.url}/general/{API_VERSION}/general",
                    files=forward_files,
                    data=data_payload,
                )

            print(f"Received response for job {job_id} from worker. Status: {response.status_code}")
            if response.status_code == 200:
                job_store[job_id]["status"] = "succeeded"
                job_store[job_id]["result"] = response.json()
            else:
                job_store[job_id]["status"] = "failed"
                job_store[job_id]["result"] = {"error": response.text, "status_code": response.status_code}

        except Exception as e:
            job_store[job_id]["status"] = "failed"
            job_store[job_id]["result"] = {"error": str(e)}
            print(f"Error processing job {job_id} for files {filenames}: {e}")

        finally:
            worker.processed_docs += 1
            worker.in_flight_requests -= 1
    else:
        job_store[job_id]["status"] = "failed"
        job_store[job_id]["result"] = {"error": "No available workers"}
        print(f"No workers available to process job {job_id} for files: {filenames}.")

        job_queue.task_done()


@app.on_event("startup")
async def on_startup():
    await worker_manager.initialize_workers(NUM_WORKERS)
    asyncio.create_task(process_jobs())


@app.on_event("shutdown")
async def on_shutdown():
    await worker_manager.shutdown_workers()

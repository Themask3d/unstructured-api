import os
import requests
import time
import argparse
from typing import List, Dict, Any

API_URL = "http://localhost:8000/v0/partition"
STATUS_URL = "http://localhost:8000/v0/status"

def submit_file(file_path: str) -> str | None:
    """Submits a single file and returns the job_id."""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None

    try:
        with open(file_path, "rb") as f:
            response = requests.post(API_URL, files={"files": (os.path.basename(file_path), f)})
        response.raise_for_status()
        return response.json().get("job_id")
    except requests.exceptions.RequestException as e:
        print(f"Error submitting {file_path}: {e}")
        return None

def get_page_count_from_result(result: List[Dict[str, Any]]) -> int:
    """Extracts the highest page number from a partitioning result."""
    if not result or not isinstance(result, list):
        return 0
    
    max_page = 0
    for item in result:
        page_number = item.get("metadata", {}).get("page_number")
        if isinstance(page_number, int) and page_number > max_page:
            max_page = page_number
    return max_page

def run_performance_test(files_to_submit: List[str]):
    """Submits all files, waits for completion, and prints a performance report."""
    if not files_to_submit:
        print("No files to submit for the test.")
        return

    print(f"Starting performance test with {len(files_to_submit)} documents...")

    jobs = {}
    for i, file_path in enumerate(files_to_submit):
        print(f"Submitting file {i+1}/{len(files_to_submit)}: {os.path.basename(file_path)}")
        job_id = submit_file(file_path)
        if job_id:
            jobs[job_id] = {
                "filename": os.path.basename(file_path),
                "start_time": time.time(),
            }
        time.sleep(0.1) # Small delay between submissions

    print("\nAll files submitted. Waiting for processing to complete...")

    results = []
    pending_jobs = list(jobs.keys())
    
    while pending_jobs:
        for job_id in list(pending_jobs):
            try:
                response = requests.get(f"{STATUS_URL}/{job_id}")
                response.raise_for_status()
                status_data = response.json()
                status = status_data.get("status")

                if status in ["succeeded", "failed"]:
                    job_info = jobs[job_id]
                    duration = time.time() - job_info["start_time"]
                    
                    page_count = 0
                    if status == "succeeded":
                        page_count = get_page_count_from_result(status_data.get("result", []))
                    
                    results.append({
                        "filename": job_info["filename"],
                        "duration": duration,
                        "pages": page_count,
                        "status": status,
                    })
                    pending_jobs.remove(job_id)
                    print(f"  - Job {job_id} ({job_info['filename']}) finished with status: {status}")

            except requests.exceptions.RequestException as e:
                print(f"Error checking status for {job_id}: {e}")
        
        if pending_jobs:
            time.sleep(2)

    print_report(results)

def print_report(results: List[Dict[str, Any]]):
    """Prints a formatted report of the performance test results."""
    print("\n--- Performance Test Results ---")
    print(f"{'File':<70} {'Time (s)':>10} {'Pages':>7} {'Time/Page (s)':>15}")
    print("-" * 107)

    total_duration = 0
    total_pages = 0
    
    # Sort results by filename for cleaner output
    results.sort(key=lambda x: x['filename'])

    for res in results:
        filename = res["filename"]
        duration = res["duration"]
        pages = res["pages"]
        status = res["status"]

        if status == "failed":
            time_per_page_str = "N/A (Failed)"
        elif pages > 0:
            time_per_page = duration / pages
            time_per_page_str = f"{time_per_page:.3f}"
        else:
            time_per_page_str = "N/A"
        
        if status == "succeeded":
            total_duration += duration
            total_pages += pages

        print(f"{filename:<70} {duration:>10.2f} {pages:>7} {time_per_page_str:>15}")
    
    print("-" * 107)
    
    avg_time_per_page = (total_duration / total_pages) if total_pages > 0 else 0
    
    print("\n--- Summary ---")
    print(f"Total documents processed successfully: {sum(1 for r in results if r['status'] == 'succeeded')}/{len(results)}")
    print(f"Total processing time (successful docs): {total_duration:.2f} seconds")
    print(f"Total pages processed (successful docs): {total_pages}")
    if avg_time_per_page > 0:
        print(f"Overall average time per page: {avg_time_per_page:.3f} seconds")
    print("-" * 25)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load test script for the Unstructured GPU Orchestrator."
    )
    parser.add_argument(
        "directory",
        type=str,
        nargs="?",
        default="pdf_for_testing",
        help="The directory containing files to submit (defaults to 'pdf_for_testing').",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"Error: Directory not found: {args.directory}")
        exit(1)

    files_to_submit = []
    for f in os.listdir(args.directory):
        if f.startswith("."):  # Ignore hidden files like .gitkeep
            continue
        full_path = os.path.join(args.directory, f)
        if os.path.isfile(full_path):
            files_to_submit.append(full_path)

    run_performance_test(files_to_submit)

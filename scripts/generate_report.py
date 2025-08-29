import os
import json
import argparse
from collections import defaultdict
import pandas as pd

def parse_results(benchmark_dir: str) -> tuple[dict, int]:
    """Parses all result JSONs from a benchmark run directory."""
    all_data = defaultdict(list)
    run_dirs = sorted([d for d in os.listdir(benchmark_dir) if d.startswith("run-") and os.path.isdir(os.path.join(benchmark_dir, d))])

    for run_dir_name in run_dirs:
        results_path = os.path.join(benchmark_dir, run_dir_name, "results")
        if not os.path.exists(results_path):
            continue

        run_results = []
        for filename in os.listdir(results_path):
            if filename.endswith(".json"):
                with open(os.path.join(results_path, filename)) as f:
                    data = json.load(f)
                    if data.get("status") == "succeeded" and "performance" in data:
                        run_results.append(data)
        
        # Sort by completion time to calculate time since last
        run_results.sort(key=lambda x: x["performance"]["end_time"])

        last_end_time = None
        for data in run_results:
            perf = data["performance"]
            original_filename = data.get("job_info", {}).get("filename", "Unknown")

            end_time = perf["end_time"]
            time_since_last = (end_time - last_end_time) if last_end_time is not None else 0.0
            last_end_time = end_time

            all_data[original_filename].append({
                "duration": perf["duration"],
                "pages": perf["pages"],
                "time_per_page": perf["duration"] / perf["pages"] if perf["pages"] > 0 else 0,
                "time_since_last": time_since_last,
            })

    return all_data, len(run_dirs)


def generate_markdown_report(all_data: dict, num_runs: int, benchmark_dir: str):
    """Generates a markdown report from the parsed benchmark data."""
    output_file = os.path.join(benchmark_dir, "benchmark-report.md")

    records = []
    for filename, run_data_list in all_data.items():
        for i, data in enumerate(run_data_list):
            records.append({"run": i + 1, "filename": filename, **data})
    df = pd.DataFrame(records)

    with open(output_file, "w") as f:
        f.write("# Benchmark Results\n\n")
        f.write(f"This report summarizes the results of {num_runs} benchmark runs.\n\n")

        # --- Per-Run Details ---
        for i in range(1, num_runs + 1):
            f.write(f"## Run {i}\n\n")
            run_df = df[df["run"] == i].copy()
            if run_df.empty:
                f.write("No successful results for this run.\n\n")
                continue

            run_df = run_df.drop(columns="run")
            run_df_display = run_df.rename(columns={
                "filename": "File", "duration": "Duration (s)", "pages": "Pages",
                "time_per_page": "Time/Page (s)", "time_since_last": "Time Since Last (s)"
            })
            for col in ["Duration (s)", "Time Since Last (s)"]:
                run_df_display[col] = run_df_display[col].map('{:.2f}'.format)
            run_df_display["Time/Page (s)"] = run_df_display["Time/Page (s)"].map('{:.3f}'.format)
            
            f.write(run_df_display.to_markdown(index=False))
            f.write("\n\n")

            # Run Summary
            total_docs = len(run_df)
            total_pages = run_df["pages"].sum()
            wall_clock_time = run_df['time_since_last'].sum()

            f.write(f"### Run {i} Summary\n\n")
            f.write(f"- **Total Documents Processed:** {total_docs}\n")
            f.write(f"- **Total Pages Processed:** {total_pages}\n")
            f.write(f"- **Total Wall-Clock Time:** {wall_clock_time:.2f} s\n")
            if wall_clock_time > 0:
                f.write(f"- **Overall Throughput (docs/sec):** {(total_docs / wall_clock_time):.2f}\n")
                f.write(f"- **Overall Throughput (pages/sec):** {(total_pages / wall_clock_time):.2f}\n")
            f.write("\n")

        # --- Final Average Results ---
        if not df.empty:
            f.write("## Final Average Results\n\n")
            avg_df = df.groupby("filename").agg(
                avg_duration=("duration", "mean"),
                avg_time_per_page=("time_per_page", "mean")
            ).reset_index()

            avg_df_display = avg_df.rename(columns={
                "filename": "File", "avg_duration": "Avg Duration (s)", 
                "avg_time_per_page": "Avg Time/Page (s)"
            })
            avg_df_display["Avg Duration (s)"] = avg_df_display["Avg Duration (s)"].map('{:.2f}'.format)
            avg_df_display["Avg Time/Page (s)"] = avg_df_display["Avg Time/Page (s)"].map('{:.3f}'.format)
            f.write(avg_df_display.to_markdown(index=False))
            f.write("\n\n")

            # Overall Summary
            run_summaries = df.groupby('run').agg(
                docs=('filename', 'size'),
                pages=('pages', 'sum'),
                time=('time_since_last', 'sum')
            ).reset_index()
            
            f.write("### Overall Summary\n\n")
            f.write(f"- **Average Documents per Run:** {run_summaries['docs'].mean():.1f}\n")
            f.write(f"- **Average Pages per Run:** {run_summaries['pages'].mean():.1f}\n")
            f.write(f"- **Average Wall-Clock Time per Run:** {run_summaries['time'].mean():.2f} s\n")
            avg_throughput_docs = (run_summaries['docs'] / run_summaries['time']).mean()
            avg_throughput_pages = (run_summaries['pages'] / run_summaries['time']).mean()
            f.write(f"- **Average Throughput (docs/sec):** {avg_throughput_docs:.2f}\n")
            f.write(f"- **Average Throughput (pages/sec):** {avg_throughput_pages:.2f}\n")
            f.write("\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a benchmark report from load test results.")
    parser.add_argument("benchmark_dir", type=str, help="The directory containing the benchmark run results.")
    args = parser.parse_args()
    
    all_data, num_runs = parse_results(args.benchmark_dir)
    if num_runs > 0:
        generate_markdown_report(all_data, num_runs, args.benchmark_dir)
        print(f"Report generated at {os.path.join(args.benchmark_dir, 'benchmark-report.md')}")
    else:
        print("No benchmark runs found to report on.")

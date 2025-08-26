import subprocess
import sys

def get_gpu_memory_gb():
    """
    Returns the total memory of the first available GPU in gigabytes.
    Returns 0 if nvidia-smi is not found or fails.
    """
    try:
        # Query memory of GPU 0 in MiB. If multiple GPUs are visible, this targets the first one.
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits", "-i", "0"],
            capture_output=True,
            text=True,
            check=True,
        )
        memory_mib = int(result.stdout.strip())
        # Convert MiB to GB (1 GiB = 1024 MiB, we'll use this as an approximation for GB)
        memory_gb = memory_mib / 1024
        return memory_gb
    except (FileNotFoundError, subprocess.CalledProcessError, ValueError) as e:
        # We print to stderr so it doesn't pollute stdout, which is used for the result
        print(f"Error querying GPU memory: {e}", file=sys.stderr)
        return 0

if __name__ == "__main__":
    memory = get_gpu_memory_gb()
    # Output an integer for easy parsing by the calling process
    print(int(memory))

# benchmark.py
# Measures pipeline speedup and memory usage across different worker counts,
# generates speedup and memory graphs.
import time
import os
import json
import multiprocessing
import psutil
import matplotlib.pyplot as plt
import config
from pipeline import run_pipeline


def measure_run(data_glob, workers):
    """
    Runs the full pipeline with a given number of workers.
    Returns (elapsed_seconds, peak_ram_mb, avg_cpu_fraction).
    """
    # Clean up previous shard/analysis outputs so each run starts fresh
    import shutil
    for d in ["partitioned", "analysis", "loitering"]:
        if os.path.exists(d):
            shutil.rmtree(d)

    process = psutil.Process(os.getpid())
    ram_samples = []
    cpu_samples = []

    import threading
    stop_flag = threading.Event()

    def sample_resources():
        while not stop_flag.is_set():
            try:
                # Measure RAM
                mem = process.memory_info().rss / (1024 * 1024)
                children = process.children(recursive=True)
                for c in children:
                    try:
                        mem += c.memory_info().rss / (1024 * 1024)
                    except Exception:
                        pass
                ram_samples.append(mem)
                
                # Measure CPU
                cpu_pct = psutil.cpu_percent(interval=0.5)
                cpu_samples.append(cpu_pct)
            except Exception:
                pass

    sampler = threading.Thread(target=sample_resources, daemon=True)
    sampler.start()

    t0 = time.time()
    # Explicitly pass the workers argument
    run_pipeline(data_glob=data_glob, workers=workers)
    elapsed = time.time() - t0

    stop_flag.set()
    sampler.join()

    peak_ram = max(ram_samples) if ram_samples else 0
    avg_cpu_pct = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
    avg_cpu_fraction = avg_cpu_pct / 100.0

    print(f"  Workers={workers} | Time={elapsed:.2f}s | Peak RAM={peak_ram:.1f} MB | Avg CPU={avg_cpu_fraction:.2f}")
    return elapsed, peak_ram, avg_cpu_fraction


def run_benchmark(data_glob="data_arch/*.csv", worker_counts=None):
    """
    Runs the pipeline for each worker count and plots speedup + memory graphs.
    """
    if worker_counts is None:
        max_cores = multiprocessing.cpu_count()
        worker_counts = [1, max_cores] if max_cores > 4 else list(range(1, max_cores + 1))
        worker_counts = sorted(set(worker_counts))

    print(f"Benchmarking worker counts: {worker_counts}")

    times = []
    rams  = []
    cpus  = []

    for w in worker_counts:
        print(f"\n--- Running with {w} worker(s) ---")
        # Now expecting 3 returned variables!
        elapsed, peak_ram, avg_cpu = measure_run(data_glob, w)
        times.append(elapsed)
        rams.append(peak_ram)
        cpus.append(avg_cpu)

    baseline   = times[0]
    speedups   = [baseline / t for t in times]

    # --- Speedup graph ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].plot(worker_counts, speedups, marker="o", color="steelblue", linewidth=2)
    axes[0].plot(worker_counts, worker_counts, linestyle="--", color="gray", label="Ideal linear speedup")
    axes[0].set_title("Speedup vs Number of Workers")
    axes[0].set_xlabel("Workers")
    axes[0].set_ylabel("Speedup (x)")
    axes[0].legend()
    axes[0].grid(True)

    # --- Memory graph ---
    axes[1].plot(worker_counts, rams, marker="s", color="tomato", linewidth=2)
    axes[1].set_title("Peak RAM vs Number of Workers")
    axes[1].set_xlabel("Workers")
    axes[1].set_ylabel("Peak RAM (MB)")
    axes[1].grid(True)

    plt.tight_layout()
    out_path = "benchmark_results.png"
    plt.savefig(out_path, dpi=150)
    plt.show()
    print(f"\nBenchmark graph saved to {out_path}")

    # Save raw numbers
    results = {
        "worker_counts": worker_counts,
        "times_seconds": times,
        "speedups":       speedups,
        "peak_ram_mb":    rams,
        "avg_cpu_frac":   cpus,
    }
    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Raw results saved to benchmark_results.json")

    return results


if __name__ == "__main__":
    run_benchmark()
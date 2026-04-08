import time
import os
import shutil
import matplotlib.pyplot as plt
import numpy as np
import partition
from pipeline import run_pipeline

def measure_chunk_performance(chunk_sizes, data_glob="data_arch/*.csv", worker_count=11):
    results_time = []
    
    print(f"=== Shadow Fleet: Chunk Optimization Test ===")
    print(f"Workers (Constant): {worker_count}")
    print(f"Testing Sizes: {chunk_sizes}\n")

    for size in chunk_sizes:
        print(f"--- STARTING TEST: CHUNK_SIZE = {size:,} ---")
        
        for d in ["partitioned", "analysis", "loitering"]:
            if os.path.exists(d):
                shutil.rmtree(d)

        partition.CHUNK_SIZE = size 
        
        t0 = time.time()
        try:
            run_pipeline(data_glob=data_glob, workers=worker_count)
            elapsed = time.time() - t0
        except Exception as e:
            print(f"  [ERROR] Run failed for size {size}: {e}")
            elapsed = 0
            
        results_time.append(elapsed)
        print(f"\n>>> FINISHED CHUNK {size:,}: {elapsed:.2f} seconds\n")

    plt.figure(figsize=(10, 6))
    plt.plot(chunk_sizes, results_time, marker='o', linestyle='-', color='darkcyan', linewidth=2, markersize=8)
    
    plt.title("Impact of Chunk Size on Total Execution Time", fontsize=14)
    plt.xlabel("Chunk Size (Rows per Shard)", fontsize=12)
    plt.ylabel("Total Pipeline Time (Seconds)", fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    
    # minimum time
    if any(t > 0 for t in results_time):
        valid_times = [t for t in results_time if t > 0]
        min_time = min(valid_times)
        best_size = chunk_sizes[results_time.index(min_time)]
        
        plt.annotate(f'OPTIMAL: {best_size:,} rows\n({min_time:.1f}s)', 
                     xy=(best_size, min_time), 
                     xytext=(best_size, min_time + (max(results_time)*0.1)),
                     arrowprops=dict(facecolor='black', shrink=0.05, width=1, headwidth=8),
                     ha='center', fontsize=10, fontweight='bold', 
                     bbox=dict(boxstyle="round,pad=0.3", fc="yellow", ec="black", alpha=0.8))

    output_png = "chunk_optimization_results.png"
    plt.savefig(output_png, dpi=150)
    print(f"Success! Optimization graph saved as '{output_png}'.")
    plt.show()

if __name__ == "__main__":
    # Define the range of chunk sizes you want to test.
    # We include very small sizes to show overhead and very large to show worker starvation.
    test_sizes = [10000, 50000, 100000, 250000, 500000, 1000000]
    
    # Run the benchmark
    measure_chunk_performance(test_sizes)

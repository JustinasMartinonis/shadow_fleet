import sys
import os
import subprocess
import memory_profiler

# Ensure imports resolve if your script is in a subfolder
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipeline import run_pipeline

def run_both_profiles():
    target_scenarios = [1, 11]
    
    for w in target_scenarios:
        print(f"\n--- Starting Memory Profiling: {w} Worker(s) ---")
        
        # We use a unique name for each data file so they don't overwrite
        dat_file = f"mprofile_{w}_workers.dat"
        output_png = f"memory_profile_{w}_workers.png"
        
        # Step 1: Run mprof. 
        # We call the 'main.py' script via subprocess to ensure a clean process memory space
        print(f"Gathering data for {w} worker(s)...")
        # We pass the worker count as a command line argument if your main.py supports it,
        # OR we temporarily overwrite the environment variable/config.
        
        # Simpler way: Run a small helper that calls run_pipeline(workers=w)
        run_cmd = [
            "mprof", "run", 
            "--output", dat_file,
            sys.executable, "-c", 
            f"from pipeline import run_pipeline; run_pipeline(workers={w})"
        ]
        subprocess.run(run_cmd, check=True)
        
        # Step 2: Generate the graph for this specific run
        print(f"Generating graph: {output_png}...")
        plot_cmd = ["mprof", "plot", "-o", output_png, dat_file]
        subprocess.run(plot_cmd, check=True)
        
        print(f"Success! Scenario {w} saved to {output_png}")

if __name__ == "__main__":
    run_both_profiles()
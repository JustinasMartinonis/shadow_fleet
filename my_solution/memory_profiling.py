import subprocess
import sys
import os

def run_memory_profiling():
    # The name of your actual pipeline script
    target_script = "main.py" 
    
    # Optional: You can put the absolute path here if needed, 
    # but since this script is in the same folder, just the name works!
    
    print(f"--- Starting Memory Profiling for {target_script} ---")
    
    # Step 1: Run the pipeline through mprof
    # sys.executable ensures it uses your exact Anaconda Python environment
    print("Gathering memory data (this will take a moment)...")
    run_command = ["mprof", "run", sys.executable, target_script]
    subprocess.run(run_command, check=True)
    
    print("\n--- Pipeline Complete ---")
    
    # Step 2: Tell mprof to generate the PNG graph
    output_image = "memory_profile.png"
    print(f"Generating graph: {output_image}...")
    plot_command = ["mprof", "plot", "-o", output_image]
    subprocess.run(plot_command, check=True)
    
    print(f"Success! Your memory graph has been saved as '{output_image}'.")

if __name__ == "__main__":
    # Make sure mprof is installed before running
    try:
        import memory_profiler
    except ImportError:
        print("Error: memory_profiler is not installed. Please run 'pip install memory_profiler matplotlib' first.")
        sys.exit(1)
        
    run_memory_profiling()
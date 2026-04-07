# main.py
# Simple entry point — delegates to pipeline.py
# For CLI usage with arguments, use cli.py instead
from pipeline import run_pipeline
from config import DATA_ARCH_GLOB

if __name__ == "__main__":
    print("Shadow Fleet AIS Anomaly Detector")
    print(f"Input: {DATA_ARCH_GLOB}\n")
    run_pipeline()

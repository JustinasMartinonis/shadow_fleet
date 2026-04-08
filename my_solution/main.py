# main.py
# delegates to pipeline.py
from pipeline import run_pipeline
from config import DATA_ARCH_GLOB

if __name__ == "__main__":
    print("Shadow Fleet AIS Anomaly Detector")
    print(f"Input: {DATA_ARCH_GLOB}\n")
    run_pipeline()

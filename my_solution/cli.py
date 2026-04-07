# cli.py
# Command-line interface for the Shadow Fleet detection pipeline
import argparse
import sys
from pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="Shadow Fleet AIS Anomaly Detector",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--data",
        type=str,
        default="data_arch/*.csv",
        help="Glob pattern for input AIS CSV files"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=".",
        help="Output directory for results"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel worker processes (overrides config.py)"
    )

    args = parser.parse_args()

    # Allow CLI to override NUM_WORKERS without editing config
    if args.workers is not None:
        import config
        config.NUM_WORKERS = args.workers
        print(f"Using {config.NUM_WORKERS} worker(s) (CLI override)")

    run_pipeline(data_glob=args.data, output_dir=args.output)


if __name__ == "__main__":
    main()

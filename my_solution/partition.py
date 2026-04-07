# partition.py
# Splits raw CSVs into fixed-size shards for parallel processing
import csv
import glob
import os
from config import DATA_ARCH_GLOB, PARTITIONED_DIR, CHUNK_SIZE


def partition_all(data_glob=DATA_ARCH_GLOB, out_dir=PARTITIONED_DIR, chunk_size=CHUNK_SIZE):
    """
    Reads all CSVs matching data_glob, writes fixed-size shards to out_dir.
    Returns list of shard file paths.
    """
    os.makedirs(out_dir, exist_ok=True)

    input_files = sorted(glob.glob(data_glob))
    if not input_files:
        raise FileNotFoundError(f"No CSV files found matching: {data_glob}")

    print(f"Partitioning {len(input_files)} input file(s) into shards of {chunk_size} rows...")

    shard_index  = 0
    shard_writer = None
    shard_file   = None
    rows_in_shard = 0
    shard_paths  = []
    headers      = None

    for filepath in input_files:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)

            # Use headers from the first file
            if headers is None:
                headers = reader.fieldnames

            for row in reader:
                # Open a new shard if needed
                if shard_writer is None or rows_in_shard >= chunk_size:
                    if shard_file:
                        shard_file.close()
                    shard_path = os.path.join(out_dir, f"ais_shard_{shard_index:04d}.csv")
                    shard_paths.append(shard_path)
                    shard_file   = open(shard_path, "w", newline="", encoding="utf-8")
                    shard_writer = csv.DictWriter(shard_file, fieldnames=headers)
                    shard_writer.writeheader()
                    rows_in_shard = 0
                    shard_index  += 1

                shard_writer.writerow(row)
                rows_in_shard += 1

    if shard_file:
        shard_file.close()

    print(f"Created {len(shard_paths)} shards in '{out_dir}/'")
    return shard_paths

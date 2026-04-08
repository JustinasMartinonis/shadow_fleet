# partition.py
# Reads raw CSVs line-by-line, filters bad rows, and writes fixed-size shards to disk.

import csv
import glob
import os
from config import (
    DATA_ARCH_GLOB, PARTITIONED_DIR, CHUNK_SIZE,
    DIRTY_MMSI, DIRTY_PREFIXES, EXCLUDED_SHIP_TYPES,
    LAT_MIN, LAT_MAX, LON_MIN, LON_MAX
)

MAX_ROWS = 500_000_000  


def _is_valid(row):
    # MMSI
    mmsi = row.get("MMSI", "").strip()
    if not mmsi or mmsi in DIRTY_MMSI or len(mmsi) != 9 or not mmsi.isdigit() or mmsi[0] == "0":
        return False
    if mmsi.startswith(DIRTY_PREFIXES):  
        return False

    # Timestamp
    if not row.get("# Timestamp"):
        return False

    # Vessel class and type
    if row.get("Type of mobile", "").strip() != "Class A":
        return False
    if row.get("Ship type", "").strip().lower() in EXCLUDED_SHIP_TYPES:
        return False

    # Coordinates
    try:
        lat = float(row.get("Latitude", ""))
        lon = float(row.get("Longitude", ""))
        if lat == 0.0 or lon == 0.0:                          
            return False
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return False
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):  # Bounding box
            return False
    except ValueError:
        return False

    # Speed over ground
    sog_str = row.get("SOG", "").strip()
    if sog_str:
        try:
            if float(sog_str) > 60:
                return False
        except ValueError:
            return False

    return True

# One ping per vessel per 2-min window
def _dedup_key(mmsi, ts):
    yr = int(ts[6:10]); mo = int(ts[3:5]); dy = int(ts[0:2])
    hr = int(ts[11:13]); mn = int(ts[14:16])
    return (mmsi, yr, mo, dy, hr, mn // 2)


def read_chunks(file_path):
    raw_rows     = 0
    rows_read    = 0
    seen_buckets = set()

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        batch  = []

        for row in reader:
            raw_rows += 1
            if raw_rows % 500_000 == 0:
                print(f"  [reader] {raw_rows:,} raw rows scanned | {rows_read:,} accepted")
            if rows_read >= MAX_ROWS:
                break

            if not _is_valid(row):
                continue

            mmsi = row["MMSI"].strip()
            ts   = row["# Timestamp"].strip()

            key = _dedup_key(mmsi, ts)
            if key in seen_buckets:
                continue
            seen_buckets.add(key)

            batch.append({
                "MMSI":           mmsi,
                "# Timestamp":    ts,
                "Latitude":       float(row["Latitude"]),
                "Longitude":      float(row["Longitude"]),
                "SOG":            float(row.get("SOG", "").strip() or 0),
                "Draught":        float(row.get("Draught", "").strip() or 0),
            })
            rows_read += 1

            if len(batch) >= CHUNK_SIZE:
                yield batch
                batch = []

        if batch:
            yield batch

    print(f"  [reader] Done. {raw_rows:,} raw | {rows_read:,} accepted")


def partition_all(data_glob=DATA_ARCH_GLOB, out_dir=PARTITIONED_DIR):
    """
    Gets clean batches from read_chunks and writes them as numbered shard CSVs; returns list of shard file paths
    """
    os.makedirs(out_dir, exist_ok=True)
    input_files = sorted(glob.glob(data_glob))
    if not input_files:
        raise FileNotFoundError(f"No CSV files found matching: {data_glob}")

    print(f"Partitioning {len(input_files)} file(s) into shards of {CHUNK_SIZE:,} rows...")

    headers     = ["MMSI", "# Timestamp", "Latitude", "Longitude", "SOG", "Draught"]
    shard_index = 0
    shard_paths = []

    for filepath in input_files:
        for batch in read_chunks(filepath):
            shard_path = os.path.join(out_dir, f"ais_shard_{shard_index:04d}.csv")
            with open(shard_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(batch)
            shard_paths.append(shard_path)
            shard_index += 1

    print(f"Created {len(shard_paths)} shards in '{out_dir}/'")
    return shard_paths

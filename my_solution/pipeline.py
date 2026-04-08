# pipeline.py
import glob
import gc
import json
import os
import time
import csv
import multiprocessing
from datetime import datetime
from collections import defaultdict

from anomalies import (
    process_shard, 
    detect_going_dark, 
    detect_draft_change, 
    detect_teleportation
)
from partition import partition_all
from loiter import run_loiter
from scoring import run_scoring
from config import NUM_WORKERS, ANALYSIS_DIR, LOITERING_DIR


def merge_event_csvs(pattern, out_path):
    input_paths = sorted(glob.glob(pattern))
    if not input_paths:
        print(f"  No files found matching {pattern}")
        return

    headers_written = False
    with open(out_path, "w", newline="", encoding="utf-8") as out_f:
        writer = None
        for path in input_paths:
            with open(path, "r", encoding="utf-8", errors="ignore") as in_f:
                reader = csv.DictReader(in_f)
                if not headers_written:
                    writer = csv.DictWriter(out_f, fieldnames=reader.fieldnames,
                                            extrasaction="ignore")
                    writer.writeheader()
                    headers_written = True
                for row in reader:
                    writer.writerow(row)

    print(f"  Merged {len(input_paths)} files -> {out_path}")


# *** CHANGED *** Added `workers=None` to the function signature
def run_pipeline(data_glob=None, output_dir=".", workers=None):
    from config import DATA_ARCH_GLOB
    if data_glob is None:
        data_glob = DATA_ARCH_GLOB

    # *** CHANGED *** Determine if we are using the benchmark's worker count or the config's
    active_workers = workers if workers is not None else NUM_WORKERS

    start_time = time.time()
    metadata   = {
        "run_start":   datetime.utcnow().isoformat(),
        "data_glob":   data_glob,
        "num_workers": active_workers,  # *** CHANGED *** 
        }

    # ------------------------------------------------------------------ #
    # STAGE 1: Partition raw CSVs into shards
    # ------------------------------------------------------------------ #
    print("\n=== STAGE 1: Partitioning ===")
    shard_paths = partition_all(data_glob=data_glob)
    metadata["num_shards"] = len(shard_paths)

    # ------------------------------------------------------------------ #
    # STAGE 2a: PASS 1 - Fully Parallel Independence
    # ------------------------------------------------------------------ #
    print(f"\n=== STAGE 2a: Detection (PASS 1 - PARALLEL) ===")
    t2 = time.time()

    events_paths  = []
    vessels_paths = []
    loiter_paths  = []
    state_paths   = []

    # *** CHANGED *** Use `active_workers` instead of `NUM_WORKERS`
    with multiprocessing.Pool(processes=active_workers) as pool:
        for ep, vp, lp, sp in pool.imap_unordered(process_shard, shard_paths):
            events_paths.append(ep)
            vessels_paths.append(vp)
            loiter_paths.append(lp)
            state_paths.append(sp)

    # ------------------------------------------------------------------ #
    # STAGE 2b: PASS 2 - Cross-Chunk Boundary Sweep (Sequential)
    # ------------------------------------------------------------------ #
    print(f"\n=== STAGE 2b: Cross-Chunk Boundary Sweep (PASS 2 - SEQUENTIAL) ===")
    prev_last_points = {}
    boundary_events = []
    boundary_counts = defaultdict(lambda: {"A": 0, "C": 0, "D": 0, "points": 0})

    # Shards MUST be processed chronologically
    for shard_path in sorted(shard_paths):
        shard_name = os.path.splitext(os.path.basename(shard_path))[0]
        state_path = os.path.join(ANALYSIS_DIR, f"{shard_name}_state.csv")

        current_firsts = {}
        current_lasts = {}

        with open(state_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    import ast
                    p = {
                        "mmsi":             row["mmsi"],
                        "timestamp_parsed": ast.literal_eval(row["timestamp_parsed"]),
                        "timestamp_str":    row["timestamp_str"],
                        "Latitude":         float(row["lat"]),
                        "Longitude":        float(row["lon"]),
                        "Draught":          float(row["draught"]) if row.get("draught") else None,
                        "SOG":              float(row["sog"]) if row.get("sog") else None,
                    }
                    if row["boundary"] == "first":
                        current_firsts[row["mmsi"]] = p
                    if row["boundary"] == "last":
                        current_lasts[row["mmsi"]] = p
                except Exception:
                    continue

        # Check gap between previous chunk's end and this chunk's start
        for mmsi, first_p in current_firsts.items():
            if mmsi in prev_last_points:
                last_p = prev_last_points[mmsi]
                points_pair = [last_p, first_p]
                
                a_ev = detect_going_dark(points_pair)
                c_ev = detect_draft_change(points_pair)
                d_ev = detect_teleportation(points_pair)

                boundary_events.extend(a_ev)
                boundary_events.extend(c_ev)
                boundary_events.extend(d_ev)

                boundary_counts[mmsi]["A"] += len(a_ev)
                boundary_counts[mmsi]["C"] += len(c_ev)
                boundary_counts[mmsi]["D"] += len(d_ev)

        # Carry forward the last known point. Missing vessels retain their older state.
        for mmsi, last_p in current_lasts.items():
            prev_last_points[mmsi] = last_p

    # Write the boundary results out so Stage 3 picks them up
    if boundary_events:
        bound_ev_path = os.path.join(ANALYSIS_DIR, "boundary_sweep_events.csv")
        event_fields = [
            "anomaly", "mmsi", "ts_start", "ts_end",
            "lat_start", "lon_start", "lat_end", "lon_end",
            "gap_hours", "dist_m", "speed_knots", "hours",
            "draught_before", "draught_after", "change_pct"
        ]
        with open(bound_ev_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=event_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(boundary_events)
        print(f"  Found {len(boundary_events)} anomalies crossing boundaries.")

    if boundary_counts:
        bound_ves_path = os.path.join(ANALYSIS_DIR, "boundary_sweep_vessels.csv")
        with open(bound_ves_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["mmsi", "A", "C", "D", "points"])
            writer.writeheader()
            for mmsi, counts in boundary_counts.items():
                writer.writerow({
                    "mmsi": mmsi,
                    "A": counts["A"], "C": counts["C"], "D": counts["D"], "points": 0
                })
        vessels_paths.append(bound_ves_path)

    metadata["detect_seconds"] = round(time.time() - t2, 2)
    print(f"Detection (Pass 1 & 2) complete in {metadata['detect_seconds']}s")

    # ------------------------------------------------------------------ #
    # STAGE 3: Merge per-shard event CSVs
    # ------------------------------------------------------------------ #
    print("\n=== STAGE 3: Merging event CSVs ===")
    all_events_path = os.path.join(output_dir, "all_anomaly_events.csv")
    merge_event_csvs(os.path.join(ANALYSIS_DIR, "*_events.csv"), all_events_path)

    # ------------------------------------------------------------------ #
    # STAGE 4: Loitering detection (cross-shard, Anomaly B)
    # ------------------------------------------------------------------ #
    print("\n=== STAGE 4: Loitering Detection ===")
    t4       = time.time()
    
    # *** CHANGED *** Pass `active_workers` into `run_loiter` so it can parallelize too
    b_counts = run_loiter(loiter_paths, out_dir=LOITERING_DIR, workers=active_workers)

    all_loiter_path = os.path.join(output_dir, "all_loitering_events.csv")
    merge_event_csvs(os.path.join(LOITERING_DIR, "loitering_events.csv"), all_loiter_path)
    metadata["loiter_seconds"] = round(time.time() - t4, 2)

    # ------------------------------------------------------------------ #
    # STAGE 5: Scoring
    # ------------------------------------------------------------------ #
    print("\n=== STAGE 5: Scoring ===")
    sorted_vessels = run_scoring(vessels_paths, b_counts, out_dir=output_dir)

    metadata["run_end"]        = datetime.utcnow().isoformat()
    metadata["total_seconds"]  = round(time.time() - start_time, 2)
    metadata["vessels_scored"] = len(sorted_vessels)

    meta_path = os.path.join(output_dir, "run_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nRun metadata -> {meta_path}")
    print(f"Total pipeline time: {metadata['total_seconds']}s")

    return sorted_vessels
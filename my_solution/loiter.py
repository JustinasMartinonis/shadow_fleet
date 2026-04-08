# loiter.py
# Anomaly B: Loitering detection:
#   MAP: for each 2-min timestamp bucket, find vessel pairs within 500m
#   REDUCE: track how long each pair stays close
#   FLAG: pairs that stay within 500m for > 2 continuous hours

import ast
import csv
import math
import multiprocessing
import os
from collections import defaultdict
from datetime import datetime

from geo import haversine, time_diff_hours
from config import LOITERING_DIR, LOITER_PROX_M, LOITER_MIN_HOURS


def _brute_force_pairs(snapshot, prox_m):
    """
    Fallback spatial search when scipy is unavailable, compares all pairs with haversine directly - O(n²)
    """
    close_pairs = set()
    for i in range(len(snapshot)):
        for j in range(i + 1, len(snapshot)):
            c1, c2 = snapshot[i], snapshot[j]
            if c1["mmsi"] == c2["mmsi"]:
                continue
            if haversine(c1["lat"], c1["lon"], c2["lat"], c2["lon"]) <= prox_m:
                close_pairs.add((i, j))
    return close_pairs


def process_snapshot(args):
    """
    MAP: takes one timestamp snapshot, finds all vessel pairs within LOITER_PROX_M, returns them as sorted (mmsi1, mmsi2) tuples
    """
    ts, snapshot, lat_scale, lon_scale, use_kdtree, prox_m = args

    if use_kdtree:
        import numpy as np
        from scipy.spatial import cKDTree
        coords = np.array([
            [c["lat"] * lat_scale, c["lon"] * lon_scale]
            for c in snapshot
        ])
        tree        = cKDTree(coords)
        index_pairs = tree.query_pairs(r=prox_m)
    else:
        index_pairs = _brute_force_pairs(snapshot, prox_m)

    # haversine verification on KDTree candidates
    close_set = set()
    for i, j in index_pairs:
        c1, c2 = snapshot[i], snapshot[j]
        if c1["mmsi"] == c2["mmsi"]:
            continue
        if haversine(c1["lat"], c1["lon"], c2["lat"], c2["lon"]) <= prox_m:
            close_set.add(tuple(sorted([c1["mmsi"], c2["mmsi"]])))

    mmsis_present = {c["mmsi"] for c in snapshot}
    ts_str        = snapshot[0]["timestamp_str"]

    return ts, close_set, mmsis_present, ts_str


def run_loiter(loiter_candidate_paths, out_dir=LOITERING_DIR, workers=None):
    """
    Anomaly B pipeline: reads per-shard candidate files produced by
    anomalies.py and runs the map/reduce loitering detection across all shards
    """
    import config
    active_workers = workers if workers is not None else config.NUM_WORKERS

    try:
        from scipy.spatial import cKDTree
        import numpy as np
        USE_KDTREE = True
    except ImportError:
        print("WARNING: scipy not installed — falling back to brute-force O(n²) search.")
        print("         Install with: pip install scipy")
        USE_KDTREE = False

    os.makedirs(out_dir, exist_ok=True)

    all_candidates = []
    for path in loiter_candidate_paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for row in csv.DictReader(f):
                try:
                    parsed_ts = ast.literal_eval(row["timestamp_parsed"])
                    all_candidates.append({
                        "mmsi":             row["mmsi"],
                        "lat":              float(row["lat"]),
                        "lon":              float(row["lon"]),
                        "timestamp_parsed": parsed_ts,
                        "timestamp_str":    row["timestamp_str"],
                        "dt":               datetime(*parsed_ts),
                    })
                except Exception:
                    continue

    print(f"Loitering: loaded {len(all_candidates):,} candidates from {len(loiter_candidate_paths)} shards")
    if not all_candidates:
        return {}

    # Group by 2-minute timestamp bucket 
    by_timestamp = defaultdict(list)
    for c in all_candidates:
        by_timestamp[c["timestamp_parsed"]].append(c)

    sorted_timestamps = sorted(by_timestamp.keys())
    print(f"  {len(sorted_timestamps)} unique timestamp buckets to process")

    # Pre-calculate degree-to-metre scaling factors for KDTree
    mean_lat  = sum(c["lat"] for c in all_candidates) / len(all_candidates)
    lat_scale = 110540.0
    lon_scale = 111320.0 * math.cos(math.radians(mean_lat))

    # Only buckets with >= 2 vessels can have a pair
    map_inputs = [
        (ts, by_timestamp[ts], lat_scale, lon_scale, USE_KDTREE, LOITER_PROX_M)
        for ts in sorted_timestamps
        if len(by_timestamp[ts]) >= 2
    ]

    print(f"  -> Mapping {len(map_inputs)} snapshots across {active_workers} core(s)...")
    if active_workers > 1:
        with multiprocessing.Pool(processes=active_workers) as pool:
            processed_snapshots = pool.map(process_snapshot, map_inputs)
    else:
        processed_snapshots = [process_snapshot(inp) for inp in map_inputs]

    # Reduce: sequential streak tracking 
    print("  -> Reducing to loitering streaks...")
    streaks          = {}   
    loitering_events = []

    for ts, close_set, mmsis_present, ts_str in processed_snapshots:
        active_pairs = {
            pair for pair in streaks
            if pair[0] in mmsis_present and pair[1] in mmsis_present
        }
        all_relevant = active_pairs | close_set

        for pair in all_relevant:
            if pair in close_set:
                # Both present AND within 500m: start or extend streak
                if pair not in streaks:
                    streaks[pair] = {
                        "start_str":    ts_str,
                        "start_parsed": ts,
                        "last_str":     ts_str,
                        "last_parsed":  ts,
                        "flagged":      False,
                    }
                else:
                    streaks[pair]["last_str"]    = ts_str
                    streaks[pair]["last_parsed"] = ts

                    hours = time_diff_hours(
                        streaks[pair]["start_parsed"],
                        streaks[pair]["last_parsed"],
                    )
                    if hours > LOITER_MIN_HOURS and not streaks[pair]["flagged"]:
                        streaks[pair]["flagged"] = True
                        loitering_events.append({
                            "mmsi1":      pair[0],
                            "mmsi2":      pair[1],
                            "ts_start":   streaks[pair]["start_str"],
                            "ts_end":     streaks[pair]["last_str"],
                            "duration_h": round(hours, 2),
                        })
            else:
                streaks.pop(pair, None)

    # Count B flags per vessel
    b_counts = defaultdict(int)
    for pair, streak in streaks.items():
        if streak["flagged"]:
            b_counts[pair[0]] += 1
            b_counts[pair[1]] += 1

    # outputs
    events_path = os.path.join(out_dir, "loitering_events.csv")
    with open(events_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mmsi1", "mmsi2", "ts_start", "ts_end", "duration_h"])
        writer.writeheader()
        writer.writerows(loitering_events)

    agg_path = os.path.join(out_dir, "loitering_aggregates.csv")
    with open(agg_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mmsi", "B"])
        writer.writeheader()
        for mmsi, b in b_counts.items():
            writer.writerow({"mmsi": mmsi, "B": b})

    print(f"  Loitering: {len(loitering_events)} encounters flagged across {len(b_counts)} vessels")
    return dict(b_counts)

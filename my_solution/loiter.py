# loiter.py
import csv
import os
import ast
import math
import multiprocessing
from collections import defaultdict
from datetime import datetime
from geo import haversine, time_diff_hours
from config import LOITERING_DIR, LOITER_PROX_M, LOITER_MIN_HOURS


def _brute_force_pairs(snapshot, max_dist_m):
    """Fallback when scipy is unavailable."""
    pairs = set()
    for i in range(len(snapshot)):
        for j in range(i + 1, len(snapshot)):
            pairs.add((i, j))
    return pairs


def process_snapshot(args):
    """
    MAP STEP (Parallelized): 
    Takes a single timestamp snapshot, builds the KDTree, and calculates Haversine distances.
    Returns the pre-calculated close pairs for the Reduce step.
    """
    ts, snapshot, lat_scale, lon_scale, use_kdtree, prox_m = args
    close_set = set()
    mmsis_present = set(c["mmsi"] for c in snapshot)
    ts_str = snapshot[0]["timestamp_str"]

    if use_kdtree:
        import numpy as np
        from scipy.spatial import cKDTree
        coords = np.array([
            [c["lat"] * lat_scale, c["lon"] * lon_scale]
            for c in snapshot
        ])
        tree = cKDTree(coords)
        close_pairs = tree.query_pairs(r=prox_m)
    else:
        close_pairs = _brute_force_pairs(snapshot, prox_m)

    # Exact haversine verification
    for i, j in close_pairs:
        c1 = snapshot[i]
        c2 = snapshot[j]
        if c1["mmsi"] == c2["mmsi"]:
            continue
        dist = haversine(c1["lat"], c1["lon"], c2["lat"], c2["lon"])
        if dist <= prox_m:
            pair = tuple(sorted([c1["mmsi"], c2["mmsi"]]))
            close_set.add(pair)

    return ts, close_set, mmsis_present, ts_str


def run_loiter(loiter_candidate_paths, out_dir=LOITERING_DIR, workers=None):
    """
    Reads candidates, utilizes parallel MapReduce for spatial proximity checks,
    and sequentially tracks duration streaks.
    """
    import config
    active_workers = workers if workers is not None else config.NUM_WORKERS

    try:
        from scipy.spatial import cKDTree
        import numpy as np
        USE_KDTREE = True
    except ImportError:
        print("WARNING: scipy not installed. Run: pip install scipy")
        USE_KDTREE = False

    os.makedirs(out_dir, exist_ok=True)

    # --- 1. Load all candidates ---
    all_candidates = []
    for path in loiter_candidate_paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    parsed = ast.literal_eval(row["timestamp_parsed"])
                    all_candidates.append({
                        "mmsi":             row["mmsi"],
                        "lat":              float(row["lat"]),
                        "lon":              float(row["lon"]),
                        "timestamp_parsed": parsed,
                        "timestamp_str":    row["timestamp_str"],
                        "dt":               datetime(*parsed),
                    })
                except Exception:
                    continue

    print(f"Loaded {len(all_candidates)} loiter candidates from {len(loiter_candidate_paths)} shards")

    if not all_candidates:
        return {}

    # --- 2. Group candidates by timestamp bucket ---
    by_timestamp = defaultdict(list)
    for c in all_candidates:
        by_timestamp[c["timestamp_parsed"]].append(c)

    sorted_timestamps = sorted(by_timestamp.keys())
    print(f"Processing {len(sorted_timestamps)} unique timestamp buckets...")

    # Pre-calculate geographic scales
    mean_lat = sum(c["lat"] for c in all_candidates) / len(all_candidates)
    lat_scale = 110540.0
    lon_scale = 111320.0 * math.cos(math.radians(mean_lat))

    # --- 3. MAP PHASE: Parallel Spatial Math ---
    map_inputs = []
    for ts in sorted_timestamps:
        if len(by_timestamp[ts]) >= 2:
            map_inputs.append((ts, by_timestamp[ts], lat_scale, lon_scale, USE_KDTREE, LOITER_PROX_M))

    print(f"  -> Mapping KDTree calculations across {active_workers} cores...")
    if active_workers > 1:
        with multiprocessing.Pool(processes=active_workers) as pool:
            # pool.map guarantees output order matches input chronological order
            processed_snapshots = pool.map(process_snapshot, map_inputs)
    else:
        processed_snapshots = [process_snapshot(inp) for inp in map_inputs]

    # --- 4. REDUCE PHASE: Sequential Streak Tracking ---
    print("  -> Reducing spatial data to calculate loitering streaks...")
    streaks = {}
    loitering_events = []

    for ts, close_set, mmsis_present, ts_str in processed_snapshots:
        # Find all active streaks where both vessels are present at this timestamp
        pairs_both_present = set()
        for pair in list(streaks.keys()):
            if pair[0] in mmsis_present and pair[1] in mmsis_present:
                pairs_both_present.add(pair)

        # Also add new pairs from close_set that aren't tracked yet
        for pair in close_set:
            pairs_both_present.add(pair)

        for pair in pairs_both_present:
            if pair in close_set:
                # Both present AND within 500m — streak continues or starts
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

                    # Check if continuous duration exceeds threshold
                    hours = time_diff_hours(
                        streaks[pair]["start_parsed"],
                        streaks[pair]["last_parsed"]
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
                # Both present BUT outside 500m — RESET the streak
                if pair in streaks:
                    del streaks[pair]

    # --- 5. Build per-vessel B counts ---
    b_counts = defaultdict(int)
    for pair, streak in streaks.items():
        if streak["flagged"]:
            b_counts[pair[0]] += 1
            b_counts[pair[1]] += 1

    # --- 6. Write outputs ---
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

    print(f"Loitering: {len(loitering_events)} encounters flagged across {len(b_counts)} vessels")
    
    return dict(b_counts)
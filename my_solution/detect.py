# detect.py
import csv
import os
from collections import defaultdict
from parsing import is_valid_row, parse_row, downsample_bucket
from models import detect_going_dark, detect_draft_change, detect_teleportation, build_loiter_candidates
from config import ANALYSIS_DIR


def save_boundary_state(vessels, state_path):
    """
    Saves the first and last point for each vessel in this shard.
    Used by Pass 2 to detect anomalies across chunk boundaries.
    """
    with open(state_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "mmsi", "boundary", "timestamp_parsed", "timestamp_str", "lat", "lon", "draught", "sog"
        ])
        writer.writeheader()
        for mmsi, points in vessels.items():
            if not points: continue
            
            # Save the first and last points of the chunk
            for b_type, p in [("first", points[0]), ("last", points[-1])]:
                writer.writerow({
                    "mmsi":             mmsi,
                    "boundary":         b_type,
                    "timestamp_parsed": p["timestamp_parsed"],
                    "timestamp_str":    p["timestamp_str"],
                    "lat":              p["Latitude"],
                    "lon":              p["Longitude"],
                    "draught":          p.get("Draught") or "",
                    "sog":              p.get("SOG") or "",
                })


def process_shard(shard_path):
    """
    PASS 1: Reads one shard CSV, runs anomaly detection for A, C, D internally,
    and saves boundary states for Pass 2.
    """
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    shard_name = os.path.splitext(os.path.basename(shard_path))[0]

    events_path    = os.path.join(ANALYSIS_DIR, f"{shard_name}_events.csv")
    vessels_path   = os.path.join(ANALYSIS_DIR, f"{shard_name}_vessels.csv")
    loiter_path    = os.path.join(ANALYSIS_DIR, f"{shard_name}_loiter_candidates.csv")
    new_state_path = os.path.join(ANALYSIS_DIR, f"{shard_name}_state.csv")

    vessels      = defaultdict(list)
    seen_buckets = set()

    with open(shard_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not is_valid_row(row):
                continue
            parsed = parse_row(row)
            if parsed is None:
                continue
            bucket = downsample_bucket(parsed["timestamp_parsed"])
            key    = (parsed["mmsi"], bucket)
            if key in seen_buckets:
                continue
            seen_buckets.add(key)
            vessels[parsed["mmsi"]].append(parsed)

    for mmsi in vessels:
        vessels[mmsi].sort(key=lambda x: x["timestamp_parsed"])

    all_events    = []
    vessel_counts = {}
    loiter_cands  = []

    for mmsi, points in vessels.items():
        a_events = detect_going_dark(points)
        c_events = detect_draft_change(points)
        d_events = detect_teleportation(points)

        all_events.extend(a_events)
        all_events.extend(c_events)
        all_events.extend(d_events)

        vessel_counts[mmsi] = {
            "mmsi":   mmsi,
            "A":      len(a_events),
            "C":      len(c_events),
            "D":      len(d_events),
            "points": len(points),
        }

        loiter_cands.extend(build_loiter_candidates(points))

    # Save the boundary edges for Pass 2
    save_boundary_state(vessels, new_state_path)

    event_fields = [
        "anomaly", "mmsi", "ts_start", "ts_end",
        "lat_start", "lon_start", "lat_end", "lon_end",
        "gap_hours", "dist_m", "speed_knots", "hours",
        "draught_before", "draught_after", "change_pct"
    ]
    with open(events_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=event_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_events)

    with open(vessels_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["mmsi", "A", "C", "D", "points"])
        writer.writeheader()
        writer.writerows(vessel_counts.values())

    with open(loiter_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "mmsi", "lat", "lon", "timestamp_parsed", "timestamp_str", "grid_id"
        ])
        writer.writeheader()
        for c in loiter_cands:
            writer.writerow({
                "mmsi":             c["mmsi"],
                "lat":              c["lat"],
                "lon":              c["lon"],
                "timestamp_parsed": c["timestamp_parsed"],
                "timestamp_str":    c["timestamp_str"],
                "grid_id":          c["grid_id"],
            })

    print(f"  [{shard_name}] {len(vessels)} vessels | {len(all_events)} events")

    return events_path, vessels_path, loiter_path, new_state_path
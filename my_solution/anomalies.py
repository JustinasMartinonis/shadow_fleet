# anomalies.py
# All anomaly detection logic in one place.
#
# Structure:
#   - Four detector functions (A, C, D, and B candidates) — pure logic, no file I/O
#   - process_shard() — reads one shard CSV, runs detectors, writes results to disk
#   - _save_boundary_state() — saves first/last vessel point per shard for the
#     boundary sweep in pipeline.py

import csv
import os
from collections import defaultdict

from parsing import parse_row, downsample_bucket
from geo import haversine, time_diff_hours, implied_speed_knots
from config import (
    ANALYSIS_DIR,
    GOING_DARK_HOURS,
    DRAFT_MIN_HOURS, DRAFT_CHANGE_PCT,
    TELEPORT_KNOTS,
    LOITER_SPEED_KNOTS,
)


# ─────────────────────────────────────────────────────────────────────────────
# ANOMALY DETECTORS  (pure functions)
# Each takes a chronologically sorted list of parsed points for one vessel.
# Each returns a list of event dicts (empty list if nothing detected).
# ─────────────────────────────────────────────────────────────────────────────

def detect_going_dark(points):
    """
    Anomaly A — AIS gap > 4 hours where the vessel kept moving.
    Requires dist > 1 NM AND implied speed > 0.5 kn to exclude anchored vessels.
    """
    events = []
    if len(points) < 2:
        return events

    for i in range(1, len(points)):
        p1, p2    = points[i - 1], points[i]
        gap_hours = time_diff_hours(p1["timestamp_parsed"], p2["timestamp_parsed"])

        if gap_hours <= GOING_DARK_HOURS:
            continue

        dist_m      = haversine(p1["Latitude"], p1["Longitude"], p2["Latitude"], p2["Longitude"])
        dist_nm     = dist_m / 1852.0
        speed_knots = dist_nm / gap_hours if gap_hours > 0 else 0

        # Must imply movement — rules out vessels simply sitting at anchor
        if dist_m > 1852.0 and speed_knots > 0.5:
            events.append({
                "anomaly":     "A",
                "mmsi":        p1["mmsi"],
                "ts_start":    p1["timestamp_str"],
                "ts_end":      p2["timestamp_str"],
                "lat_start":   p1["Latitude"],
                "lon_start":   p1["Longitude"],
                "lat_end":     p2["Latitude"],
                "lon_end":     p2["Longitude"],
                "gap_hours":   round(gap_hours, 2),
                "dist_m":      round(dist_m, 1),
                "speed_knots": round(speed_knots, 2),
            })
    return events


def detect_draft_change(points):
    """
    Anomaly C — Draught changes > 5% during a gap > 2 hours.
    Implies cargo was loaded/unloaded at sea (illegal STS transfer).
    """
    events = []
    for i in range(1, len(points)):
        prev, curr = points[i - 1], points[i]
        d1 = prev.get("Draught") or 0
        d2 = curr.get("Draught") or 0

        if d1 <= 0 or d2 <= 0:
            continue

        hours = time_diff_hours(prev["timestamp_parsed"], curr["timestamp_parsed"])
        if hours < DRAFT_MIN_HOURS:
            continue

        if abs(d2 - d1) / d1 > DRAFT_CHANGE_PCT:
            events.append({
                "anomaly":        "C",
                "mmsi":           curr["mmsi"],
                "ts_start":       prev["timestamp_str"],
                "ts_end":         curr["timestamp_str"],
                "lat_start":      prev["Latitude"],
                "lon_start":      prev["Longitude"],
                "lat_end":        curr["Latitude"],
                "lon_end":        curr["Longitude"],
                "draught_before": d1,
                "draught_after":  d2,
                "change_pct":     round(abs(d2 - d1) / d1 * 100, 2),
            })
    return events


def detect_teleportation(points):
    """
    Anomaly D — Implied speed between two consecutive pings exceeds 60 knots.
    Physically impossible for a ship → same MMSI broadcast by two vessels (identity cloning).
    """
    events = []
    for i in range(1, len(points)):
        prev, curr = points[i - 1], points[i]
        hours = time_diff_hours(prev["timestamp_parsed"], curr["timestamp_parsed"])

        if hours <= 0:
            continue

        dist_m = haversine(prev["Latitude"], prev["Longitude"], curr["Latitude"], curr["Longitude"])
        speed  = implied_speed_knots(dist_m, hours)

        if speed > TELEPORT_KNOTS:
            events.append({
                "anomaly":     "D",
                "mmsi":        curr["mmsi"],
                "ts_start":    prev["timestamp_str"],
                "ts_end":      curr["timestamp_str"],
                "lat_start":   prev["Latitude"],
                "lon_start":   prev["Longitude"],
                "lat_end":     curr["Latitude"],
                "lon_end":     curr["Longitude"],
                "speed_knots": round(speed, 1),
                "dist_m":      round(dist_m, 1),
                "hours":       round(hours, 4),
            })
    return events


def build_loiter_candidates(points):
    """
    Anomaly B (prep) — Collects points where a vessel is moving slowly (< 1 kn)
    but not anchored (reported SOG > 0). These candidates are passed to loiter.py
    which checks for pairs of vessels near each other for > 2 continuous hours.

    Hybrid filter:
      - reported SOG > 0   → engine is running, vessel is not at anchor/moored
      - haversine speed < 1 kn → vessel is barely moving (loitering threshold)
    """
    candidates = []
    if len(points) < 2:
        return candidates

    for i in range(1, len(points)):
        p1, p2    = points[i - 1], points[i]
        gap_hours = time_diff_hours(p1["timestamp_parsed"], p2["timestamp_parsed"])

        if gap_hours <= 0:
            continue

        dist_m       = haversine(p1["Latitude"], p1["Longitude"], p2["Latitude"], p2["Longitude"])
        speed_knots  = (dist_m / 1852.0) / gap_hours
        reported_sog = p2.get("SOG")

        if reported_sog is not None and reported_sog > 0 and speed_knots < LOITER_SPEED_KNOTS:
            candidates.append({
                "mmsi":             p2["mmsi"],
                "lat":              p2["Latitude"],
                "lon":              p2["Longitude"],
                "timestamp_parsed": p2["timestamp_parsed"],
                "timestamp_str":    p2["timestamp_str"],
                "grid_id":          "",
            })
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# SHARD WORKER  (called in parallel by pipeline.py)
# ─────────────────────────────────────────────────────────────────────────────

def _save_boundary_state(vessels, state_path):
    """
    Saves the first and last ping for each vessel in this shard.
    pipeline.py reads these to detect anomalies that span two shard boundaries.
    """
    fields = ["mmsi", "boundary", "timestamp_parsed", "timestamp_str", "lat", "lon", "draught", "sog"]
    with open(state_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for mmsi, points in vessels.items():
            if not points:
                continue
            for label, p in [("first", points[0]), ("last", points[-1])]:
                writer.writerow({
                    "mmsi":             mmsi,
                    "boundary":         label,
                    "timestamp_parsed": p["timestamp_parsed"],
                    "timestamp_str":    p["timestamp_str"],
                    "lat":              p["Latitude"],
                    "lon":              p["Longitude"],
                    "draught":          p.get("Draught") or "",
                    "sog":              p.get("SOG") or "",
                })


def process_shard(shard_path):
    """
    Worker function — runs in a separate process for each shard.

    Steps:
      1. Read shard CSV and group pings by MMSI
      2. Sort each vessel's pings chronologically
      3. Run detectors A, C, D on each vessel's track
      4. Collect loiter candidates for Anomaly B (handled globally in loiter.py)
      5. Save boundary state (first/last ping) for cross-shard gap detection
      6. Write per-shard results to disk, return output paths to pipeline.py

    Returns: (events_path, vessels_path, loiter_path, state_path)
    """
    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    shard_name = os.path.splitext(os.path.basename(shard_path))[0]

    events_path  = os.path.join(ANALYSIS_DIR, f"{shard_name}_events.csv")
    vessels_path = os.path.join(ANALYSIS_DIR, f"{shard_name}_vessels.csv")
    loiter_path  = os.path.join(ANALYSIS_DIR, f"{shard_name}_loiter_candidates.csv")
    state_path   = os.path.join(ANALYSIS_DIR, f"{shard_name}_state.csv")

    # Step 1 & 2: Read, group by MMSI, deduplicate, sort chronologically
    vessels      = defaultdict(list)
    seen_buckets = set()

    with open(shard_path, "r", encoding="utf-8", errors="ignore") as f:
        for row in csv.DictReader(f):
            parsed = parse_row(row)
            if parsed is None:
                continue
            key = (parsed["mmsi"], downsample_bucket(parsed["timestamp_parsed"]))
            if key in seen_buckets:
                continue
            seen_buckets.add(key)
            vessels[parsed["mmsi"]].append(parsed)

    for pts in vessels.values():
        pts.sort(key=lambda x: x["timestamp_parsed"])

    # Step 3 & 4: Run detectors on each vessel's full track
    all_events    = []
    vessel_counts = {}
    loiter_cands  = []

    for mmsi, points in vessels.items():
        a_events = detect_going_dark(points)
        c_events = detect_draft_change(points)
        d_events = detect_teleportation(points)

        all_events.extend(a_events + c_events + d_events)

        vessel_counts[mmsi] = {
            "mmsi":   mmsi,
            "A":      len(a_events),
            "C":      len(c_events),
            "D":      len(d_events),
            "points": len(points),
        }

        loiter_cands.extend(build_loiter_candidates(points))

    # Step 5: Save boundary edges so pipeline.py can check cross-shard gaps
    _save_boundary_state(vessels, state_path)

    # Step 6: Write results to disk
    event_fields = [
        "anomaly", "mmsi", "ts_start", "ts_end",
        "lat_start", "lon_start", "lat_end", "lon_end",
        "gap_hours", "dist_m", "speed_knots", "hours",
        "draught_before", "draught_after", "change_pct",
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
            writer.writerow(c)

    print(f"  [{shard_name}] {len(vessels)} vessels | {len(all_events)} events")
    return events_path, vessels_path, loiter_path, state_path
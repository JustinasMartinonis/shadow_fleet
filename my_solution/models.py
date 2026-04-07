# models.py
# Anomaly detection logic — pure functions operating on sorted vessel track points
from geo import haversine, time_diff_hours, implied_speed_knots
from config import (
    GOING_DARK_HOURS,
    LOITER_SPEED_KNOTS, LOITER_GRID_DEG, LOITER_PROX_M,
    DRAFT_MIN_HOURS, DRAFT_CHANGE_PCT,
    TELEPORT_KNOTS,
)


def detect_going_dark(points):
    """
    Anomaly A: AIS gap > 4 hours where the vessel KEPT MOVING.
    Filters out anchored vessels by requiring a minimum distance traveled.
    """
    events = []
    if len(points) < 2:
        return events

    for i in range(1, len(points)):
        p1 = points[i-1]
        p2 = points[i]
        
        gap_hours = time_diff_hours(p1["timestamp_parsed"], p2["timestamp_parsed"])
        
        # Rule 1: Gap must be > 4 hours
        if gap_hours > 4.0:
            dist_m = haversine(p1["Latitude"], p1["Longitude"], p2["Latitude"], p2["Longitude"])
            
            # Convert meters to nautical miles (1 NM = 1852 meters) to find speed
            dist_nm = dist_m / 1852.0
            speed_knots = dist_nm / gap_hours if gap_hours > 0 else 0
            
            # Rule 2: Must imply movement (Not simply anchored)
            # We enforce a minimum distance of 1 NM (1852m) and a speed of > 0.5 knots
            if dist_m > 1852.0 and speed_knots > 0.5:
                events.append({
                    "anomaly": "A",
                    "mmsi": p1["mmsi"],
                    "ts_start": p1["timestamp_str"],
                    "ts_end": p2["timestamp_str"],
                    "lat_start": p1["Latitude"],
                    "lon_start": p1["Longitude"],
                    "lat_end": p2["Latitude"],
                    "lon_end": p2["Longitude"],
                    "gap_hours": round(gap_hours, 2),
                    "dist_m": round(dist_m, 1),
                    "speed_knots": round(speed_knots, 2)  # This fixes your 'nan' bug!
                })
    return events


def detect_draft_change(points):
    """
    Anomaly C: Significant draught change between two points > DRAFT_MIN_HOURS apart.
    Suggests loading/offloading cargo at sea (STS transfer).

    points: list of dicts sorted by timestamp_parsed
    Returns: list of event dicts
    """
    events = []
    for i in range(1, len(points)):
        prev = points[i - 1]
        curr = points[i]
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
    Anomaly D: Implied speed between two consecutive points exceeds physical limit.

    points: list of dicts sorted by timestamp_parsed
    Returns: list of event dicts
    """
    events = []
    for i in range(1, len(points)):
        prev = points[i - 1]
        curr = points[i]
        hours = time_diff_hours(prev["timestamp_parsed"], curr["timestamp_parsed"])
        if hours <= 0:
            continue
        dist_m = haversine(prev["Latitude"], prev["Longitude"],
                           curr["Latitude"], curr["Longitude"])
        speed  = implied_speed_knots(dist_m, hours)
        if speed > TELEPORT_KNOTS:
            events.append({
                "anomaly":      "D",
                "mmsi":         curr["mmsi"],
                "ts_start":     prev["timestamp_str"],
                "ts_end":       curr["timestamp_str"],
                "lat_start":    prev["Latitude"],
                "lon_start":    prev["Longitude"],
                "lat_end":      curr["Latitude"],
                "lon_end":      curr["Longitude"],
                "speed_knots":  round(speed, 1),
                "dist_m":       round(dist_m, 1),
                "hours":        round(hours, 4),
            })
    return events


def build_loiter_candidates(points):
    """
    Builds candidates for Anomaly B (Loitering).
    Filters points to only include moments where the vessel is effectively stationary.
    Calculates speed mathematically via Haversine rather than trusting the AIS SOG field.
    """
    candidates = []
    
    # We need at least 2 points to calculate a speed
    if len(points) < 2:
        return candidates

    for i in range(1, len(points)):
        p1 = points[i-1]
        p2 = points[i]
        
        gap_hours = time_diff_hours(p1["timestamp_parsed"], p2["timestamp_parsed"])
        
        # Avoid division by zero if two pings happen at the exact same second
        if gap_hours > 0:
            dist_m = haversine(p1["Latitude"], p1["Longitude"], p2["Latitude"], p2["Longitude"])
            
            # Convert meters to Nautical Miles (1 NM = 1852 meters)
            dist_nm = dist_m / 1852.0
            
            # Calculate actual speed over the curvature of the earth
            speed_knots = dist_nm / gap_hours
            
            # Rule: Speed must be < 1 knot to be considered "loitering"
            if speed_knots < 1.0:
                candidates.append({
                    "mmsi":             p2["mmsi"],
                    "lat":              p2["Latitude"],
                    "lon":              p2["Longitude"],
                    "timestamp_parsed": p2["timestamp_parsed"],
                    "timestamp_str":    p2["timestamp_str"],
                    "grid_id":          ""  # Placeholder if your code expects this key
                })
                
    return candidates

from collections import defaultdict
from utils import fast_parse, haversine, time_diff_hours

def default_stats():
    return {"A": 0, "C": 0, "D": 0}


def process_chunk(rows):
    """
    Process one chunk of AIS rows.

    Returns (4 values):
        vessel_stats  : dict[mmsi] -> {"A","C","D"} anomaly counts within this chunk
        loiter_points : flat list of (bucket, mmsi, lat, lon, sog) for B detection in main
        vessel_last   : dict[mmsi] -> last state seen in this chunk (for boundary check)
        vessel_first  : dict[mmsi] -> first state seen in this chunk (for boundary check)
    """

    vessel_last  = {}
    vessel_first = {}
    vessel_stats = defaultdict(default_stats)
    loiter_points = []

    for row in rows:
        try:
            mmsi, timestamp, lat, lon, sog, draught = row
        except:
            continue

        if sog is None:
            sog = 0.0
        if draught is None:
            draught = 0.0

        t = fast_parse(timestamp)

        # ---- LOITERING BUCKET ----
        bucket = (t[0], t[1], t[2], t[3], t[4] // 2)
        loiter_points.append((bucket, mmsi, lat, lon, sog))

        state = {"t": t, "lat": lat, "lon": lon, "draught": draught}

        # Record the first time we see the vessel in the chunk
        if mmsi not in vessel_first:
            vessel_first[mmsi] = state

    
        # Compares current ping to previous ping for same vessel WITHIN this chunk.
        # Gaps spanning chunk boundaries are caught in main.py via check_boundary().
        if mmsi in vessel_last:
            prev = vessel_last[mmsi]

            hours = time_diff_hours(prev["t"], t)
            dist  = haversine(prev["lat"], prev["lon"], lat, lon)

            # A: Going Dark — AIS gap > 4h AND vessel moved (not anchored)
            if hours > 4 and dist > 5000:
                vessel_stats[mmsi]["A"] += 1

            # C: Draft Change at Sea — draught changed >5% during gap > 2h
            if hours > 2 and prev["draught"] > 0:
                if abs(draught - prev["draught"]) / prev["draught"] > 0.05:
                    vessel_stats[mmsi]["C"] += 1

            # D: Teleportation — implied speed > 60 knots
            if hours > 0:
                speed_knots = (dist / 1852) / hours
                if speed_knots > 60:
                    vessel_stats[mmsi]["D"] += 1

        # Always update last state
        vessel_last[mmsi] = state

    # Return plain dicts — defaultdict cannot be safely returned across processes
    return dict(vessel_stats), loiter_points, dict(vessel_last), dict(vessel_first)

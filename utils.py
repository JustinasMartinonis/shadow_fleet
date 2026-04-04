# utils.py
import math
from datetime import datetime, timedelta
from scipy.spatial import cKDTree

# ---- Faster custom datetime parser ----
def fast_parse(ts):
    # DD/MM/YYYY HH:MM:SS
    year = int(ts[6:10])
    month = int(ts[3:5])
    day = int(ts[0:2])
    hour = int(ts[11:13])
    minute = int(ts[14:16])
    second = int(ts[17:19])
    return (year, month, day, hour, minute, second)

def time_diff_hours(t1, t2):
    dt1 = datetime(*t1)
    dt2 = datetime(*t2)
    return (dt2 - dt1).total_seconds() / 3600

# ---- Haversine distance ----
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # meters
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

# Detect loitering for Anomaly B
def detect_loitering(events):
    """
    Detect two distinct vessels within 500m with SOG < 1 knot for >2 hours.
    Uses sliding time window + KDTree spatial search.
    """
    B = 0
    loitering_pairs = []

    # Only consider slow vessels
    filtered = [e for e in events if e.get("SOG", 0) < 1]

    # Sort by timestamp
    filtered.sort(key=lambda x: x["timestamp_parsed"])

    counted_pairs = set()
    n = len(filtered)
    i = 0

    while i < n:
        start_time = datetime(*filtered[i]["timestamp_parsed"])
        window_end = start_time + timedelta(hours=2)
        window_points = []
        j = i
        while j < n:
            t = datetime(*filtered[j]["timestamp_parsed"])
            if t > window_end:
                break
            window_points.append(filtered[j])
            j += 1

        if len(window_points) < 2:
            i += 1
            continue

        coords = [(p["Latitude"], p["Longitude"]) for p in window_points]
        tree = cKDTree(coords)
        for idx, p1 in enumerate(window_points):
            neighbors = tree.query_ball_point(
                [p1["Latitude"], p1["Longitude"]],
                r=0.0045   # ≈ 500m in degrees
            )
            for nb in neighbors:
                if nb == idx:
                    continue
                p2 = window_points[nb]
                if p1["MMSI"] == p2["MMSI"]:
                    continue
                pair = tuple(sorted([p1["MMSI"], p2["MMSI"]]))

                if pair in counted_pairs:
                    continue

                t1 = datetime(*p1["timestamp_parsed"])
                t2 = datetime(*p2["timestamp_parsed"])

                if abs((t2 - t1).total_seconds()) >= 7200:
                    B += 1
                    counted_pairs.add(pair)

                    loitering_pairs.append({
                        "vessel1": p1["MMSI"],
                        "vessel2": p2["MMSI"],
                        "start_time": t1,
                        "end_time": t2,
                        "location": (p1["Latitude"], p1["Longitude"])
                    })
        i += 1
    return B, loitering_pairs

# ---- Detect anomalies for a single vessel ----
def detect_anomalies(events):
    A = C = D = 0
    B = 0
    loitering_pairs = []
    n = len(events)

    for i in range(1, n):
        prev = events[i-1]
        curr = events[i]

        t1 = prev["timestamp_parsed"]
        t2 = curr["timestamp_parsed"]
        hours = time_diff_hours(t1, t2)

        lat1, lon1 = prev["Latitude"], prev["Longitude"]
        lat2, lon2 = curr["Latitude"], curr["Longitude"]
        dist_m = haversine(lat1, lon1, lat2, lon2)

        # A: Going Dark
        if hours > 4 and dist_m > 5000:
            A += 1

        # C: Draft Change
        draft1 = float(prev.get("Draught",0) or 0)
        draft2 = float(curr.get("Draught",0) or 0)
        if hours > 2 and draft1 > 0 and abs(draft2 - draft1)/draft1 > 0.05:
            C += 1

        # D: Teleportation
        speed_knots = (dist_m / 1852)/hours if hours>0 else 0
        if speed_knots > 60:
            D += 1

    # ---- B: Loitering / Ship-to-ship transfers ----
    # check for two points within 500m, speed < 1 knot, duration > 2h
    # disclaimer: currently i am not sure if this works correctly and efficiently - need to look into this B part
    # for i in range(n):
    #     t_start = events[i]["timestamp_parsed"]
    #     lat1, lon1 = events[i]["Latitude"], events[i]["Longitude"]
    #     j = i + 1
    #     while j < n:
    #         t_j = events[j]["timestamp_parsed"]
    #         hours = time_diff_hours(t_start, t_j)
    #         if hours > 2:
    #             break
    #         lat2, lon2 = events[j]["Latitude"], events[j]["Longitude"]
    #         dist_m = haversine(lat1, lon1, lat2, lon2)
    #         sog = dist_m / 1852 / hours if hours > 0 else 0
    #         if dist_m <= 500 and sog <= 1:
    #             B += 1
    #             break  # count only once per loitering window
    #         j += 1

# B: Loitering Updated Version: 
    B, loitering_pairs = detect_loitering(events)
    
    DFSI = A*3 + B*4 + C*2 + D*5
    
    return {"A":A, "B":B, "C":C, "D":D, "DFSI":DFSI, "loitering_pairs": loitering_pairs}

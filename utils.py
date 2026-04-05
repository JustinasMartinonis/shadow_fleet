import math
from datetime import datetime

def fast_parse(ts):
    return (
        int(ts[6:10]), int(ts[3:5]), int(ts[0:2]),
        int(ts[11:13]), int(ts[14:16]), int(ts[17:19])
    )

def time_diff_hours(t1, t2):
    return (datetime(*t2) - datetime(*t1)).total_seconds() / 3600

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))
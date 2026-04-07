# geo.py
# Geospatial utility functions
import math
from datetime import datetime


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculates great-circle distance between two coordinates.
    Returns distance in meters.
    """
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def time_diff_hours(t1, t2):
    """
    Takes two parsed timestamp tuples (y, m, d, h, mi, s), returns difference in hours.
    """
    dt1 = datetime(*t1)
    dt2 = datetime(*t2)
    return (dt2 - dt1).total_seconds() / 3600


def implied_speed_knots(dist_m, hours):
    """
    Returns implied speed in knots given distance in meters and time in hours.
    """
    if hours <= 0:
        return float("inf")
    return (dist_m / 1852) / hours

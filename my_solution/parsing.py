# parsing.py
# Type conversion helpers for pre-cleaned shard rows.
# Validation already happened in partition.py — these functions just parse types.

from config import DOWNSAMPLE_MINUTES


def fast_parse(ts):
    """
    Converts AIS timestamp string to a comparable tuple.
    Format: DD/MM/YYYY HH:MM:SS  →  (year, month, day, hour, minute, second)
    Tuple comparison gives correct chronological ordering with no datetime overhead.
    """
    return (
        int(ts[6:10]),   # year
        int(ts[3:5]),    # month
        int(ts[0:2]),    # day
        int(ts[11:13]),  # hour
        int(ts[14:16]),  # minute
        int(ts[17:19]),  # second
    )


def parse_row(row):
    """
    Converts a clean shard row (dict) into typed fields.
    Returns None only if a float conversion fails (should be rare on clean shards).
    """
    try:
        ts = row["# Timestamp"].strip()
        return {
            "mmsi":             row["MMSI"].strip(),
            "timestamp_parsed": fast_parse(ts),
            "timestamp_str":    ts,
            "Latitude":         float(row["Latitude"]),
            "Longitude":        float(row["Longitude"]),
            "SOG":              float(row["SOG"]) if row.get("SOG", "").strip() else None,
            "Draught":          float(row["Draught"]) if row.get("Draught", "").strip() else None,
        }
    except Exception:
        return None


def downsample_bucket(parsed_time):
    """
    Maps a parsed timestamp to a 2-minute bucket ID.
    Used to deduplicate pings within detect.py (second pass after partition's dedup).
    """
    y, mo, d, h, mi, _ = parsed_time
    total_minutes = y * 525600 + mo * 43800 + d * 1440 + h * 60 + mi
    return total_minutes // DOWNSAMPLE_MINUTES
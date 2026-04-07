# parsing.py
# Row parsing, validation and downsampling logic
from config import DIRTY_MMSI, DOWNSAMPLE_MINUTES


def fast_parse(ts):
    """
    Parses AIS timestamp string into a tuple for fast comparison.
    Format: DD/MM/YYYY HH:MM:SS
    """
    day    = int(ts[0:2])
    month  = int(ts[3:5])
    year   = int(ts[6:10])
    hour   = int(ts[11:13])
    minute = int(ts[14:16])
    second = int(ts[17:19])
    return (year, month, day, hour, minute, second)


def is_valid_row(row):
    """
    Validates a raw AIS CSV row. Returns False if the row should be discarded.
    """
    mmsi = row.get("MMSI", "").strip()
    if not mmsi or mmsi in DIRTY_MMSI or len(mmsi) != 9 or not mmsi.isdigit() or mmsi[0] == "0":
        return False
    if not row.get("# Timestamp"):
        return False
    if row.get("Type of mobile", "").strip() != "Class A":
        return False

    try:
        lat = float(row.get("Latitude", ""))
        lon = float(row.get("Longitude", ""))
        if lat == 0.0 or lon == 0.0:
            return False
        if not (-90.0 <= lat <= 90.0):
            return False
        if not (-180.0 <= lon <= 180.0):
            return False
    except ValueError:
        return False

    rot_str = row.get("ROT", "").strip()
    if rot_str:
        try:
            rot = float(rot_str)
            if rot > 126.0 or rot < -126.0:
                return False
        except ValueError:
            return False

    cog_str = row.get("COG", "").strip()
    if cog_str:
        try:
            cog = float(cog_str)
            if cog >= 360.0:
                return False
        except ValueError:
            return False

    sog_str = row.get("SOG", "").strip()
    if sog_str:
        try:
            sog = float(sog_str)
            if sog > 102.2:
                return False
        except ValueError:
            return False

    heading_str = row.get("Heading", "").strip()
    if heading_str:
        try:
            heading = float(heading_str)
            if heading != 511 and not (0 <= heading <= 359):
                return False
        except ValueError:
            return False

    for dim_col in ["Width", "Length", "A", "B", "C", "D"]:
        val_str = row.get(dim_col, "").strip()
        if val_str:
            try:
                val = float(val_str)
                if val < 0:
                    return False
            except ValueError:
                return False

    return True


def parse_row(row):
    """
    Parses a validated raw row into a clean dict with typed fields.
    Returns None if parsing fails.
    """
    try:
        mmsi    = row["MMSI"].strip()
        ts      = row["# Timestamp"].strip()
        parsed  = fast_parse(ts)
        lat     = float(row["Latitude"])
        lon     = float(row["Longitude"])
        sog     = float(row["SOG"]) if row.get("SOG", "").strip() else None
        draught = float(row["Draught"]) if row.get("Draught", "").strip() else None
        return {
            "mmsi":             mmsi,
            "timestamp_parsed": parsed,
            "timestamp_str":    ts,
            "Latitude":         lat,
            "Longitude":        lon,
            "SOG":              sog,
            "Draught":          draught,
        }
    except Exception:
        return None


def downsample_bucket(parsed_time):
    """
    Returns a bucket ID for 2-minute downsampling.
    """
    y, mo, d, h, mi, s = parsed_time
    t_minutes = y * 525600 + mo * 43800 + d * 1440 + h * 60 + mi
    return t_minutes // DOWNSAMPLE_MINUTES

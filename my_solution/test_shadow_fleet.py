# tests/test_shadow_fleet.py
import sys
import os
import csv
import json
import tempfile
import shutil

# Make sure imports resolve from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from parsing import fast_parse, is_valid_row, parse_row, downsample_bucket
from geo import haversine, time_diff_hours, implied_speed_knots
from models import detect_going_dark, detect_draft_change, detect_teleportation, build_loiter_candidates
from loiter import run_loiter
from pipeline import run_pipeline


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def make_point(mmsi, ts_str, lat, lon, sog=None, draught=None):
    return {
        "mmsi":             mmsi,
        "timestamp_parsed": fast_parse(ts_str),
        "timestamp_str":    ts_str,
        "Latitude":         lat,
        "Longitude":        lon,
        "SOG":              sog,
        "Draught":          draught,
    }


# ------------------------------------------------------------------ #
# parsing.py tests
# ------------------------------------------------------------------ #

def test_fast_parse():
    result = fast_parse("10/12/2025 08:30:00")
    assert result == (2025, 12, 10, 8, 30, 0), f"Got {result}"
    print("PASS test_fast_parse")


def test_is_valid_row_rejects_dirty_mmsi():
    row = {
        "MMSI": "000000000", "# Timestamp": "10/12/2025 08:00:00",
        "Type of mobile": "Class A", "Latitude": "55.0", "Longitude": "10.0",
    }
    assert not is_valid_row(row)
    print("PASS test_is_valid_row_rejects_dirty_mmsi")


def test_is_valid_row_rejects_non_class_a():
    row = {
        "MMSI": "123456789", "# Timestamp": "10/12/2025 08:00:00",
        "Type of mobile": "Class B", "Latitude": "55.0", "Longitude": "10.0",
    }
    assert not is_valid_row(row)
    print("PASS test_is_valid_row_rejects_non_class_a")


def test_downsample_bucket_same_bucket():
    t1 = fast_parse("10/12/2025 08:00:00")
    t2 = fast_parse("10/12/2025 08:01:30")
    assert downsample_bucket(t1) == downsample_bucket(t2)
    print("PASS test_downsample_bucket_same_bucket")


def test_downsample_bucket_different_bucket():
    t1 = fast_parse("10/12/2025 08:00:00")
    t2 = fast_parse("10/12/2025 08:02:00")
    assert downsample_bucket(t1) != downsample_bucket(t2)
    print("PASS test_downsample_bucket_different_bucket")


# ------------------------------------------------------------------ #
# geo.py tests
# ------------------------------------------------------------------ #

def test_haversine_zero():
    assert haversine(55.0, 10.0, 55.0, 10.0) == 0.0
    print("PASS test_haversine_zero")


def test_haversine_known():
    # Copenhagen to Malmo is ~16km
    d = haversine(55.676, 12.568, 55.607, 12.988)
    assert 25000 < d < 35000, f"Got {d}"
    print("PASS test_haversine_known")


def test_time_diff_hours():
    t1 = fast_parse("10/12/2025 08:00:00")
    t2 = fast_parse("10/12/2025 12:00:00")
    assert time_diff_hours(t1, t2) == 4.0
    print("PASS test_time_diff_hours")


def test_implied_speed():
    speed = implied_speed_knots(1852, 1.0)  # 1 nautical mile in 1 hour = 1 knot
    assert abs(speed - 1.0) < 0.01
    print("PASS test_implied_speed")


# ------------------------------------------------------------------ #
# models.py tests
# ------------------------------------------------------------------ #

def test_detect_going_dark_flags_gap():
    points = [
        make_point("123456789", "10/12/2025 08:00:00", 55.0, 10.0),
        make_point("123456789", "10/12/2025 13:00:00", 55.1, 10.1),  # 5hr gap, moved
    ]
    events = detect_going_dark(points)
    assert len(events) == 1
    assert events[0]["anomaly"] == "A"
    print("PASS test_detect_going_dark_flags_gap")


def test_detect_going_dark_no_movement():
    points = [
        make_point("123456789", "10/12/2025 08:00:00", 55.0, 10.0),
        make_point("123456789", "10/12/2025 13:00:00", 55.0, 10.0),  # 5hr gap, no movement
    ]
    events = detect_going_dark(points)
    assert len(events) == 0  # No movement = not suspicious
    print("PASS test_detect_going_dark_no_movement")


def test_detect_going_dark_short_gap():
    points = [
        make_point("123456789", "10/12/2025 08:00:00", 55.0, 10.0),
        make_point("123456789", "10/12/2025 09:00:00", 55.5, 10.5),  # Only 1hr gap
    ]
    events = detect_going_dark(points)
    assert len(events) == 0
    print("PASS test_detect_going_dark_short_gap")


def test_detect_draft_change():
    points = [
        make_point("123456789", "10/12/2025 08:00:00", 55.0, 10.0, draught=10.0),
        make_point("123456789", "10/12/2025 12:00:00", 55.1, 10.1, draught=7.0),  # 30% change
    ]
    events = detect_draft_change(points)
    assert len(events) == 1
    assert events[0]["anomaly"] == "C"
    print("PASS test_detect_draft_change")


def test_detect_teleportation():
    points = [
        make_point("123456789", "10/12/2025 08:00:00", 55.0, 10.0),
        make_point("123456789", "10/12/2025 08:01:00", 60.0, 20.0),  # ~700km in 1 min
    ]
    events = detect_teleportation(points)
    assert len(events) == 1
    assert events[0]["anomaly"] == "D"
    print("PASS test_detect_teleportation")


def test_build_loiter_candidates():
    points = [
        make_point("123456789", "10/12/2025 08:00:00", 55.0, 10.0, sog=0.5),
        make_point("123456789", "10/12/2025 09:00:00", 55.0, 10.0, sog=5.0),  # moving, excluded
    ]
    candidates = build_loiter_candidates(points)
    assert len(candidates) == 1
    print("PASS test_build_loiter_candidates")


# ------------------------------------------------------------------ #
# loiter.py tests
# ------------------------------------------------------------------ #

def test_loiter_cross_shard_detection():
    """
    Two vessels loiter together across what would be two separate shards.
    Tests that loiter.py correctly flags the pair.
    """
    tmpdir = tempfile.mkdtemp()
    try:
        # Write two fake loiter candidate CSVs (simulating two shards)
        fields = ["mmsi", "lat", "lon", "timestamp_parsed", "timestamp_str", "grid_id"]

        shard1_path = os.path.join(tmpdir, "shard_0000_loiter_candidates.csv")
        shard2_path = os.path.join(tmpdir, "shard_0001_loiter_candidates.csv")

        grid = "(55.0, 10.0)"

        with open(shard1_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerow({"mmsi": "111111112", "lat": 55.0, "lon": 10.0,
                        "timestamp_parsed": "(2025, 12, 10, 8, 0, 0)",
                        "timestamp_str": "10/12/2025 08:00:00", "grid_id": grid})
            w.writerow({"mmsi": "222222223", "lat": 55.001, "lon": 10.001,
                        "timestamp_parsed": "(2025, 12, 10, 8, 5, 0)",
                        "timestamp_str": "10/12/2025 08:05:00", "grid_id": grid})

        with open(shard2_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerow({"mmsi": "111111112", "lat": 55.0, "lon": 10.0,
                        "timestamp_parsed": "(2025, 12, 10, 11, 0, 0)",
                        "timestamp_str": "10/12/2025 11:00:00", "grid_id": grid})
            w.writerow({"mmsi": "222222223", "lat": 55.001, "lon": 10.001,
                        "timestamp_parsed": "(2025, 12, 10, 11, 5, 0)",
                        "timestamp_str": "10/12/2025 11:05:00", "grid_id": grid})

        b_counts = run_loiter([shard1_path, shard2_path], out_dir=tmpdir)

        assert "111111112" in b_counts or "222222223" in b_counts, \
            f"Expected loitering pair flagged, got: {b_counts}"
        print("PASS test_loiter_cross_shard_detection")

    finally:
        shutil.rmtree(tmpdir)


# ------------------------------------------------------------------ #
# pipeline.py integration test
# ------------------------------------------------------------------ #

def test_pipeline_runs_on_minimal_csv():
    """
    Creates a minimal valid AIS CSV and runs the full pipeline end-to-end.
    """
    tmpdir = tempfile.mkdtemp()
    try:
        data_dir = os.path.join(tmpdir, "data_arch")
        os.makedirs(data_dir)
        csv_path = os.path.join(data_dir, "test.csv")

        fields = [
            "# Timestamp", "Type of mobile", "MMSI", "Latitude", "Longitude",
            "Navigational status", "ROT", "SOG", "COG", "Heading",
            "IMO", "Callsign", "Name", "Ship type", "Cargo type",
            "Width", "Length", "Type of position fixing device",
            "Draught", "Destination", "ETA", "Data source type",
            "A", "B", "C", "D"
        ]

        rows = [
            {"# Timestamp": "10/12/2025 08:00:00", "Type of mobile": "Class A",
             "MMSI": "123456789", "Latitude": "55.0", "Longitude": "10.0",
             "SOG": "0.5", "Draught": "5.0"},
            {"# Timestamp": "10/12/2025 13:30:00", "Type of mobile": "Class A",
             "MMSI": "123456789", "Latitude": "55.5", "Longitude": "10.5",
             "SOG": "0.5", "Draught": "7.0"},
        ]

        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                full_row = {k: "" for k in fields}
                full_row.update(row)
                writer.writerow(full_row)

        # Patch config paths for the temp test
        import config
        orig_glob = config.DATA_ARCH_GLOB
        orig_part = config.PARTITIONED_DIR
        orig_ana  = config.ANALYSIS_DIR
        orig_loit = config.LOITERING_DIR
        config.DATA_ARCH_GLOB  = os.path.join(data_dir, "*.csv")
        config.PARTITIONED_DIR = os.path.join(tmpdir, "partitioned")
        config.ANALYSIS_DIR    = os.path.join(tmpdir, "analysis")
        config.LOITERING_DIR   = os.path.join(tmpdir, "loitering")
        config.NUM_WORKERS     = 1

        try:
            results = run_pipeline(data_glob=config.DATA_ARCH_GLOB, output_dir=tmpdir)
            assert len(results) > 0, "Pipeline returned no vessels"
            print("PASS test_pipeline_runs_on_minimal_csv")
        finally:
            config.DATA_ARCH_GLOB  = orig_glob
            config.PARTITIONED_DIR = orig_part
            config.ANALYSIS_DIR    = orig_ana
            config.LOITERING_DIR   = orig_loit

    finally:
        shutil.rmtree(tmpdir)


# ------------------------------------------------------------------ #
# Run all tests
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    print("Running Shadow Fleet tests...\n")

    test_fast_parse()
    test_is_valid_row_rejects_dirty_mmsi()
    test_is_valid_row_rejects_non_class_a()
    test_downsample_bucket_same_bucket()
    test_downsample_bucket_different_bucket()

    test_haversine_zero()
    test_haversine_known()
    test_time_diff_hours()
    test_implied_speed()

    test_detect_going_dark_flags_gap()
    test_detect_going_dark_no_movement()
    test_detect_going_dark_short_gap()
    test_detect_draft_change()
    test_detect_teleportation()
    test_build_loiter_candidates()

    test_loiter_cross_shard_detection()
    test_pipeline_runs_on_minimal_csv()

    print("\nAll tests passed.")

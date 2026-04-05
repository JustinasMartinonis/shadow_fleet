import csv
import time
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
from collections import defaultdict
from math import cos, radians

from worker import process_chunk
from utils import haversine, time_diff_hours


CHUNK_SIZE  = 50_000
MAX_PENDING = 8
MAX_ROWS    = 500_000       # just for testing, remove for full run

# Just Class A + valid MMSI
VALID_CLASSES = {"Class A"}
DIRTY_MMSI    = {"000000000", "111111111", "123456789", "999999999"}

# Baltic sea bounding 
LAT_MIN, LAT_MAX = 53, 66
LON_MIN, LON_MAX = 10, 30


def default_global():
    return {"A": 0, "B": 0, "C": 0, "D": 0, "DFSI": 0}


# filtering before rows reach workers
# 2-minute deduplication per vessel (keeps first ping per window)
def read_chunks(file_path):
    raw_rows     = 0    # every line in the file
    rows_read    = 0    # rows that passed all filters
    seen_buckets = set()

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        batch  = []

        for row in reader:
            raw_rows += 1

            if raw_rows % 500_000 == 0:
                print(f"  [reader] {raw_rows:,} raw rows scanned | {rows_read:,} accepted")

            if rows_read >= MAX_ROWS:
                break

            # Class filter
            if row.get("Type of mobile", "").strip() not in VALID_CLASSES:
                continue

            # MMSI filter
            mmsi = row["MMSI"].strip()
            if mmsi in DIRTY_MMSI or not mmsi.isdigit():
                continue

            try:
                lat     = float(row["Latitude"])
                lon     = float(row["Longitude"])
                sog     = float(row.get("SOG",     0) or 0)
                draught = float(row.get("Draught", 0) or 0)

                # Coordinate sanity
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    continue
                if lat == 0.0 and lon == 0.0:
                    continue

                # Baltic bounding box
                if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
                    continue

                # 2-minute deduplication, keep only first ping per vessel per 2-min window
                ts = row["# Timestamp"]
                yr = int(ts[6:10]); mo = int(ts[3:5]); dy = int(ts[0:2])
                hr = int(ts[11:13]); mn = int(ts[14:16])
                bucket_key = (mmsi, yr, mo, dy, hr, mn // 2)

                if bucket_key in seen_buckets:
                    continue
                seen_buckets.add(bucket_key)

                batch.append((mmsi, ts, lat, lon, sog, draught))

            except:
                continue

            rows_read += 1

            if len(batch) >= CHUNK_SIZE:
                yield batch
                batch = []

        if batch:
            yield batch

    print(f"  [reader] Done. {raw_rows:,} raw rows scanned | {rows_read:,} accepted after filtering")


# Detects A/C/D anomalies that span the gap between two chunks.
# Called with: last state of vessel from chunk N, first state of vessel from chunk N+1.
def check_boundary(mmsi, prev, curr, global_results):
    hours = time_diff_hours(prev["t"], curr["t"])
    if hours <= 0:
        return

    dist = haversine(prev["lat"], prev["lon"], curr["lat"], curr["lon"])

    # A: Going Dark
    if hours > 4 and dist > 5000:
        global_results[mmsi]["A"] += 1

    # C: Draft Change
    if hours > 2 and prev["draught"] > 0:
        if abs(curr["draught"] - prev["draught"]) / prev["draught"] > 0.05:
            global_results[mmsi]["C"] += 1

    # D: Teleportation
    if hours > 0 and (dist / 1852) / hours > 60:
        global_results[mmsi]["D"] += 1


# Assigns a vessel position to a ~400m grid cell. Only vessels in the same or adjacent cells are compared, reducing O(n²) to O(k²) where k is typically 2-5 per cell.
def get_grid_cell(lat, lon, cell_m=400):
    lat_step = cell_m / 111_000
    lon_step = cell_m / (111_000 * cos(radians(lat)))
    return (int(lat / lat_step), int(lon / lon_step))


# Unpacks all 4 return values from process_chunk.
def merge_result(result, global_results, global_time_buckets, global_vessel_last):
    # Unpack all 4 return values from worker
    stats, loiter_points, vessel_last_chunk, vessel_first_chunk = result

    # Add per-vessel anomaly counts found inside this chunk
    for mmsi, s in stats.items():
        for k in ("A", "C", "D"):
            global_results[mmsi][k] += s[k]

    # Add loitering points to global time buckets
    for (bucket, mmsi, lat, lon, sog) in loiter_points:
        global_time_buckets[bucket].append((mmsi, lat, lon, sog))

    # Cross-chunk boundary check:
    # Compare end of previous chunk (global_vessel_last[mmsi]) to START of this chunk (vessel_first_chunk[mmsi]).
    # This catches gaps that would otherwise be invisible.
    for mmsi, first_state in vessel_first_chunk.items():
        if mmsi in global_vessel_last:
            check_boundary(mmsi, global_vessel_last[mmsi], first_state, global_results)

    # Update global last state using the END of this chunk
    for mmsi, last_state in vessel_last_chunk.items():
        if mmsi not in global_vessel_last or last_state["t"] > global_vessel_last[mmsi]["t"]:
            global_vessel_last[mmsi] = last_state

# Anomaly B
# Runs once after ALL chunks are merged.
# Uses a spatial grid to find vessel pairs within 500m, both moving < 1 knot, for at least 60 consecutive 2-min buckets (= 2h)
def detect_loitering(global_time_buckets, global_results):
    print("Running loitering detection (Anomaly B)...")
    pair_duration = defaultdict(int)

    for t in sorted(global_time_buckets):
        vessels = global_time_buckets[t]

        # Build spatial grid for this time bucket
        grid = defaultdict(list)
        for entry in vessels:
            mmsi, lat, lon, sog = entry
            cell = get_grid_cell(lat, lon)
            grid[cell].append(entry)

        seen_pairs = set()

        for cell, cell_vessels in grid.items():
            # Compare only vessels in this cell and its 8 neighbours
            candidates = []
            for di in (-1, 0, 1):
                for dj in (-1, 0, 1):
                    candidates.extend(grid.get((cell[0]+di, cell[1]+dj), []))

            for v1 in cell_vessels:
                for v2 in candidates:
                    m1, lat1, lon1, sog1 = v1
                    m2, lat2, lon2, sog2 = v2

                    if m1 >= m2:        # skip self-pairs and duplicates
                        continue
                    pair = (m1, m2)
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)

                    dist = haversine(lat1, lon1, lat2, lon2)
                    if dist <= 500 and sog1 < 1 and sog2 < 1:
                        pair_duration[pair] += 1
                    else:
                        pair_duration[pair] = 0     # reset if they separate

    flagged = 0
    for (m1, m2), count in pair_duration.items():
        if count >= 60:     # 60 x 2-min buckets = 2 hours minimum
            global_results[m1]["B"] += 1
            global_results[m2]["B"] += 1
            flagged += 1

    print(f"  Loitering: {flagged} pairs flagged")


def main(file_path):
    global_results      = defaultdict(default_global)
    global_time_buckets = defaultdict(list)
    global_vessel_last  = {}   # last known state per vessel across all chunks

    pending       = {}     # future -> chunk_number (for ordered merging)
    chunk_results = {}     # chunk_number -> result
    chunk_counter = 0
    next_to_merge = 1

    start = time.time()

    def drain_done(done_futures):
        """Store completed futures keyed by chunk number."""
        for f in done_futures:
            chunk_num = pending.pop(f)
            chunk_results[chunk_num] = f.result()

    def merge_in_order():
        """Merge chunks in strict order so boundary checks are chronological."""
        nonlocal next_to_merge
        while next_to_merge in chunk_results:
            result = chunk_results.pop(next_to_merge)
            merge_result(result, global_results, global_time_buckets, global_vessel_last)
            print(f"  Merged chunk {next_to_merge}")
            next_to_merge += 1

    with ProcessPoolExecutor() as pool:
        for chunk in read_chunks(file_path):
            chunk_counter += 1
            f = pool.submit(process_chunk, chunk)
            pending[f] = chunk_counter

            # Backpressure — cap in-flight chunks to avoid memory spikes
            if len(pending) >= MAX_PENDING:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                drain_done(done)
                merge_in_order()

        # Drain any remaining futures after file is fully read
        if pending:
            done, _ = wait(pending.keys())
            drain_done(done)
            merge_in_order()

    # Anomaly B — needs full global_time_buckets so runs after all merging
    detect_loitering(global_time_buckets, global_results)

    # DFSI formula: A*3 + B*4 + C*2 + D*5
    for m, s in global_results.items():
        s["DFSI"] = s["A"]*3 + s["B"]*4 + s["C"]*2 + s["D"]*5

    # Write all vessels
    with open("all_vessels_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["MMSI", "A", "B", "C", "D", "DFSI"])
        for m, s in global_results.items():
            writer.writerow([m, s["A"], s["B"], s["C"], s["D"], s["DFSI"]])
    print("Saved: all_vessels_results.csv")

    # Top 5 by DFSI
    top5 = sorted(global_results.items(), key=lambda x: x[1]["DFSI"], reverse=True)[:5]
    with open("top5_vessels.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["MMSI", "A", "B", "C", "D", "DFSI"])
        for m, s in top5:
            writer.writerow([m, s["A"], s["B"], s["C"], s["D"], s["DFSI"]])
    print("Saved: top5_vessels.csv")

    print(f"\nTop 5 suspicious vessels:")
    for m, s in top5:
        print(f"  MMSI {m} | A={s['A']} B={s['B']} C={s['C']} D={s['D']} DFSI={s['DFSI']}")

    print(f"\nFinished in {time.time() - start:.2f}s")


if __name__ == "__main__":
    main("aisdk-2025-12-11.csv")

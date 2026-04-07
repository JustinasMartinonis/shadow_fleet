# scoring.py
# Merges per-vessel anomaly counts from all shards + loitering,
# computes precise DFSI based on physics metrics, 
# writes vessel_scores.csv and top5_vessels.csv
import csv
import os
from collections import defaultdict
from config import TOP_N_VESSELS


def run_scoring(vessel_csv_paths, b_counts, out_dir="."):
    """
    Merges vessel anomaly counts from all shard vessel CSVs,
    reads event metrics to compute the exact DFSI formula,
    and returns a sorted list of (mmsi, score_dict) tuples.
    """
    # Initialize dictionary with all required fields
    merged = defaultdict(lambda: {"A": 0, "B": 0, "C": 0, "D": 0, "points": 0, "max_gap_hours": 0.0, "total_dist_nm": 0.0})

    # --- 1. Merge basic anomaly counts across all shards ---
    for path in vessel_csv_paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = row["mmsi"]
                merged[mmsi]["A"]      += int(row.get("A", 0))
                merged[mmsi]["C"]      += int(row.get("C", 0))
                merged[mmsi]["D"]      += int(row.get("D", 0))
                merged[mmsi]["points"] += int(row.get("points", 0))

    # --- 2. Add Anomaly B counts from loitering ---
    for mmsi, b in b_counts.items():
        merged[mmsi]["B"] += b

    # --- 3. Extract the exact physical metrics for Anomaly A and D ---
    # We read the main events file generated earlier in the pipeline
    events_path = os.path.join(out_dir, "all_anomaly_events.csv")
    if os.path.exists(events_path):
        with open(events_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi = row["mmsi"]
                anomaly = row["anomaly"]
                
                if anomaly == "A":
                    try:
                        gap = float(row.get("gap_hours", 0))
                        # We only want the MAXIMUM gap for this vessel
                        if gap > merged[mmsi]["max_gap_hours"]:
                            merged[mmsi]["max_gap_hours"] = gap
                    except ValueError:
                        pass
                
                elif anomaly == "D":
                    try:
                        dist_m = float(row.get("dist_m", 0))
                        # Formula requires distance in Nautical Miles (1 NM = 1852 meters)
                        dist_nm = dist_m / 1852.0
                        merged[mmsi]["total_dist_nm"] += dist_nm
                    except ValueError:
                        pass

    # --- 4. Compute the exact DFSI formula ---
    for mmsi, data in merged.items():
        max_gap       = data["max_gap_hours"]
        total_dist_nm = data["total_dist_nm"]
        count_c       = data["C"]
        
        # DFSI = (Max Gap / 2) + (Total Impossible Distance NM / 10) + (C * 15)
        dfsi = (max_gap / 2.0) + (total_dist_nm / 10.0) + (count_c * 15.0)
        
        data["DFSI"] = round(dfsi, 2)
        data["mmsi"] = mmsi

    # --- 5. Sort by DFSI descending ---
    sorted_vessels = sorted(merged.items(), key=lambda x: x[1]["DFSI"], reverse=True)

    # --- 6. Write vessel_scores.csv ---
    scores_path = os.path.join(out_dir, "vessel_scores.csv")
    # Added the new metrics to the output fields so you can see the math in the CSV
    fields = ["mmsi", "A", "B", "C", "D", "DFSI", "points", "max_gap_hours", "total_dist_nm"]
    
    with open(scores_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for mmsi, data in sorted_vessels:
            writer.writerow(data)

    # --- 7. Write top5_vessels.csv ---
    top_n    = sorted_vessels[:TOP_N_VESSELS]
    top_path = os.path.join(out_dir, "top5_vessels.csv")
    
    with open(top_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for mmsi, data in top_n:
            writer.writerow(data)

    # --- Print clean output for your terminal ---
    print(f"\nScoring complete: {len(merged)} unique vessels scored")
    print(f"  -> {scores_path}")
    print(f"  -> {top_path}")
    print(f"\nTop {TOP_N_VESSELS} Vessels by DFSI:")
    for mmsi, data in top_n:
        print(f"  MMSI: {mmsi} | MaxGap: {data['max_gap_hours']:.1f}h | DistJumps: {data['total_dist_nm']:.1f}NM | C: {data['C']} | DFSI: {data['DFSI']}")

    return sorted_vessels
import glob
import csv
import os

# Count how many shards are created and their total row count
shards = sorted(glob.glob("partitioned/ais_shard_*.csv"))
print(f"Shards: {len(shards)}")

total_rows = 0
for s in shards:
    with open(s, "r") as f:
        total_rows += sum(1 for _ in f) - 1  
print(f"Total rows across all shards: {total_rows:,}")

# Number of vessels detected
vessel_files = sorted(glob.glob("analysis/*_vessels.csv"))
total_vessels = 0
total_points  = 0
for vf in vessel_files:
    with open(vf, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_vessels += 1
            total_points  += int(row.get("points", 0))
print(f"Unique vessels tracked: {total_vessels:,}")
print(f"Total track points processed: {total_points:,}")

# Counting anomaly events
event_files = sorted(glob.glob("analysis/*_events.csv"))
total_events = 0
for ef in event_files:
    with open(ef, "r") as f:
        total_events += sum(1 for _ in f) - 1
print(f"Total anomaly events (A/C/D): {total_events:,}")

# Loiter candidates
loiter_files = sorted(glob.glob("analysis/*_loiter_candidates.csv"))
total_candidates = 0
for lf in loiter_files:
    with open(lf, "r") as f:
        total_candidates += sum(1 for _ in f) - 1
print(f"Total loiter candidates: {total_candidates:,}")

# Final scores
if os.path.exists("vessel_scores.csv"):
    with open("vessel_scores.csv") as f:
        scored = sum(1 for _ in f) - 1
    print(f"Vessels in final scores: {scored:,}")

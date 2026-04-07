import csv
from collections import Counter

mmsi_counts = Counter()
import glob
for vf in sorted(glob.glob("analysis/*_vessels.csv")):
    with open(vf) as f:
        reader = csv.DictReader(f)
        for row in reader:
            mmsi_counts[row["mmsi"].strip()] += 1

# How many MMSIs appear in more than one shard vessel file?
multi_shard = {k: v for k, v in mmsi_counts.items() if v > 1}
print(f"Total unique MMSIs across all vessel files: {len(mmsi_counts):,}")
print(f"MMSIs appearing in more than 1 shard: {len(multi_shard):,}")
print(f"Max appearances of a single MMSI: {max(mmsi_counts.values())}")
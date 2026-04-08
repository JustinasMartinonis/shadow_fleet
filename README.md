# Shadow Fleet Detection in the Baltic Sea
Analysis and detection of "shadow fleet" vessels in the Baltic and North Seas using AIS data.

Authors: Edgaras Žurauskas, Ieva Juzumaitė, Justinas Martinonis, Paulius Vidutis (Vilnius University)

## Overview
The primary goal of this project is to identify vessels exhibiting suspicious behavior, such as signal manipulation (teleportation) or intentional gaps in transmission (loitering). The system filters out noisy data, processes valid AIS signals, and scores vessels based on their anomaly patterns.

## Features

- Dirty Data Filtering: Removes invalid MMSIs, specific vessel types (tug, towing, SAR), and data outside the Baltic/North Sea region (51°N-66°N, 5°W-30°E).
- Low-Memory Architecture: Processes large datasets row-by-row in chunks, using temporary shard files to keep RAM usage under 1GB.
- Anomaly Detection: Identifies "teleportation" (impossible distance jumps) and "loitering" (transmission gaps) to calculate a Dark Fleet Shadow Index (DFSI).

## Top Results

| Rank | MMSI      | DFSI  | Key Anomaly |
| :--- | :-------- | :---- | :---------- |
| 1    | 246843000 | 288.1 | 2881.0 NM Dist Jump |
| 2    | 219009229 | 141.96| 1419.6 NM Dist Jump |
| 3    | 246830000 | 130.96| 1309.6 NM Dist Jump |

## Conclusion

The pipeline efficiently handles large-scale AIS data with minimal memory usage, providing scalable and accurate detection of shadow fleet activities.
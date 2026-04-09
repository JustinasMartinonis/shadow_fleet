# Shadow Fleet Detection in the Baltic Sea
Analysis and detection of "shadow fleet" vessels in the Baltic and North Seas using AIS data.

Authors: Edgaras Žurauskas, Ieva Juzumaitė, Justinas Martinonis, Paulius Vidutis (Vilnius University)

## Overview

The primary goal of this project is to identify vessels in the Baltic Sea that exhibit suspicious behavior, including signal manipulation, intentional gaps in transmission, and performing illegal ship-to-ship cargo transfers. To identify this behavior, noisy data is filtered, valid AIS signals are processed, and vessels are scored based on various anomaly patterns to identify the most suspicious vessels.

## Dataset

- Source: Danish Maritime Authority AIS Data  
  http://aisdata.ais.dk/
- 2 days of AIS data were used: 2025-12-10 and 2025-12-11
- To run the code, a folder data_arch needs to be created locally where the Data files would be stored

## Anomalies 

- Anomaly A ("Going Dark"): AIS gaps of > 4 hours where the geographic distance between the disappearance and reappearance coordinates, which implies that the ship kept moving (it was not simply anchored).
- Anomaly B (Loitering & Transfers): Two distinct, valid MMSI numbers located within 500 meters of each other, maintaining a speed (SOG) of < 1 knot, for > 2 hours.
- Anomaly C (Draft Changes at Sea): Vessels whose draught (depth in water) changes by more than 5% during an AIS blackout of > 2 hours (implying cargo was loaded/unloaded illegally).
- Anomaly D (Identity Cloning / "Teleportation"): Identify instances where the same MMSI pings from two locations requiring an impossible travel speed (> 60 knots), indicating two physical ships are broadcasting the same stolen ID.


## Data Cleaning

AIS data contains significant noise. To improve reliability, the following filtering rules were applied:

- Invalid MMSIs are filtered out (e.g. "000000000", "111111111", "123456789")
- MMSI prefixes such as "111" are excluded, as they often represent malformed or special-purpose broadcasts (e.g. SAR-related signals)
- Certain vessel types are excluded: tug, towing, SAR
  - These vessels can distort loitering and teleportation detection due to operational patterns
- Geographic filtering is applied to restrict analysis to the Baltic and North Sea region:
  - Latitude: 51°N – 66°N  
  - Longitude: 5°W – 30°E
     
## Features

- Dirty Data Filtering: Removes invalid MMSIs, specific vessel types (tug, towing, SAR), and data outside the Baltic/North Sea region (51°N-66°N, 5°W-30°E).
- Low-Memory Architecture: Processes large datasets row-by-row in chunks, using temporary shard files to keep RAM usage under 1GB.
- Anomaly Detection: Identifies "teleportation" (impossible distance jumps) and "loitering" (transmission gaps) to calculate a Dark Fleet Shadow Index (DFSI).

## Additional Scripts

This repository contains extra scripts, with the label "extra" or "not in use". These are not part of the main pipeline and are used only for testing. Therefore, they are not executed in the final workflow.

## Low-Memory Processing Architecture

To handle large AIS datasets efficiently, the pipeline is designed for memory-constrained execution:

- The dataset is processed row-by-row in small chunks
- Each record is validated immediately (missing fields, duplicates, invalid values)
- Valid data is written into temporary shard files on disk
- Only one shard is kept in memory at any time
- After processing, shards are combined for downstream anomaly detection

This approach ensures that total RAM usage remains below ~1GB, even for large datasets.

## DFSI (Shadow Fleet Suspicion Index)

For every flagged vessel, the DFSI is calculated as follows: 

DFSI = (Max Gap in Hours / 2) + (Total Impossible Distance Jump (nm) / 10) + (C × 15) 
where C counts illicit draft change events detected for that vessel

## Top Results

| Rank | MMSI      | DFSI  | Key Anomaly |
| :--- | :-------- | :---- | :---------- |
| 1    | 246843000 | 288.1 | 2881.0 NM Dist Jump |
| 2    | 219009229 | 141.96| 1419.6 NM Dist Jump |
| 3    | 246830000 | 130.96| 1309.6 NM Dist Jump |

## Interpretation of Results

The highest DFSI scoring vessels show extremely large distance jumps between AIS signals.

These jumps range from 1300 to 2800+ nautical miles, which is physically impossible under normal maritime conditions.

Possible explanations include:
- AIS spoofing or identity misuse (multiple vessels broadcasting the same MMSI)
- Data glitches not fully removed by preprocessing filters
- Intentional signal manipulation ("teleportation" events)

After the top-ranked vessels, DFSI values decrease sharply and stabilize, suggesting that extreme anomalies are rare and well isolated.

## Performance: Speedup and Scalability

We evaluated the system using different numbers of parallel workers.

### Observations:
- Overall speedup remains close to 1.0x even with additional workers
- This is due to I/O-bound bottlenecks (disk reading and writing dominates runtime)
- The anomaly detection phase itself is highly parallelized and executes efficiently

### Interpretation:
This behavior is consistent with Amdahl’s Law, where the sequential I/O portion limits overall scalability.

Most execution time is spent on:
- Reading large CSV files
- Writing intermediate shard files

CPU parallelism has limited effect because disk throughput becomes the bottleneck.

## Memory Profiling

Memory usage was analyzed across different worker configurations.

Key findings:
- The main orchestrator process consistently stays below ~300MB RAM
- No significant memory spikes occur in the main process
- Memory usage scales linearly with the number of workers
- This indicates good scalability and balanced workload distribution

A Python memory profiler was used to inspect memory usage at a per-line level during execution.

## Conclusion

The pipeline efficiently handles large-scale AIS data with minimal memory usage, providing scalable detection of "shadow fleet" activities in the Baltic Sea.

# Shadow Fleet Detection in the Baltic Sea
Analysis and detection of "shadow fleet" vessels in the Baltic and North Seas using AIS data.

Authors: Edgaras Žurauskas, Ieva Juzumaitė, Justinas Martinonis, Paulius Vidutis (Vilnius University)

## Overview

The primary goal of this project is to identify vessels in the Baltic Sea that exhibit suspicious behavior, including signal manipulation, intentional gaps in transmission, and performing illegal ship-to-ship cargo transfers. To identify this behavior, noisy data is filtered, valid AIS signals are processed, and vessels are scored based on various anomaly patterns to identify the most suspicious vessels.

## Dataset

- Source: Danish Maritime Authority AIS Data  
  http://aisdata.ais.dk/
- 2 days of AIS data were used: 2025-12-10 and 2025-12-11

## Anomalies 

- Anomaly A ("Going Dark"): AIS gaps of > 4 hours where the geographic distance between the disappearance and reappearance coordinates, which implies that the ship kept moving (it was not simply anchored).
- Anomaly B (Loitering & Transfers): Two distinct, valid MMSI numbers located within 500 meters of each other, maintaining a speed (SOG) of < 1 knot, for > 2 hours.
- Anomaly C (Draft Changes at Sea): Vessels whose draught (depth in water) changes by more than 5% during an AIS blackout of > 2 hours (implying cargo was loaded/unloaded illegally).
- Anomaly D (Identity Cloning / "Teleportation"): Identify instances where the same MMSI pings from two locations requiring an impossible travel speed (> 60 knots), indicating two physical ships are broadcasting the same stolen ID.
- 
## Features

- Dirty Data Filtering: Removes invalid MMSIs, specific vessel types (tug, towing, SAR), and data outside the Baltic/North Sea region (51°N-66°N, 5°W-30°E).
- Low-Memory Architecture: Processes large datasets row-by-row in chunks, using temporary shard files to keep RAM usage under 1GB.
- Anomaly Detection: Identifies "teleportation" (impossible distance jumps) and "loitering" (transmission gaps) to calculate a Dark Fleet Shadow Index (DFSI).

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

According to this result, the top three scoring vessels showed a distance jump as the key anomaly. 

These vessels experienced a distance jump of 1309.6 to 2881.0 nautical miles between AIS points. As this is physically impossible at sea, this distance jump strongly indicates that these ships enabled AIS spoofing or are using another ship's MMSI.

## Conclusion

The pipeline efficiently handles large-scale AIS data with minimal memory usage, providing scalable detection of "shadow fleet" activities in the Baltic Sea.

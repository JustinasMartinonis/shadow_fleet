import pandas as pd
import folium
import os
import glob
import csv
from config import DATA_ARCH_GLOB
from parsing import parse_row
from partition import _is_valid 

def generate_full_vessel_map():
    if not os.path.exists("top5_vessels.csv"):
        print("Error: top5_vessels.csv not found.")
        return
        
    top_df = pd.read_csv("top5_vessels.csv")
    if top_df.empty:
        print("No vessels in top5_vessels.csv")
        return
        
    # Standardizing the MMSI to string
    top_mmsi = str(int(top_df.iloc[0]["mmsi"]))
    dfsi_score = top_df.iloc[0]["DFSI"]
    print(f"Top Offender MMSI: {top_mmsi} (DFSI Score: {dfsi_score})")

    # extract the full track from the raw dataset
    print(f"Scanning raw data to build the full route for {top_mmsi}...")
    all_points = []
    
    for file_path in glob.glob(DATA_ARCH_GLOB):
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi_val = row.get("MMSI", "").strip()
                if mmsi_val == top_mmsi:
                    if _is_valid(row):
                        parsed = parse_row(row)
                        if parsed and parsed["Latitude"] != 0 and parsed["Longitude"] != 0:
                            all_points.append(parsed)
                            
    if not all_points:
        print("Could not find raw GPS points for this vessel.")
        return

    # Sort the full track chronologically
    all_points.sort(key=lambda x: x["timestamp_parsed"])
    route_coords = [(p["Latitude"], p["Longitude"]) for p in all_points]

    # Create the map centered on the vessel's first known location
    start_lat, start_lon = route_coords[0]
    m = folium.Map(location=[start_lat, start_lon], zoom_start=6)

    folium.PolyLine(
        locations=route_coords,
        color="blue",
        weight=2,
        opacity=0.6,
        tooltip="Vessel's Full AIS Track"
    ).add_to(m)

    first_p = all_points[0]
    last_p = all_points[-1]
    
    folium.Marker(
        location=[first_p["Latitude"], first_p["Longitude"]],
        tooltip=f"<b>Data Start</b><br>Time: {first_p['timestamp_str']}",
        icon=folium.Icon(color="green", icon="play")
    ).add_to(m)

    folium.Marker(
        location=[last_p["Latitude"], last_p["Longitude"]],
        tooltip=f"<b>Data End</b><br>Time: {last_p['timestamp_str']}",
        icon=folium.Icon(color="darkred", icon="stop")
    ).add_to(m)

    step = max(1, len(all_points) // 20)
    for i in range(step, len(all_points) - step, step):
        p = all_points[i]
        folium.CircleMarker(
            location=[p["Latitude"], p["Longitude"]],
            radius=4,
            color="blue",
            fill=True,
            fill_opacity=0.7,
            tooltip=f"<b>Normal Sailing</b><br>Time: {p['timestamp_str']}"
        ).add_to(m)

    #  Overlay anomalies
    if os.path.exists("all_anomaly_events.csv"):
        events_df = pd.read_csv("all_anomaly_events.csv")
        vessel_events = events_df[events_df["mmsi"].astype(str) == top_mmsi]

        for _, row in vessel_events.iterrows():
            lat1, lon1 = row["lat_start"], row["lon_start"]
            lat2, lon2 = row["lat_end"], row["lon_end"]
            ts_start, ts_end = row["ts_start"], row["ts_end"]
            anomaly = row["anomaly"]

            if pd.isna(lat1) or pd.isna(lat2):
                continue

            if anomaly == 'A':
                color, label = 'red', f"Anomaly A (Dark for {row.get('gap_hours', '?')} hrs)"
            elif anomaly == 'D':
                color, label = 'purple', f"Anomaly D (Teleported)"
            else:
                color, label = 'orange', f"Anomaly {anomaly}"

            folium.CircleMarker(
                location=[lat1, lon1], radius=7, color=color, fill=True, fill_color=color, fill_opacity=1,
                tooltip=f"<b>{label} - VANISHED</b><br>Time: {ts_start}"
            ).add_to(m)
            
            folium.CircleMarker(
                location=[lat2, lon2], radius=7, color=color, fill=True, fill_color=color, fill_opacity=1,
                tooltip=f"<b>{label} - REAPPEARED</b><br>Time: {ts_end}"
            ).add_to(m)

            folium.PolyLine(
                locations=[(lat1, lon1), (lat2, lon2)],
                color=color,
                weight=4,
                dash_array='10, 10', 
                tooltip=f"{label}<br>From: {ts_start}<br>To: {ts_end}"
            ).add_to(m)

    map_filename = f"mmsi_{top_mmsi}_full_map.html"
    m.save(map_filename)
    print(f"Success! Map saved to '{map_filename}'.")

if __name__ == "__main__":
    generate_full_vessel_map()

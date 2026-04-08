# loitering_map.py
import pandas as pd
import folium
import os
import glob
import csv
from datetime import datetime
from config import PARTITIONED_DIR 
from parsing import parse_row

def generate_loitering_map():
    if not os.path.exists("all_loitering_events.csv"):
        print("Error: all_loitering_events.csv not found.")
        return

    events_df = pd.read_csv("all_loitering_events.csv")
    if events_df.empty:
        print("No loitering events found.")
        return

    # Sort by duration to get the longest 
    top_event = events_df.sort_values(by="duration_h", ascending=False).iloc[0]
    
    mmsi1 = str(int(top_event["mmsi1"]))
    mmsi2 = str(int(top_event["mmsi2"]))
    duration = top_event["duration_h"]
    ts_start_str = top_event["ts_start"]
    ts_end_str = top_event["ts_end"]

    print(f"Plotting Top Rendezvous: {mmsi1} and {mmsi2} (Duration: {duration} hours)")

    # Extract full tracks for both vessels from the clean shards
    print("Scanning partitioned shards for both vessels... (this will be fast!)")
    points_mmsi1 = []
    points_mmsi2 = []

    shard_pattern = os.path.join(PARTITIONED_DIR, "*.csv")
    
    for file_path in glob.glob(shard_pattern):
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi_val = row.get("MMSI", "").strip()
                
                if mmsi_val == mmsi1:
                    parsed = parse_row(row)
                    if parsed and parsed["Latitude"] != 0 and parsed["Longitude"] != 0:
                        points_mmsi1.append(parsed)
                        
                elif mmsi_val == mmsi2:
                    parsed = parse_row(row)
                    if parsed and parsed["Latitude"] != 0 and parsed["Longitude"] != 0:
                        points_mmsi2.append(parsed)

    # Sort chronologically
    points_mmsi1.sort(key=lambda x: x["timestamp_parsed"])
    points_mmsi2.sort(key=lambda x: x["timestamp_parsed"])

    if not points_mmsi1 or not points_mmsi2:
        print("Could not find raw GPS points for one or both vessels.")
        return

    # Extract the exact segment where they were loitering together
    def parse_time(ts):
        return datetime.strptime(ts, "%d/%m/%Y %H:%M:%S")

    start_dt = parse_time(ts_start_str)
    end_dt = parse_time(ts_end_str)

    # Filter points that fall inside the loitering time window
    loiter_segment_1 = [p for p in points_mmsi1 if start_dt <= parse_time(p["timestamp_str"]) <= end_dt]
    loiter_segment_2 = [p for p in points_mmsi2 if start_dt <= parse_time(p["timestamp_str"]) <= end_dt]

    coords1 = [(p["Latitude"], p["Longitude"]) for p in points_mmsi1]
    coords2 = [(p["Latitude"], p["Longitude"]) for p in points_mmsi2]
    
    loiter_coords1 = [(p["Latitude"], p["Longitude"]) for p in loiter_segment_1]
    loiter_coords2 = [(p["Latitude"], p["Longitude"]) for p in loiter_segment_2]

    # Center map on the start
    center_loc = loiter_coords1[0] if loiter_coords1 else coords1[len(coords1)//2]
    m = folium.Map(location=[center_loc[0], center_loc[1]], zoom_start=9)

    # Draw the full commute tracks
    folium.PolyLine(
        locations=coords1, color="blue", weight=2, opacity=0.4, 
        tooltip=f"Vessel 1 Full Track ({mmsi1})"
    ).add_to(m)
    
    folium.PolyLine(
        locations=coords2, color="orange", weight=2, opacity=0.4, 
        tooltip=f"Vessel 2 Full Track ({mmsi2})"
    ).add_to(m)

    if loiter_coords1:
        folium.PolyLine(
            locations=loiter_coords1, color="red", weight=6, opacity=0.8, 
            tooltip=f"Vessel 1 Loitering Drift ({mmsi1})"
        ).add_to(m)
        
    if loiter_coords2:
        folium.PolyLine(
            locations=loiter_coords2, color="darkred", weight=6, opacity=0.8, 
            tooltip=f"Vessel 2 Loitering Drift ({mmsi2})"
        ).add_to(m)

    if loiter_coords1:
        folium.Marker(
            loiter_coords1[0], 
            popup=f"Rendezvous Started<br>{ts_start_str}", 
            icon=folium.Icon(color="green", icon="play")
        ).add_to(m)
        
        folium.Marker(
            loiter_coords1[-1], 
            popup=f"Rendezvous Ended<br>{ts_end_str}", 
            icon=folium.Icon(color="red", icon="stop")
        ).add_to(m)

    map_filename = f"rendezvous_{mmsi1}_{mmsi2}.html"
    m.save(map_filename)
    print(f"Success! Map saved to '{map_filename}'.")

if __name__ == "__main__":
    generate_loitering_map()

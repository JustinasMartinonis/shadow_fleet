import pandas as pd
import folium
import os
import glob
import csv
from config import DATA_ARCH_GLOB
from parsing import is_valid_row, parse_row

def generate_loitering_map():
    # 1. Read the loitering events and find the most severe one
    if not os.path.exists("all_loitering_events.csv"):
        print("Error: all_loitering_events.csv not found.")
        return

    events_df = pd.read_csv("all_loitering_events.csv")
    if events_df.empty:
        print("No loitering events found.")
        return

    # Sort by duration to get the longest rendezvous
    top_event = events_df.sort_values(by="duration_h", ascending=False).iloc[0]
    mmsi1 = str(int(top_event["mmsi1"]))
    mmsi2 = str(int(top_event["mmsi2"]))
    duration = top_event["duration_h"]
    ts_start_str = top_event["ts_start"]

    print(f"Plotting Top Rendezvous: {mmsi1} and {mmsi2} (Duration: {duration} hours)")

    # 2. Extract full tracks for BOTH vessels
    print("Scanning raw data for both vessels... (this takes a moment)")
    points_mmsi1 = []
    points_mmsi2 = []

    for file_path in glob.glob(DATA_ARCH_GLOB):
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mmsi_val = row.get("MMSI", "").strip()
                if mmsi_val == mmsi1:
                    if is_valid_row(row):
                        parsed = parse_row(row)
                        if parsed and parsed["Latitude"] != 0 and parsed["Longitude"] != 0:
                            points_mmsi1.append(parsed)
                elif mmsi_val == mmsi2:
                    if is_valid_row(row):
                        parsed = parse_row(row)
                        if parsed and parsed["Latitude"] != 0 and parsed["Longitude"] != 0:
                            points_mmsi2.append(parsed)

    # Sort both tracks chronologically
    points_mmsi1.sort(key=lambda x: x["timestamp_parsed"])
    points_mmsi2.sort(key=lambda x: x["timestamp_parsed"])

    if not points_mmsi1 or not points_mmsi2:
        print("Could not find raw GPS points for one or both vessels.")
        return

    coords1 = [(p["Latitude"], p["Longitude"]) for p in points_mmsi1]
    coords2 = [(p["Latitude"], p["Longitude"]) for p in points_mmsi2]

    # 3. Find the exact coordinate where they met up
    # We look for where Vessel 1 was at the start of the rendezvous
    rendezvous_point = None
    for p in points_mmsi1:
        if p["timestamp_str"] == ts_start_str:
            rendezvous_point = (p["Latitude"], p["Longitude"])
            break
            
    # Fallback in case of exact string mismatch
    if not rendezvous_point:
        rendezvous_point = coords1[len(coords1)//2]

    # 4. Create the Map centered on the rendezvous point
    m = folium.Map(location=[rendezvous_point[0], rendezvous_point[1]], zoom_start=8)

    # 5. Draw the tracks
    # Vessel 1 in Blue
    folium.PolyLine(
        locations=coords1, color="blue", weight=3, opacity=0.7, 
        tooltip=f"Track for Vessel 1 ({mmsi1})"
    ).add_to(m)

    # Vessel 2 in Orange
    folium.PolyLine(
        locations=coords2, color="orange", weight=3, opacity=0.7, 
        tooltip=f"Track for Vessel 2 ({mmsi2})"
    ).add_to(m)

    # 6. Mark the Rendezvous location with a massive red warning circle
    folium.CircleMarker(
        location=rendezvous_point,
        radius=12,
        color="red",
        fill=True,
        fill_color="red",
        fill_opacity=0.5,
        tooltip=f"<b>Suspicious Ship-to-Ship Transfer</b><br>Vessels: {mmsi1} & {mmsi2}<br>Duration: {duration} hours<br>Started: {ts_start_str}"
    ).add_to(m)

    # 7. Save the map
    map_filename = f"rendezvous_{mmsi1}_{mmsi2}.html"
    m.save(map_filename)
    print(f"Success! Map saved to '{map_filename}'.")

if __name__ == "__main__":
    generate_loitering_map()
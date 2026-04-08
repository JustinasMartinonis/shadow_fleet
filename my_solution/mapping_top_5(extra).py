# map_vessel.py
import csv
import glob
import os
from datetime import datetime
try:
    import folium
except ImportError:
    print("Please run: pip install folium")
    exit()

# === INSERT YOUR SUSPECT MMSI HERE ===
TARGET_MMSI = "219013178" 

# Read from your highly optimized, pre-filtered shards!
DATA_GLOB = "partitioned/ais_shard_*.csv" 

def map_vessel_track(mmsi, data_glob):
    print(f"Scanning dense shards for MMSI {mmsi}...")
    
    points = []
    input_files = sorted(glob.glob(data_glob))
    
    if not input_files:
        print(f"No files found in {data_glob}. Run your pipeline first!")
        return

    # 1. Sweep the shards for the target MMSI
    for filepath in input_files:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("MMSI", "").strip() == mmsi:
                    ts_str = row["# Timestamp"]
                    # Parse timestamp so we can sort them perfectly
                    try:
                        dt = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
                    except ValueError:
                        continue # Skip malformed dates
                        
                    lat = float(row["Latitude"])
                    lon = float(row["Longitude"])
                    sog = float(row.get("SOG", 0) or 0)
                    draught = float(row.get("Draught", 0) or 0)
                    
                    points.append({
                        "lat": lat,
                        "lon": lon,
                        "dt": dt,
                        "ts_str": ts_str,
                        "sog": sog,
                        "draught": draught
                    })
                    
    if not points:
        print(f"No data found for MMSI {mmsi}. Are you sure it's in the Baltic/North Sea?")
        return
        
    # 2. Sort all points chronologically
    points.sort(key=lambda x: x["dt"])
    
    print(f"Found {len(points)} chronological pings. Painting map...")
    
    # 3. Center the map based on the ship's average location
    avg_lat = sum(p["lat"] for p in points) / len(points)
    avg_lon = sum(p["lon"] for p in points) / len(points)
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=7)
    
    # 4. Draw the continuous path (The Commute)
    coords = [(p["lat"], p["lon"]) for p in points]
    folium.PolyLine(coords, color="blue", weight=2.5, opacity=0.6).add_to(m)
    
    # 5. Add markers for Start and End of the dataset
    folium.Marker(
        coords[0], 
        popup=f"START<br>Time: {points[0]['ts_str']}<br>Draft: {points[0]['draught']}", 
        icon=folium.Icon(color="green", icon="play")
    ).add_to(m)
    
    folium.Marker(
        coords[-1], 
        popup=f"END<br>Time: {points[-1]['ts_str']}<br>Draft: {points[-1]['draught']}", 
        icon=folium.Icon(color="red", icon="stop")
    ).add_to(m)
    
    # 6. Add interactive dots for EVERY ping
    for p in points:
        # Visual trick: Paint the dot RED if it is loitering/anchored, BLUE if transiting
        dot_color = "red" if p["sog"] < 1.0 else "blue"
        
        folium.CircleMarker(
            location=(p["lat"], p["lon"]),
            radius=3,
            color=dot_color,
            fill=True,
            fill_opacity=0.7,
            tooltip=f"Time: {p['ts_str']} | SOG: {p['sog']} kn | Draft: {p['draught']} m"
        ).add_to(m)
        
    # 7. Save the output
    out_file = f"track_suspect_{mmsi}.html"
    m.save(out_file)
    print(f"Success! Open '{out_file}' in your web browser.")

if __name__ == "__main__":
    map_vessel_track(TARGET_MMSI, DATA_GLOB)
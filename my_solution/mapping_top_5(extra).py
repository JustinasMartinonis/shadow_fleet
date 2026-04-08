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

TARGET_MMSI = "219013178" 
DATA_GLOB = "partitioned/ais_shard_*.csv" 

def map_vessel_track(mmsi, data_glob):
    print(f"Scanning dense shards for MMSI {mmsi}...")
    
    points = []
    input_files = sorted(glob.glob(data_glob))
    
    if not input_files:
        print(f"No files found in {data_glob}. Run your pipeline first!")
        return

    # locate target MMSI
    for filepath in input_files:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("MMSI", "").strip() == mmsi:
                    ts_str = row["# Timestamp"]
                    try:
                        dt = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
                    except ValueError:
                        continue 
                        
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
        
    points.sort(key=lambda x: x["dt"])
    
    print(f"Found {len(points)} chronological pings. Painting map...")
    
    # Center map based on ship's average location
    avg_lat = sum(p["lat"] for p in points) / len(points)
    avg_lon = sum(p["lon"] for p in points) / len(points)
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=7)

    #commute
    coords = [(p["lat"], p["lon"]) for p in points]
    folium.PolyLine(coords, color="blue", weight=2.5, opacity=0.6).add_to(m)
    
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
    
    for p in points:
        dot_color = "red" if p["sog"] < 1.0 else "blue"
        
        folium.CircleMarker(
            location=(p["lat"], p["lon"]),
            radius=3,
            color=dot_color,
            fill=True,
            fill_opacity=0.7,
            tooltip=f"Time: {p['ts_str']} | SOG: {p['sog']} kn | Draft: {p['draught']} m"
        ).add_to(m)
        
    out_file = f"track_suspect_{mmsi}.html"
    m.save(out_file)
    print(f"Success! Open '{out_file}' in your web browser.")

if __name__ == "__main__":
    map_vessel_track(TARGET_MMSI, DATA_GLOB)

import pandas as pd
import glob
import csv

TARGET_MMSI = "266394000" 

def check_sogs():
    try:
        events = pd.read_csv("all_loitering_events.csv")
    except FileNotFoundError:
        print("Error: all_loitering_events.csv not found.")
        return

    # Verifying this MMSI actually got flagged in the final output
    events['mmsi1'] = events['mmsi1'].astype(str).str.split('.').str[0]
    events['mmsi2'] = events['mmsi2'].astype(str).str.split('.').str[0]
    
    suspect_events = events[(events['mmsi1'] == TARGET_MMSI) | (events['mmsi2'] == TARGET_MMSI)]
    
    if suspect_events.empty:
        print(f"MMSI {TARGET_MMSI} is NOT in the loitering events CSV. (The filters successfully killed it!)")
        return
        
    print(f"Confirmed: MMSI {TARGET_MMSI} was flagged {len(suspect_events)} time(s).")
    print("Scanning partitioned shards for actual SOG values... hold tight.")
    
    # Add up every SOG value recorded for this MMSI
    sog_tally = {}
    total_pings = 0
    
    for filepath in glob.glob("partitioned/*.csv"):
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("MMSI", "").strip() == TARGET_MMSI:
                    total_pings += 1
                    sog = float(row.get("SOG", "").strip() or 0)
                    
                    if sog in sog_tally:
                        sog_tally[sog] += 1
                    else:
                        sog_tally[sog] = 1

    if total_pings == 0:
        print(f"No data found for MMSI {TARGET_MMSI} in partitioned shards.")
        return

    print(f"\n--- SOG Breakdown for MMSI {TARGET_MMSI} ({total_pings} total pings) ---")
    sorted_sogs = sorted(sog_tally.items(), key=lambda x: x[1], reverse=True)
    
    for sog, count in sorted_sogs:
        percentage = (count / total_pings) * 100
        print(f"SOG: {sog:5.1f} knots  ->  {count:5} pings  ({percentage:5.1f}%)")

if __name__ == "__main__":
    check_sogs()

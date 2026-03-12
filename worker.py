# worker.py
from collections import defaultdict
from utils import detect_anomalies, fast_parse

def worker_loop(worker_id, num_workers, queue, result_queue):
    vessels = defaultdict(list)

    while True:
        item = queue.get()
        if item is None:
            break

        # item is now a list of rows (batch), not a single row
        for mmsi, timestamp, lat, lon, sog, draught in item:
            vessels[mmsi].append({
                "MMSI": mmsi,
                "Latitude": lat,
                "Longitude": lon,
                "SOG": sog,
                "Draught": draught,
                "timestamp_parsed": fast_parse(timestamp)
            })

    results = {}
    for mmsi, events in vessels.items():
        events.sort(key=lambda x: x["timestamp_parsed"]) # sort by timestamp
        results[mmsi] = detect_anomalies(events) # run detect_anomalies function from utils.py


    result_queue.put(results)

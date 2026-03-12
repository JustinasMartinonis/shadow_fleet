# main.py
import csv, time, multiprocessing as mp
from worker import worker_loop
from collections import defaultdict
import psutil
import threading
import matplotlib.pyplot as plt

# ---------------- Configuration ----------------
#TOTAL_ROWS = 17_262_193      # adjust to match max num of rows in the file - used only for progress tracking
TOTAL_ROWS = 1_000_000      # test run for quicker results - used only for progress tracking
PROGRESS_INTERVAL = 250_000  # progress update frequency - used only for progress tracking
CHUNK_SIZE = 50000         # rows per batch sent to each worker
VALID_CLASSES = {"Class A", "Class B"} # just include Class A and Class B ships (excluding Base Stations, AtoN (Navigation aids), Man Overboard Devices, Search and Rescue Transponders, SAR Airborne)
DIRTY_MMSI = {"000000000","111111111","123456789", "999999999"}  # ignore invalid vessels (might need to add more codes or run a loop to fill array with values)

# ---------------- Main Function ----------------
def main(file_path):

    num_workers = mp.cpu_count() # takes number of cpu cores and assigns that value to num_workers variable
    print(f"Using {num_workers} workers")

    queue = mp.Queue(maxsize=num_workers*2) # Using mp.queue - found it works fastest for this task
    result_queue = mp.Queue()

    # ---- Start worker processes ----
    workers = []
    for wid in range(num_workers):
        p = mp.Process(target=worker_loop, args=(wid, num_workers, queue, result_queue)) # initiate each worker on a different cpu core
        p.start()
        workers.append(p)

    # ---- Resource monitoring ----
    cpu_list, ram_list, time_list = [], [], [] # this piece of code is used only to be output at the end about resource usage - does not affect anything else - review it, add suggestions or changes if there is a more accurate way to measure it
    monitor_stop = threading.Event()

    def monitor():
        proc = psutil.Process()
        start_mon = time.time()
        while not monitor_stop.is_set():
            cpu_list.append(psutil.cpu_percent(interval=None))
            ram_list.append(proc.memory_info().rss / 1024**2)
            time_list.append(time.time() - start_mon)
            time.sleep(0.5)

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()

    start = time.time()
    rows_processed = 0
    batch = []

    # ---- Read CSV and batch rows ----
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f: # read the file and it's structure in csv format
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("Type of mobile", "").strip() not in VALID_CLASSES:
                continue
            mmsi = row["MMSI"].strip()
            if mmsi in DIRTY_MMSI or mmsi == "":
                continue
            if not mmsi.isdigit() or len(mmsi) != 9:
                continue

            try:
                lat = float(row["Latitude"])
                lon = float(row["Longitude"])
                sog = float(row.get("SOG", 0) or 0)
                draft = float(row.get("Draught", 0) or 0)
                
                # Ensure latitude/longitdue coordinates are within a legitimate range 
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    continue
                if lat == 0 and lon == 0:
                    continue
                batch.append((
                    mmsi,
                    row["# Timestamp"],
                    lat,
                    lon,
                    sog,
                    draft
                ))
            except:
                continue

            rows_processed += 1

            # ---- Send batch to queue ----
            if len(batch) >= CHUNK_SIZE: # send each bach to a free worker in queue
                queue.put(batch)
                batch = []

            # ---- Progress update ----
            if rows_processed % PROGRESS_INTERVAL == 0: # update progress - only for informational purposes
                elapsed = time.time() - start
                percent = (rows_processed / TOTAL_ROWS) * 100
                print(f"Progress: {percent:.2f}% | Rows: {rows_processed:,} | Elapsed: {elapsed:.1f}s")

    # ---- Send remaining batch ----
    if batch:
        queue.put(batch)

    # ---- Stop workers ----
    for _ in workers:
        queue.put(None)

    # ---- Collect worker results ----
    global_results = defaultdict(lambda: {"A":0,"B":0,"C":0,"D":0,"DFSI":0}) # getting the results and assigning scores
    global_loitering_pairs = [] 
    for _ in workers:
        worker_result = result_queue.get()
        for mmsi, stats in worker_result.items():
            # Aggregate anomaly counts
            for key in ["A","B","C","D","DFSI"]:
                global_results[mmsi][key] += stats[key]

            # Loitering vessel pairs
            if "loitering_pairs" in stats:
                global_loitering_pairs.extend(stats["loitering_pairs"])

    # ---- Join workers ----
    for p in workers: # join workers to the main thread
        p.join()

    # ---- Stop monitor ----
    monitor_stop.set() # stop resource monitoring and print time used
    monitor_thread.join()

    end = time.time()
    print(f"\nFinished in {end-start:.2f}s")

    # ---- Top 5 DFSI vessels ----
    top5 = sorted(global_results.items(), key=lambda x: x[1]["DFSI"], reverse=True)[:5] # output the results of suspicious vessels
    print("\nTop 5 Suspicious Vessels:")
    for mmsi, stats in top5:
        print(mmsi, stats)

    # Loitering vessel pairs
    print(f"\nTotal loitering events detected: {len(global_loitering_pairs)}")
    print("\nExample loitering vessel pairs (first 10):")
    for pair in global_loitering_pairs[:10]:
        print(pair)

    # ---- Resource summary ----
    print(f"\nPeak RAM usage: {max(ram_list):.2f} MB")
    print(f"Average RAM usage: {sum(ram_list)/len(ram_list):.2f} MB")
    print(f"Peak CPU usage: {max(cpu_list):.1f}%")
    print(f"Average CPU usage: {sum(cpu_list)/len(cpu_list):.1f}%")

    # ---- Plot hardware usage ----
    plt.figure(figsize=(10,5))
    plt.plot(time_list, ram_list, label="RAM (MB)")
    plt.plot(time_list, cpu_list, label="CPU (%)")
    plt.xlabel("Time (s)")
    plt.ylabel("Usage")
    plt.legend()
    plt.title("Hardware Usage During Shadow Fleet Detection")
    plt.savefig("hardware_usage.png", dpi=200)
    plt.show()

# ---------------- Run ----------------
if __name__ == "__main__":
    #main("test_2.csv") # test file with 1 million observations

    #main("aisdk-2025-12-11.csv") # full file

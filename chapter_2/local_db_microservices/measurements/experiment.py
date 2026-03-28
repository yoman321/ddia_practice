import sys
import os
sys.stdout.reconfigure(line_buffering=True)

import sqlite3
import threading
import requests
import random
import time
import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

RESERVATIONS_URL = "http://127.0.0.1:5005"
AVAILABILITIES_URL = "http://127.0.0.1:5007"
RESERVATIONS_DB = "../reservations.db"
AVAILABILITIES_DB = "../availabilities/availabilities.db"

TOTAL_REQUESTS = 1000
NUM_WORKERS = 50
REQUESTS_PER_WORKER = TOTAL_REQUESTS // NUM_WORKERS

DATES = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
TIME_SLOTS = ["12:00", "13:00", "14:00", "15:00", "16:00", "17:00",
              "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"]
NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"]
CAPACITY_PER_SLOT = 50

METHOD_COLORS = {"GET": "steelblue", "POST": "seagreen", "PUT": "darkorange", "DELETE": "crimson"}

created_ids = []
ids_lock = threading.Lock()
completed_timestamps = []
timestamps_lock = threading.Lock()

LOG_FILE = "request.log"
log_lock = threading.Lock()


def log_request(method, url, time_started, time_ended):
    duration_ms = (time_ended - time_started) * 1000
    started_str = datetime.datetime.fromtimestamp(time_started).strftime("%H:%M:%S.%f")[:-3]
    ended_str = datetime.datetime.fromtimestamp(time_ended).strftime("%H:%M:%S.%f")[:-3]
    line = f"{method:<8} | started: {started_str} | ended: {ended_str} | duration: {duration_ms:>8.3f}ms | {url}\n"
    with log_lock:
        log_file.write(line)


# --- Setup ---

def truncate_db(db_path, tables):
    conn = sqlite3.connect(db_path)
    for table in tables:
        conn.execute(f"DELETE FROM {table}")
        try:
            conn.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
        except Exception:
            pass
    conn.commit()
    conn.close()


def setup():
    print("Truncating databases...")
    truncate_db(RESERVATIONS_DB, ["reservations", "latency"])
    truncate_db(AVAILABILITIES_DB, ["availabilities", "latency"])

    print("Creating availability slots...")
    session = requests.Session()
    for date in DATES:
        for slot in TIME_SLOTS:
            session.post(f"{AVAILABILITIES_URL}/availabilities", json={
                "date": date,
                "time_slot": slot,
                "total_capacity": CAPACITY_PER_SLOT,
            })
    session.close()
    print(f"  Created {len(DATES) * len(TIME_SLOTS)} slots ({CAPACITY_PER_SLOT} capacity each).")


# --- Workers ---

def timed_request(session, method, url, **kwargs):
    t_start = time.time()
    resp = session.request(method, url, **kwargs)
    t_end = time.time()
    log_request(method, url, t_start, t_end)
    return resp


def user_worker(worker_id):
    session = requests.Session()
    for _ in range(REQUESTS_PER_WORKER):
        r = random.random()

        with ids_lock:
            has_ids = len(created_ids) > 0
            local_ids = list(created_ids) if has_ids else []

        if r < 0.3 or not has_ids:
            resp = timed_request(session, "POST", f"{RESERVATIONS_URL}/reservations", json={
                "guest_name": random.choice(NAMES),
                "date": random.choice(DATES),
                "time": random.choice(TIME_SLOTS),
                "party_size": random.randint(1, 4),
            })
            if resp.status_code == 201:
                with ids_lock:
                    created_ids.append(resp.json()["id"])

        elif r < 0.5:
            timed_request(session, "GET", f"{RESERVATIONS_URL}/reservations")

        elif r < 0.6:
            if local_ids:
                timed_request(session, "GET", f"{RESERVATIONS_URL}/reservations/{random.choice(local_ids)}")

        elif r < 0.7:
            timed_request(session, "GET", f"{AVAILABILITIES_URL}/availabilities")

        elif r < 0.85 and local_ids:
            rid = random.choice(local_ids)
            timed_request(session, "PUT", f"{RESERVATIONS_URL}/reservations/{rid}", json={
                "date": random.choice(DATES),
                "time": random.choice(TIME_SLOTS),
            })

        elif local_ids:
            rid = random.choice(local_ids)
            resp = timed_request(session, "DELETE", f"{RESERVATIONS_URL}/reservations/{rid}")
            if resp.status_code == 200:
                with ids_lock:
                    if rid in created_ids:
                        created_ids.remove(rid)

        with timestamps_lock:
            completed_timestamps.append(time.perf_counter())

    session.close()


def run():
    global created_ids, completed_timestamps
    created_ids = []
    completed_timestamps = []

    print(f"\nStarting {NUM_WORKERS} concurrent workers ({REQUESTS_PER_WORKER} requests each)...")
    start_time = time.perf_counter()
    threads = [threading.Thread(target=user_worker, args=(i+1,)) for i in range(NUM_WORKERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time = time.perf_counter() - start_time
    print(f"Done. {TOTAL_REQUESTS} requests in {total_time:.2f}s = {TOTAL_REQUESTS/total_time:.1f} req/s")
    return completed_timestamps, start_time, total_time


# --- Fetch latency ---

def fetch_latency_from_db(db_path):
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT id, method, endpoint, duration_ms FROM latency ORDER BY id").fetchall()
    conn.close()
    data = {}
    for row_id, method, endpoint, duration_ms in rows:
        key = f"{method}"
        if key not in data:
            data[key] = {"x": [], "y": []}
        data[key]["x"].append(row_id)
        data[key]["y"].append(duration_ms)
    return data


# --- Stats ---

def percentile(values, p):
    sv = sorted(values)
    idx = (p / 100) * (len(sv) - 1)
    lo, hi = int(idx), int(idx) + 1
    if hi >= len(sv):
        return sv[lo]
    return sv[lo] + (idx - lo) * (sv[hi] - sv[lo])


def print_stats(label, data):
    print(f"\n  [{label}]")
    print(f"  {'Method':<10} {'Count':<8} {'p50':>8} {'p90':>8} {'p95':>8} {'Avg':>8} {'Max':>8}")
    print(f"  {'-'*58}")
    for method in sorted(data.keys()):
        vals = data[method]["y"]
        print(f"  {method:<10} {len(vals):<8} "
              f"{percentile(vals,50):>7.2f}ms {percentile(vals,90):>7.2f}ms "
              f"{percentile(vals,95):>7.2f}ms {sum(vals)/len(vals):>7.2f}ms "
              f"{max(vals):>7.2f}ms")


# --- Plots ---

def plot_latency(res_data, avail_data, output_path):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=False)

    for method, series in res_data.items():
        ax1.plot(series["x"], series["y"],
                 label=method, color=METHOD_COLORS.get(method, "gray"),
                 linewidth=0.6, alpha=0.8)
    ax1.set_title("Reservations Service — Latency per Request (50 Workers)", fontsize=13, fontweight="bold")
    ax1.set_ylabel("Duration (ms)")
    ax1.legend(title="Method", fontsize=9)
    ax1.grid(True, linestyle="--", alpha=0.4)

    for method, series in avail_data.items():
        ax2.plot(series["x"], series["y"],
                 label=method, color=METHOD_COLORS.get(method, "gray"),
                 linewidth=0.6, alpha=0.8)
    ax2.set_title("Availabilities Service — Latency per Request (50 Workers)", fontsize=13, fontweight="bold")
    ax2.set_xlabel("Request Number")
    ax2.set_ylabel("Duration (ms)")
    ax2.legend(title="Method", fontsize=9)
    ax2.grid(True, linestyle="--", alpha=0.4)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")


def plot_throughput(timestamps, start_time, total_time, output_path):
    bucket_size = 0.2
    n_buckets = int(total_time / bucket_size) + 1
    buckets = [0] * n_buckets
    for ts in timestamps:
        idx = int((ts - start_time) / bucket_size)
        if idx < n_buckets:
            buckets[idx] += 1

    x = [i * bucket_size for i in range(n_buckets)]
    y = [count / bucket_size for count in buckets]
    avg = TOTAL_REQUESTS / total_time

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.fill_between(x, y, alpha=0.4, color="steelblue")
    ax.plot(x, y, color="steelblue", linewidth=1.2)
    ax.axhline(y=avg, color="crimson", linestyle="--", linewidth=1, label=f"Avg: {avg:.1f} req/s")
    ax.set_title(f"Throughput — {TOTAL_REQUESTS} Requests, {NUM_WORKERS} Concurrent Workers (Microservices)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Requests / second")
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")


# --- Main ---

os.makedirs("graphs", exist_ok=True)
log_file = open(LOG_FILE, "w")
log_file.write(f"{'METHOD':<8} | {'TIME STARTED':<22} | {'TIME ENDED':<22} | {'DURATION':>14} | URL\n")
log_file.write("-" * 100 + "\n")

setup()
timestamps, start_time, total_time = run()
log_file.close()
print(f"Log written to: {LOG_FILE}")

print("\nFetching latency data...")
res_data = fetch_latency_from_db(RESERVATIONS_DB)
avail_data = fetch_latency_from_db(AVAILABILITIES_DB)
print_stats("Reservations Service", res_data)
print_stats("Availabilities Service", avail_data)

print("\nPlotting...")
plot_latency(res_data, avail_data, "graphs/latency_50_workers.png")
plot_throughput(timestamps, start_time, total_time, "graphs/throughput_50_workers.png")

print("\nDone.")

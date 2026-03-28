import sys
import os
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, os.path.abspath(".."))

import sqlite3
import threading
import requests
import random
import time
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

BASE = "http://127.0.0.1:5005"
DATABASE = "../reservations.db"
TOTAL_REQUESTS = 1000
WORKER_COUNTS = [10, 30, 50]

NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"]
DATES = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
TIMES = ["18:00", "19:00", "20:00", "21:00"]

created_ids = []
ids_lock = threading.Lock()
completed_timestamps = []
timestamps_lock = threading.Lock()


def truncate_tables():
    conn = sqlite3.connect(DATABASE)
    conn.execute("DELETE FROM reservations")
    conn.execute("DELETE FROM latency")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='reservations'")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='latency'")
    conn.commit()
    conn.close()


def user_worker(n_requests):
    session = requests.Session()
    for _ in range(n_requests):
        r = random.random()

        with ids_lock:
            has_ids = len(created_ids) > 0
            local_ids = list(created_ids) if has_ids else []

        if r < 0.3 or not has_ids:
            resp = session.post(f"{BASE}/reservations", json={
                "guest_name": random.choice(NAMES),
                "date": random.choice(DATES),
                "time": random.choice(TIMES),
                "party_size": random.randint(1, 8),
            })
            if resp.status_code == 201:
                with ids_lock:
                    created_ids.append(resp.json()["id"])

        elif r < 0.6:
            session.get(f"{BASE}/reservations")

        elif r < 0.75 and local_ids:
            session.get(f"{BASE}/reservations/{random.choice(local_ids)}")

        elif r < 0.9 and local_ids:
            session.put(f"{BASE}/reservations/{random.choice(local_ids)}", json={
                "party_size": random.randint(1, 10)
            })

        elif local_ids:
            rid = random.choice(local_ids)
            resp = session.delete(f"{BASE}/reservations/{rid}")
            if resp.status_code == 200:
                with ids_lock:
                    if rid in created_ids:
                        created_ids.remove(rid)

        with timestamps_lock:
            completed_timestamps.append(time.perf_counter())

    session.close()


def run_experiment(num_workers):
    global created_ids, completed_timestamps
    created_ids = []
    completed_timestamps = []

    print(f"  Truncating tables...")
    truncate_tables()

    requests_per_worker = TOTAL_REQUESTS // num_workers
    remainder = TOTAL_REQUESTS % num_workers

    print(f"  Launching {num_workers} workers...")
    start_time = time.perf_counter()
    threads = []
    for i in range(num_workers):
        n = requests_per_worker + (1 if i < remainder else 0)
        t = threading.Thread(target=user_worker, args=(n,))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time = time.perf_counter() - start_time

    total_completed = len(completed_timestamps)
    overall_rps = total_completed / total_time
    print(f"  Done. {total_completed} requests in {total_time:.2f}s = {overall_rps:.1f} req/s overall")

    return completed_timestamps, start_time, total_time


def plot_throughput(timestamps, start_time, total_time, num_workers, output_path):
    # Bucket into 0.1s windows
    bucket_size = 0.1
    n_buckets = int(total_time / bucket_size) + 1
    buckets = [0] * n_buckets

    for ts in timestamps:
        idx = int((ts - start_time) / bucket_size)
        if idx < n_buckets:
            buckets[idx] += 1

    # Convert to req/s
    x = [i * bucket_size for i in range(n_buckets)]
    y = [count / bucket_size for count in buckets]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.fill_between(x, y, alpha=0.4, color="steelblue")
    ax.plot(x, y, color="steelblue", linewidth=1.2)
    ax.axhline(y=sum(y) / len([v for v in y if v > 0]) if any(y) else 0,
               color="crimson", linestyle="--", linewidth=1, label="Avg throughput")

    ax.set_title(f"Throughput Over Time — {TOTAL_REQUESTS} Requests, {num_workers} Workers", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)", fontsize=11)
    ax.set_ylabel("Requests / second", fontsize=11)
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")


os.makedirs("graphs", exist_ok=True)

for num_workers in WORKER_COUNTS:
    print(f"\n{'='*50}")
    print(f"Experiment: {TOTAL_REQUESTS} requests, {num_workers} workers")
    print(f"{'='*50}")
    timestamps, start_time, total_time = run_experiment(num_workers)
    output_path = f"graphs/throughput_{num_workers}.png"
    plot_throughput(timestamps, start_time, total_time, num_workers, output_path)

print("\nAll experiments complete.")

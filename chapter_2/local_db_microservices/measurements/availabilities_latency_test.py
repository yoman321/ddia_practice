import sys
import os
sys.path.insert(0, os.path.abspath("../availabilities"))

import sqlite3
import time
import random
import threading
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from scipy.interpolate import make_interp_spline
from app import app as flask_app

DATABASE = os.path.join(os.path.dirname(__file__), "../availabilities/availabilities.db")
TOTAL_REQUESTS = 1000

DATES = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
TIME_SLOTS = ["12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"]

# Ramp: each step sends a batch with increasing concurrency
# (concurrent_workers, requests_in_batch)
RAMP_STEPS = [
    (1, 100),
    (2, 100),
    (5, 100),
    (10, 100),
    (15, 100),
    (20, 100),
    (30, 100),
    (40, 100),
    (50, 100),
    (50, 100),
]  # total = 1000


def truncate_latency():
    conn = sqlite3.connect(DATABASE)
    conn.execute("DELETE FROM latency")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='latency'")
    conn.commit()
    conn.close()


def seed_data():
    client = flask_app.test_client()
    ids = []
    for _ in range(20):
        resp = client.post("/availabilities", json={
            "date": random.choice(DATES),
            "time_slot": random.choice(TIME_SLOTS),
            "total_capacity": random.randint(30, 100),
        })
        if resp.status_code == 201:
            ids.append(resp.get_json()["id"])
    return ids


def send_request(client, created_ids):
    r = random.random()
    if r < 0.25 or not created_ids:
        client.post("/availabilities", json={
            "date": random.choice(DATES),
            "time_slot": random.choice(TIME_SLOTS),
            "total_capacity": random.randint(10, 100),
        })
    elif r < 0.50:
        client.get("/availabilities")
    elif r < 0.65:
        client.get(f"/availabilities/{random.choice(created_ids)}")
    elif r < 0.75:
        client.put(f"/availabilities/{random.choice(created_ids)}", json={
            "total_capacity": random.randint(20, 100),
        })
    elif r < 0.85:
        client.post(f"/availabilities/{random.choice(created_ids)}/book")
    elif r < 0.92:
        client.post(f"/availabilities/{random.choice(created_ids)}/release")
    else:
        client.delete(f"/availabilities/{random.choice(created_ids)}")


def run_batch(num_requests, num_workers, created_ids):
    """Send num_requests using num_workers concurrent threads. Returns list of latencies."""
    latencies = []
    lock = threading.Lock()
    work_queue = list(range(num_requests))
    queue_lock = threading.Lock()

    def worker():
        c = flask_app.test_client()
        while True:
            with queue_lock:
                if not work_queue:
                    return
                work_queue.pop()
            t0 = time.perf_counter()
            send_request(c, created_ids)
            elapsed = (time.perf_counter() - t0) * 1000
            with lock:
                latencies.append(elapsed)

    start = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(num_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_time = time.perf_counter() - start

    throughput = num_requests / wall_time
    return latencies, throughput


def percentile(values, p):
    if not values:
        return 0
    sorted_vals = sorted(values)
    idx = (p / 100) * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= len(sorted_vals):
        return sorted_vals[lo]
    return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])


def plot_results(results, output_path):
    throughputs = [r["throughput"] for r in results]
    p50s = [r["p50"] for r in results]
    p90s = [r["p90"] for r in results]
    p99s = [r["p99"] for r in results]
    p999s = [r["p999"] for r in results]
    workers = [r["workers"] for r in results]

    fig, ax = plt.subplots(figsize=(12, 7))

    # Sort by throughput for smooth curves
    order = np.argsort(throughputs)
    t_sorted = np.array(throughputs)[order]

    def smooth_curve(x, y, num_points=300):
        x = np.array(x)
        y = np.array(y)
        # Need at least 4 points for cubic spline
        k = min(3, len(x) - 1)
        spline = make_interp_spline(x, y, k=k)
        x_smooth = np.linspace(x.min(), x.max(), num_points)
        y_smooth = spline(x_smooth)
        y_smooth = np.maximum(y_smooth, 0)  # no negative latencies
        return x_smooth, y_smooth

    curves = [
        (p50s, "#2ecc71", "-", 2.0, "p50"),
        (p90s, "#f39c12", "--", 2.0, "p90"),
        (p99s, "#e74c3c", "-.", 2.0, "p99"),
        (p999s, "#8e44ad", ":", 2.5, "p99.9"),
    ]

    for vals, color, ls, lw, label in curves:
        y_sorted = np.array(vals)[order]
        x_sm, y_sm = smooth_curve(t_sorted, y_sorted)
        ax.plot(x_sm, y_sm, color=color, linestyle=ls, linewidth=lw, label=label)
        ax.scatter(t_sorted, y_sorted, color=color, s=30, zorder=3, alpha=0.6)

    ax.axhline(y=200, color="gray", linestyle="--", alpha=0.5, label="SLO (200ms)")

    # Annotate worker count at each point
    p999_sorted = np.array(p999s)[order]
    workers_sorted = np.array(workers)[order]
    for i, w in enumerate(workers_sorted):
        ax.annotate(f"{w}w", (t_sorted[i], p999_sorted[i]), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=8, color="#8e44ad")

    ax.set_title(f"Latency vs Throughput — Availabilities Service ({sum(r['requests'] for r in results):,} Total Requests)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Throughput (req/s)", fontsize=11)
    ax.set_ylabel("Latency (ms)", fontsize=11)
    ax.legend(fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"\n  Saved graph: {output_path}")


if __name__ == "__main__":
    os.makedirs("graphs", exist_ok=True)

    print("Truncating latency table...")
    truncate_latency()

    print("Seeding data...")
    created_ids = seed_data()
    truncate_latency()

    results = []
    total_sent = 0

    print(f"\nRamping up to {TOTAL_REQUESTS} total requests...\n")
    print(f"  {'Workers':>8}  {'Requests':>10}  {'Throughput':>12}  {'p50':>10}  {'p90':>10}  {'p99':>10}  {'p99.9':>10}")
    print(f"  {'-'*72}")

    for workers, batch_size in RAMP_STEPS:
        latencies, throughput = run_batch(batch_size, workers, created_ids)
        total_sent += batch_size

        p50 = percentile(latencies, 50)
        p90 = percentile(latencies, 90)
        p99 = percentile(latencies, 99)
        p999 = percentile(latencies, 99.9)

        results.append({
            "workers": workers,
            "requests": batch_size,
            "throughput": round(throughput, 1),
            "p50": p50, "p90": p90, "p99": p99, "p999": p999,
        })

        print(f"  {workers:>8}  {batch_size:>10}  {throughput:>10.1f}/s  {p50:>8.2f}ms  {p90:>8.2f}ms  {p99:>8.2f}ms  {p999:>8.2f}ms")

    print(f"\n  Total requests sent: {total_sent}")

    # SLO check on all latencies combined
    all_lats = []
    for r in results:
        pass  # we need the raw latencies; re-fetch from DB
    conn = sqlite3.connect(DATABASE)
    rows = conn.execute("SELECT duration_ms, slo_latency, slo_error, slo_available FROM latency").fetchall()
    conn.close()
    all_durations = [r[0] for r in rows]
    total = len(rows)
    error_met = sum(1 for r in rows if r[2])
    available_met = sum(1 for r in rows if r[3])
    p99_all = percentile(all_durations, 99)

    print(f"\n  SLO Compliance (all {total} requests)")
    print(f"  {'─' * 40}")
    print(f"  Latency (p99 < 200ms):   {'MET' if p99_all < 200 else 'BREACHED'}  (p99 = {p99_all:.3f} ms)")
    print(f"  Error rate (< 0.1%):     {'MET' if (1 - error_met / total) * 100 < 0.1 else 'BREACHED'}  ({(1 - error_met / total) * 100:.3f}% errors)")
    print(f"  Availability (> 99.9%):  {'MET' if (available_met / total) * 100 > 99.9 else 'BREACHED'}  ({(available_met / total) * 100:.3f}% available)")

    plot_results(results, "graphs/availabilities_latency_percentiles.png")
    print("\nDone.")

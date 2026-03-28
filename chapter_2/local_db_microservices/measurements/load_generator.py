import sys
import os
sys.path.insert(0, os.path.abspath("../availabilities"))

import sqlite3
import time
import random
import threading
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from app import app as flask_app
from database import DATABASE

DATES = ["2026-04-01", "2026-04-02", "2026-04-03"]
TIME_SLOTS = ["12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00"]

# Burst/quiet cycles, gradually increasing burst size up to 1000
BURST_DURATION = 3     # seconds of burst
QUIET_DURATION = 3     # seconds of quiet
QUIET_RATE = 2         # req/s during quiet period
BURST_RATES = [10, 20, 40, 60, 80, 100]  # gradually increase, cap at 100 req/s


def truncate_latency():
    conn = sqlite3.connect(DATABASE)
    conn.execute("DELETE FROM latency")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='latency'")
    conn.commit()
    conn.close()


def send_request(client, created_ids):
    r = random.random()
    if r < 0.30 or not created_ids:
        client.post("/availabilities", json={
            "date": random.choice(DATES),
            "time_slot": random.choice(TIME_SLOTS),
            "total_capacity": random.randint(10, 100),
        })
    elif r < 0.55:
        client.get("/availabilities")
    elif r < 0.70:
        if created_ids:
            client.get(f"/availabilities/{random.choice(created_ids)}")
    elif r < 0.80:
        if created_ids:
            client.put(f"/availabilities/{random.choice(created_ids)}", json={
                "total_capacity": random.randint(20, 100),
            })
    elif r < 0.90:
        if created_ids:
            client.post(f"/availabilities/{random.choice(created_ids)}/book")
    else:
        if created_ids:
            client.post(f"/availabilities/{random.choice(created_ids)}/release")


def seed_data(client):
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


def fire_requests(target_rate, duration, created_ids):
    """Fire requests at target_rate req/s for duration seconds. Returns (latencies, timestamps, actual_throughput)."""
    total_requests = target_rate * duration
    interval = 1.0 / target_rate

    latencies = []
    timestamps = []
    lock = threading.Lock()
    start_wall = time.perf_counter()

    threads = []

    def worker(request_time):
        c = flask_app.test_client()
        t0 = time.perf_counter()
        send_request(c, created_ids)
        elapsed = (time.perf_counter() - t0) * 1000
        with lock:
            latencies.append(elapsed)
            timestamps.append(request_time)

    for i in range(total_requests):
        expected_start = start_wall + i * interval
        now = time.perf_counter()
        sleep_time = expected_start - now
        if sleep_time > 0:
            time.sleep(sleep_time)

        t = threading.Thread(target=worker, args=(time.perf_counter() - start_wall,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    wall_time = time.perf_counter() - start_wall
    actual_throughput = len(latencies) / wall_time if wall_time > 0 else 0

    return latencies, timestamps, actual_throughput


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


def plot_results(all_points, cycle_summaries, output_path):
    """
    all_points: list of (absolute_time, latency_ms, phase)
    cycle_summaries: list of dicts with burst_rate, burst/quiet percentiles
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), gridspec_kw={"height_ratios": [3, 2]})

    # --- Top: latency over time scatter with burst/quiet shading ---
    burst_times = [p[0] for p in all_points if p[2] == "burst"]
    burst_lats = [p[1] for p in all_points if p[2] == "burst"]
    quiet_times = [p[0] for p in all_points if p[2] == "quiet"]
    quiet_lats = [p[1] for p in all_points if p[2] == "quiet"]

    ax1.scatter(burst_times, burst_lats, s=3, alpha=0.4, color="#e74c3c", label="Burst", zorder=2)
    ax1.scatter(quiet_times, quiet_lats, s=3, alpha=0.4, color="#2ecc71", label="Quiet", zorder=2)

    # Shade burst/quiet regions
    t_offset = 0
    for cs in cycle_summaries:
        ax1.axvspan(t_offset, t_offset + BURST_DURATION, alpha=0.08, color="#e74c3c")
        ax1.text(t_offset + BURST_DURATION / 2, ax1.get_ylim()[0] if ax1.get_ylim()[0] > 0 else 0,
                 f"{cs['burst_rate']}/s", ha="center", va="bottom", fontsize=8, color="#e74c3c", fontweight="bold")
        t_offset += BURST_DURATION
        ax1.axvspan(t_offset, t_offset + QUIET_DURATION, alpha=0.08, color="#2ecc71")
        t_offset += QUIET_DURATION

    ax1.axhline(y=200, color="gray", linestyle="--", alpha=0.5, label="SLO (200ms)")
    ax1.set_title("Burst/Quiet Load Pattern — Latency Over Time", fontsize=14, fontweight="bold")
    ax1.set_ylabel("Latency (ms)", fontsize=11)
    ax1.legend(fontsize=9, loc="upper left")
    ax1.grid(True, linestyle="--", alpha=0.3)

    # --- Bottom: percentiles per burst rate ---
    burst_rates = [cs["burst_rate"] for cs in cycle_summaries]
    burst_p50 = [cs["burst_p50"] for cs in cycle_summaries]
    burst_p90 = [cs["burst_p90"] for cs in cycle_summaries]
    burst_p99 = [cs["burst_p99"] for cs in cycle_summaries]
    burst_p999 = [cs["burst_p999"] for cs in cycle_summaries]
    quiet_p99 = [cs["quiet_p99"] for cs in cycle_summaries]

    x = range(len(burst_rates))
    ax2.plot(burst_rates, burst_p50, "o-", color="#2ecc71", linewidth=2, markersize=6, label="Burst p50")
    ax2.plot(burst_rates, burst_p90, "s--", color="#f39c12", linewidth=2, markersize=6, label="Burst p90")
    ax2.plot(burst_rates, burst_p99, "^-.", color="#e74c3c", linewidth=2, markersize=6, label="Burst p99")
    ax2.plot(burst_rates, burst_p999, "d:", color="#8e44ad", linewidth=2.5, markersize=6, label="Burst p99.9")
    ax2.plot(burst_rates, quiet_p99, "x-", color="steelblue", linewidth=1.5, markersize=6, label="Quiet p99 (recovery)")
    ax2.axhline(y=200, color="gray", linestyle="--", alpha=0.5, label="SLO (200ms)")

    ax2.set_title("Percentiles Per Burst Level", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Burst Rate (req/s)", fontsize=11)
    ax2.set_ylabel("Latency (ms)", fontsize=11)
    ax2.legend(fontsize=9)
    ax2.grid(True, linestyle="--", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"\n  Saved graph: {output_path}")


if __name__ == "__main__":
    os.makedirs("graphs", exist_ok=True)

    print("Truncating latency table...")
    truncate_latency()

    print("Seeding availability data...")
    client = flask_app.test_client()
    created_ids = seed_data(client)
    truncate_latency()

    all_points = []       # (absolute_time, latency_ms, "burst"|"quiet")
    cycle_summaries = []
    time_offset = 0

    for burst_rate in BURST_RATES:
        # --- Burst phase ---
        print(f"\n  BURST: {burst_rate} req/s for {BURST_DURATION}s ({burst_rate * BURST_DURATION} requests)...")
        burst_lats, burst_ts, burst_throughput = fire_requests(burst_rate, BURST_DURATION, created_ids)
        for ts, lat in zip(burst_ts, burst_lats):
            all_points.append((time_offset + ts, lat, "burst"))

        bp50 = percentile(burst_lats, 50)
        bp90 = percentile(burst_lats, 90)
        bp99 = percentile(burst_lats, 99)
        bp999 = percentile(burst_lats, 99.9)
        print(f"  Actual: {burst_throughput:.1f} req/s | p50={bp50:.2f}ms  p90={bp90:.2f}ms  p99={bp99:.2f}ms  p99.9={bp999:.2f}ms")

        time_offset += BURST_DURATION

        # --- Quiet phase ---
        print(f"  QUIET: {QUIET_RATE} req/s for {QUIET_DURATION}s (recovery)...")
        quiet_lats, quiet_ts, quiet_throughput = fire_requests(QUIET_RATE, QUIET_DURATION, created_ids)
        for ts, lat in zip(quiet_ts, quiet_lats):
            all_points.append((time_offset + ts, lat, "quiet"))

        qp99 = percentile(quiet_lats, 99)
        print(f"  Quiet p99={qp99:.2f}ms (recovered: {'yes' if qp99 < 200 else 'no'})")

        time_offset += QUIET_DURATION

        cycle_summaries.append({
            "burst_rate": burst_rate,
            "burst_throughput": round(burst_throughput, 1),
            "burst_p50": bp50, "burst_p90": bp90, "burst_p99": bp99, "burst_p999": bp999,
            "quiet_p99": qp99,
        })

    # Summary table
    print(f"\n{'='*90}")
    print(f"  {'Burst':>8}  {'Actual':>10}  {'p50':>10}  {'p90':>10}  {'p99':>10}  {'p99.9':>10}  {'Quiet p99':>12}")
    print(f"  {'-'*80}")
    for cs in cycle_summaries:
        print(f"  {cs['burst_rate']:>6}/s  {cs['burst_throughput']:>8.1f}/s  "
              f"{cs['burst_p50']:>8.2f}ms  {cs['burst_p90']:>8.2f}ms  {cs['burst_p99']:>8.2f}ms  "
              f"{cs['burst_p999']:>8.2f}ms  {cs['quiet_p99']:>10.2f}ms")

    plot_results(all_points, cycle_summaries, "graphs/latency_vs_throughput.png")
    print("\nDone.")

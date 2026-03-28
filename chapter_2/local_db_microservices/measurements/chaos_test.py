import os
import sys
import time
import signal
import random
import shutil
import subprocess
import requests
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
from scipy.interpolate import make_interp_spline

VENV_PYTHON = "/Users/luoph/Desktop/DDIA_practice/chapter_2/venv/bin/python"
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AVAIL_DIR = os.path.join(BASE_DIR, "availabilities")
AVAIL_APP = os.path.join(AVAIL_DIR, "app.py")
RESERV_APP = os.path.join(BASE_DIR, "app.py")
AVAIL_DB = os.path.join(AVAIL_DIR, "availabilities.db")
AVAIL_DB_BACKUP = AVAIL_DB + ".bak"

RESERV_URL = "http://127.0.0.1:5005"
AVAIL_URL = "http://127.0.0.1:5007"

TOTAL_REQUESTS = 1000
DATES = ["2026-04-01", "2026-04-02", "2026-04-03"]
TIMES = ["12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00"]

# Chaos events: (request_number, action)
CHAOS_EVENTS = [
    (150, "kill_disk"),
    (175, "restore_disk"),
    (300, "kill_service"),
    (325, "restart_service"),
    (600, "kill_disk"),
    (625, "restore_disk"),
    (800, "kill_service"),
    (825, "restart_service"),
]


def start_service(app_path, name):
    proc = subprocess.Popen(
        [VENV_PYTHON, app_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=os.path.dirname(app_path),
    )
    print(f"  Started {name} (PID {proc.pid})")
    return proc


def wait_for_service(url, timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        try:
            requests.get(url, timeout=1)
            return True
        except Exception:
            time.sleep(0.2)
    return False


def kill_disk():
    """Rename the availabilities DB to simulate disk failure."""
    if os.path.exists(AVAIL_DB):
        shutil.copy2(AVAIL_DB, AVAIL_DB_BACKUP)
        os.remove(AVAIL_DB)
        print(f"    -> Killed disk (removed {os.path.basename(AVAIL_DB)})")


def restore_disk():
    """Restore the availabilities DB."""
    if os.path.exists(AVAIL_DB_BACKUP):
        shutil.copy2(AVAIL_DB_BACKUP, AVAIL_DB)
        os.remove(AVAIL_DB_BACKUP)
        print(f"    -> Restored disk")


avail_proc = None


def kill_service():
    """Kill the availabilities service process."""
    global avail_proc
    if avail_proc and avail_proc.poll() is None:
        avail_proc.terminate()
        avail_proc.wait()
        print(f"    -> Killed availabilities service (PID {avail_proc.pid})")


def restart_service():
    """Restart the availabilities service."""
    global avail_proc
    avail_proc = start_service(AVAIL_APP, "availabilities")
    if wait_for_service(f"{AVAIL_URL}/availabilities"):
        print(f"    -> Availabilities service back up")
    else:
        print(f"    -> WARNING: Availabilities service failed to start")


def send_reservation_request(created_ids):
    """Send a random request to the reservation service. Returns (latency_ms, status_code, method)."""
    r = random.random()
    t0 = time.perf_counter()
    try:
        if r < 0.30 or not created_ids:
            resp = requests.post(f"{RESERV_URL}/reservations", json={
                "guest_name": random.choice(["Alice", "Bob", "Carol", "David", "Eve"]),
                "date": random.choice(DATES),
                "time": random.choice(TIMES),
                "party_size": random.randint(1, 8),
            }, timeout=5)
            if resp.status_code == 201:
                created_ids.append(resp.json()["id"])
            method = "POST"
        elif r < 0.55:
            resp = requests.get(f"{RESERV_URL}/reservations", timeout=5)
            method = "GET"
        elif r < 0.75:
            rid = random.choice(created_ids) if created_ids else 1
            resp = requests.get(f"{RESERV_URL}/reservations/{rid}", timeout=5)
            method = "GET"
        elif r < 0.90:
            rid = random.choice(created_ids) if created_ids else 1
            resp = requests.put(f"{RESERV_URL}/reservations/{rid}", json={
                "party_size": random.randint(1, 10),
            }, timeout=5)
            method = "PUT"
        else:
            rid = random.choice(created_ids) if created_ids else 1
            resp = requests.delete(f"{RESERV_URL}/reservations/{rid}", timeout=5)
            if resp.status_code == 200 and rid in created_ids:
                created_ids.remove(rid)
            method = "DELETE"
    except requests.exceptions.RequestException:
        elapsed = (time.perf_counter() - t0) * 1000
        return elapsed, 503, "TIMEOUT"

    elapsed = (time.perf_counter() - t0) * 1000
    return elapsed, resp.status_code, method


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


def plot_results(data, output_path):
    """
    data: list of dicts with keys: request_num, latency_ms, status_code, method, timestamp
    """
    # Compute throughput and percentiles in rolling windows
    window = 50
    step = 10
    windows = []
    for start in range(0, len(data) - window + 1, step):
        chunk = data[start:start + window]
        lats = [d["latency_ms"] for d in chunk]
        wall_time = chunk[-1]["timestamp"] - chunk[0]["timestamp"]
        throughput = window / wall_time if wall_time > 0 else 0
        errors = sum(1 for d in chunk if d["status_code"] >= 500 or d["status_code"] == 503)
        error_rate = (errors / window) * 100

        windows.append({
            "throughput": throughput,
            "p50": percentile(lats, 50),
            "p90": percentile(lats, 90),
            "p99": percentile(lats, 99),
            "p999": percentile(lats, 99.9),
            "error_rate": error_rate,
            "mid_request": chunk[window // 2]["request_num"],
        })

    # Classify windows as normal, during chaos, or recovering
    chaos_ranges = []
    for i in range(0, len(CHAOS_EVENTS), 2):  # pairs of (kill, restore/restart)
        start_req = CHAOS_EVENTS[i][0]
        end_req = CHAOS_EVENTS[i + 1][0] + 25  # recovery buffer
        chaos_ranges.append((start_req, end_req))

    def is_chaos(mid_req):
        return any(s <= mid_req <= e for s, e in chaos_ranges)

    normal = [w for w in windows if not is_chaos(w["mid_request"])]
    chaos = [w for w in windows if is_chaos(w["mid_request"])]

    fig, ax = plt.subplots(figsize=(12, 7))

    # Plot normal windows as one group, chaos as another
    for group, color_scatter, marker, label_prefix in [
        (normal, "steelblue", "o", "Normal"),
        (chaos, "red", "x", "Chaos"),
    ]:
        if not group:
            continue
        throughputs = [w["throughput"] for w in group]
        ax.scatter(throughputs, [w["p50"] for w in group], s=30, alpha=0.5, color="#2ecc71", marker=marker, zorder=2)
        ax.scatter(throughputs, [w["p90"] for w in group], s=30, alpha=0.5, color="#f39c12", marker=marker, zorder=2)
        ax.scatter(throughputs, [w["p99"] for w in group], s=30, alpha=0.5, color="#e74c3c", marker=marker, zorder=2)
        ax.scatter(throughputs, [w["p999"] for w in group], s=30, alpha=0.5, color="#8e44ad", marker=marker, zorder=2)

    # Smooth curves through ALL windows sorted by throughput
    all_tp = np.array([w["throughput"] for w in windows])
    order = np.argsort(all_tp)
    tp_sorted = all_tp[order]

    def smooth_curve(x, y):
        k = min(3, len(x) - 1)
        try:
            spline = make_interp_spline(x, y, k=k)
            x_sm = np.linspace(x.min(), x.max(), 300)
            y_sm = np.maximum(spline(x_sm), 0)
            return x_sm, y_sm
        except Exception:
            return x, y

    for key, color, ls, lw, label in [
        ("p50", "#2ecc71", "-", 2.0, "p50"),
        ("p90", "#f39c12", "--", 2.0, "p90"),
        ("p99", "#e74c3c", "-.", 2.0, "p99"),
        ("p999", "#8e44ad", ":", 2.5, "p99.9"),
    ]:
        y_sorted = np.array([windows[i][key] for i in order])
        x_sm, y_sm = smooth_curve(tp_sorted, y_sorted)
        ax.plot(x_sm, y_sm, color=color, linestyle=ls, linewidth=lw, label=label, zorder=4)

    ax.axhline(y=200, color="gray", linestyle="--", alpha=0.5, label="SLO (200ms)")

    # Add legend entries for normal vs chaos markers
    ax.scatter([], [], s=30, color="steelblue", marker="o", label="Normal window")
    ax.scatter([], [], s=30, color="red", marker="x", label="Chaos window")

    ax.set_title("Chaos Test — Latency vs Throughput (1,000 Requests)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Throughput (req/s)", fontsize=11)
    ax.set_ylabel("Latency (ms)", fontsize=11)
    ax.legend(fontsize=9)
    ax.grid(True, linestyle="--", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"\n  Saved graph: {output_path}")


if __name__ == "__main__":
    os.makedirs("graphs", exist_ok=True)

    print("Starting services...")
    avail_proc = start_service(AVAIL_APP, "availabilities")
    reserv_proc = start_service(RESERV_APP, "reservations")

    print("Waiting for services to be ready...")
    assert wait_for_service(f"{AVAIL_URL}/availabilities"), "Availabilities service failed to start"
    assert wait_for_service(f"{RESERV_URL}/reservations"), "Reservations service failed to start"
    print("  Both services ready.\n")

    # Seed some availability data
    print("Seeding availability data...")
    for date in DATES:
        for t in TIMES:
            requests.post(f"{AVAIL_URL}/availabilities", json={
                "date": date, "time_slot": t, "total_capacity": 50,
            })
    print("  Done.\n")

    # Build chaos schedule as a dict for quick lookup
    chaos_schedule = {req_num: action for req_num, action in CHAOS_EVENTS}

    created_ids = []
    data = []

    print(f"Running {TOTAL_REQUESTS} requests with chaos injection...\n")

    for i in range(1, TOTAL_REQUESTS + 1):
        # Check for chaos event
        if i in chaos_schedule:
            action = chaos_schedule[i]
            print(f"  [Request {i}] CHAOS: {action}")
            if action == "kill_disk":
                kill_disk()
            elif action == "restore_disk":
                restore_disk()
            elif action == "kill_service":
                kill_service()
            elif action == "restart_service":
                restart_service()

        wall_start = time.perf_counter()
        latency_ms, status_code, method = send_reservation_request(created_ids)
        data.append({
            "request_num": i,
            "latency_ms": latency_ms,
            "status_code": status_code,
            "method": method,
            "timestamp": wall_start,
        })

        if i % 100 == 0:
            errors_so_far = sum(1 for d in data if d["status_code"] >= 500 or d["status_code"] == 503)
            print(f"  {i}/{TOTAL_REQUESTS} done  |  errors so far: {errors_so_far}")

    # Cleanup
    print("\nStopping services...")
    reserv_proc.terminate()
    reserv_proc.wait()
    if avail_proc and avail_proc.poll() is None:
        avail_proc.terminate()
        avail_proc.wait()
    # Restore disk if left broken
    if os.path.exists(AVAIL_DB_BACKUP):
        restore_disk()
    print("  Done.")

    # Summary
    all_lats = [d["latency_ms"] for d in data]
    total = len(data)
    errors = sum(1 for d in data if d["status_code"] >= 500 or d["status_code"] == 503)
    timeouts = sum(1 for d in data if d["status_code"] == 503)
    p50 = percentile(all_lats, 50)
    p90 = percentile(all_lats, 90)
    p99 = percentile(all_lats, 99)
    p999 = percentile(all_lats, 99.9)

    print(f"\n  Results ({total} requests)")
    print(f"  {'─' * 40}")
    print(f"  p50:   {p50:.3f} ms")
    print(f"  p90:   {p90:.3f} ms")
    print(f"  p99:   {p99:.3f} ms")
    print(f"  p99.9: {p999:.3f} ms")
    print(f"  Errors: {errors} ({errors/total*100:.3f}%)  |  Timeouts: {timeouts}")

    print(f"\n  SLO Compliance")
    print(f"  {'─' * 40}")
    print(f"  Latency (p99 < 200ms):   {'MET' if p99 < 200 else 'BREACHED'}  (p99 = {p99:.3f} ms)")
    print(f"  Error rate (< 0.1%):     {'MET' if (errors / total) * 100 < 0.1 else 'BREACHED'}  ({(errors / total) * 100:.3f}%)")
    print(f"  Availability (> 99.9%):  {'MET' if ((total - errors) / total) * 100 > 99.9 else 'BREACHED'}  ({((total - errors) / total) * 100:.3f}%)")

    plot_results(data, "graphs/chaos_test.png")
    print("\nDone.")

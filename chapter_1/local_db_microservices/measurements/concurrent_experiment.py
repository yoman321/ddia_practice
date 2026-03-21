import sys
import os
sys.stdout.reconfigure(line_buffering=True)

import sqlite3
import threading
import requests
import random
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

BASE = "http://127.0.0.1:5005"
DATABASE = "../reservations.db"
TOTAL_REQUESTS = 1000
NUM_USERS = 50
REQUESTS_PER_USER = TOTAL_REQUESTS // NUM_USERS  # 20 per user

NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"]
DATES = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
TIMES = ["18:00", "19:00", "20:00", "21:00"]
METHOD_COLORS = {"GET": "steelblue", "POST": "seagreen", "PUT": "darkorange", "DELETE": "crimson"}

created_ids = []
ids_lock = threading.Lock()


def truncate_tables():
    conn = sqlite3.connect(DATABASE)
    conn.execute("DELETE FROM reservations")
    conn.execute("DELETE FROM latency")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='reservations'")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='latency'")
    conn.commit()
    conn.close()
    print("Tables truncated.")


def user_worker(user_id):
    session = requests.Session()
    for _ in range(REQUESTS_PER_USER):
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

    session.close()
    print(f"  User {user_id:>2} done.")


def fetch_latency():
    conn = sqlite3.connect(DATABASE)
    rows = conn.execute("SELECT id, method, duration_ms FROM latency ORDER BY id").fetchall()
    conn.close()
    data = {}
    for row_id, method, duration_ms in rows:
        if method not in data:
            data[method] = {"x": [], "y": []}
        data[method]["x"].append(row_id)
        data[method]["y"].append(duration_ms)
    return data


def percentile(values, p):
    sorted_vals = sorted(values)
    idx = (p / 100) * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= len(sorted_vals):
        return sorted_vals[lo]
    return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])


def print_percentiles(data):
    print(f"\n{'Method':<10} {'Count':<8} {'p50':>8} {'p90':>8} {'p95':>8} {'Avg':>8} {'Max':>8}")
    print("-" * 60)
    for method in sorted(data.keys()):
        vals = data[method]["y"]
        print(f"{method:<10} {len(vals):<8} "
              f"{percentile(vals,50):>7.3f}ms {percentile(vals,90):>7.3f}ms "
              f"{percentile(vals,95):>7.3f}ms {sum(vals)/len(vals):>7.3f}ms "
              f"{max(vals):>7.3f}ms")


def plot(data, output_path):
    fig, ax = plt.subplots(figsize=(14, 6))
    for method, series in data.items():
        ax.plot(series["x"], series["y"],
                label=method, color=METHOD_COLORS.get(method, "gray"),
                linewidth=0.6, alpha=0.8)
    ax.set_title(f"Request Latency — {TOTAL_REQUESTS} Requests, {NUM_USERS} Concurrent Users", fontsize=14, fontweight="bold")
    ax.set_xlabel("Request Number", fontsize=11)
    ax.set_ylabel("Duration (ms)", fontsize=11)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.legend(title="Method", fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"Saved: {output_path}")


# --- Run ---
print(f"Truncating tables...")
truncate_tables()

print(f"\nStarting {NUM_USERS} concurrent users ({REQUESTS_PER_USER} requests each)...")
threads = [threading.Thread(target=user_worker, args=(i+1,)) for i in range(NUM_USERS)]
for t in threads:
    t.start()
for t in threads:
    t.join()

print("\nFetching latency data...")
data = fetch_latency()
print_percentiles(data)

output_path = "graphs/concurrent_1000_calls_50_users.png"
os.makedirs("graphs", exist_ok=True)
print("\nPlotting...")
plot(data, output_path)

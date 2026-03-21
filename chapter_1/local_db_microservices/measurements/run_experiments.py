import sys
import os
sys.path.insert(0, os.path.abspath(".."))

import sqlite3
import random
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from app import app as flask_app
DATABASE = "../reservations.db"
EXPERIMENTS = [1000, 10000, 100000]

NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"]
DATES = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
TIMES = ["18:00", "19:00", "20:00", "21:00"]
METHOD_COLORS = {"GET": "steelblue", "POST": "seagreen", "PUT": "darkorange", "DELETE": "crimson"}


def truncate_tables():
    conn = sqlite3.connect(DATABASE)
    conn.execute("DELETE FROM reservations")
    conn.execute("DELETE FROM latency")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='reservations'")
    conn.execute("DELETE FROM sqlite_sequence WHERE name='latency'")
    conn.commit()
    conn.close()


def run_requests(n):
    created_ids = []
    client = flask_app.test_client()
    for i in range(n):
        r = random.random()
        if r < 0.3 or not created_ids:
            resp = client.post("/reservations", json={
                "guest_name": random.choice(NAMES),
                "date": random.choice(DATES),
                "time": random.choice(TIMES),
                "party_size": random.randint(1, 8),
            })
            if resp.status_code == 201:
                created_ids.append(resp.get_json()["id"])
        elif r < 0.6:
            client.get("/reservations")
        elif r < 0.75:
            client.get(f"/reservations/{random.choice(created_ids)}")
        elif r < 0.9:
            client.put(f"/reservations/{random.choice(created_ids)}", json={
                "party_size": random.randint(1, 10)
            })
        else:
            rid = random.choice(created_ids)
            resp = client.delete(f"/reservations/{rid}")
            if resp.status_code == 200:
                created_ids.remove(rid)

        if (i + 1) % (n // 10) == 0:
            print(f"  {i + 1}/{n} requests done...")


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


def plot_experiment(n, data, output_path):
    fig, ax = plt.subplots(figsize=(14, 6))

    for method, series in data.items():
        ax.plot(
            series["x"],
            series["y"],
            label=method,
            color=METHOD_COLORS.get(method, "gray"),
            linewidth=0.6,
            alpha=0.8,
        )

    ax.set_title(f"Request Latency Over Time — {n:,} Requests", fontsize=14, fontweight="bold")
    ax.set_xlabel("Request Number", fontsize=11)
    ax.set_ylabel("Duration (ms)", fontsize=11)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.legend(title="Method", fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")


def print_percentiles(n, data):
    def percentile(values, p):
        sorted_vals = sorted(values)
        idx = (p / 100) * (len(sorted_vals) - 1)
        lo = int(idx)
        hi = lo + 1
        if hi >= len(sorted_vals):
            return sorted_vals[lo]
        return sorted_vals[lo] + (idx - lo) * (sorted_vals[hi] - sorted_vals[lo])

    print(f"\n  {'Method':<10} {'Count':<8} {'p50':>8} {'p90':>8} {'p95':>8} {'Avg':>8} {'Max':>8}")
    print(f"  {'-'*60}")
    for method in sorted(data.keys()):
        vals = data[method]["y"]
        print(f"  {method:<10} {len(vals):<8} "
              f"{percentile(vals,50):>7.3f}ms {percentile(vals,90):>7.3f}ms "
              f"{percentile(vals,95):>7.3f}ms {sum(vals)/len(vals):>7.3f}ms "
              f"{max(vals):>7.3f}ms")


# --- Run all experiments ---
os.makedirs("graphs", exist_ok=True)

for n in EXPERIMENTS:
    print(f"\n{'='*50}")
    print(f"Experiment: {n:,} requests")
    print(f"{'='*50}")

    print("Truncating tables...")
    truncate_tables()

    print("Running requests...")
    run_requests(n)

    print("Fetching latency data...")
    data = fetch_latency()

    print_percentiles(n, data)

    output_path = f"graphs/latency_{n}.png"
    print(f"Plotting graph...")
    plot_experiment(n, data, output_path)

print("\nAll experiments complete.")

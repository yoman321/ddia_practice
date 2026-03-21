import sqlite3
import statistics

DATABASE = "../reservations.db"


def percentile(data, p):
    sorted_data = sorted(data)
    index = (p / 100) * (len(sorted_data) - 1)
    lower = int(index)
    upper = lower + 1
    if upper >= len(sorted_data):
        return sorted_data[lower]
    fraction = index - lower
    return sorted_data[lower] + fraction * (sorted_data[upper] - sorted_data[lower])


conn = sqlite3.connect(DATABASE)
methods = [row[0] for row in conn.execute("SELECT DISTINCT method FROM latency ORDER BY method").fetchall()]

print(f"{'Method':<10} {'Count':<8} {'p50 (ms)':<12} {'p90 (ms)':<12} {'p95 (ms)':<12} {'Avg (ms)':<12} {'Min (ms)':<12} {'Max (ms)'}")
print("-" * 90)

for method in methods:
    rows = conn.execute(
        "SELECT duration_ms FROM latency WHERE method = ?", (method,)
    ).fetchall()
    durations = [r[0] for r in rows]

    p50 = round(percentile(durations, 50), 3)
    p90 = round(percentile(durations, 90), 3)
    p95 = round(percentile(durations, 95), 3)
    avg = round(statistics.mean(durations), 3)
    min_ms = round(min(durations), 3)
    max_ms = round(max(durations), 3)
    count = len(durations)

    print(f"{method:<10} {count:<8} {p50:<12} {p90:<12} {p95:<12} {avg:<12} {min_ms:<12} {max_ms}")

conn.close()

import sys
import os
sys.stdout.reconfigure(line_buffering=True)
sys.path.insert(0, os.path.abspath(".."))

import random
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import boto3
import json
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv
from app import app as flask_app

load_dotenv(find_dotenv())

ENDPOINT = os.environ.get("MINIO_ENDPOINT")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
LATENCY_BUCKET = "latency"
RESERVATIONS_BUCKET = "reservations"

EXPERIMENTS = [1000]
NAMES = ["Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank"]
DATES = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
TIMES = ["18:00", "19:00", "20:00", "21:00"]
METHOD_COLORS = {"GET": "steelblue", "POST": "seagreen", "PUT": "darkorange", "DELETE": "crimson"}


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )


def truncate_bucket(client, bucket):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def truncate_all():
    client = get_s3()
    for bucket in [RESERVATIONS_BUCKET, LATENCY_BUCKET]:
        truncate_bucket(client, bucket)
    print("  Buckets truncated.")


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
    s3 = get_s3()
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=LATENCY_BUCKET):
        for obj in page.get("Contents", []):
            resp = s3.get_object(Bucket=LATENCY_BUCKET, Key=obj["Key"])
            records.append(json.loads(resp["Body"].read().decode()))
    records.sort(key=lambda x: x["created_at"])
    data = {}
    for i, rec in enumerate(records):
        method = rec["method"]
        if method not in data:
            data[method] = {"x": [], "y": []}
        data[method]["x"].append(i + 1)
        data[method]["y"].append(rec["duration_ms"])
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
    print(f"\n  {'Method':<10} {'Count':<8} {'p50':>8} {'p90':>8} {'p95':>8} {'Avg':>8} {'Max':>8}")
    print(f"  {'-'*60}")
    for method in sorted(data.keys()):
        vals = data[method]["y"]
        print(f"  {method:<10} {len(vals):<8} "
              f"{percentile(vals,50):>7.1f}ms {percentile(vals,90):>7.1f}ms "
              f"{percentile(vals,95):>7.1f}ms {sum(vals)/len(vals):>7.1f}ms "
              f"{max(vals):>7.1f}ms")


def plot_experiment(n, data, output_path):
    fig, ax = plt.subplots(figsize=(14, 6))
    for method, series in data.items():
        ax.plot(series["x"], series["y"],
                label=method, color=METHOD_COLORS.get(method, "gray"),
                linewidth=0.6, alpha=0.8)
    ax.set_title(f"Request Latency Over Time — {n:,} Requests (S3 Object Store)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Request Number", fontsize=11)
    ax.set_ylabel("Duration (ms)", fontsize=11)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.legend(title="Method", fontsize=10)
    ax.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()
    print(f"  Saved: {output_path}")


# --- Run experiments ---
os.makedirs("graphs", exist_ok=True)

for n in EXPERIMENTS:
    print(f"\n{'='*50}")
    print(f"Experiment: {n:,} requests (S3)")
    print(f"{'='*50}")
    print("Truncating buckets...")
    truncate_all()
    print("Running requests...")
    run_requests(n)
    print("Fetching latency data from S3...")
    data = fetch_latency()
    print_percentiles(data)
    output_path = f"graphs/latency_{n}.png"
    print("Plotting graph...")
    plot_experiment(n, data, output_path)

print("\nAll experiments complete.")

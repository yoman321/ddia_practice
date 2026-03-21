import boto3
import json
import uuid
import os
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

ENDPOINT = os.environ.get("MINIO_ENDPOINT")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
RESERVATIONS_BUCKET = "reservations"
LATENCY_BUCKET = "latency"


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )


def init_storage():
    client = get_client()
    for bucket in [RESERVATIONS_BUCKET, LATENCY_BUCKET]:
        try:
            client.create_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
                raise


def get_next_id(client, bucket):
    try:
        resp = client.get_object(Bucket=bucket, Key="_counter")
        counter = int(resp["Body"].read().decode())
    except ClientError:
        counter = 0
    next_id = counter + 1
    client.put_object(Bucket=bucket, Key="_counter", Body=str(next_id).encode())
    return next_id


def put_object(client, bucket, key, data):
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data).encode(),
        ContentType="application/json",
    )


def get_object(client, bucket, key):
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def list_objects(client, bucket):
    results = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if obj["Key"].startswith("_"):
                continue
            data = get_object(client, bucket, obj["Key"])
            if data:
                results.append(data)
    return results


def delete_object(client, bucket, key):
    client.delete_object(Bucket=bucket, Key=key)


def record_latency(method, endpoint, status_code, duration_ms):
    client = get_client()
    record_id = str(uuid.uuid4())
    put_object(client, LATENCY_BUCKET, record_id, {
        "id": record_id,
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        "duration_ms": round(duration_ms, 3),
        "created_at": datetime.utcnow().isoformat(),
    })

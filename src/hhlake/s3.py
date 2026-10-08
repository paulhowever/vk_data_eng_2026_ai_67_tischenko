import hashlib
from functools import lru_cache

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

from . import config

STATS = {"get_requests": 0, "get_bytes": 0, "put_requests": 0, "put_bytes": 0}


@lru_cache(maxsize=1)
def client():
    return boto3.client(
        "s3",
        endpoint_url=config.ENDPOINT,
        aws_access_key_id=config.ACCESS_KEY,
        aws_secret_access_key=config.SECRET_KEY,
        region_name=config.REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 5, "mode": "standard"},
            max_pool_connections=32,
        ),
    )


def ensure_bucket(bucket=config.BUCKET):
    existing = {b["Name"] for b in client().list_buckets().get("Buckets", [])}
    if bucket not in existing:
        client().create_bucket(Bucket=bucket)


def object_size(key, bucket=config.BUCKET):
    return client().head_object(Bucket=bucket, Key=key)["ContentLength"]


def get_range(key, start, end, bucket=config.BUCKET):
    if end <= start:
        return b""
    body = client().get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end - 1}")["Body"].read()
    STATS["get_requests"] += 1
    STATS["get_bytes"] += len(body)
    return body


def get_bytes(key, bucket=config.BUCKET):
    body = client().get_object(Bucket=bucket, Key=key)["Body"].read()
    STATS["get_requests"] += 1
    STATS["get_bytes"] += len(body)
    return body


def put_bytes(key, data, bucket=config.BUCKET):
    client().put_object(Bucket=bucket, Key=key, Body=data)
    STATS["put_requests"] += 1
    STATS["put_bytes"] += len(data)


def put_text(key, text, bucket=config.BUCKET):
    put_bytes(key, text.encode("utf-8"), bucket)


def get_text(key, bucket=config.BUCKET):
    return get_bytes(key, bucket).decode("utf-8")


def list_keys(prefix, bucket=config.BUCKET):
    keys = []
    for page in client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def delete_prefix(prefix, bucket=config.BUCKET):
    keys = list_keys(prefix, bucket)
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i:i + 1000]]
        client().delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
    return len(keys)


def upload_file(path, key, bucket=config.BUCKET):
    transfer = TransferConfig(multipart_threshold=64 << 20, multipart_chunksize=64 << 20, max_concurrency=8)
    client().upload_file(str(path), bucket, key, Config=transfer)


def sha256_object(key, bucket=config.BUCKET, chunk=8 << 20):
    digest = hashlib.sha256()
    body = client().get_object(Bucket=bucket, Key=key)["Body"]
    for part in body.iter_chunks(chunk):
        digest.update(part)
    return digest.hexdigest()


def sha256_file(path, chunk=8 << 20):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(chunk):
            digest.update(block)
    return digest.hexdigest()

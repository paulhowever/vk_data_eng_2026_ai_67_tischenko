import time

from . import config, s3


def main():
    s3.ensure_bucket()
    print(f"bucket: {config.BUCKET} @ {config.ENDPOINT}")
    local = config.DATASET_PATH
    local_size = local.stat().st_size
    started = time.perf_counter()
    s3.upload_file(local, config.RAW_KEY)
    elapsed = time.perf_counter() - started
    head = s3.client().head_object(Bucket=config.BUCKET, Key=config.RAW_KEY)
    print(f"uploaded {local} -> {config.s3_url(config.RAW_KEY)}")
    print(f"local size   {local_size:,} B")
    print(f"remote size  {head['ContentLength']:,} B  etag {head['ETag']}")
    print(f"upload time  {elapsed:.2f} s  ({local_size / elapsed / 2**20:.0f} MiB/s)")
    local_hash = s3.sha256_file(local)
    remote_hash = s3.sha256_object(config.RAW_KEY)
    print(f"sha256 local  {local_hash}")
    print(f"sha256 remote {remote_hash}")
    print("integrity     OK" if local_hash == remote_hash else "integrity     MISMATCH")


if __name__ == "__main__":
    main()

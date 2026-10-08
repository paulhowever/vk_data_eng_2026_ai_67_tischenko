import argparse
import hashlib
import os
import shlex
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from . import config, s3

ALIAS = os.environ.get("MC_ALIAS", "hh")
DISKS = [config.ROOT / ".data" / f"disk{i}" for i in range(1, 5)]
CHUNK = 32 << 20
PARALLEL = (1, 2, 4, 10)


def shown(argv):
    return shlex.join(str(arg).replace(str(config.ROOT) + "/", "") for arg in argv)


def run(argv):
    print(f"\n$ {shown(argv)}", flush=True)
    if shutil.which(argv[0]) is None:
        print(f"{argv[0]}: not found")
        return None
    result = subprocess.run(
        [str(arg) for arg in argv], cwd=config.ROOT, capture_output=True, text=True, check=False
    )
    print((result.stdout + result.stderr).rstrip())
    return result.stdout


def kib_on_disk(path):
    output = subprocess.run(["du", "-sk", str(path)], capture_output=True, text=True, check=True).stdout
    return int(output.split()[0]) * 1024


def erasure_layout(size):
    shards = [disk / config.BUCKET / config.RAW_KEY for disk in DISKS]
    run(["du", "-sh", *shards])
    if not all(path.exists() for path in shards):
        return
    used = [kib_on_disk(path) for path in shards]
    print(f"object {size:,} B; on disk: " + ", ".join(f"disk{i} {u / size:.3f}" for i, u in enumerate(used, 1))
          + f" of the object; all disks {sum(used) / size:.2f}x")
    print("each disk keeps 1/2 of the object and 4 disks keep 2x: erasure set of 4 drives, "
          "2 data + 2 parity shards per block (EC 2+2), any 2 drives may be lost")


def mc_sha256(target):
    print(f"\n$ mc cat {target} | sha256", flush=True)
    if shutil.which("mc") is None:
        print("mc: not found")
        return None
    digest = hashlib.sha256()
    with subprocess.Popen(["mc", "cat", target], stdout=subprocess.PIPE) as proc:
        while block := proc.stdout.read(8 << 20):
            digest.update(block)
    print(digest.hexdigest())
    return digest.hexdigest()


def stream_rate():
    started = time.perf_counter()
    body = s3.client().get_object(Bucket=config.BUCKET, Key=config.RAW_KEY)["Body"]
    size = sum(len(part) for part in body.iter_chunks(8 << 20))
    return size, time.perf_counter() - started


def ranged(index, size):
    started = time.perf_counter()
    data = s3.get_range(config.RAW_KEY, index * CHUNK, min((index + 1) * CHUNK, size))
    return len(data), time.perf_counter() - started


def throughput(size):
    print("\nS3 read throughput from this host (boto3, threads in one process)")
    total, seconds = stream_rate()
    print(f"  1 GET of the whole object     {total / 2**20:7.0f} MiB  {seconds:5.2f} s  {total / 2**20 / seconds:5.0f} MiB/s")
    for workers in PARALLEL:
        count = 2 * workers
        started = time.perf_counter()
        with ThreadPoolExecutor(workers) as pool:
            results = list(pool.map(ranged, range(count), [size] * count))
        seconds = time.perf_counter() - started
        total = sum(length for length, _ in results)
        per_get = sum(t for _, t in results) / count
        print(f"  {workers:>2} parallel x {count:>2} Range GETs of 32 MiB  {seconds:5.2f} s  "
              f"{total / 2**20 / seconds:5.0f} MiB/s total, {per_get:.2f} s per GET")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.storage",
                                     description="Evidence for the storage stages: cluster, bucket, object, disks")
    parser.add_argument("--no-throughput", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    target = f"{ALIAS}/{config.BUCKET}/{config.RAW_KEY}"
    print(f"RustFS {config.ENDPOINT}, bucket {config.BUCKET}, mc alias {ALIAS}")
    run(["docker", "compose", "ps"])
    run(["mc", "stat", target])
    run(["mc", "ls", "-r", f"{ALIAS}/{config.BUCKET}"])
    run(["mc", "du", f"{ALIAS}/{config.BUCKET}"])
    run(["mc", "du", "--depth", "2", f"{ALIAS}/{config.BUCKET}"])
    run(["du", "-sh", *DISKS])
    head = s3.client().head_object(Bucket=config.BUCKET, Key=config.RAW_KEY)
    size = head["ContentLength"]
    erasure_layout(size)
    print(f"\nhead_object {config.s3_url(config.RAW_KEY)}: {size:,} B, ETag {head['ETag']} "
          f"(multipart: ETag is not an MD5 of the content, integrity is checked with sha256)")
    by_mc = mc_sha256(target)
    by_boto = s3.sha256_object(config.RAW_KEY)
    print(f"\nsha256 via boto3 GetObject stream: {by_boto}")
    if by_mc is not None:
        print(f"mc cat and boto3 agree: {'yes' if by_mc == by_boto else 'NO'}; "
              "the local file hash is printed by python -m hhlake.upload")
    if not args.no_throughput:
        throughput(size)


if __name__ == "__main__":
    main()

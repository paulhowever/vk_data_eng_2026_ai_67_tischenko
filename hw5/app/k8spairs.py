import argparse
import json
import os
import resource
import sys
import time
from collections import Counter
from functools import partial
from itertools import pairwise
from pathlib import Path

from hhlake import config, s3
from hhlake.mr import jobs, split
from hhlake.segment import FIELDS

K = 2
MIB = 1 << 20
CGROUP = Path("/sys/fs/cgroup")
TMP_PREFIX = "tmp/k8s"
RESULTS_PREFIX = "results/k8s"


def bounds(size, parts, index):
    return size * index // parts, size * (index + 1) // parts


def cgroup_value(name):
    try:
        raw = (CGROUP / name).read_text().strip()
    except OSError:
        return None
    return raw if raw == "max" else int(raw)


def memory_report():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_mib = rss / MIB if sys.platform == "darwin" else rss / 1024
    peak = cgroup_value("memory.peak")
    limit = cgroup_value("memory.max")
    return {
        "ru_maxrss_mib": round(rss_mib, 1),
        "cgroup_memory_peak_mib": round(peak / MIB, 1) if isinstance(peak, int) else None,
        "cgroup_memory_max_mib": round(limit / MIB, 1) if isinstance(limit, int) else limit,
    }


def fetcher(key):
    return partial(s3.get_range, key), s3.object_size(key)


def map_range(fetch, size, start, end, chunk, log=print):
    stats = {"records": 0, "variant": 0, "with_skills": 0, "emitted": 0, "skill_sizes": Counter()}
    counts = Counter()
    first = last = None
    for sub_start, sub_end in split.plan(end - start, chunk):
        piece = split.read_split(fetch, size, start + sub_start, start + sub_end)
        result = jobs.map_rows(split.rows(piece.data), (K,), False)
        counts.update(result.counts[K])
        stats["records"] += result.records
        stats["variant"] += result.variant
        stats["with_skills"] += result.with_skills
        stats["emitted"] += result.emitted[K]
        stats["skill_sizes"].update(result.skill_sizes)
        first = piece.first if first is None else first
        last = piece.last
        log(f"  chunk [{piece.first:,}, {piece.last:,})  records {result.records:,}  variant {result.variant}", flush=True)
        del piece, result
    stats["first"], stats["last"] = first, last
    return counts, stats


def checks(counts, stats, tiles, data_start, size):
    contiguous = (
        tiles[0][0] == data_start
        and tiles[-1][1] == size
        and all(left[1] == right[0] for left, right in pairwise(tiles))
    )
    keys, total, key, count = jobs.CONTROL[K]
    rows = [
        ("aligned ranges tile [header_end, size)", contiguous, True),
        ("records", stats["records"], jobs.EXPECTED["records"]),
        ("variant", stats["variant"], jobs.EXPECTED["variant"]),
        ("with skills", stats["with_skills"], jobs.EXPECTED["with_skills"]),
        (f"k={K} sum(count) == emitted", sum(counts.values()), stats["emitted"]),
        (f"k={K} unique keys", len(counts), keys),
        (f"k={K} sum count", sum(counts.values()), total),
        (f"k={K} top", jobs.top(counts)[0] if counts else None, (key, count)),
    ]
    return [(name, actual, expected, actual == expected) for name, actual, expected in rows]


def finish(counts, stats, tiles, fetch, size, out_key, log=print):
    header, data_start = split.read_header(fetch, size)
    if header != FIELDS:
        raise ValueError(f"unexpected header: {header}")
    table = checks(counts, stats, tiles, data_start, size)
    for name, actual, expected, ok in table:
        log(f"check {'OK  ' if ok else 'FAIL'} {name}: {actual} (expected {expected})")
    text = jobs.render_csv(counts, K, False)
    s3.put_text(out_key, text)
    log(f"written {config.s3_url(out_key)}  {len(text.encode('utf-8')):,} B")
    log("top-10:")
    for (a, b), n in jobs.top(counts, 10):
        log(f"  {a}+{b} {n}")
    return all(ok for *_, ok in table)


def run_full(args):
    began = time.perf_counter()
    fetch, size = fetcher(args.key)
    print(f"full: object {config.s3_url(args.key)} size {size:,} B, chunk {args.chunk_mib} MiB", flush=True)
    counts, stats = map_range(fetch, size, 0, size, int(args.chunk_mib * MIB))
    ok = finish(counts, stats, [(stats["first"], stats["last"])], fetch, size, f"{args.out}/full/k2.csv")
    print(f"time {time.perf_counter() - began:.1f} s")
    print("memory " + json.dumps(memory_report()), flush=True)
    return 0 if ok else 1


def run_map(args):
    began = time.perf_counter()
    index = int(os.environ["JOB_COMPLETION_INDEX"]) if args.index is None else args.index
    fetch, size = fetcher(args.key)
    start, end = bounds(size, args.parts, index)
    print(f"map {index}/{args.parts}: range [{start:,}, {end:,}) chunk {args.chunk_mib} MiB "
          f"pod {os.environ.get('HOSTNAME', '-')} node {os.environ.get('NODE_NAME', '-')}", flush=True)
    counts, stats = map_range(fetch, size, start, end, int(args.chunk_mib * MIB))
    blobs, _ = jobs.shuffle_blobs({K: counts}, 1)
    prefix = f"{TMP_PREFIX}/{args.run}"
    s3.put_bytes(f"{prefix}/part-{index:03d}.jsonl", blobs.get(0, b""))
    seconds = time.perf_counter() - began
    meta = {
        "index": index,
        "parts": args.parts,
        "range": [start, end],
        "aligned": [stats["first"], stats["last"]],
        "records": stats["records"],
        "variant": stats["variant"],
        "with_skills": stats["with_skills"],
        "emitted": stats["emitted"],
        "keys": len(counts),
        "seconds": round(seconds, 2),
        "node": os.environ.get("NODE_NAME"),
        "memory": memory_report(),
    }
    s3.put_text(f"{prefix}/meta-{index:03d}.json", json.dumps(meta))
    print(f"aligned [{stats['first']:,}, {stats['last']:,}) records {stats['records']:,} "
          f"variant {stats['variant']} keys {len(counts):,} time {seconds:.1f} s")
    print("memory " + json.dumps(meta["memory"]), flush=True)
    return 0


def run_reduce(args):
    began = time.perf_counter()
    fetch, size = fetcher(args.key)
    prefix = f"{TMP_PREFIX}/{args.run}"
    metas = sorted((json.loads(s3.get_text(key)) for key in s3.list_keys(f"{prefix}/meta-")), key=lambda m: m["index"])
    indexes = [meta["index"] for meta in metas]
    if indexes != list(range(args.parts)):
        raise RuntimeError(f"expected parts 0..{args.parts - 1}, found {indexes}")
    totals = {K: Counter()}
    stats = {"records": 0, "variant": 0, "with_skills": 0, "emitted": 0}
    for meta in metas:
        jobs.merge_blob(s3.get_bytes(f"{prefix}/part-{meta['index']:03d}.jsonl"), totals)
        for name in stats:
            stats[name] += meta[name]
        print(f"part {meta['index']:>2} node {meta['node']!s:<16} aligned [{meta['aligned'][0]:,}, {meta['aligned'][1]:,}) "
              f"records {meta['records']:>6,} variant {meta['variant']:>4} keys {meta['keys']:>5,} "
              f"time {meta['seconds']:>5.1f} s peak {meta['memory']['cgroup_memory_peak_mib']} MiB")
    tiles = [tuple(meta["aligned"]) for meta in metas]
    ok = finish(totals[K], stats, tiles, fetch, size, f"{args.out}/{args.run}/k2.csv")
    print(f"time {time.perf_counter() - began:.1f} s")
    print("memory " + json.dumps(memory_report()), flush=True)
    return 0 if ok else 1


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m k8spairs", description="Skill pairs (k=2) for Kubernetes Jobs")
    parser.add_argument("mode", choices=("full", "map", "reduce"))
    parser.add_argument("--key", default=config.RAW_KEY)
    parser.add_argument("--chunk-mib", type=float, default=32)
    parser.add_argument("--parts", type=int, default=8)
    parser.add_argument("--index", type=int)
    parser.add_argument("--run", default="indexed")
    parser.add_argument("--out", default=RESULTS_PREFIX)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    print(f"endpoint {config.ENDPOINT} bucket {config.BUCKET}", flush=True)
    return {"full": run_full, "map": run_map, "reduce": run_reduce}[args.mode](args)


if __name__ == "__main__":
    raise SystemExit(main())

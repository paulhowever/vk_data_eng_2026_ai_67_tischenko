import argparse
import csv
import json
import sys
import time
from itertools import combinations, groupby
from operator import itemgetter

from .. import config, s3
from ..segment import COL, FIELDS, is_variant, normalize_skills
from . import engine, jobs

RESULTS_NAIVE = f"{config.RESULTS_MR}_naive"
SEPARATOR = "|"

csv.field_size_limit(sys.maxsize)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m hhlake.mr.naive", description="Single-process MapReduce without combiner"
    )
    parser.add_argument("--k", type=int, nargs="+", choices=(1, 2, 3), default=[1, 2, 3])
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args(argv)


class Stream:
    def __init__(self, key, chunk=1 << 20):
        self.body = s3.client().get_object(Bucket=config.BUCKET, Key=key)["Body"]
        self.chunk = chunk
        self.bytes = 0

    def __iter__(self):
        for line in self.body.iter_lines(chunk_size=self.chunk, keepends=True):
            self.bytes += len(line)
            yield line.decode("utf-8")


def emit_all(reader, ks):
    pairs = {k: [] for k in ks}
    stats = {"records": 0, "variant": 0, "with_skills": 0}
    for row in reader:
        stats["records"] += 1
        if not is_variant(row):
            continue
        stats["variant"] += 1
        skills = normalize_skills(row[COL["key_skills"]])
        if not skills:
            continue
        stats["with_skills"] += 1
        for k in ks:
            pairs[k].extend((SEPARATOR.join(combo), 1) for combo in combinations(skills, k))
    return pairs, stats


def reduce_sorted(pairs):
    pairs.sort()
    return {
        tuple(key.split(SEPARATOR)): sum(one for _, one in group)
        for key, group in groupby(pairs, key=itemgetter(0))
    }


def matches_control(k, counts):
    keys, total, key, count = jobs.CONTROL[k]
    return (len(counts), sum(counts.values()), jobs.top(counts)[0]) == (keys, total, (key, count))


def main(argv=None):
    args = parse_args(argv)
    ks = tuple(sorted(set(args.k)))
    began = time.perf_counter()
    stream = Stream(config.RAW_KEY)
    reader = csv.reader(stream)
    header = next(reader)
    if header != FIELDS:
        raise ValueError(f"unexpected header: {header}")
    pairs, stats = emit_all(reader, ks)
    mapped = time.perf_counter()
    print(f"naive MapReduce, 1 process, no combiner, keys joined with '{SEPARATOR}'")
    print(f"input {config.s3_url(config.RAW_KEY)} streamed: 1 GET, {stream.bytes:,} B")
    print(f"records {stats['records']:,}  variant {stats['variant']:,}  with skills {stats['with_skills']:,}")
    print(f"map (stream + parse + emit) {mapped - began:.2f} s, peak RSS {engine.peak_rss_mib():.0f} MiB")
    per_k = {}
    for k in ks:
        emitted = len(pairs[k])
        started = time.perf_counter()
        counts = reduce_sorted(pairs.pop(k))
        reduce_seconds = time.perf_counter() - started
        top_key, top_count = jobs.top(counts)[0]
        entry = {
            "emitted": emitted,
            "unique_keys": len(counts),
            "sum_count": sum(counts.values()),
            "time_sort_group": round(reduce_seconds, 3),
            "top": [list(top_key), top_count],
            "control_ok": matches_control(k, counts),
        }
        line = (
            f"k={k}: emitted {emitted:,} pairs  sort+group {reduce_seconds:.2f} s  keys {len(counts):,}  "
            f"sum {entry['sum_count']:,}  top ({', '.join(top_key)}) {top_count:,}"
        )
        if not args.no_write:
            key = jobs.output_key(RESULTS_NAIVE, k, False)
            size, digest = engine.publish(key, jobs.render_csv(counts, k, False))
            reference = engine.object_sha256(jobs.output_key(config.RESULTS_MR, k, False))
            same = digest == reference
            entry.update(output_key=key, output_bytes=size, output_sha256=digest, same_as_mapreduce=same)
            line += (
                f"\n     {config.s3_url(key)} {size:,} B sha256 {digest}"
                f"  identical to {config.RESULTS_MR}/k{k}.csv: {'yes' if same else 'NO'}"
            )
        per_k[str(k)] = entry
        print(line)
    checks_ok = all(stats[name] == jobs.EXPECTED[name] for name in jobs.EXPECTED) and all(
        entry["control_ok"] for entry in per_k.values()
    )
    time_total = time.perf_counter() - began
    print(f"checks vs control numbers: {'ok' if checks_ok else 'MISMATCH'}")
    print(f"total {time_total:.2f} s, peak RSS {engine.peak_rss_mib():.0f} MiB")
    metrics = {
        "mode": "naive",
        "time_total": round(time_total, 3),
        "time_map": round(mapped - began, 3),
        **stats,
        "get_requests": 1,
        "get_bytes": stream.bytes,
        "peak_rss_mib": round(engine.peak_rss_mib(), 1),
        "checks_ok": checks_ok,
        "k": per_k,
    }
    print(json.dumps(metrics, ensure_ascii=False))


if __name__ == "__main__":
    main()

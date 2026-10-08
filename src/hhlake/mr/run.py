import argparse
import hashlib
import json
import os
import statistics
import time
from collections import Counter
from itertools import chain, pairwise
from math import comb

from .. import config, s3
from ..segment import REGIONS
from . import engine, jobs

HEAVY = 20
STAGES = (
    ("fetch_s", "fetch: Range GET + record alignment"),
    ("parse_s", "parse: UTF-8 decode + csv.reader"),
    ("combine_s", "combine: filter, normalize, combinations, Counter"),
    ("serialize_s", "serialize: JSON lines per reducer"),
    ("put_s", "put: shuffle PUT to S3"),
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.mr.run", description="Skill combinations MapReduce over S3")
    parser.add_argument("--k", type=int, nargs="+", choices=(1, 2, 3), default=[1, 2, 3])
    parser.add_argument("--regions", action="store_true")
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    parser.add_argument("--chunk-mib", type=float, default=32)
    parser.add_argument("--reducers", type=int, default=4)
    parser.add_argument("--fail-mapper", type=int)
    parser.add_argument("--fail-attempt", type=int, default=1)
    parser.add_argument("--job")
    parser.add_argument("--keep-tmp", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args(argv)


def fmt(value):
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], tuple):
        return f"({', '.join(value[0])}) {value[1]:,}"
    return str(value)


def combos_total(sizes, k):
    return sum(vacancies * comb(n, k) for n, vacancies in sizes.items())


def checks(result, totals):
    job = result.job
    maps = result.maps
    tiles = [(report.first, report.last) for report in maps]
    contiguous = (
        tiles[0][0] == result.data_start
        and tiles[-1][1] == job.size
        and all(left[1] == right[0] for left, right in pairwise(tiles))
    )
    rows = [
        ("aligned ranges tile [header_end, size)", contiguous, True),
        ("records", totals["records"], jobs.EXPECTED["records"]),
        ("unique vacancy_id", totals["unique_ids"], totals["records"]),
        ("variant", totals["variant"], jobs.EXPECTED["variant"]),
        ("with skills", totals["with_skills"], jobs.EXPECTED["with_skills"]),
        ("vacancies in skills-per-vacancy histogram", sum(totals["skill_sizes"].values()), totals["with_skills"]),
    ]
    for k, stats in totals["k"].items():
        rows.append((f"k={k} sum(count) == emitted", stats["sum_count"], stats["emitted"]))
        if not job.regional:
            rows.append((f"k={k} emitted == sum vac(n)*C(n,{k})", stats["emitted"], combos_total(totals["skill_sizes"], k)))
        rows.append((f"k={k} reducer key sets disjoint", stats["reducer_keys"], stats["unique_keys"]))
        if not job.regional:
            keys, total, key, count = jobs.CONTROL[k]
            rows.append((f"k={k} unique keys", stats["unique_keys"], keys))
            rows.append((f"k={k} sum count", stats["sum_count"], total))
            rows.append((f"k={k} top", stats["top"], (key, count)))
    return [(name, actual, expected, actual == expected) for name, actual, expected in rows]


def aggregate(result):
    maps = result.maps
    records = sum(report.records for report in maps)
    totals = {
        "records": records,
        "unique_ids": len(set(chain.from_iterable(report.ids for report in maps))),
        "variant": sum(report.variant for report in maps),
        "with_skills": sum(report.with_skills for report in maps),
        "region_variant": sum((report.region_variant for report in maps), Counter()),
        "region_with_skills": sum((report.region_with_skills for report in maps), Counter()),
        "skill_sizes": sum((report.skill_sizes for report in maps), Counter()),
        "k": {},
    }
    for k in result.job.ks:
        counter = result.counts[k]
        totals["k"][k] = {
            "emitted": sum(report.emitted[k] for report in maps),
            "combined": sum(report.combined[k] for report in maps),
            "shuffle_bytes": sum(report.shuffle_bytes[k] for report in maps),
            "unique_keys": len(counter),
            "reducer_keys": sum(len(report.counts[k]) for report in result.reduces),
            "sum_count": sum(counter.values()),
            "top": jobs.top(counter)[0] if counter and not result.job.regional else None,
        }
    return totals


def print_attempts(result):
    failed = [a for a in result.attempts if a.status != "ok"]
    charged = sum(1 for a in failed if a.charged)
    print(f"attempts: {len(result.attempts)} total, {len(failed)} failed and retried "
          f"({charged} charged to the failing task, {len(failed) - charged} collateral: "
          f"terminated or never started because the pool broke)")


def breakdown(result, workers):
    maps = result.maps
    stages = {}
    for name, _ in STAGES:
        values = [getattr(report, name) for report in maps]
        stages[name] = {"sum": sum(values), "mean": statistics.fmean(values), "max": max(values)}
    ready = [report.worker_ready_s for report in maps if report.worker_ready_s is not None]
    busy = sum(report.seconds for report in maps)
    fetch_rates = [report.get_bytes / 2**20 / report.fetch_s for report in maps if report.fetch_s > 0]
    return {
        "stages": stages,
        "task_seconds_sum": busy,
        "ideal_map_s": busy / workers,
        "fetch_mib_s_per_task": statistics.median(fetch_rates) if fetch_rates else None,
        "fetch_mib_s_aggregate": sum(report.get_bytes for report in maps) / 2**20 / result.time_map,
        "worker_ready_s": [min(ready), max(ready)] if ready else None,
        "workers_started": len(ready),
    }


def print_breakdown(result, workers, parts):
    title = f"map stages over {len(result.maps)} accepted attempts, s"
    print(f"\n{title:<54}{'sum':>7} {'mean':>6} {'max':>6} {'share':>6}")
    total = parts["task_seconds_sum"]
    for name, label in STAGES:
        stage = parts["stages"][name]
        print(f"  {label:<52}{stage['sum']:>7.2f} {stage['mean']:>6.2f} {stage['max']:>6.2f} "
              f"{stage['sum'] / total:>6.1%}")
    print(f"  {'task total':<52}{total:>7.2f}")
    print(f"  fetch throughput: median {parts['fetch_mib_s_per_task']:.0f} MiB/s per task, "
          f"{parts['fetch_mib_s_aggregate']:.0f} MiB/s for the whole map phase")
    if parts["worker_ready_s"]:
        low, high = parts["worker_ready_s"]
        print(f"  worker start (spawn + interpreter + import boto3/hhlake): {parts['workers_started']} processes, "
              f"{low:.2f}..{high:.2f} s after the pool was created")
    print(f"  map phase {result.time_map:.2f} s vs sum of task time / {workers} workers = {parts['ideal_map_s']:.2f} s")


def print_growth(totals):
    sizes = totals["skill_sizes"]
    values = list(sizes.elements())
    heavy = Counter({n: v for n, v in sizes.items() if n >= HEAVY})
    print(f"\nskills per vacancy ({len(values):,} variant vacancies with skills): mean {statistics.fmean(values):.2f}, "
          f"median {statistics.median(values):g}, max {max(values)}")
    shares = []
    for k in totals["k"]:
        total = combos_total(sizes, k)
        shares.append(f"k={k} {combos_total(heavy, k) / total:.1%}")
    print(f"  {sum(heavy.values())} vacancies with n >= {HEAVY} skills ({sum(heavy.values()) / len(values):.1%}) "
          f"emit {', '.join(shares)} of all combinations; full table: python -m hhlake.growth")


def print_tops(result, n):
    for k in result.job.ks:
        counter = result.counts[k]
        if result.job.regional:
            for area in REGIONS:
                local = {key[1:]: count for key, count in counter.items() if key[0] == area}
                line = "; ".join(fmt(item) for item in jobs.top(local, n))
                print(f"  k={k} {area}: {line}")
        else:
            line = "; ".join(fmt(item) for item in jobs.top(counter, n))
            print(f"  k={k}: {line}")


def print_tmp(job):
    inventory = engine.tmp_inventory(job)
    accepted, orphans = inventory["accepted"], inventory["orphans"]
    print(f"\ntmp kept: {config.s3_url(job.prefix)}  {inventory['objects']} objects")
    print("  _manifest.json lists one accepted attempt per map task; reducers read only those prefixes")
    print(f"  accepted  {len(accepted):>3} prefixes, {sum(map(len, accepted.values())):>4} parts")
    print(f"  orphans   {len(orphans):>3} prefixes, {sum(map(len, orphans.values())):>4} parts "
          f"(crashed or terminated attempts, never read)")
    for head, parts in orphans.items():
        print(f"    {head}/: {' '.join(parts)}")
    if job.fail_mapper is not None:
        entry = next(e for e in inventory["manifest"]["maps"] if e["task"] == job.fail_mapper)
        print(f"  manifest entry of mapper {job.fail_mapper}: attempt {entry['attempt']}, range {entry['range']}, "
              f"aligned {entry['aligned']}, parts {entry['parts']}")
    print(f"  inspect: mc ls -r hh/{config.BUCKET}/{job.prefix}")
    print(f"  remove:  mc rm -r --force hh/{config.BUCKET}/{job.prefix}")


def write_outputs(result, ks, regional, write, compare):
    outputs = {}
    if not (write or compare):
        return outputs
    print("\noutputs")
    for k in ks:
        key = jobs.output_key(config.RESULTS_MR, k, regional)
        text = jobs.render_csv(result.counts[k], k, regional)
        data = text.encode("utf-8")
        digest = hashlib.sha256(data).hexdigest()
        previous = engine.object_sha256(key) if compare else None
        if write:
            engine.publish(key, text)
        if not compare:
            state = ""
        elif previous is None:
            state = "  object in S3 before this run: absent"
        else:
            state = f"  object in S3 before this run: {'identical' if previous == digest else 'DIFFERENT'}"
        print(f"  {config.s3_url(key)}  {len(data):,} B  sha256 {digest}{'' if write else '  (not written)'}{state}")
        outputs[k] = (key, len(data), digest, None if previous is None else previous == digest)
    return outputs


def main(argv=None):
    args = parse_args(argv)
    began = time.perf_counter()
    ks = tuple(sorted(set(args.k)))
    mode = "regions" if args.regions else "skills"
    name = args.job or f"{mode}-k{''.join(map(str, ks))}-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"
    job = engine.Job(
        name=name,
        ks=ks,
        regional=args.regions,
        reducers=args.reducers,
        size=s3.object_size(config.RAW_KEY),
        fail_mapper=args.fail_mapper,
        fail_attempt=args.fail_attempt,
    )
    chunk = int(args.chunk_mib * 2**20)
    print(f"job {job.name}  mode {mode}  k {' '.join(map(str, ks))}")
    print(f"input {config.s3_url(job.key)}  {job.size:,} B  endpoint {config.ENDPOINT}")
    print(f"chunk {args.chunk_mib:g} MiB  workers {args.workers}  reducers {job.reducers}")
    print(f"tmp {config.s3_url(job.prefix)}")
    if job.fail_mapper is not None:
        code, attempt = engine.CRASH_EXIT_CODE, job.fail_attempt
        print(f"fault injection: mapper {job.fail_mapper} exits with code {code} on attempt {attempt}")
    print(flush=True)

    result = engine.execute(job, chunk, args.workers, keep_tmp=args.keep_tmp)
    totals = aggregate(result)
    parts = breakdown(result, args.workers)
    print()
    print_attempts(result)
    print(f"map {result.time_map:.2f} s   reduce {result.time_reduce:.2f} s   map tasks {len(result.tasks)}")

    get_requests = result.header_requests + sum(report.get_requests for report in result.maps)
    get_bytes = result.header_bytes + sum(report.get_bytes for report in result.maps)
    print(f"S3 input: {get_requests} GET, {get_bytes:,} B ({get_bytes - job.size:+,} B vs object)")
    print(
        f"S3 shuffle: {sum(len(r.parts) for r in result.maps)} PUT, "
        f"{sum(r.reads for r in result.reduces)} GET, {sum(r.read_bytes for r in result.reduces):,} B"
    )
    print_breakdown(result, args.workers, parts)

    print("\nchecks")
    results = checks(result, totals)
    for check_name, actual, expected, ok in results:
        print(f"  {'ok ' if ok else 'BAD'}  {check_name:<41} {fmt(actual):>25}  expected {fmt(expected)}")
    passed = all(ok for *_, ok in results)

    if args.regions:
        print("\nregions (variant / with skills)")
        for area in REGIONS:
            print(f"  {area:<16} {totals['region_variant'][area]:>6,} / {totals['region_with_skills'][area]:,}")
    else:
        print_growth(totals)

    print(f"\n{'k':>2} {'emitted':>10} {'combined':>10} {'ratio':>6} {'shuffle B':>12} {'keys':>9} {'sum':>9}")
    for k, stats in totals["k"].items():
        ratio = stats["combined"] / stats["emitted"] if stats["emitted"] else 0
        print(
            f"{k:>2} {stats['emitted']:>10,} {stats['combined']:>10,} {ratio:>6.1%} "
            f"{stats['shuffle_bytes']:>12,} {stats['unique_keys']:>9,} {stats['sum_count']:>9,}"
        )
    print("\ntop")
    print_tops(result, 3 if args.regions else 5)

    if not passed:
        raise SystemExit("invariant check failed, results are not written")

    write_started = time.perf_counter()
    outputs = write_outputs(result, ks, args.regions, not args.no_write,
                            not args.no_write or job.fail_mapper is not None)
    time_write = time.perf_counter() - write_started
    if args.keep_tmp:
        print_tmp(job)
    else:
        print(f"\ntmp removed: {result.tmp_removed} objects under {config.s3_url(job.prefix)}")

    time_total = time.perf_counter() - began
    peak_worker = max(report.peak_rss_mib for report in [*result.maps, *result.reduces])
    print(f"ru_maxrss of single processes: master {engine.peak_rss_mib():.0f} MiB, largest worker {peak_worker:.0f} MiB "
          f"(whole process tree: python -m hhlake.bench)")
    print(f"total {time_total:.2f} s")
    failed = [a for a in result.attempts if a.status != "ok"]
    metrics = {
        "job": job.name,
        "mode": mode,
        "workers": args.workers,
        "chunk_mib": args.chunk_mib,
        "map_tasks": len(result.tasks),
        "reducers": job.reducers,
        "attempts": len(result.attempts),
        "failed_attempts": len(failed),
        "charged_failures": sum(1 for a in failed if a.charged),
        "time_total": round(time_total, 3),
        "time_map": round(result.time_map, 3),
        "time_reduce": round(result.time_reduce, 3),
        "time_write": round(time_write, 3),
        "map_breakdown_s": {name: round(parts["stages"][name]["sum"], 3) for name, _ in STAGES},
        "map_task_seconds_sum": round(parts["task_seconds_sum"], 3),
        "worker_ready_s": [round(v, 3) for v in parts["worker_ready_s"]] if parts["worker_ready_s"] else None,
        "records": totals["records"],
        "unique_ids": totals["unique_ids"],
        "variant": totals["variant"],
        "with_skills": totals["with_skills"],
        "skills_per_vacancy": {str(n): totals["skill_sizes"][n] for n in sorted(totals["skill_sizes"])},
        "get_requests": get_requests,
        "get_bytes": get_bytes,
        "shuffle_put_requests": sum(len(r.parts) for r in result.maps),
        "shuffle_get_requests": sum(r.reads for r in result.reduces),
        "peak_rss_mib_master": round(engine.peak_rss_mib(), 1),
        "peak_rss_mib_worker": round(peak_worker, 1),
        "checks_ok": passed,
        "k": {
            str(k): {
                "emitted": stats["emitted"],
                "combined": stats["combined"],
                "unique_keys": stats["unique_keys"],
                "sum_count": stats["sum_count"],
                "shuffle_bytes": stats["shuffle_bytes"],
                "output_key": outputs[k][0] if k in outputs else None,
                "output_bytes": outputs[k][1] if k in outputs else None,
                "output_sha256": outputs[k][2] if k in outputs else None,
                "same_as_previous_object": outputs[k][3] if k in outputs else None,
            }
            for k, stats in totals["k"].items()
        },
    }
    print(json.dumps(metrics, ensure_ascii=False))


if __name__ == "__main__":
    main()

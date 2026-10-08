import argparse
import io
import json
import statistics
import time
from math import comb

import matplotlib
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from matplotlib.figure import Figure

from . import bench, config, duck, s3, verify

KS = (1, 2, 3)
HEAVY = 20
OUTPUT_KEY = "results/growth/skills_per_vacancy.csv"
PLOT = config.ROOT / "report" / "img" / "growth.png"

HISTOGRAM_SQL = f"""
SELECT n, count(*) AS vacancies
FROM (
    SELECT len({duck.NORMALIZED_SKILLS}) AS n
    FROM {duck.source_sql()}
    WHERE {duck.VARIANT}
)
WHERE n > 0
GROUP BY n
ORDER BY n
"""


def histogram(con):
    started = time.perf_counter()
    rows = con.execute(HISTOGRAM_SQL).fetchall()
    print(f"skills per vacancy counted by DuckDB over {config.s3_url(config.RAW_KEY)} in {time.perf_counter() - started:.2f} s")
    return {int(n): int(v) for n, v in rows}


def table_rows(sizes):
    totals = {k: sum(v * comb(n, k) for n, v in sizes.items()) for k in KS}
    vacancies = sum(sizes.values())
    rows, tail = [], dict.fromkeys(KS, 0)
    for n in sorted(sizes, reverse=True):
        v = sizes[n]
        for k in KS:
            tail[k] += v * comb(n, k)
        rows.append({
            "n": n,
            "vacancies": v,
            "share_vacancies": v / vacancies,
            **{f"c{k}": comb(n, k) for k in (2, 3)},
            **{f"sum_c{k}": v * comb(n, k) for k in KS},
            **{f"share_c{k}": v * comb(n, k) / totals[k] for k in KS},
            **{f"tail_share_c{k}": tail[k] / totals[k] for k in KS},
        })
    return rows[::-1], totals


def print_table(rows, totals):
    header = (f"  {'n':>3} {'vacancies':>9} {'share':>6} {'C(n,2)':>7} {'C(n,3)':>7} {'sum C(n,1)':>10} "
              f"{'sum C(n,2)':>10} {'sum C(n,3)':>10} {'k2 share':>8} {'k3 share':>8} {'k3 from >=n':>11}")
    print(f"\ntable: n skills -> vacancies and combinations they emit\n{header}")
    for r in rows:
        print(f"  {r['n']:>3} {r['vacancies']:>9,} {r['share_vacancies']:>6.1%} {r['c2']:>7,} {r['c3']:>7,} "
              f"{r['sum_c1']:>10,} {r['sum_c2']:>10,} {r['sum_c3']:>10,} {r['share_c2']:>8.1%} {r['share_c3']:>8.1%} "
              f"{r['tail_share_c3']:>11.1%}")
    vacancies = sum(r["vacancies"] for r in rows)
    print(f"  {'all':>3} {vacancies:>9,} {1:>6.1%} {'':>7} {'':>7} {totals[1]:>10,} {totals[2]:>10,} {totals[3]:>10,}")


def band(rows, low, high=None):
    selected = [r for r in rows if r["n"] >= low and (high is None or r["n"] <= high)]
    vacancies = sum(r["vacancies"] for r in selected)
    shares = {k: sum(r[f"share_c{k}"] for r in selected) for k in KS}
    return vacancies, shares


def print_summary(sizes, rows, totals):
    values = [n for n, v in sorted(sizes.items()) for _ in range(v)]
    total = len(values)
    mean = statistics.fmean(values)
    print(f"\n{total:,} variant vacancies with skills, {totals[1]:,} skill mentions")
    print(f"skills per vacancy: mean {mean:.2f}, median {statistics.median(values):g}, "
          f"p90 {statistics.quantiles(values, n=10)[-1]:g}, max {max(values)}")
    print("\nwhere the combinations come from")
    for title, low, high in (("n <= 6 (median and below)", 1, 6), ("7 <= n <= 19", 7, 19), (f"n >= {HEAVY}", HEAVY, None)):
        vacancies, shares = band(rows, low, high)
        print(f"  {title:<26} {vacancies:>5,} vacancies ({vacancies / total:5.1%}) -> "
              + ", ".join(f"k={k} {shares[k]:5.1%}" for k in KS))
    print("\nwhy: C(n,k) grows like n^k/k!, so the mean of C(n,3) is far above C(mean n,3)")
    for k in (2, 3):
        print(f"  k={k}: mean C(n,{k}) per vacancy {totals[k] / total:.1f}  vs  C({mean:.2f},{k}) = "
              f"{falling(mean, k):.1f};  C(30,{k}) = {comb(30, k):,}  vs  C(6,{k}) = {comb(6, k)}")


def falling(x, k):
    value = 1.0
    for i in range(k):
        value *= (x - i) / (i + 1)
    return value


def result_sums(con):
    sums = {}
    for prefix, name in ((config.RESULTS_MR, "mapreduce"), (config.RESULTS_DUCK, "duckdb")):
        for k in KS:
            columns = duck.result_columns(k)
            table = f"{name}_k{k}"
            header = verify.load(con, table, f"{prefix}/k{k}.csv", columns)
            if header != columns:
                raise SystemExit(f"{config.s3_url(f'{prefix}/k{k}.csv')}: header {header}")
            keys, total = con.execute(f"SELECT count(*), sum(count) FROM {table}").fetchone()
            sums[name, k] = (keys, int(total), s3.object_size(f"{prefix}/k{k}.csv"))
    return sums


def bench_frame(name):
    try:
        return pd.read_csv(io.StringIO(s3.get_text(f"{config.RESULTS_BENCH}/{name}")), dtype={"k": str, "job_json": str})
    except (ClientError, BotoCoreError):
        return pd.DataFrame()


def mr_job(runs):
    if runs.empty:
        return None
    rows = runs[(runs["suite"] == "main") & (runs["scenario"] == "mr_k123") & (runs["exit_code"] == 0)
                & (~runs["cold"].astype(bool))]
    return json.loads(rows.iloc[0]["job_json"]) if not rows.empty else None


def bench_cell(summary, scenario):
    if summary.empty:
        return "-", "-"
    row = summary[(summary["suite"] == "main") & (summary["scenario"] == scenario)]
    if row.empty:
        return "-", "-"
    row = row.iloc[0]
    return f"{row['time_mean_s']:.2f}±{row['time_std_s']:.2f}", f"{row['rss_mean_mib']:.0f}"


def print_growth_by_k(sums, job, summary, vacancies, skills):
    print(f"\ngrowth by k ({vacancies:,} variant vacancies with skills, {skills:,} distinct skills)")
    print(f"  {'k':>2} {'emitted':>9} {'per vac':>7} {'combined':>9} {'kept':>6} {'unique':>8} {'unique/emit':>11} "
          f"{'possible C(955,k)':>17} {'filled':>7} {'shuffle B':>11} {'result B':>10} "
          f"{'MR s':>10} {'MR MiB':>7} {'Duck s':>10} {'Duck MiB':>8}")
    for k in KS:
        keys, total, size = sums["mapreduce", k]
        stats = (job or {}).get("k", {}).get(str(k), {})
        combined = stats.get("combined")
        shuffle = stats.get("shuffle_bytes")
        mr_time, mr_mem = bench_cell(summary, f"mr_k{k}")
        duck_time, duck_mem = bench_cell(summary, f"duck_k{k}")
        possible = comb(skills, k)
        print(f"  {k:>2} {total:>9,} {total / vacancies:>7.1f} {fmt_int(combined):>9} "
              f"{(combined / total if combined else 0):>6.1%} {keys:>8,} {keys / total:>11.1%} {possible:>17,} "
              f"{keys / possible:>7.2%} {fmt_int(shuffle):>11} {size:>10,} {mr_time:>10} {mr_mem:>7} {duck_time:>10} "
              f"{duck_mem:>8}")
    if job is None:
        print("  combined / shuffle: no MapReduce metrics in results/bench/runs.csv yet (run python -m hhlake.bench)")
    else:
        print(f"  combined, shuffle: map-side counts after the combiner, MapReduce job {job['job']} from results/bench/runs.csv")
    print("  MR s / Duck s: bench mean ± std of warm runs, MiB: peak RSS of the whole process tree (results/bench/summary.csv)")


def fmt_int(value):
    return "-" if value is None else f"{value:,}"


def checks(sizes, totals, sums, job):
    rows = [("vacancies with skills", sum(sizes.values()), verify.CONTROL["with_skills"])]
    for k in KS:
        rows.append((f"k={k} sum C(n,{k}) == control", totals[k], verify.CONTROL[k]["sum_count"]))
        for name in ("mapreduce", "duckdb"):
            rows.append((f"k={k} sum C(n,{k}) == sum(count) {name}", totals[k], sums[name, k][1]))
    if job and job.get("skills_per_vacancy"):
        mr = {int(n): v for n, v in job["skills_per_vacancy"].items()}
        rows.append(("histogram DuckDB == histogram MapReduce mappers", sizes, mr))
    print("\nchecks")
    ok = True
    for name, actual, expected in rows:
        good = actual == expected
        ok &= good
        shown = (f"{actual:,}", f"{expected:,}") if isinstance(actual, int) else (f"{len(actual)} bins", f"{len(expected)} bins")
        print(f"  {'ok ' if good else 'BAD'}  {name:<48} {shown[0]:>12}  expected {shown[1]}")
    return ok


def plot(rows, path):
    ns = [r["n"] for r in rows]
    with matplotlib.rc_context(bench.STYLE):
        fig = Figure(figsize=(11, 4), layout="constrained")
        left, right = fig.subplots(1, 2)
        left.bar(ns, [r["vacancies"] for r in rows], color=bench.BLUE, width=0.8)
        left.set_title("Вакансий с n навыками (вариант PHP, 4 380 с навыками)", loc="left")
        left.set_xlabel("n — навыков в вакансии")
        left.set_ylabel("вакансий")
        width = 0.27
        for offset, k, color in ((-width, 1, bench.BLUE_LIGHT), (0, 2, bench.BLUE), (width, 3, bench.ORANGE)):
            right.bar([n + offset for n in ns], [100 * r[f"share_c{k}"] for r in rows], width=width, color=color,
                      label=f"k={k}: доля Σ C(n,{k})")
        right.axvline(HEAVY - 0.5, color=bench.INK_2, linestyle="--", linewidth=1)
        heavy = next(r for r in rows if r["n"] >= HEAVY)
        right.annotate(f"n ≥ {HEAVY}: {heavy['tail_share_c3']:.0%} троек", (HEAVY, 100 * max(r["share_c3"] for r in rows)),
                       xytext=(4, 0), textcoords="offset points", fontsize=8, color=bench.INK_2)
        right.set_title("Вклад вакансий с n навыками в число сочетаний, %", loc="left")
        right.set_xlabel("n — навыков в вакансии")
        right.set_ylabel("% от всех сочетаний данного k")
        right.legend(loc="upper right")
        fig.savefig(path)


def publish(rows):
    frame = pd.DataFrame(rows)
    text = frame.to_csv(index=False, float_format="%.6f")
    s3.put_text(OUTPUT_KEY, text)
    local = config.ROOT / OUTPUT_KEY
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(text, encoding="utf-8")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.growth",
                                     description="Why the number of skill combinations grows with k")
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    started = time.perf_counter()
    con = duck.connect()
    sizes = histogram(con)
    rows, totals = table_rows(sizes)
    sums = result_sums(con)
    runs, summary = bench_frame("runs.csv"), bench_frame("summary.csv")
    job = mr_job(runs)
    print_summary(sizes, rows, totals)
    print_table(rows, totals)
    print_growth_by_k(sums, job, summary, sum(sizes.values()), sums["mapreduce", 1][0])
    ok = checks(sizes, totals, sums, job)
    if not args.no_write:
        publish(rows)
        PLOT.parent.mkdir(parents=True, exist_ok=True)
        plot(rows, PLOT)
        print(f"\nwrote {config.s3_url(OUTPUT_KEY)} (local {OUTPUT_KEY}), plot {PLOT.relative_to(config.ROOT)}")
    print(f"total {time.perf_counter() - started:.2f} s")
    if not ok:
        raise SystemExit("growth checks failed")


if __name__ == "__main__":
    main()

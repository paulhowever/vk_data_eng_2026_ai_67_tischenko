import argparse
import sys
import time
from itertools import zip_longest

from . import config, duck, s3, segment

CONTROL = {
    "records": 229_388,
    "variant": 5_382,
    "with_skills": 4_380,
    1: {"unique_keys": 955, "sum_count": 31_835, "top": (("php",), 4_033)},
    2: {"unique_keys": 15_921, "sum_count": 135_362, "top": (("mysql", "php"), 2_096)},
    3: {"unique_keys": 127_466, "sum_count": 472_071, "top": (("git", "mysql", "php"), 1_030)},
}
KS = (1, 2, 3)
NAIVE_PREFIX = f"{config.RESULTS_MR}_naive"
CSV_OPTIONS = """header = true, all_varchar = true, delim = ',', quote = '"', escape = '"'"""


class Verdict:
    def __init__(self):
        self.failed = []

    def check(self, label, ok):
        if not ok:
            self.failed.append(label)
        return "ok" if ok else "FAIL"

    @property
    def match(self):
        return not self.failed


def exists(key):
    return key in s3.list_keys(key)


def load(con, table, key, columns):
    source = f"read_csv({duck.quote(config.s3_url(key))}, {CSV_OPTIONS})"
    header = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()]
    if header == columns:
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {table} AS
            SELECT {', '.join(columns[:-1])}, CAST(count AS BIGINT) AS count
            FROM {source}
        """)
    return header


def summary(con, table, k, regions=False):
    keys = ", ".join(duck.result_columns(k, regions)[:-1])
    rows, unique, total = con.execute(f"SELECT count(*), count(DISTINCT ({keys})), sum(count) FROM {table}").fetchone()
    return {"rows": rows, "unique_keys": unique, "sum_count": int(total or 0)}


def except_all(con, left, right, columns):
    cols = ", ".join(columns)
    return con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM {left} EXCEPT ALL SELECT {cols} FROM {right})").fetchone()[0]


def show_top(con, left, right, k, names):
    print(f"  top-10      {'#':>2}  {names[0]:<34}{'count':>7}   {names[1]:<34}{'count':>7}")
    pairs = zip_longest(duck.top_rows(con, left, k), duck.top_rows(con, right, k), fillvalue=((), 0))
    for rank, ((ls, lc), (rs, rc)) in enumerate(pairs, 1):
        mark = "" if (ls, lc) == (rs, rc) else "  <-- differs"
        print(f"              {rank:>2}  {duck.fmt_combo(ls):<34}{lc:>7,}   {duck.fmt_combo(rs):<34}{rc:>7,}{mark}")


def show_areas(con, left, right, names):
    query = "SELECT area, count(*), sum(count) FROM {} GROUP BY area ORDER BY area"
    sides = [{a: (n, int(t)) for a, n, t in con.execute(query.format(table)).fetchall()} for table in (left, right)]
    for area in segment.REGIONS:
        (ln, lt), (rn, rt) = (side.get(area, (0, 0)) for side in sides)
        print(f"  {area:<16} keys {ln:>7,} | {rn:>7,}   sum(count) {lt:>8,} | {rt:>8,}   ({names[0]} | {names[1]})")


def compare(con, verdict, k, regions, left_prefix, left_name):
    name = f"{'regions_' if regions else ''}k{k}"
    columns = duck.result_columns(k, regions)
    keys = {left_name: f"{left_prefix}/{name}.csv", "duckdb": f"{config.RESULTS_DUCK}/{name}.csv"}
    tables = {left_name: f"{left_name}_{name}", "duckdb": f"duckdb_{name}"}
    print(f"\n{name}  {config.s3_url(keys[left_name])}  vs  {config.s3_url(keys['duckdb'])}")
    missing = [side for side, key in keys.items() if not exists(key)]
    if missing:
        status = verdict.check(f"{name} {left_name} present", False)
        print(f"  missing in S3: {', '.join(config.s3_url(keys[side]) for side in missing)}  {status}")
        return None
    headers = {side: load(con, tables[side], keys[side], columns) for side in keys}
    header_ok = all(header == columns for header in headers.values())
    print(f"  header      {' | '.join(','.join(h) for h in headers.values())}  {verdict.check(f'{name} header', header_ok)}")
    if not header_ok:
        return None
    stats = {side: summary(con, tables[side], k, regions) for side in keys}
    for metric in ("rows", "unique_keys", "sum_count"):
        values = [stats[side][metric] for side in keys]
        print(f"  {metric:<11} {values[0]:>9,} | {values[1]:>9,}  {verdict.check(f'{name} {metric}', values[0] == values[1])}")
    left_only = except_all(con, tables[left_name], tables["duckdb"], columns)
    right_only = except_all(con, tables["duckdb"], tables[left_name], columns)
    status = verdict.check(f"{name} except", left_only == 0 and right_only == 0)
    print(f"  EXCEPT ALL  ({left_name} EXCEPT ALL duckdb) = {left_only} rows, "
          f"(duckdb EXCEPT ALL {left_name}) = {right_only} rows  {status}")
    if regions:
        show_areas(con, tables[left_name], tables["duckdb"], list(keys))
    else:
        show_top(con, tables[left_name], tables["duckdb"], k, list(keys))
        for side in keys:
            stats[side]["top"] = duck.top_rows(con, tables[side], k, limit=1)[0]
    return stats


def raw_counts(con, verdict):
    started = time.perf_counter()
    records, variant, with_skills = con.execute(f"""
        SELECT
            count(*) AS records,
            count(*) FILTER (WHERE {duck.VARIANT}) AS variant,
            count(*) FILTER (WHERE {duck.VARIANT} AND len({duck.NORMALIZED_SKILLS}) > 0) AS with_skills
        FROM {duck.source_sql()}
    """).fetchone()
    print(f"\nraw {config.s3_url(config.RAW_KEY)} counted by DuckDB in {time.perf_counter() - started:.2f} s")
    print(f"  {'metric':<12}{'control':>10}{'duckdb':>10}")
    for metric, value in (("records", records), ("variant", variant), ("with_skills", with_skills)):
        print(f"  {metric:<12}{CONTROL[metric]:>10,}{value:>10,}  {verdict.check(metric, value == CONTROL[metric])}")


def fmt_top(top):
    return f"{duck.fmt_combo(top[0])} {top[1]:,}" if top else "-"


def control_table(verdict, results):
    print("\ncontrol numbers (reference) vs MapReduce vs DuckDB, all read from S3")
    print(f"  {'k':<2} {'metric':<12}{'control':>26}{'mapreduce':>26}{'duckdb':>26}")
    for k in KS:
        stats = results.get(k) or {}
        mr, dd = stats.get("mapreduce", {}), stats.get("duckdb", {})
        for metric in ("unique_keys", "sum_count", "top"):
            expected = CONTROL[k][metric]
            got = [side.get(metric) for side in (mr, dd)]
            ok = all(value == expected for value in got)
            if metric == "top":
                cells = [fmt_top(expected), *(fmt_top(value) for value in got)]
            else:
                cells = [f"{expected:,}", *(f"{value:,}" if value is not None else "-" for value in got)]
            status = verdict.check(f"k{k} control {metric}", ok)
            print(f"  {k:<2} {metric:<12}{cells[0]:>26}{cells[1]:>26}{cells[2]:>26}  {status}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.verify")
    parser.add_argument("--mr-prefix", default=config.RESULTS_MR)
    parser.add_argument("--naive", action="store_true")
    parser.add_argument("--no-regions", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    started = time.perf_counter()
    verdict = Verdict()
    con = duck.connect()
    print(f"verify: {config.s3_url(args.mr_prefix)}/ vs {config.s3_url(config.RESULTS_DUCK)}/, "
          f"both read from S3 by DuckDB httpfs @ {config.duck_endpoint()}")
    raw_counts(con, verdict)
    results = {k: compare(con, verdict, k, False, args.mr_prefix, "mapreduce") for k in KS}
    control_table(verdict, results)
    if not args.no_regions:
        print(f"\nregions ({', '.join(segment.REGIONS)}), key (area, combo)")
        for k in KS:
            compare(con, verdict, k, True, args.mr_prefix, "mapreduce")
    if args.naive:
        print("\nnaive MapReduce v1 vs DuckDB")
        for k in KS:
            compare(con, verdict, k, False, NAIVE_PREFIX, "naive")
    print(f"\nchecked in {time.perf_counter() - started:.2f} s")
    if verdict.match:
        print("verdict: MATCH")
    else:
        print(f"verdict: MISMATCH ({len(verdict.failed)} failed: {', '.join(verdict.failed)})")
    sys.exit(0 if verdict.match else 1)


if __name__ == "__main__":
    main()

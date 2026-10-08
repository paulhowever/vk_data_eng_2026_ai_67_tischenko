import argparse
import json
import resource
import sys
import time
from itertools import pairwise

import duckdb

from . import config, s3, segment

COLLATION_PROBE = [
    "php", "php 8", "php-fpm", "php7", "php7+", "1c", "1с", "_rest", "~git", "c#", "c++",
    "é", "z", "битрикс", "ёж", "яндекс", "€", "\uff10", "\U0001d538",
]
QUERY_COLUMNS = ["vacancy_id", "area", "role", "name", "key_skills"]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


VARIANT = """role = 'Программист, разработчик'
      AND name ILIKE '%php%'"""

REGION_FILTER = "area IN ('Москва', 'Санкт-Петербург', 'Екатеринбург')"

NORMALIZED_SKILLS = """list_sort(list_distinct(list_filter(
            list_transform(string_split(key_skills, ';'), x -> lower(trim(x))),
            x -> x <> ''
        )))"""


def connect(threads=None, memory_limit=None):
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"""
        CREATE OR REPLACE SECRET hh_lake (
            TYPE s3,
            KEY_ID {quote(config.ACCESS_KEY)},
            SECRET {quote(config.SECRET_KEY)},
            ENDPOINT {quote(config.duck_endpoint())},
            URL_STYLE 'path',
            USE_SSL false,
            REGION {quote(config.REGION)}
        )
    """)
    con.execute("SET enable_progress_bar = false")
    con.execute("SET enable_external_file_cache = false")
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit = {quote(memory_limit)}")
    return con


def setting(con, name):
    return con.execute("SELECT current_setting(?)", [name]).fetchone()[0]


def source_sql(source="csv"):
    if source == "parquet":
        return f"read_parquet({quote(config.s3_url(config.PARQUET_KEY))})"
    return f"read_csv({quote(config.s3_url(config.RAW_KEY))}, header = true, all_varchar = true)"


def base_ctes(source="csv", regions=False):
    region_filter = f"\n      AND {REGION_FILTER}" if regions else ""
    return f"""WITH vac AS (
    SELECT vacancy_id, area, key_skills
    FROM {source_sql(source)}
    WHERE {VARIANT}{region_filter}
),
skills AS (
    SELECT
        vacancy_id,
        area,
        {NORMALIZED_SKILLS} AS skills
    FROM vac
),
s AS (
    SELECT vacancy_id, area, unnest(skills) AS skill
    FROM skills
)"""


def skill_columns(k):
    return [f"skill{i}" for i in range(1, k + 1)]


def result_columns(k, regions=False):
    return (["area"] if regions else []) + skill_columns(k) + ["count"]


def order_by(k, regions=False):
    return ", ".join((["area"] if regions else []) + ["count DESC"] + skill_columns(k))


def combos_from(k, table, regions=False):
    area = "s1.area, " if regions else ""
    skills = ", ".join(f"s{i}.skill AS skill{i}" for i in range(1, k + 1))
    joins = "".join(
        f"\nJOIN {table} AS s{i} ON s{i}.vacancy_id = s1.vacancy_id AND s{i}.skill > s{i - 1}.skill"
        for i in range(2, k + 1)
    )
    return f"""SELECT {area}{skills}, count(*) AS count
FROM {table} AS s1{joins}
GROUP BY ALL
ORDER BY {order_by(k, regions)}"""


def combos_sql(k, source="csv", regions=False):
    return f"{base_ctes(source, regions)}\n{combos_from(k, 's', regions)}"


def skills_table_sql(table, source="csv", regions=False):
    return f"CREATE OR REPLACE TEMP TABLE {table} AS\n{base_ctes(source, regions)}\nSELECT vacancy_id, area, skill FROM s"


def output_key(k, regions=False):
    return f"{config.RESULTS_DUCK}/{'regions_' if regions else ''}k{k}.csv"


def check_collation(con):
    expected = sorted(COLLATION_PROBE)
    listed = con.execute("SELECT list_sort(?)", [COLLATION_PROBE]).fetchone()[0]
    ordered = [row[0] for row in con.execute("SELECT x FROM unnest(?) AS t(x) ORDER BY x", [COLLATION_PROBE]).fetchall()]
    less = all(con.execute("SELECT ? < ?", [a, b]).fetchone()[0] for a, b in pairwise(expected))
    ok = listed == expected and ordered == expected and less
    collation = setting(con, "default_collation") or "binary"
    print(f"string order: default_collation={collation}; list_sort / ORDER BY / '<' vs Python sorted() "
          f"on {len(expected)} probes (cyrillic, ё, 1c/1с, BMP vs astral): {'identical' if ok else 'DIFFERENT'}")
    if not ok:
        raise SystemExit("DuckDB string order differs from Python sorted(), combos would not match")


def digest(con, table, k, regions=False):
    columns = ", ".join(result_columns(k, regions))
    return con.execute(
        f"SELECT md5(string_agg(concat_ws(chr(31), {columns}), chr(30) ORDER BY {order_by(k, regions)})) FROM {table}"
    ).fetchone()[0]


def fmt_combo(skills):
    return " + ".join(skills)


def top_rows(con, table, k, limit=10):
    columns = ", ".join(skill_columns(k))
    rows = con.execute(f"SELECT {columns}, count FROM {table} ORDER BY {order_by(k)} LIMIT {limit}").fetchall()
    return [(row[:-1], row[-1]) for row in rows]


def top_by_area(con, table, k, limit=3):
    columns = ", ".join(skill_columns(k))
    rows = con.execute(f"""
        SELECT area, {columns}, count
        FROM {table}
        QUALIFY row_number() OVER (PARTITION BY area ORDER BY count DESC, {columns}) <= {limit}
        ORDER BY {order_by(k, regions=True)}
    """).fetchall()
    tops = {}
    for row in rows:
        tops.setdefault(row[0], []).append((row[1:-1], row[-1]))
    return tops


def write_result(con, table, k, regions):
    key = output_key(k, regions)
    query = f"SELECT * FROM {table} ORDER BY {order_by(k, regions)}"
    con.execute(f"COPY ({query}) TO {quote(config.s3_url(key))} (HEADER, DELIMITER ',')")
    local = config.LOCAL_RESULTS / "duckdb" / key.rsplit("/", 1)[-1]
    local.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {quote(local)} (HEADER, DELIMITER ',')")
    return key


def run_combos(con, k, source, regions, write, skills_table=None):
    table = f"{'regions_' if regions else ''}k{k}"
    query = combos_from(k, skills_table, regions) if skills_table else combos_sql(k, source, regions)
    started = time.perf_counter()
    con.execute(f"CREATE OR REPLACE TEMP TABLE {table} AS {query}")
    computed = time.perf_counter()
    key = write_result(con, table, k, regions) if write else None
    written = time.perf_counter()
    unique_keys, sum_count = con.execute(f"SELECT count(*), sum(count) FROM {table}").fetchone()
    result = {
        "unique_keys": unique_keys,
        "sum_count": int(sum_count),
        "time_compute": round(computed - started, 3),
        "time_write": round(written - computed, 3),
        "time": round(written - started, 3),
        "output_key": key,
        "digest": digest(con, table, k, regions),
    }
    print(f"\nk={k}  unique keys {unique_keys:,}  sum(count) {int(sum_count):,}  "
          f"compute {result['time_compute']:.2f} s  write {result['time_write']:.2f} s  "
          f"-> {config.s3_url(key) if key else '(not written)'}")
    print(f"  md5 of ordered rows {result['digest']}")
    if regions:
        per_area = con.execute(f"SELECT area, count(*), sum(count) FROM {table} GROUP BY area ORDER BY area").fetchall()
        tops = top_by_area(con, table, k)
        for area, keys, total in per_area:
            print(f"  {area}: keys {keys:,}  sum(count) {int(total):,}")
            for skills, count in tops[area]:
                print(f"      {count:>6,}  {fmt_combo(skills)}")
        result["per_area"] = {area: {"unique_keys": keys, "sum_count": int(total)} for area, keys, total in per_area}
        result["top"] = {area: {"skills": list(rows[0][0]), "count": rows[0][1]} for area, rows in tops.items()}
    else:
        top = top_rows(con, table, k)
        for rank, (skills, count) in enumerate(top, 1):
            print(f"  {rank:>2}. {count:>6,}  {fmt_combo(skills)}")
        result["top"] = {"skills": list(top[0][0]), "count": top[0][1]}
    return result


def combos(args):
    con = connect(args.threads, args.memory_limit)
    print(f"duckdb {duckdb.__version__}  threads={setting(con, 'threads')}  memory_limit={setting(con, 'memory_limit')}  "
          f"external_file_cache={setting(con, 'enable_external_file_cache')}")
    source_key = config.PARQUET_KEY if args.source == "parquet" else config.RAW_KEY
    print(f"source: {config.s3_url(source_key)} ({s3.object_size(source_key):,} B) via httpfs @ {config.duck_endpoint()}")
    print(f"mode: {'regions ' + ', '.join(segment.REGIONS) if args.regions else 'all areas'}")
    check_collation(con)
    report = {"source": args.source, "regions": args.regions, "threads": int(setting(con, "threads"))}
    skills_table = None
    if args.materialize:
        skills_table = "variant_skill"
        started = time.perf_counter()
        con.execute(skills_table_sql(skills_table, args.source, args.regions))
        elapsed = time.perf_counter() - started
        vacancies, rows = con.execute(f"SELECT count(DISTINCT vacancy_id), count(*) FROM {skills_table}").fetchone()
        print(f"scan: source read once into TEMP TABLE {skills_table} in {elapsed:.2f} s "
              f"({vacancies:,} vacancies with skills, {rows:,} (vacancy, skill) rows); k={', '.join(map(str, args.k))} "
              "are computed from it")
        report["scan"] = {"time": round(elapsed, 3), "vacancies": vacancies, "rows": rows}
    else:
        print(f"scan: source read once per k ({len(args.k)} scans)")
    report["materialize"] = args.materialize
    report["k"] = {str(k): run_combos(con, k, args.source, args.regions, not args.no_write, skills_table)
                   for k in args.k}
    return report


def http_summary(con):
    cursor = con.execute("""
        SELECT
            request.type AS method,
            request.url AS url,
            response.status AS status,
            request.duration_ms AS ms,
            TRY_CAST(response.headers['content-length'] AS BIGINT) AS bytes,
            TRY_CAST(regexp_extract(request.headers['Range'], 'bytes=(\\d+)-', 1) AS BIGINT) AS first_byte,
            TRY_CAST(regexp_extract(request.headers['Range'], 'bytes=\\d+-(\\d+)', 1) AS BIGINT) AS last_byte
        FROM duckdb_logs_parsed('HTTP')
        ORDER BY first_byte NULLS FIRST
    """)
    names = [column[0] for column in cursor.description]
    rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    gets = [r for r in rows if r["method"] == "GET"]
    heads = [r for r in rows if r["method"] == "HEAD"]
    get_bytes = sum(r["bytes"] for r in gets)
    contiguous = all(a["last_byte"] + 1 == b["first_byte"] for a, b in pairwise(gets))
    durations = [r["ms"] for r in gets]
    print("\nHTTP requests logged by DuckDB (CALL enable_logging('HTTP') -> duckdb_logs_parsed('HTTP')):")
    for url in sorted({r["url"] for r in rows}):
        print(f"  url      {url}")
    print(f"  HEAD     {len(heads)}  status {', '.join(sorted({r['status'] for r in heads}))}")
    print(f"  GET      {len(gets)}  status {', '.join(sorted({r['status'] for r in gets}))}")
    print(f"  GET body {get_bytes:,} B ({get_bytes / 2**20:.1f} MiB)")
    print(f"  ranges   bytes={gets[0]['first_byte']}-{gets[-1]['last_byte']}, "
          f"up to {max(r['bytes'] for r in gets):,} B per GET, contiguous without overlap: {contiguous}")
    print(f"  GET ms   min {min(durations)}  avg {sum(durations) / len(durations):.0f}  max {max(durations)}")
    return {"head": len(heads), "get": len(gets), "get_bytes": get_bytes, "contiguous": contiguous}


def explain(args):
    con = connect(args.threads, args.memory_limit)
    size = s3.object_size(config.RAW_KEY)
    print(f"duckdb {duckdb.__version__}  threads={setting(con, 'threads')}")
    print(f"object {config.s3_url(config.RAW_KEY)}: {size:,} B")
    con.execute("CALL enable_logging('HTTP')")
    started = time.perf_counter()
    plan = con.execute(f"EXPLAIN ANALYZE {combos_sql(1)}").fetchall()[0][1]
    elapsed = time.perf_counter() - started
    print(plan)
    summary = http_summary(con)
    print(f"  object size {size:,} B, downloaded / size = {summary['get_bytes'] / size:.4f}")
    print(f"EXPLAIN ANALYZE wall {elapsed:.2f} s")
    return {**summary, "object_size": size, "time": round(elapsed, 3)}


def to_parquet(args):
    con = connect(args.threads or 2, args.memory_limit or "4GB")
    print(f"duckdb {duckdb.__version__}  threads={setting(con, 'threads')}  memory_limit={setting(con, 'memory_limit')}")
    target = config.s3_url(config.PARQUET_KEY)
    started = time.perf_counter()
    con.execute(f"""
        COPY (SELECT * FROM {source_sql('csv')})
        TO {quote(target)} (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 20000)
    """)
    elapsed = time.perf_counter() - started
    csv_size = s3.object_size(config.RAW_KEY)
    size = s3.object_size(config.PARQUET_KEY)
    metadata = f"parquet_metadata({quote(target)})"
    rows, groups, smallest, largest = con.execute(f"""
        SELECT sum(num_rows), count(*), min(num_rows), max(num_rows)
        FROM (SELECT DISTINCT row_group_id, row_group_num_rows AS num_rows FROM {metadata})
    """).fetchone()
    projected = con.execute(
        f"SELECT sum(total_compressed_size) FROM {metadata} WHERE path_in_schema IN ({', '.join(map(quote, QUERY_COLUMNS))})"
    ).fetchone()[0]
    print(f"{config.s3_url(config.RAW_KEY)} ({csv_size:,} B) -> {target}")
    print(f"COPY time   {elapsed:.2f} s")
    print(f"size        {size:,} B ({size / 2**20:.1f} MiB), csv/parquet = {csv_size / size:.2f}x")
    print(f"rows        {int(rows):,} in {groups} row groups of {smallest:,}..{largest:,} rows (zstd, ROW_GROUP_SIZE 20000)")
    print(f"columns read by the combos query ({', '.join(QUERY_COLUMNS)}): "
          f"{projected:,} B compressed = {projected / size:.1%} of the file")
    return {
        "time": round(elapsed, 3), "size": size, "csv_size": csv_size, "rows": int(rows),
        "row_groups": groups, "projected_bytes": int(projected),
    }


def peak_rss_mib():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return round(peak / (2**20 if sys.platform == "darwin" else 2**10), 1)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.duck")
    parser.add_argument("--k", type=int, nargs="+", choices=[1, 2, 3])
    parser.add_argument("--source", choices=["csv", "parquet"], default="csv")
    parser.add_argument("--regions", action="store_true")
    parser.add_argument("--materialize", action="store_true",
                        help="scan the source once into a TEMP TABLE of (vacancy, skill) and compute every k from it")
    parser.add_argument("--explain", action="store_true")
    parser.add_argument("--to-parquet", action="store_true")
    parser.add_argument("--threads", type=int)
    parser.add_argument("--memory-limit")
    parser.add_argument("--no-write", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    started = time.perf_counter()
    report = {"engine": "duckdb", "version": duckdb.__version__}
    if args.to_parquet:
        report["to_parquet"] = to_parquet(args)
    if args.explain:
        report["explain"] = explain(args)
    if args.k or not (args.to_parquet or args.explain):
        args.k = args.k or [1, 2, 3]
        report.update(combos(args))
    report["time_total"] = round(time.perf_counter() - started, 3)
    report["peak_rss_mib"] = peak_rss_mib()
    print(f"\ntotal {report['time_total']:.2f} s, peak RSS {report['peak_rss_mib']} MiB")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()

import argparse
import csv
import io
import statistics
import time
from collections import namedtuple
from itertools import combinations

from . import config, duck, s3, segment, verify

Stat = namedtuple("Stat", "n emp med")

VACANCY_COLUMNS = [
    "vacancy_id", "employer_id", "area", "experience", "schedule",
    "salary_from", "salary_to", "currency", "gross", "key_skills",
]
SALARY_COLUMNS = ["salary_from", "salary_to", "currency", "gross"]

def vacancies_sql(source):
    return f"""
CREATE OR REPLACE TEMP TABLE vac AS
WITH raw AS (
    SELECT
        {', '.join(VACANCY_COLUMNS)},
        CAST(salary_from AS DOUBLE) AS low,
        CAST(salary_to AS DOUBLE) AS high
    FROM {source}
    WHERE {duck.VARIANT}
),
priced AS (
    SELECT
        *,
        CASE WHEN currency = 'RUR' THEN coalesce((low + high) / 2, low, high) END
            * CASE WHEN gross = 'true' THEN 0.87::DOUBLE ELSE 1 END AS mid
    FROM raw
)
SELECT
    {', '.join(VACANCY_COLUMNS)},
    coalesce({duck.NORMALIZED_SKILLS}, []) AS skills,
    CASE WHEN mid BETWEEN 20000 AND 1000000 THEN mid END AS net
FROM priced
"""

PAID_SKILL_SQL = """
CREATE OR REPLACE TEMP TABLE paid_skill AS
SELECT vacancy_id, employer_id, net, unnest(skills) AS skill
FROM vac
WHERE net IS NOT NULL
"""

PAIR_COLUMNS = [
    "skill1", "skill2", "base", "added", "n", "emp", "med_pair",
    "n_base", "med_base", "uplift", "uplift_pct",
]
TRIPLE_COLUMNS = [
    "skill1", "skill2", "skill3", "base1", "base2", "added", "n", "emp", "med_triple",
    "n_base", "med_base", "uplift", "uplift_pct", "base_skill", "med_base_skill", "uplift_vs_skill",
]
SINGLE_COLUMNS = ["skill", "count", "n", "emp", "med"]
TOP = 10


def load_vacancies(con):
    started = time.perf_counter()
    con.execute(vacancies_sql(duck.source_sql()))
    rows = con.execute("SELECT count(*) FROM vac").fetchone()[0]
    print(f"variant rows read from {config.s3_url(config.RAW_KEY)} by DuckDB httpfs: {rows:,} "
          f"in {time.perf_counter() - started:.2f} s")
    check_rules(con)


def as_segment_row(values):
    record = dict(zip(SALARY_COLUMNS, values, strict=True))
    return [record.get(name) for name in segment.FIELDS]


def check_rules(con):
    rows = con.execute(f"SELECT {', '.join(SALARY_COLUMNS)}, key_skills, net, skills FROM vac").fetchall()
    mismatches = sum(
        segment.salary_net(as_segment_row(salary)) != net or segment.normalize_skills(raw) != skills
        for *salary, raw, net, skills in rows
    )
    print(f"SQL net salary and skills vs segment.salary_net / segment.normalize_skills: "
          f"{len(rows):,} rows, {mismatches} mismatches")
    if mismatches:
        raise SystemExit("SQL rules differ from segment.py")


def mr_counts(con, name, columns):
    key = f"{config.RESULTS_MR}/{name}.csv"
    temp = f"mr_{name}"
    header = verify.load(con, temp, key, columns)
    if header != columns:
        raise SystemExit(f"{config.s3_url(key)}: header {header}, expected {columns}")
    counts = {tuple(row[:-1]): row[-1] for row in con.execute(f"SELECT * FROM {temp}").fetchall()}
    print(f"MapReduce {config.s3_url(key)}: {len(counts):,} keys, sum(count) {sum(counts.values()):,}")
    return counts


def combo_stats(con, k):
    skills = ", ".join(f"s{i}.skill" for i in range(1, k + 1))
    joins = "".join(
        f"\n        JOIN paid_skill AS s{i} ON s{i}.vacancy_id = s1.vacancy_id AND s{i}.skill > s{i - 1}.skill"
        for i in range(2, k + 1)
    )
    rows = con.execute(f"""
        SELECT [{skills}] AS combo, count(*), count(DISTINCT s1.employer_id), median(s1.net), list(s1.net)
        FROM paid_skill AS s1{joins}
        GROUP BY combo
    """).fetchall()
    if any(med != statistics.median(nets) for *_, med, nets in rows):
        raise SystemExit(f"k={k}: DuckDB median differs from statistics.median")
    return {tuple(combo): Stat(n, emp, med) for combo, n, emp, med, _ in rows}


def overview(con):
    total, with_skills, paid, paid_skills, med, med_skills = con.execute("""
        SELECT
            count(*),
            count(*) FILTER (WHERE len(skills) > 0),
            count(net),
            count(net) FILTER (WHERE len(skills) > 0),
            median(net),
            median(net) FILTER (WHERE len(skills) > 0)
        FROM vac
    """).fetchone()
    print(f"\nvariant: role = '{segment.ROLE}' and name contains '{segment.KEYWORD}'; net = RUR mid-range, "
          f"gross x {segment.NET_FACTOR}, kept {segment.MIN_SALARY:,}..{segment.MAX_SALARY:,}")
    table(["subset", "vacancies", "median net"], [
        ["all", total, None],
        ["with skills", with_skills, None],
        ["with salary", paid, med],
        ["with salary and skills", paid_skills, med_skills],
    ])


def most_frequent(candidates, freq):
    return min(candidates, key=lambda combo: (-freq[combo], combo))


def added_skill(combo, base):
    return next(skill for skill in combo if skill not in base)


def uplift(stat, base):
    return stat.med - base.med, 100 * (stat.med - base.med) / base.med


def supported(stats, min_n, min_emp):
    return ((combo, stat) for combo, stat in stats.items() if stat.n >= min_n and stat.emp >= min_emp)


def pair_rows(stats, freq, min_n, min_emp):
    rows = []
    for pair, stat in supported(stats[2], min_n, min_emp):
        base = most_frequent(combinations(pair, 1), freq[1])
        base_stat = stats[1][base]
        delta, pct = uplift(stat, base_stat)
        rows.append({
            "skill1": pair[0], "skill2": pair[1], "base": base[0], "added": added_skill(pair, base),
            "n": stat.n, "emp": stat.emp, "med_pair": stat.med,
            "n_base": base_stat.n, "med_base": base_stat.med, "uplift": delta, "uplift_pct": pct,
        })
    return rows


def triple_rows(stats, freq, min_n, min_emp):
    rows = []
    for triple, stat in supported(stats[3], min_n, min_emp):
        base = most_frequent(combinations(triple, 2), freq[2])
        base_stat = stats[2][base]
        base_skill = most_frequent(combinations(triple, 1), freq[1])
        skill_stat = stats[1][base_skill]
        delta, pct = uplift(stat, base_stat)
        rows.append({
            "skill1": triple[0], "skill2": triple[1], "skill3": triple[2],
            "base1": base[0], "base2": base[1], "added": added_skill(triple, base),
            "n": stat.n, "emp": stat.emp, "med_triple": stat.med,
            "n_base": base_stat.n, "med_base": base_stat.med, "uplift": delta, "uplift_pct": pct,
            "base_skill": base_skill[0], "med_base_skill": skill_stat.med,
            "uplift_vs_skill": uplift(stat, skill_stat)[0],
        })
    return rows


def single_rows(stats, freq):
    empty = Stat(0, 0, None)
    return [
        {"skill": skill[0], "count": count, **stats[1].get(skill, empty)._asdict()}
        for skill, count in sorted(freq[1].items(), key=lambda item: (-item[1], item[0]))
    ]


def skills_of(row):
    return tuple(row[name] for name in ("skill1", "skill2", "skill3") if name in row)


def ranked(rows, descending):
    sign = -1 if descending else 1
    return sorted(rows, key=lambda row: (sign * row["uplift"], skills_of(row)))


def rounded(value):
    return round(value, 4) if isinstance(value, float) else value


def publish(prefix, name, columns, rows):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows({column: rounded(row.get(column)) for column in columns} for row in rows)
    key = f"{prefix}/{name}.csv"
    s3.put_text(key, buffer.getvalue())
    local = config.ROOT / key
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(buffer.getvalue(), encoding="utf-8")
    return key


def fmt(value):
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.0f}"
    return "" if value is None else str(value)


def table(header, rows, left=1):
    cells = [header, *([fmt(value) for value in row] for row in rows)]
    widths = [max(len(row[i]) for row in cells) for i in range(len(header))]
    for row in cells:
        print("  " + "  ".join(
            cell.ljust(width) if i < left else cell.rjust(width)
            for i, (cell, width) in enumerate(zip(row, widths, strict=True))
        ).rstrip())


def signed(value):
    return f"{value:+,.0f}"


def percent(value):
    return f"{value:+.1f}%"


def print_pairs(title, rows):
    print(f"\n{title}")
    table(
        ["base", "added", "n", "emp", "med_pair", "n_base", "med_base", "uplift", "uplift_pct"],
        [[r["base"], r["added"], r["n"], r["emp"], r["med_pair"], r["n_base"], r["med_base"],
          signed(r["uplift"]), percent(r["uplift_pct"])] for r in rows],
        left=2,
    )


def print_triples(title, rows):
    print(f"\n{title}")
    table(
        ["base pair", "added", "n", "emp", "med_triple", "n_base", "med_base", "uplift", "uplift_pct",
         "base_skill", "med_base_skill", "uplift_vs_skill"],
        [[f"{r['base1']} + {r['base2']}", r["added"], r["n"], r["emp"], r["med_triple"], r["n_base"],
          r["med_base"], signed(r["uplift"]), percent(r["uplift_pct"]), r["base_skill"], r["med_base_skill"],
          signed(r["uplift_vs_skill"])] for r in rows],
        left=2,
    )


def print_singles(rows, min_n, min_emp):
    eligible = [row for row in rows if row["n"] >= min_n and row["emp"] >= min_emp]
    by_median = sorted(eligible, key=lambda row: (-row["med"], row["skill"]))
    print(f"\nsingle skills with n >= {min_n} and emp >= {min_emp}: {len(eligible)}; highest and lowest medians")
    table(
        ["skill", "count", "n", "emp", "med"],
        [[r["skill"], r["count"], r["n"], r["emp"], r["med"]] for r in by_median[:TOP]]
        + [["...", "", "", "", ""]]
        + [[r["skill"], r["count"], r["n"], r["emp"], r["med"]] for r in by_median[-TOP:]],
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.salary")
    parser.add_argument("--min-n", type=int, default=30)
    parser.add_argument("--min-emp", type=int, default=10)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    started = time.perf_counter()
    con = duck.connect()
    load_vacancies(con)
    con.execute(PAID_SKILL_SQL)
    overview(con)
    print()
    freq = {k: mr_counts(con, f"k{k}", duck.result_columns(k)) for k in (1, 2)}
    stats = {k: combo_stats(con, k) for k in (1, 2, 3)}
    print("DuckDB median == statistics.median in all "
          f"{sum(map(len, stats.values())):,} skill groups (k=1: {len(stats[1]):,}, k=2: {len(stats[2]):,}, "
          f"k=3: {len(stats[3]):,})")

    pairs = ranked(pair_rows(stats, freq, args.min_n, args.min_emp), descending=True)
    triples = ranked(triple_rows(stats, freq, args.min_n, args.min_emp), descending=True)
    singles = single_rows(stats, freq)
    print(f"\nsupport threshold: n >= {args.min_n} vacancies with salary, emp >= {args.min_emp} employers")
    print(f"  pairs   {len(pairs):>6,} of {len(stats[2]):,} with salary")
    print(f"  triples {len(triples):>6,} of {len(stats[3]):,} with salary")

    outputs = {
        "pairs": (PAIR_COLUMNS, pairs),
        "pairs_up": (PAIR_COLUMNS, pairs[:TOP]),
        "pairs_down": (PAIR_COLUMNS, ranked(pairs, descending=False)[:TOP]),
        "triples": (TRIPLE_COLUMNS, triples),
        "triples_up": (TRIPLE_COLUMNS, triples[:TOP]),
        "triples_down": (TRIPLE_COLUMNS, ranked(triples, descending=False)[:TOP]),
        "singles": (SINGLE_COLUMNS, singles),
    }
    print_pairs(f"pairs that raise salary: top {TOP} by uplift = med_pair - med_base (base = more frequent skill by k1)",
                outputs["pairs_up"][1])
    print_pairs(f"pairs that lower salary: top {TOP}", outputs["pairs_down"][1])
    print_triples(f"triples that raise salary: top {TOP} by uplift = med_triple - med_base "
                  "(base = most frequent pair by k2; control: most frequent skill by k1)", outputs["triples_up"][1])
    print_triples(f"triples that lower salary: top {TOP}", outputs["triples_down"][1])
    print_singles(singles, args.min_n, args.min_emp)

    print()
    for name, (columns, rows) in outputs.items():
        key = publish(config.RESULTS_SALARY, name, columns, rows)
        print(f"wrote {config.s3_url(key)}  rows {len(rows):,}  (local {key})")
    print(f"\ntotal {time.perf_counter() - started:.2f} s")


if __name__ == "__main__":
    main()

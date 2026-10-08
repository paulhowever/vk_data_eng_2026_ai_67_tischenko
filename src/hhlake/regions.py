import argparse
import time
from functools import partial
from itertools import combinations
from operator import itemgetter

from . import config, duck, segment
from .salary import load_vacancies, mr_counts, publish, table

ALL = "весь вариант"
SCOPES = [*segment.REGIONS, ALL]
REMOTE = "Удаленная работа"
EXPERIENCE = {
    "exp_none": "Нет опыта",
    "exp_1_3": "От 1 года до 3 лет",
    "exp_3_6": "От 3 до 6 лет",
    "exp_6_plus": "Более 6 лет",
}
SUBSETS = {
    "exp_1_3": f"experience = {duck.quote(EXPERIENCE['exp_1_3'])}",
    "exp_3_6": f"experience = {duck.quote(EXPERIENCE['exp_3_6'])}",
    "remote": f"schedule = {duck.quote(REMOTE)}",
    "non_remote": f"schedule <> {duck.quote(REMOTE)}",
}
FOCUS_SKILLS = ["html", "css", "symfony", "postgresql"]
KS = (1, 2, 3)

PROFILE_COLUMNS = [
    "area", "vacancies", "with_skills", "with_salary", "salary_share", "both_bounds_share",
    "q1", "median", "q3", "remote_share", *EXPERIENCE, "employers",
    *(f"{prefix}_{name}" for name in SUBSETS for prefix in ("n", "median")),
]
COMBO_COLUMNS = ["area", "k", "rank", "skill1", "skill2", "skill3", "count", "share", "n_salary", "median_salary"]


def profile_sql():
    experience = ",\n        ".join(
        f"avg(CAST(experience = {duck.quote(label)} AS INT)) AS {name}" for name, label in EXPERIENCE.items()
    )
    subsets = ",\n        ".join(
        f"count(net) FILTER (WHERE {condition}) AS n_{name},\n        "
        f"median(net) FILTER (WHERE {condition}) AS median_{name}"
        for name, condition in SUBSETS.items()
    )
    return f"""
    WITH scoped AS (
        SELECT area AS scope, * FROM vac WHERE {duck.REGION_FILTER}
        UNION ALL
        SELECT {duck.quote(ALL)} AS scope, * FROM vac
    )
    SELECT
        scope AS area,
        count(*) AS vacancies,
        count(*) FILTER (WHERE len(skills) > 0) AS with_skills,
        count(net) AS with_salary,
        count(net) / count(*) AS salary_share,
        count(net) FILTER (WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL) / count(net) AS both_bounds_share,
        quantile_cont(net, 0.25) AS q1,
        median(net) AS median,
        quantile_cont(net, 0.75) AS q3,
        avg(CAST(schedule = {duck.quote(REMOTE)} AS INT)) AS remote_share,
        {experience},
        count(DISTINCT employer_id) AS employers,
        {subsets}
    FROM scoped
    GROUP BY scope
    """


def profile(con):
    cursor = con.execute(profile_sql())
    names = [column[0] for column in cursor.description]
    rows = {row[0]: dict(zip(names, row, strict=True)) for row in cursor.fetchall()}
    return {scope: rows[scope] for scope in SCOPES}


def money(value):
    return "-" if value is None else f"{value:,.0f}"


def share(value):
    return f"{value:.1%}"


def money_n(row, name):
    return f"{money(row[f'median_{name}'])} (n={row[f'n_{name}']:,})"


def profile_view(row):
    return {
        "vacancies": f"{row['vacancies']:,}",
        "with skills": f"{row['with_skills']:,}",
        "with salary": f"{row['with_salary']:,}",
        "salary share": share(row["salary_share"]),
        "both bounds, of with salary": share(row["both_bounds_share"]),
        "Q1 net": money(row["q1"]),
        "median net": money(row["median"]),
        "Q3 net": money(row["q3"]),
        "remote share": share(row["remote_share"]),
        **{f"experience: {label}": share(row[name]) for name, label in EXPERIENCE.items()},
        "employers": f"{row['employers']:,}",
        "median, 1-3 years": money_n(row, "exp_1_3"),
        "median, 3-6 years": money_n(row, "exp_3_6"),
        "median, remote": money_n(row, "remote"),
        "median, not remote": money_n(row, "non_remote"),
    }


def print_profile(rows):
    views = {scope: profile_view(row) for scope, row in rows.items()}
    print("\ntable 1. region profile (DuckDB over S3, net salary in RUB)")
    table(["metric", *SCOPES], [[label, *(views[scope][label] for scope in SCOPES)] for label in views[ALL]])


def top_combos(con, counts, with_skills, limit):
    rows = []
    for k in KS:
        for area in segment.REGIONS:
            ranked = sorted(
                ((key[1:], count) for key, count in counts[k].items() if key[0] == area),
                key=lambda item: (-item[1], item[0]),
            )
            for rank, (combo, count) in enumerate(ranked[:limit], 1):
                n, med, recount = con.execute(
                    "SELECT count(net), median(net), count(*) FROM vac WHERE area = ? AND list_has_all(skills, ?)",
                    [area, list(combo)],
                ).fetchone()
                if recount != count:
                    raise SystemExit(f"{area} {combo}: MapReduce count {count}, DuckDB recount {recount}")
                rows.append({
                    "area": area, "k": k, "rank": rank,
                    **{f"skill{i}": skill for i, skill in enumerate(combo, 1)},
                    "count": count, "share": count / with_skills[area], "n_salary": n, "median_salary": med,
                })
    print(f"MapReduce counts of all {len(rows)} top combos == DuckDB recount over vacancies with skills")
    return rows


def print_top_combos(rows, with_skills, limit):
    print(f"\ntable 2. top {limit} skills, pairs and triples per region: count from MapReduce regions_k1..k3, "
          "share of the region's vacancies with skills, net salary median from DuckDB")
    for area in segment.REGIONS:
        print(f"\n{area} (vacancies with skills {with_skills[area]:,})")
        table(
            ["k", "rank", "combo", "count", "share", "n_salary", "median_salary"],
            [
                [r["k"], r["rank"], " + ".join(r[f"skill{i}"] for i in range(1, r["k"] + 1)), r["count"],
                 share(r["share"]), r["n_salary"], money(r["median_salary"])]
                for r in rows if r["area"] == area
            ],
            left=3,
        )


def gap(high, low):
    return f"{100 * (high / low - 1):+.1f}%"


def by_region(rows, render):
    return {area: render(rows[area]) for area in segment.REGIONS}


def listing(values):
    return ", ".join(f"{area} {value}" for area, value in values.items())


def gaps(values):
    return ", ".join(f"{a}/{b} {gap(values[a], values[b])}" for a, b in combinations(values, 2))


def experience_gap(rows, name):
    medians = by_region(rows, itemgetter(f"median_{name}"))
    return f"{listing(by_region(rows, partial(money_n, name=name)))}, gap {gaps(medians)}"


def remote_gap(row):
    return (f"{money_n(row, 'remote')} vs {money_n(row, 'non_remote')} "
            f"({gap(row['median_remote'], row['median_non_remote'])})")


def skill_shares(rows, skill_counts, skill):
    return " / ".join(
        share(skill_counts.get((area, skill), 0) / rows[area]["with_skills"]) for area in segment.REGIONS
    )


def insights(rows, skill_counts):
    facts = [
        f"median net salary: {listing(by_region(rows, lambda row: money(row['median'])))}; "
        f"gap {gaps(by_region(rows, itemgetter('median')))}",
        f"same experience keeps the gap: 1-3 years {experience_gap(rows, 'exp_1_3')}; "
        f"3-6 years {experience_gap(rows, 'exp_3_6')}",
        "experience 3+ years: "
        f"{listing(by_region(rows, lambda row: share(row['exp_3_6'] + row['exp_6_plus'])))} of vacancies "
        f"(whole variant {share(rows[ALL]['exp_3_6'] + rows[ALL]['exp_6_plus'])})",
        f"salary shown in {listing(by_region(rows, lambda row: share(row['salary_share'])))} of vacancies "
        f"(whole variant {share(rows[ALL]['salary_share'])}); both range bounds among them: "
        f"{listing(by_region(rows, lambda row: share(row['both_bounds_share'])))}",
        f"remote share {listing(by_region(rows, lambda row: share(row['remote_share'])))}; "
        f"median remote vs not remote: {listing(by_region(rows, remote_gap))}",
        f"share of vacancies with skills (MapReduce regions_k1), {' / '.join(segment.REGIONS)}: "
        + "; ".join(f"{skill} {skill_shares(rows, skill_counts, skill)}" for skill in FOCUS_SKILLS),
    ]
    print("\nfacts for the report")
    for number, fact in enumerate(facts, 1):
        print(f"  {number}. {fact}")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.regions")
    parser.add_argument("--top", type=int, default=5)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    started = time.perf_counter()
    con = duck.connect()
    load_vacancies(con)
    rows = profile(con)
    with_skills = {area: rows[area]["with_skills"] for area in segment.REGIONS}
    print(f"regions: {', '.join(segment.REGIONS)}")
    counts = {k: mr_counts(con, f"regions_k{k}", duck.result_columns(k, regions=True)) for k in KS}
    combos = top_combos(con, counts, with_skills, args.top)

    print_profile(rows)
    print_top_combos(combos, with_skills, args.top)
    insights(rows, counts[1])

    print()
    for name, columns, data in (
        ("profile", PROFILE_COLUMNS, list(rows.values())),
        ("top_combos", COMBO_COLUMNS, combos),
    ):
        key = publish(config.RESULTS_REGIONS, name, columns, data)
        print(f"wrote {config.s3_url(key)}  rows {len(data):,}  (local {key})")
    print(f"\ntotal {time.perf_counter() - started:.2f} s")


if __name__ == "__main__":
    main()

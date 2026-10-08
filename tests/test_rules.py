from collections import Counter
from itertools import combinations

import duckdb
import pytest

from hhlake import duck, salary, segment
from hhlake.segment import FIELDS

PHP_ROLE = "Программист, разработчик"


def row(number, **values):
    record = dict.fromkeys(FIELDS, "")
    record.update(source="hh", vacancy_id=str(number), area="Москва", role=PHP_ROLE, name="PHP developer")
    record.update(values)
    return [record[name] for name in FIELDS]


ROWS = [
    row(1, key_skills="PHP; MySQL;git", salary_from="100000", salary_to="200000", currency="RUR", gross="false"),
    row(2, name="Senior pHp-разработчик", key_skills="php;PHP; php ;", salary_from="100000", currency="RUR",
        gross="true"),
    row(3, name="Python developer", key_skills="python", salary_to="300000", currency="RUR"),
    row(4, role="Тестировщик", name="PHP QA", key_skills="php;qa"),
    row(5, key_skills="", salary_from="3000", salary_to="4000", currency="USD"),
    row(6, key_skills=" ; ;", salary_from="10000", currency="RUR"),
    row(7, key_skills="1С-Битрикс;php7+;a,b;ООП", salary_from="1100000", currency="RUR", gross="true"),
    row(8, key_skills="Laravel;Docker;PostgreSQL;php;Git;Redis", salary_from="2000000", currency="RUR"),
    row(9, name="Веб-программист PHP", key_skills="Ёж;ёж;яндекс;z", salary_to="50000", currency="RUR",
        gross="false"),
    row(10, role="Программист, разработчик ", name="PHP"),
]


@pytest.mark.parametrize(("raw", "expected"), [
    ("PHP; MySQL;git", ["git", "mysql", "php"]),
    ("php;PHP; php ;", ["php"]),
    ("", []),
    (None, []),
    (" ; ;", []),
    ("1С-Битрикс;php7+;a,b", ["1с-битрикс", "a,b", "php7+"]),
    ("Ёж;ёж;яндекс;z", ["z", "яндекс", "ёж"]),
])
def test_normalize_skills(raw, expected):
    assert segment.normalize_skills(raw) == expected


@pytest.mark.parametrize(("role", "name", "expected"), [
    (PHP_ROLE, "PHP-разработчик", True),
    (PHP_ROLE, "Senior pHp developer", True),
    (PHP_ROLE, "Веб-программист (php)", True),
    (PHP_ROLE, "Python developer", False),
    ("Тестировщик", "PHP QA", False),
    (PHP_ROLE + " ", "PHP", False),
])
def test_is_variant(role, name, expected):
    assert segment.is_variant(row(1, role=role, name=name)) is expected


@pytest.mark.parametrize(("low", "high", "currency", "gross", "expected"), [
    ("100000", "200000", "RUR", "false", 150000),
    ("100000", "", "RUR", "true", 87000),
    ("", "50000", "RUR", "", 50000),
    ("3000", "4000", "USD", "false", None),
    ("10000", "", "RUR", "false", None),
    ("2000000", "", "RUR", "false", None),
    ("1100000", "", "RUR", "true", 957000),
    ("", "", "RUR", "false", None),
])
def test_salary_net(low, high, currency, gross, expected):
    value = segment.salary_net(row(1, salary_from=low, salary_to=high, currency=currency, gross=gross))
    assert value == pytest.approx(expected) if expected is not None else value is None


@pytest.fixture(scope="module")
def con():
    connection = duckdb.connect()
    columns = ", ".join(f"{name} VARCHAR" for name in FIELDS)
    connection.execute(f"CREATE TABLE raw ({columns})")
    connection.executemany(
        f"INSERT INTO raw VALUES ({', '.join('?' * len(FIELDS))})",
        [[value or None for value in r] for r in ROWS],
    )
    return connection


def test_sql_variant_matches_python(con):
    selected = {r[0] for r in con.execute(f"SELECT vacancy_id FROM raw WHERE {duck.VARIANT}").fetchall()}
    assert selected == {r[segment.COL["vacancy_id"]] for r in ROWS if segment.is_variant(r)}


def test_sql_skills_and_net_salary_match_python(con):
    con.execute(salary.vacancies_sql("raw"))
    got = {vid: (skills, net) for vid, skills, net in con.execute("SELECT vacancy_id, skills, net FROM vac").fetchall()}
    expected = {
        r[segment.COL["vacancy_id"]]: (segment.normalize_skills(r[segment.COL["key_skills"]]), segment.salary_net(r))
        for r in ROWS if segment.is_variant(r)
    }
    assert got.keys() == expected.keys()
    for vid, (skills, net) in expected.items():
        assert got[vid][0] == skills
        assert got[vid][1] == pytest.approx(net) if net is not None else got[vid][1] is None


@pytest.mark.parametrize("k", [1, 2, 3])
def test_sql_self_join_equals_itertools_combinations(con, k):
    rows = [(r[segment.COL["vacancy_id"]], r[segment.COL["area"]], segment.normalize_skills(r[segment.COL["key_skills"]]))
            for r in ROWS if segment.is_variant(r)]
    con.execute("CREATE OR REPLACE TABLE s (vacancy_id VARCHAR, area VARCHAR, skill VARCHAR)")
    con.executemany("INSERT INTO s VALUES (?, ?, ?)", [(vid, area, skill) for vid, area, skills in rows for skill in skills])
    got = {tuple(r[:-1]): r[-1] for r in con.execute(duck.combos_from(k, "s")).fetchall()}
    expected = Counter(combo for _, _, skills in rows for combo in combinations(skills, k))
    assert got == expected


def test_known_differences_of_sql_trim_and_lower_on_unusual_characters():
    connection = duckdb.connect()
    raw = "php;\tmysql;İ"
    sql = connection.execute(f"SELECT {duck.NORMALIZED_SKILLS} FROM (SELECT ? AS key_skills)", [raw]).fetchone()[0]
    assert segment.normalize_skills(raw) == ["i̇", "mysql", "php"]
    assert sql == ["\tmysql", "i", "php"]
